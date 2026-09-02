package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
)

func TestCreateLimiterAdmitsUpToItsLimit(t *testing.T) {
	limiter := newCreateLimiter(3)

	var releases []func()
	for i := 0; i < 3; i++ {
		release, ok := limiter.acquire()
		if !ok {
			t.Fatalf("acquire %d refused below the limit", i)
		}
		releases = append(releases, release)
	}

	if _, ok := limiter.acquire(); ok {
		t.Fatal("acquire above the limit must be refused")
	}

	releases[0]()
	release, ok := limiter.acquire()
	if !ok {
		t.Fatal("a released slot must become available again")
	}
	release()
}

// A refused acquire must not consume a slot, or the gateway ratchets itself
// closed under exactly the load shedding exists to survive.
func TestCreateLimiterRefusalDoesNotConsumeASlot(t *testing.T) {
	limiter := newCreateLimiter(1)

	release, ok := limiter.acquire()
	if !ok {
		t.Fatal("first acquire should succeed")
	}
	for i := 0; i < 100; i++ {
		if _, ok := limiter.acquire(); ok {
			t.Fatal("acquire above the limit must be refused")
		}
	}
	release()

	if _, ok := limiter.acquire(); !ok {
		t.Fatal("repeated refusals must not have consumed the slot")
	}
}

// Release is called from a defer that can run more than once on some paths;
// double-releasing must not hand out phantom capacity.
func TestCreateLimiterReleaseIsIdempotent(t *testing.T) {
	limiter := newCreateLimiter(1)

	release, _ := limiter.acquire()
	release()
	release()
	release()

	if got := limiter.currentInFlight(); got != 0 {
		t.Fatalf("in flight = %d, want 0", got)
	}
	if _, ok := limiter.acquire(); !ok {
		t.Fatal("slot should be available")
	}
	if _, ok := limiter.acquire(); ok {
		t.Fatal("double release must not have created a second slot")
	}
}

func TestCreateLimiterIsSafeUnderConcurrency(t *testing.T) {
	const limit = 8
	limiter := newCreateLimiter(limit)

	var wg sync.WaitGroup
	var mu sync.Mutex
	admitted := 0
	held := make([]func(), 0, limit)

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, ok := limiter.acquire()
			if !ok {
				return
			}
			mu.Lock()
			admitted++
			held = append(held, release)
			mu.Unlock()
		}()
	}
	wg.Wait()

	if admitted > limit {
		t.Fatalf("admitted %d concurrently, want at most %d", admitted, limit)
	}
	for _, release := range held {
		release()
	}
	if got := limiter.currentInFlight(); got != 0 {
		t.Fatalf("in flight = %d after releasing everything, want 0", got)
	}
}

// A nil limiter is the disabled case and must never refuse.
func TestNilCreateLimiterAdmitsEverything(t *testing.T) {
	var limiter *createLimiter
	for i := 0; i < 10; i++ {
		release, ok := limiter.acquire()
		if !ok {
			t.Fatal("a disabled limiter must admit")
		}
		release()
	}
}

// blockingScheduler parks its first Schedule call until released and answers
// every call with the same upstream, which is how a create burst is held in
// flight deterministically.
type blockingScheduler struct {
	stubSchedulerClient
	endpoint string
	entered  chan struct{}
	release  chan struct{}
	calls    atomic.Int32
}

func (b *blockingScheduler) Schedule(ctx context.Context, _ *schedulerv1.ScheduleRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
	if b.calls.Add(1) == 1 {
		close(b.entered)
		select {
		case <-b.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &schedulerv1.ScheduleResponse{
		Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: b.endpoint},
	}, nil
}

func (b *blockingScheduler) RecordAssignment(_ context.Context, _ *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
	return &schedulerv1.RecordAssignmentResponse{}, nil
}

// holdOneCreateInFlight builds a gateway that admits one create at a time and
// parks a create inside it. The returned function releases the parked create
// and waits for it to finish.
func holdOneCreateInFlight(t *testing.T, upstreamURL string) (http.Handler, *blockingScheduler, func()) {
	t.Helper()
	scheduler := &blockingScheduler{
		endpoint: upstreamURL,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	server := newTestServer(t, scheduler, 5*time.Second, 1<<20, func(o *ServerOptions) {
		o.MaxInFlightCreates = 1
	})
	handler := server.Handler()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
		req.Header.Set(headerAPIKey, testAPIKey)
		handler.ServeHTTP(httptest.NewRecorder(), req)
	}()
	<-scheduler.entered

	return handler, scheduler, func() {
		close(scheduler.release)
		wg.Wait()
	}
}

// The limiter only protects anything if NewServer builds it. This drives the
// shed through the public handler: a second create arriving while the gateway
// is at its limit must be refused before it reaches the scheduler.
func TestASecondConcurrentCreateIsShed(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(headerSandboxID, "sbx-1")
		w.WriteHeader(http.StatusCreated)
	}))
	defer upstream.Close()

	handler, scheduler, release := holdOneCreateInFlight(t, upstream.URL)
	defer release()

	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 while the gateway is at its create limit", rec.Code)
	}
	if got := rec.Header().Get(headerRefusalReason); got != refusalGatewayShed {
		t.Fatalf("%s = %q, want %q", headerRefusalReason, got, refusalGatewayShed)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("a shed must tell the client when to come back")
	}
	if got := scheduler.calls.Load(); got != 1 {
		t.Fatalf("Schedule called %d times, want only the parked create's", got)
	}
}

// Shedding is for creates. A management-plane read arriving during a create
// burst has no fan-out to protect the scheduler from, and turning it away
// would fail build polls and operator tooling exactly when the fleet is
// busiest.
func TestAManagementReadIsNotShedByACreateBurst(t *testing.T) {
	var paths []string
	var mu sync.Mutex
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer upstream.Close()

	handler, scheduler, release := holdOneCreateInFlight(t, upstream.URL)
	defer release()

	req := httptest.NewRequest(http.MethodGet, "/templates", nil)
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: a read must not be shed", rec.Code)
	}
	if got := rec.Header().Get(headerRefusalReason); got != "" {
		t.Fatalf("a read carried refusal %q", got)
	}
	if got := scheduler.calls.Load(); got != 2 {
		t.Fatalf("Schedule called %d times, want the parked create's and the read's", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 || paths[0] != "/templates" {
		t.Fatalf("upstream saw %v, want the read forwarded once", paths)
	}
}

// A node refusing a management request has answered it. Offering the request
// to another node is only safe for a create; a DELETE re-run elsewhere is a
// second DELETE.
func TestAManagementRequestRefusedByANodeIsNotRePlaced(t *testing.T) {
	refusal := loadNodeAdmission503(t)
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		refusal.replay(w)
	}))
	defer upstream.Close()

	recorder := &scheduleRecorder{nodes: []string{"node-a", "node-b", "node-c"}}
	server := newRescheduleTestServer(t, recorder, upstream)

	req := httptest.NewRequest(http.MethodDelete, "/snapshots/snap-1", nil)
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want the node's 503 surfaced", rec.Code)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("upstream executed the DELETE %d times, want 1", got)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.calls != 1 {
		t.Fatalf("Schedule called %d times, want 1: a management request is placed once", recorder.calls)
	}
}
