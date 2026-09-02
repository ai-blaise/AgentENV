package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

// nodeAdmission503 is a node's refusal of a create for capacity, in the shape
// the stub upstream replays it.
type nodeAdmission503 struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    json.RawMessage   `json:"body"`
}

// loadNodeAdmission503 reads testdata/node_admission_503.json.
//
// PROVISIONAL. The fixture was written by hand to the agreed contract — 503,
// a retry-after, x-agentenv-refusal-reason: node_at_capacity, a JSON Error
// body, lowercase header keys — rather than captured from a node. The node
// side regenerates it from a real admission refusal and these tests are re-run
// against that file unchanged; until then the fixture pins the gateway's half
// of the seam so the two cannot drift apart unnoticed.
func loadNodeAdmission503(t *testing.T) nodeAdmission503 {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "node_admission_503.json"))
	if err != nil {
		t.Fatalf("read admission fixture: %v", err)
	}
	var fixture nodeAdmission503
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatalf("decode admission fixture: %v", err)
	}
	return fixture
}

// replay answers a request exactly as the node did.
func (f nodeAdmission503) replay(w http.ResponseWriter) {
	for name, value := range f.Headers {
		w.Header().Set(name, value)
	}
	w.WriteHeader(f.Status)
	_, _ = w.Write(f.Body)
}

// The fixture is the contract. If the node's regenerated capture stops meeting
// these, the gateway's retry keying has to change with it, not silently stop
// firing.
func TestNodeAdmissionFixtureCarriesTheRefusalContract(t *testing.T) {
	fixture := loadNodeAdmission503(t)

	if fixture.Status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", fixture.Status)
	}
	for name := range fixture.Headers {
		if name != strings.ToLower(name) {
			t.Fatalf("header %q must be lowercase", name)
		}
	}
	if got := fixture.Headers[headerRefusalReason]; got != refusalNodeAtCapacity {
		t.Fatalf("%s = %q, want %q", headerRefusalReason, got, refusalNodeAtCapacity)
	}
	if _, err := strconv.Atoi(fixture.Headers["retry-after"]); err != nil {
		t.Fatalf("retry-after must be seconds, got %q", fixture.Headers["retry-after"])
	}
	var body struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(fixture.Body, &body); err != nil || body.Code != 503 || body.Message == "" {
		t.Fatalf("body must be the API's Error shape with code 503, got %s", fixture.Body)
	}
}

// scheduleRecorder hands out nodes in order and records the exclusions the
// gateway asked for on each attempt.
type scheduleRecorder struct {
	mu         sync.Mutex
	nodes      []string
	calls      int
	exclusions [][]string
}

func (s *scheduleRecorder) next(_ context.Context, req *schedulerv1.ScheduleRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.exclusions = append(s.exclusions, req.GetExcludeNodeIds())
	if s.calls >= len(s.nodes) {
		return nil, fmt.Errorf("no nodes available")
	}
	nodeID := s.nodes[s.calls]
	s.calls++
	return &schedulerv1.ScheduleResponse{
		Node: &schedulerv1.Node{NodeId: nodeID, Endpoint: "http://" + nodeID},
	}, nil
}

// newRescheduleTestServer points every placement at one upstream.
func newRescheduleTestServer(t *testing.T, recorder *scheduleRecorder, upstream *httptest.Server, opts ...testServerOption) *Server {
	t.Helper()
	return newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(ctx context.Context, req *schedulerv1.ScheduleRequest, opts ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			resp, err := recorder.next(ctx, req, opts...)
			if err != nil {
				return nil, err
			}
			resp.Node.Endpoint = upstream.URL
			return resp, nil
		},
		recordAssignmentFunc: func(_ context.Context, _ *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, 5*time.Second, 1<<20, opts...)
}

// TestScheduledCreateRetriesOnNodeRejection covers the half of admission
// control that makes it safe: a node refusing a create must steer the
// placement, not surface as a client-visible failure.
func TestScheduledCreateRetriesOnNodeRejection(t *testing.T) {
	refusal := loadNodeAdmission503(t)
	var refused []string
	var mu sync.Mutex

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		// The first two nodes are at capacity; the third accepts.
		if len(refused) < 2 {
			refused = append(refused, r.Host)
			refusal.replay(w)
			return
		}
		w.Header().Set(headerSandboxID, "sbx-accepted")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sandboxID":"sbx-accepted"}`))
	}))
	defer upstream.Close()

	recorder := &scheduleRecorder{nodes: []string{"node-a", "node-b", "node-c"}}
	server := newRescheduleTestServer(t, recorder, upstream)

	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
	req.Header.Set(headerAPIKey, testAPIKey)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201 after rescheduling past two full nodes; body=%q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(headerRefusalReason); got != "" {
		t.Fatalf("a placed create must not carry a refusal, got %s=%q", headerRefusalReason, got)
	}

	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.calls != 3 {
		t.Fatalf("Schedule called %d times, want 3", recorder.calls)
	}
	// Each retry must tell the scheduler which nodes already refused, or it
	// would hand back the same node and the loop would make no progress.
	if len(recorder.exclusions) != 3 {
		t.Fatalf("recorded %d schedule calls, want 3", len(recorder.exclusions))
	}
	if len(recorder.exclusions[0]) != 0 {
		t.Fatalf("first attempt excluded %v, want none", recorder.exclusions[0])
	}
	if len(recorder.exclusions[1]) != 1 || recorder.exclusions[1][0] != "node-a" {
		t.Fatalf("second attempt excluded %v, want [node-a]", recorder.exclusions[1])
	}
	if len(recorder.exclusions[2]) != 2 {
		t.Fatalf("third attempt excluded %v, want both refusing nodes", recorder.exclusions[2])
	}
}

// TestScheduledCreateStopsRetryingAtTheBound pins that a saturated fleet does
// not turn every create into a walk of the whole node list. The last attempt's
// rejection reaches the client as the gateway's answer, retries_exhausted, with
// the node's Retry-After kept: the client learns that every node it was
// offered to was full, which is a different thing from one node being full.
func TestScheduledCreateStopsRetryingAtTheBound(t *testing.T) {
	refusal := loadNodeAdmission503(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		refusal.replay(w)
	}))
	defer upstream.Close()

	recorder := &scheduleRecorder{nodes: []string{"a", "b", "c", "d", "e", "f"}}
	server := newRescheduleTestServer(t, recorder, upstream)

	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want the final rejection surfaced as 503", rec.Code)
	}
	if got := rec.Header().Get(headerRefusalReason); got != refusalRetriesExhausted {
		t.Fatalf("%s = %q, want %q after every offered node refused", headerRefusalReason, got, refusalRetriesExhausted)
	}
	if got := rec.Header().Get("Retry-After"); got != refusal.Headers["retry-after"] {
		t.Fatalf("Retry-After = %q, want the node's own %q kept", got, refusal.Headers["retry-after"])
	}
	if !strings.Contains(rec.Body.String(), "503") {
		t.Fatalf("body = %q, want the node's error body preserved", rec.Body.String())
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if want := defaultMaxScheduleRetries + 1; recorder.calls != want {
		t.Fatalf("Schedule called %d times, want it bounded at %d", recorder.calls, want)
	}
}

// With retrying turned off there was one attempt and the node's answer is the
// whole story; rewriting it would claim the gateway tried things it did not.
// This is also the off switch reproducing the pre-retry gateway exactly.
func TestScheduledCreateWithRetriesOffPassesTheNodeRefusalThrough(t *testing.T) {
	refusal := loadNodeAdmission503(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		refusal.replay(w)
	}))
	defer upstream.Close()

	recorder := &scheduleRecorder{nodes: []string{"a", "b"}}
	server := newRescheduleTestServer(t, recorder, upstream, func(o *ServerOptions) {
		o.MaxScheduleRetries = -1
	})

	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get(headerRefusalReason); got != refusalNodeAtCapacity {
		t.Fatalf("%s = %q, want the node's own %q with retries off", headerRefusalReason, got, refusalNodeAtCapacity)
	}
	if got := rec.Header().Get("Retry-After"); got != refusal.Headers["retry-after"] {
		t.Fatalf("Retry-After = %q, want the node's %q untouched", got, refusal.Headers["retry-after"])
	}
}

// A body the gateway could not hold gets one attempt. When that attempt is a
// capacity refusal the client is told why there was no second one — the fix
// on its side is a smaller request, not a wait — and still gets a Retry-After
// because the node's refusal is transient.
func TestScheduledCreateNamesAnUnreplayableBody(t *testing.T) {
	refusal := loadNodeAdmission503(t)
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		refusal.replay(w)
	}))
	defer upstream.Close()

	recorder := &scheduleRecorder{nodes: []string{"a", "b", "c"}}
	server := newRescheduleTestServer(t, recorder, upstream)

	body := `{"templateID":"tpl","metadata":{"pad":"` + strings.Repeat("x", maxHintBodyBytes) + `"}}`
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(body))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get(headerRefusalReason); got != refusalBodyNotReplayable {
		t.Fatalf("%s = %q, want %q", headerRefusalReason, got, refusalBodyNotReplayable)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("a capacity refusal is transient and must carry Retry-After even when it could not be retried")
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("upstream attempts = %d, want exactly one for a body that cannot be replayed", got)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.calls != 1 {
		t.Fatalf("Schedule called %d times, want 1", recorder.calls)
	}
}

// The bound is an operator's choice. Zero is the unset default; negative
// means one attempt and nothing more.
func TestScheduledCreateRetryBoundIsConfigurable(t *testing.T) {
	refusal := loadNodeAdmission503(t)
	for _, tc := range []struct {
		name         string
		retries      int
		wantAttempts int
	}{
		{name: "unset takes the default", retries: 0, wantAttempts: defaultMaxScheduleRetries + 1},
		{name: "negative disables retrying", retries: -1, wantAttempts: 1},
		{name: "one retry is two attempts", retries: 1, wantAttempts: 2},
		{name: "raised above the default", retries: 4, wantAttempts: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				refusal.replay(w)
			}))
			defer upstream.Close()

			recorder := &scheduleRecorder{nodes: []string{"a", "b", "c", "d", "e", "f", "g", "h"}}
			server := newRescheduleTestServer(t, recorder, upstream, func(o *ServerOptions) {
				o.MaxScheduleRetries = tc.retries
			})

			req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
			req.Header.Set(headerAPIKey, testAPIKey)
			rec := httptest.NewRecorder()
			server.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503", rec.Code)
			}
			if got := hits.Load(); int(got) != tc.wantAttempts {
				t.Fatalf("upstream attempts = %d, want %d", got, tc.wantAttempts)
			}
		})
	}
}

// A non-503 answer is the node's real answer about the request and must reach
// the client unchanged rather than being retried elsewhere.
func TestScheduledCreateDoesNotRetryNonRejections(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad template"))
	}))
	defer upstream.Close()

	recorder := &scheduleRecorder{nodes: []string{"a", "b", "c"}}
	server := newRescheduleTestServer(t, recorder, upstream)

	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want the node's own 400 passed through", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "bad template") {
		t.Fatalf("body = %q, want the upstream body preserved", body)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.calls != 1 {
		t.Fatalf("Schedule called %d times, want no retry on a non-rejection", recorder.calls)
	}
}

// A 503 that the node does not name as capacity is not a placement signal. It
// is what a node mid-fault says, or what an older node says for anything at
// all, and neither is cured by trying the same create somewhere else.
func TestScheduledCreateDoesNotRetryAnUnnamed503(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason string
	}{
		{name: "bare 503", reason: ""},
		{name: "503 with a different reason", reason: refusalGatewayShed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hits atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				if tc.reason != "" {
					w.Header().Set(headerRefusalReason, tc.reason)
				}
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("orchestrator is shutting down"))
			}))
			defer upstream.Close()

			recorder := &scheduleRecorder{nodes: []string{"a", "b", "c"}}
			server := newRescheduleTestServer(t, recorder, upstream)

			req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
			req.Header.Set(headerAPIKey, testAPIKey)
			rec := httptest.NewRecorder()
			server.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want the node's 503 passed through", rec.Code)
			}
			if !strings.Contains(rec.Body.String(), "shutting down") {
				t.Fatalf("body = %q, want the node's body preserved", rec.Body.String())
			}
			if got := hits.Load(); got != 1 {
				t.Fatalf("upstream attempts = %d, want exactly one", got)
			}
			recorder.mu.Lock()
			defer recorder.mu.Unlock()
			if recorder.calls != 1 {
				t.Fatalf("Schedule called %d times, want 1", recorder.calls)
			}
		})
	}
}

// The retried request must carry the original body; a truncated create would
// be worse than not retrying at all.
func TestScheduledCreateReplaysTheRequestBody(t *testing.T) {
	refusal := loadNodeAdmission503(t)
	const body = `{"templateID":"tpl","metadata":{"k":"v"}}`
	var seen []string
	var mu sync.Mutex

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received, _ := io.ReadAll(r.Body)
		mu.Lock()
		seen = append(seen, string(received))
		attempt := len(seen)
		mu.Unlock()

		if attempt == 1 {
			refusal.replay(w)
			return
		}
		w.Header().Set(headerSandboxID, "sbx-1")
		w.WriteHeader(http.StatusCreated)
	}))
	defer upstream.Close()

	recorder := &scheduleRecorder{nodes: []string{"a", "b"}}
	server := newRescheduleTestServer(t, recorder, upstream)

	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(body))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("upstream saw %d attempts, want 2", len(seen))
	}
	for i, got := range seen {
		if got != body {
			t.Fatalf("attempt %d body = %q, want the original body", i+1, got)
		}
	}
}

// scheduleRetryCounter reads agentenv_gateway_schedule_retries_total from the
// default registry along with the label names on every series it has. A
// family with no series yet reads as zero rather than failing, so the test
// below fails on the shape of what is exported, not on when it first appears.
func scheduleRetryCounter(t *testing.T) (value float64, labels []string) {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "agentenv_gateway_schedule_retries_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			value += metric.GetCounter().GetValue()
			for _, pair := range metric.GetLabel() {
				labels = append(labels, pair.GetName())
			}
		}
	}
	return value, labels
}

// The retry counter is a rate of the whole. A per-node label would grow one
// series per refusing node per gateway, fastest during exactly the capacity
// incident it exists to explain; the refusing node belongs in the log.
func TestScheduleRetriesAreCountedWithoutANodeLabel(t *testing.T) {
	refusal := loadNodeAdmission503(t)
	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits.Add(1) == 1 {
			refusal.replay(w)
			return
		}
		w.Header().Set(headerSandboxID, "sbx-1")
		w.WriteHeader(http.StatusCreated)
	}))
	defer upstream.Close()

	before, _ := scheduleRetryCounter(t)

	recorder := &scheduleRecorder{nodes: []string{"refusing-node", "accepting-node"}}
	server := newRescheduleTestServer(t, recorder, upstream)
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{"templateID":"tpl"}`))
	req.Header.Set(headerAPIKey, testAPIKey)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}

	after, labels := scheduleRetryCounter(t)
	if after-before != 1 {
		t.Fatalf("retries counted = %v, want exactly 1 for one re-placement", after-before)
	}
	if len(labels) != 0 {
		t.Fatalf("retry counter carries labels %v, want none", labels)
	}
}

// An attempt a node refused is a 5xx to the upstream duration metric, and a
// create it accepted is a 2xx. Buffering the attempt must not hide either
// behind "other": the split is what the metric is for.
func TestABufferedAttemptIsLabelledByItsStatus(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	for _, tc := range []struct {
		name   string
		status int
		ctx    context.Context
		want   string
	}{
		{name: "refused", status: http.StatusServiceUnavailable, ctx: context.Background(), want: "5xx"},
		{name: "accepted", status: http.StatusCreated, ctx: context.Background(), want: "2xx"},
		{name: "unwritten and cancelled", status: 0, ctx: cancelled, want: "client_closed"},
		{name: "written status wins over cancel", status: http.StatusBadGateway, ctx: cancelled, want: "5xx"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buffered := newBoundedBufferedResponse(1<<20, httptest.NewRecorder())
			if tc.status != 0 {
				buffered.WriteHeader(tc.status)
			}
			if got := httpStatusLabel(buffered, tc.ctx); got != tc.want {
				t.Fatalf("httpStatusLabel() = %q, want %q", got, tc.want)
			}
		})
	}
}
