package gateway

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func lookupStub(counter *atomic.Int64, resp *schedulerv1.LookupNodeResponse, err error) stubSchedulerClient {
	return stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			counter.Add(1)
			return resp, err
		},
	}
}

func lookup(t *testing.T, cache *CachingSchedulerClient, sandboxID string) (*schedulerv1.LookupNodeResponse, error) {
	t.Helper()
	return cache.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: sandboxID})
}

// cacheWithTTL is the shipped size with an explicit positive TTL.
func cacheWithTTL(delegate schedulerv1.SchedulerClient, ttl time.Duration) *CachingSchedulerClient {
	return NewCachingSchedulerClient(delegate, BindingCacheOptions{Size: config.DefaultGatewayBindingCacheSize, TTL: ttl})
}

func TestBindingCacheServesRepeatLookupsLocally(t *testing.T) {
	var calls atomic.Int64
	node := &schedulerv1.Node{NodeId: "node-a", Endpoint: "http://node-a"}
	cache := cacheWithTTL(lookupStub(&calls, &schedulerv1.LookupNodeResponse{Node: node}, nil), time.Minute)

	for i := 0; i < 5; i++ {
		resp, err := lookup(t, cache, "sbx-1")
		if err != nil {
			t.Fatalf("LookupNode: %v", err)
		}
		if resp.GetNode().GetNodeId() != "node-a" {
			t.Fatalf("node = %q, want node-a", resp.GetNode().GetNodeId())
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("scheduler called %d times, want 1", got)
	}
}

// A not-found is cached only briefly: a sandbox that does not resolve yet
// usually resolves imminently, because its create is still in flight.
func TestBindingCacheCachesNotFoundBriefly(t *testing.T) {
	var calls atomic.Int64
	cache := cacheWithTTL(lookupStub(&calls, nil, status.Error(codes.NotFound, "missing")), time.Minute)

	if _, err := lookup(t, cache, "sbx-1"); status.Code(err) != codes.NotFound {
		t.Fatalf("err = %v, want NotFound", err)
	}
	if _, err := lookup(t, cache, "sbx-1"); status.Code(err) != codes.NotFound {
		t.Fatalf("err = %v, want NotFound from cache", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("scheduler called %d times, want the negative result cached", got)
	}

	if cache.negativeTTL >= cache.ttl {
		t.Fatal("negative entries must expire sooner than positive ones")
	}
}

// The negative TTL is the operator's to set, and a "no" may never outlive a
// "yes": a create's first request must not be answered from a stale absence.
func TestBindingCacheNegativeTTLIsConfiguredAndCapped(t *testing.T) {
	var calls atomic.Int64
	delegate := lookupStub(&calls, nil, status.Error(codes.NotFound, "missing"))

	cache := NewCachingSchedulerClient(delegate, BindingCacheOptions{Size: 16, TTL: time.Minute, NegativeTTL: 5 * time.Second})
	if cache.negativeTTL != 5*time.Second {
		t.Fatalf("negativeTTL = %s, want the configured 5s", cache.negativeTTL)
	}
	now := time.Unix(1000, 0)
	cache.now = func() time.Time { return now }
	if _, err := lookup(t, cache, "sbx-1"); status.Code(err) != codes.NotFound {
		t.Fatalf("err = %v, want NotFound", err)
	}
	now = now.Add(4 * time.Second)
	if _, err := lookup(t, cache, "sbx-1"); status.Code(err) != codes.NotFound {
		t.Fatalf("err = %v, want NotFound", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("scheduler called %d times, want the NotFound held for the configured 5s", got)
	}
	now = now.Add(2 * time.Second)
	if _, err := lookup(t, cache, "sbx-1"); status.Code(err) != codes.NotFound {
		t.Fatalf("err = %v, want NotFound", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("scheduler called %d times, want the NotFound re-resolved after 5s", got)
	}

	capped := NewCachingSchedulerClient(delegate, BindingCacheOptions{Size: 16, TTL: time.Second, NegativeTTL: time.Minute})
	if capped.negativeTTL != time.Second {
		t.Fatalf("negativeTTL = %s, want it capped at the positive TTL", capped.negativeTTL)
	}
}

// A transport or store failure says nothing about the binding, so caching it
// would turn a scheduler blip into a routing outage that outlives it.
func TestBindingCacheDoesNotCacheTransportFailures(t *testing.T) {
	var calls atomic.Int64
	cache := cacheWithTTL(lookupStub(&calls, nil, status.Error(codes.Unavailable, "binding store unavailable")), time.Minute)

	for i := 0; i < 3; i++ {
		if _, err := lookup(t, cache, "sbx-1"); status.Code(err) != codes.Unavailable {
			t.Fatalf("err = %v, want Unavailable", err)
		}
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("scheduler called %d times, want every attempt to reach it", got)
	}
}

func TestBindingCacheExpiresEntries(t *testing.T) {
	var calls atomic.Int64
	node := &schedulerv1.Node{NodeId: "node-a", Endpoint: "http://node-a"}
	cache := cacheWithTTL(lookupStub(&calls, &schedulerv1.LookupNodeResponse{Node: node}, nil), time.Minute)

	now := time.Unix(1000, 0)
	cache.now = func() time.Time { return now }

	if _, err := lookup(t, cache, "sbx-1"); err != nil {
		t.Fatalf("LookupNode: %v", err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := lookup(t, cache, "sbx-1"); err != nil {
		t.Fatalf("LookupNode: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("scheduler called %d times, want the expired entry re-resolved", got)
	}
}

// The owning node disowning a sandbox means the cached binding is wrong now,
// not once the TTL runs out.
func TestBindingCacheInvalidateForcesReResolution(t *testing.T) {
	var calls atomic.Int64
	node := &schedulerv1.Node{NodeId: "node-a", Endpoint: "http://node-a"}
	cache := cacheWithTTL(lookupStub(&calls, &schedulerv1.LookupNodeResponse{Node: node}, nil), time.Minute)

	if _, err := lookup(t, cache, "sbx-1"); err != nil {
		t.Fatalf("LookupNode: %v", err)
	}
	cache.Invalidate("sbx-1")
	if _, err := lookup(t, cache, "sbx-1"); err != nil {
		t.Fatalf("LookupNode: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("scheduler called %d times, want invalidation to force a re-resolve", got)
	}
}

// The cache is an optimization, so refusing to grow is always safe.
func TestBindingCacheIsBounded(t *testing.T) {
	var calls atomic.Int64
	node := &schedulerv1.Node{NodeId: "node-a", Endpoint: "http://node-a"}
	cache := NewCachingSchedulerClient(
		lookupStub(&calls, &schedulerv1.LookupNodeResponse{Node: node}, nil),
		BindingCacheOptions{Size: 4, TTL: time.Minute},
	)

	for i := 0; i < 50; i++ {
		if _, err := lookup(t, cache, string(rune('a'+i%50))+"-sbx"); err != nil {
			t.Fatalf("LookupNode: %v", err)
		}
	}

	cache.mu.Lock()
	size := len(cache.entries)
	cache.mu.Unlock()
	if size > 4 {
		t.Fatalf("cache holds %d entries, want at most 4", size)
	}
}

// Size is the off switch: zero or negative means every lookup reaches the
// scheduler, concurrent ones included — off is the client the gateway had
// before the cache existed, not a cache that happens to hold nothing. A
// negative TTL keeps meaning the same, so a config that disabled the cache
// that way before still does.
func TestBindingCacheSizeAndNegativeTTLDisable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options BindingCacheOptions
	}{
		{name: "size zero", options: BindingCacheOptions{Size: 0, TTL: time.Minute}},
		{name: "size negative", options: BindingCacheOptions{Size: -1, TTL: time.Minute}},
		{name: "ttl negative", options: BindingCacheOptions{Size: 64, TTL: -1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const concurrent = 8
			var calls atomic.Int64
			var inFlight atomic.Int64
			everyoneIn := make(chan struct{})
			node := &schedulerv1.Node{NodeId: "node-a", Endpoint: "http://node-a"}
			delegate := stubSchedulerClient{
				lookupNodeFunc: func(_ context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
					calls.Add(1)
					if inFlight.Add(1) == concurrent {
						close(everyoneIn)
					}
					// Held until every concurrent caller has arrived, so a
					// cache that coalesced them could not have got here.
					select {
					case <-everyoneIn:
					case <-time.After(5 * time.Second):
					}
					return &schedulerv1.LookupNodeResponse{Node: node}, nil
				},
			}
			cache := NewCachingSchedulerClient(delegate, tc.options)

			var wg sync.WaitGroup
			for i := 0; i < concurrent; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if _, err := lookup(t, cache, "sbx-1"); err != nil {
						t.Errorf("LookupNode: %v", err)
					}
				}()
			}
			wg.Wait()
			if got := calls.Load(); got != concurrent {
				t.Fatalf("scheduler called %d times, want every one of %d concurrent lookups to pass through a disabled cache", got, concurrent)
			}

			for i := 0; i < 3; i++ {
				if _, err := lookup(t, cache, "sbx-1"); err != nil {
					t.Fatalf("LookupNode: %v", err)
				}
			}
			if got := calls.Load(); got != concurrent+3 {
				t.Fatalf("scheduler called %d times, want every lookup to pass through a disabled cache", got)
			}
			// Nothing to record into or throw out of, and neither may panic.
			cache.Record("sbx-1", node)
			cache.Invalidate("sbx-1")
		})
	}
}

// A fill reads the scheduler outside the lock, so a lookup that began before
// a disown can deliver its answer after the invalidation. Installing it would
// put the binding that was just thrown out back for a full TTL, which is
// exactly what Invalidate promises does not happen.
func TestBindingCacheDeclinesAFillThatStraddlesAnInvalidation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	delegate := stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if calls.Add(1) == 1 {
				// The answer computed before the move, delivered after it.
				close(entered)
				<-release
				return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "node-old"}}, nil
			}
			return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "node-new"}}, nil
		},
	}
	cache := cacheWithTTL(delegate, time.Minute)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = lookup(t, cache, "sbx-1")
	}()
	<-entered
	cache.Invalidate("sbx-1")
	close(release)
	wg.Wait()

	resp, err := lookup(t, cache, "sbx-1")
	if err != nil {
		t.Fatalf("LookupNode: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != "node-new" {
		t.Fatalf("served %q after the invalidation, want a fresh re-resolve to node-new", got)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("scheduler called %d times, want 2: the stale fill must not have been installed", got)
	}

	// And the fresh answer, which did not straddle anything, is cached.
	if _, err := lookup(t, cache, "sbx-1"); err != nil {
		t.Fatalf("LookupNode: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("scheduler called %d times, want the fresh fill served from cache", got)
	}
}

// A cold hot key — a sandbox every client just learned the id of — must cost
// one scheduler round trip, not one per concurrent request. The first miss
// leads the fill and the rest wait for its answer.
func TestBindingCacheCoalescesConcurrentMissesForOneKey(t *testing.T) {
	const waiters = 32
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	delegate := stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if calls.Add(1) == 1 {
				close(entered)
			}
			<-release
			return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "node-a"}}, nil
		},
	}
	cache := cacheWithTTL(delegate, time.Minute)

	var wg sync.WaitGroup
	var started sync.WaitGroup
	results := make([]string, waiters)
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		started.Add(1)
		go func(i int) {
			defer wg.Done()
			started.Done()
			resp, err := lookup(t, cache, "sbx-hot")
			if err != nil {
				results[i] = err.Error()
				return
			}
			results[i] = resp.GetNode().GetNodeId()
		}(i)
	}
	started.Wait()
	<-entered
	// Every waiter is parked on the leader by the time the leader is released:
	// they all entered LookupNode before the delegate was allowed to answer,
	// so any of them issuing its own call would already have been counted.
	waitForInflight(t, cache, "sbx-hot")
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("scheduler called %d times for one cold key, want 1", got)
	}
	for i, got := range results {
		if got != "node-a" {
			t.Fatalf("waiter %d got %q, want the leader's answer node-a", i, got)
		}
	}
}

// waitForInflight blocks until no goroutine can still be on its way into the
// leader's fill for sandboxID: the fill is registered and every other caller
// has had a scheduling opportunity to observe it.
func waitForInflight(t *testing.T, cache *CachingSchedulerClient, sandboxID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		cache.mu.Lock()
		_, ok := cache.inflight[sandboxID]
		cache.mu.Unlock()
		if ok {
			// Give the waiters a beat to reach their select.
			time.Sleep(20 * time.Millisecond)
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no fill in flight")
		}
		time.Sleep(time.Millisecond)
	}
}

// A waiter whose own request ends does not wait for the leader, and the
// leader giving up does not become every waiter's answer.
func TestBindingCacheWaitersHonourTheirOwnContext(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	var calls atomic.Int64
	delegate := stubSchedulerClient{
		lookupNodeFunc: func(ctx context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			if calls.Add(1) == 1 {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return nil, status.FromContextError(ctx.Err()).Err()
				}
			}
			return &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "node-a"}}, nil
		},
	}
	cache := cacheWithTTL(delegate, time.Minute)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	var leaderErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, leaderErr = cache.LookupNode(leaderCtx, &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"})
	}()
	<-entered
	waitForInflight(t, cache, "sbx-1")

	// A waiter with a short deadline leaves on its own terms. Run apart from
	// the test goroutine so a waiter that ignores its deadline is a failure
	// rather than a hang.
	shortCtx, cancelShort := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShort()
	shortDone := make(chan error, 1)
	go func() {
		_, err := cache.LookupNode(shortCtx, &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"})
		shortDone <- err
	}()
	select {
	case err := <-shortDone:
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("waiter err = %v, want its own DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a waiter whose own deadline passed is still waiting on the leader")
	}

	// A patient waiter is not handed the leader's cancellation; it re-asks.
	patientDone := make(chan string, 1)
	go func() {
		resp, err := lookup(t, cache, "sbx-1")
		if err != nil {
			patientDone <- err.Error()
			return
		}
		patientDone <- resp.GetNode().GetNodeId()
	}()
	waitForInflight(t, cache, "sbx-1")
	cancelLeader()
	wg.Wait()
	if status.Code(leaderErr) != codes.Canceled {
		t.Fatalf("leader err = %v, want Canceled", leaderErr)
	}
	close(release)
	if got := <-patientDone; got != "node-a" {
		t.Fatalf("patient waiter got %q, want a fresh fill's node-a", got)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("scheduler called %d times, want the abandoned fill plus one retry", got)
	}
}

// A create response is the freshest possible truth about where a sandbox
// lives. Recording it into the cache means the client's very next request is
// served locally rather than racing the scheduler's write.
func TestBindingCacheRecordServesWithoutALookup(t *testing.T) {
	var calls atomic.Int64
	cache := cacheWithTTL(lookupStub(&calls, nil, status.Error(codes.NotFound, "not yet")), time.Minute)

	node := &schedulerv1.Node{NodeId: "node-a", Endpoint: "http://node-a"}
	cache.Record("sbx-new", node)

	resp, err := lookup(t, cache, "sbx-new")
	if err != nil {
		t.Fatalf("LookupNode: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != "node-a" {
		t.Fatalf("node = %q, want the recorded node-a", got)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("scheduler called %d times, want the recorded binding served without one", got)
	}
}

// A lookup that began before a create was recorded carries the scheduler's
// answer from before the binding existed. Its NotFound must not install over
// the binding the gateway learned first-hand, and the caller that asked must
// not be told the sandbox does not exist either.
func TestBindingCacheANewerGenerationNeverLosesToAnOlderFill(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	delegate := stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, _ *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			calls.Add(1)
			// The scheduler had no binding when it was asked.
			close(entered)
			<-release
			return nil, status.Error(codes.NotFound, "sandbox assignment not found")
		},
	}
	cache := cacheWithTTL(delegate, time.Minute)

	var fillResp *schedulerv1.LookupNodeResponse
	var fillErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fillResp, fillErr = lookup(t, cache, "sbx-1")
	}()
	<-entered
	node := &schedulerv1.Node{NodeId: "node-a", Endpoint: "http://node-a"}
	cache.Record("sbx-1", node)
	close(release)
	wg.Wait()

	if fillErr != nil {
		t.Fatalf("the straddling lookup returned %v, want the recorded binding", fillErr)
	}
	if got := fillResp.GetNode().GetNodeId(); got != "node-a" {
		t.Fatalf("the straddling lookup served %q, want node-a", got)
	}
	resp, err := lookup(t, cache, "sbx-1")
	if err != nil {
		t.Fatalf("LookupNode after the record: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != "node-a" {
		t.Fatalf("served %q, want the recorded node-a to have survived the stale fill", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("scheduler called %d times, want the recorded binding to have needed no re-resolve", got)
	}
}

// bindingCacheCounts reads agentenv_gateway_binding_cache_total by result.
func bindingCacheCounts(t *testing.T) map[string]float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	counts := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "agentenv_gateway_binding_cache_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == "result" {
					counts[pair.GetValue()] += metric.GetCounter().GetValue()
				}
			}
		}
	}
	return counts
}

// The four results are what an operator reads the cache by: hits against
// misses is the rate it earns, negative hits is how often a create's first
// request arrives before its binding, and evictions are disowns.
func TestBindingCacheCountsEveryResult(t *testing.T) {
	before := bindingCacheCounts(t)

	var calls atomic.Int64
	answers := []struct {
		resp *schedulerv1.LookupNodeResponse
		err  error
	}{
		{resp: &schedulerv1.LookupNodeResponse{Node: &schedulerv1.Node{NodeId: "node-a"}}},
		{err: status.Error(codes.NotFound, "missing")},
	}
	delegate := stubSchedulerClient{
		lookupNodeFunc: func(_ context.Context, req *schedulerv1.LookupNodeRequest, _ ...grpc.CallOption) (*schedulerv1.LookupNodeResponse, error) {
			calls.Add(1)
			if req.GetSandboxId() == "sbx-present" {
				return answers[0].resp, nil
			}
			return nil, answers[1].err
		},
	}
	cache := cacheWithTTL(delegate, time.Minute)

	_, _ = lookup(t, cache, "sbx-present") // miss
	_, _ = lookup(t, cache, "sbx-present") // hit
	_, _ = lookup(t, cache, "sbx-present") // hit
	_, _ = lookup(t, cache, "sbx-absent")  // miss
	_, _ = lookup(t, cache, "sbx-absent")  // negative_hit
	cache.Invalidate("sbx-present")        // evict
	cache.Invalidate("sbx-never-cached")   // nothing to evict

	after := bindingCacheCounts(t)
	for result, want := range map[string]float64{
		bindingCacheResultHit:         2,
		bindingCacheResultMiss:        2,
		bindingCacheResultNegativeHit: 1,
		bindingCacheResultEvict:       1,
	} {
		if got := after[result] - before[result]; got != want {
			t.Fatalf("result=%s counted %v, want %v", result, got, want)
		}
	}
	for result := range after {
		switch result {
		case bindingCacheResultHit, bindingCacheResultMiss, bindingCacheResultNegativeHit, bindingCacheResultEvict:
		default:
			t.Fatalf("unexpected result label %q; the set is closed", result)
		}
	}
}
