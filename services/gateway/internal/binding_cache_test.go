package gateway

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

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

func TestBindingCacheServesRepeatLookupsLocally(t *testing.T) {
	var calls atomic.Int64
	node := &schedulerv1.Node{NodeId: "node-a", Endpoint: "http://node-a"}
	cache := NewCachingSchedulerClient(
		lookupStub(&calls, &schedulerv1.LookupNodeResponse{Node: node}, nil),
		time.Minute,
	)

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
	cache := NewCachingSchedulerClient(
		lookupStub(&calls, nil, status.Error(codes.NotFound, "missing")),
		time.Minute,
	)

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

// A transport or store failure says nothing about the binding, so caching it
// would turn a scheduler blip into a routing outage that outlives it.
func TestBindingCacheDoesNotCacheTransportFailures(t *testing.T) {
	var calls atomic.Int64
	cache := NewCachingSchedulerClient(
		lookupStub(&calls, nil, status.Error(codes.Unavailable, "binding store unavailable")),
		time.Minute,
	)

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
	cache := NewCachingSchedulerClient(
		lookupStub(&calls, &schedulerv1.LookupNodeResponse{Node: node}, nil),
		time.Minute,
	)

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
	cache := NewCachingSchedulerClient(
		lookupStub(&calls, &schedulerv1.LookupNodeResponse{Node: node}, nil),
		time.Minute,
	)

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
		time.Minute,
	)
	cache.maxEntries = 4

	for i := 0; i < 50; i++ {
		if _, err := lookup(t, cache, string(rune('a'+i%50))+"-sbx"); err != nil {
			t.Fatalf("LookupNode: %v", err)
		}
	}

	cache.mu.Lock()
	size := len(cache.entries)
	cache.mu.Unlock()
	if size > cache.maxEntries {
		t.Fatalf("cache holds %d entries, want at most %d", size, cache.maxEntries)
	}
}
