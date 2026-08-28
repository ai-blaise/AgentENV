package gateway

import (
	"context"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Every data-plane request resolves its sandbox through the scheduler, which in
// a Redis-backed deployment means a gRPC round trip plus a Redis GET per
// request. Bindings change only when a sandbox is created, deleted, or moved,
// so almost all of that traffic re-reads an answer that has not changed.
//
// The cache is a decorator over the scheduler client rather than a change to
// the request path, so routing logic stays unaware of it.
const (
	// defaultBindingCacheTTL is deliberately far below the binding TTL. A
	// cached entry outlives the truth by at most this long, which bounds how
	// long a deleted or moved sandbox keeps routing to its old node.
	defaultBindingCacheTTL = 2 * time.Second
	// defaultBindingCacheNegativeTTL is shorter still: a sandbox that does not
	// resolve yet usually resolves imminently — the create is in flight — so
	// caching "no" for long would turn a momentary gap into a visible failure.
	defaultBindingCacheNegativeTTL = 200 * time.Millisecond
	defaultBindingCacheMaxEntries  = 65536
)

type bindingCacheEntry struct {
	node      *schedulerv1.Node
	found     bool
	expiresAt time.Time
}

// CachingSchedulerClient serves sandbox lookups from a short-lived local cache.
//
// It implements schedulerv1.SchedulerClient by delegating everything except
// LookupNode, so it can stand in wherever the query-only client is used.
type CachingSchedulerClient struct {
	schedulerv1.SchedulerClient

	mu          sync.Mutex
	entries     map[string]bindingCacheEntry
	ttl         time.Duration
	negativeTTL time.Duration
	maxEntries  int
	now         func() time.Time
}

func NewCachingSchedulerClient(delegate schedulerv1.SchedulerClient, ttl time.Duration) *CachingSchedulerClient {
	if ttl <= 0 {
		ttl = defaultBindingCacheTTL
	}
	negative := defaultBindingCacheNegativeTTL
	if negative > ttl {
		negative = ttl
	}
	return &CachingSchedulerClient{
		SchedulerClient: delegate,
		entries:         make(map[string]bindingCacheEntry),
		ttl:             ttl,
		negativeTTL:     negative,
		maxEntries:      defaultBindingCacheMaxEntries,
		now:             time.Now,
	}
}

func (c *CachingSchedulerClient) LookupNode(
	ctx context.Context,
	req *schedulerv1.LookupNodeRequest,
	opts ...grpc.CallOption,
) (*schedulerv1.LookupNodeResponse, error) {
	sandboxID := req.GetSandboxId()
	if sandboxID == "" {
		return c.SchedulerClient.LookupNode(ctx, req, opts...)
	}

	if entry, ok := c.lookup(sandboxID); ok {
		if !entry.found {
			return nil, status.Error(codes.NotFound, "sandbox assignment not found")
		}
		return &schedulerv1.LookupNodeResponse{Node: entry.node}, nil
	}

	resp, err := c.SchedulerClient.LookupNode(ctx, req, opts...)
	switch {
	case err == nil:
		c.store(sandboxID, bindingCacheEntry{
			node:      resp.GetNode(),
			found:     true,
			expiresAt: c.now().Add(c.ttl),
		})
	case status.Code(err) == codes.NotFound:
		c.store(sandboxID, bindingCacheEntry{
			found:     false,
			expiresAt: c.now().Add(c.negativeTTL),
		})
	default:
		// Transport and store failures say nothing about the binding, so
		// caching them would turn a scheduler blip into a routing outage that
		// outlives it.
	}
	return resp, err
}

// Invalidate drops a cached entry. Called when the upstream contradicts the
// cache — the sandbox has moved or is gone — so the next request re-resolves
// rather than waiting out the TTL.
func (c *CachingSchedulerClient) Invalidate(sandboxID string) {
	if sandboxID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, sandboxID)
}

func (c *CachingSchedulerClient) lookup(sandboxID string) (bindingCacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[sandboxID]
	if !ok {
		return bindingCacheEntry{}, false
	}
	if !entry.expiresAt.After(c.now()) {
		delete(c.entries, sandboxID)
		return bindingCacheEntry{}, false
	}
	return entry, true
}

func (c *CachingSchedulerClient) store(sandboxID string, entry bindingCacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.maxEntries {
		c.evictExpiredLocked()
		// Still full of live entries: skip caching rather than grow without
		// bound. The cache is an optimization, so declining to add one is
		// always safe.
		if len(c.entries) >= c.maxEntries {
			return
		}
	}
	c.entries[sandboxID] = entry
}

func (c *CachingSchedulerClient) evictExpiredLocked() {
	now := c.now()
	for sandboxID, entry := range c.entries {
		if !entry.expiresAt.After(now) {
			delete(c.entries, sandboxID)
		}
	}
}
