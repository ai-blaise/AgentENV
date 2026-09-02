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
)

const (
	bindingCacheResultHit         = "hit"
	bindingCacheResultMiss        = "miss"
	bindingCacheResultNegativeHit = "negative_hit"
	bindingCacheResultEvict       = "evict"
)

// BindingCacheOptions configures the sandbox-to-node lookup cache.
type BindingCacheOptions struct {
	// Size bounds the entries held. Zero or negative disables the cache. The
	// config layer supplies the shipped default for an unwritten key
	// (config.DefaultGatewayBindingCacheSize), so an operator who never
	// touched the setting does not arrive here with zero; a zero here is
	// someone's decision.
	Size int
	// TTL bounds how long a positive lookup is reused. Zero uses
	// defaultBindingCacheTTL; negative disables the cache.
	TTL time.Duration
	// NegativeTTL bounds how long a NotFound is reused. Zero uses
	// defaultBindingCacheNegativeTTL. It is capped at TTL: a "no" must never
	// outlive a "yes".
	NegativeTTL time.Duration
}

type bindingCacheEntry struct {
	node      *schedulerv1.Node
	found     bool
	expiresAt time.Time
	// gen orders this entry against the other writers for the same sandbox.
	// See CachingSchedulerClient.gen.
	gen uint64
}

// inflight is one scheduler round trip that every concurrent miss for the
// same sandbox waits on instead of issuing its own.
type inflight struct {
	done chan struct{}
	resp *schedulerv1.LookupNodeResponse
	err  error
	// abandoned means the leader's own request ended before the scheduler
	// answered. Its error is about that request, not the binding, so waiters
	// do not inherit it. A scheduler-side timeout is not this: that answer is
	// shared, or a slow scheduler would face one retry per waiter.
	abandoned bool
}

// CachingSchedulerClient serves sandbox lookups from a short-lived local cache.
//
// It implements schedulerv1.SchedulerClient by delegating everything except
// LookupNode, so it can stand in wherever the query-only client is used.
type CachingSchedulerClient struct {
	schedulerv1.SchedulerClient

	// disabled makes every lookup pass through, for a deployment that would
	// rather pay the scheduler round trip than ever serve a stale binding.
	disabled bool

	mu      sync.Mutex
	entries map[string]bindingCacheEntry
	// inflight holds the fill in progress per sandbox. A cold hot key — a
	// sandbox every client just learned the id of — would otherwise turn one
	// cache miss into one scheduler round trip per concurrent request.
	inflight map[string]*inflight
	// gen advances on every write that asserts a newer truth than a fill in
	// flight can: a binding recorded from a create response, and an
	// invalidation from a disown. A fill reads the scheduler outside the lock,
	// so its answer can be older than a write that landed while it ran. Each
	// entry carries the generation it was written under and a fill installs
	// only over an older one, so a newer generation never loses to an older.
	//
	// The proto carries no binding incarnation on LookupNode yet; this is the
	// gateway's own ordering of what it has learned, and a wire-carried
	// incarnation slots into the same field when one exists.
	gen uint64
	// invalidatedAt is gen as of the most recent Invalidate. One cache-wide
	// mark is enough because invalidations fire only on disowns, which are
	// rare; a coincident fill declining costs one more scheduler round trip
	// on the next request, and declining is always safe.
	invalidatedAt uint64
	ttl           time.Duration
	negativeTTL   time.Duration
	maxEntries    int
	now           func() time.Time
}

func NewCachingSchedulerClient(delegate schedulerv1.SchedulerClient, options BindingCacheOptions) *CachingSchedulerClient {
	if options.Size <= 0 || options.TTL < 0 {
		return &CachingSchedulerClient{SchedulerClient: delegate, disabled: true}
	}
	ttl := options.TTL
	if ttl == 0 {
		ttl = defaultBindingCacheTTL
	}
	negative := options.NegativeTTL
	if negative <= 0 {
		negative = defaultBindingCacheNegativeTTL
	}
	if negative > ttl {
		negative = ttl
	}
	return &CachingSchedulerClient{
		SchedulerClient: delegate,
		entries:         make(map[string]bindingCacheEntry),
		inflight:        make(map[string]*inflight),
		ttl:             ttl,
		negativeTTL:     negative,
		maxEntries:      options.Size,
		now:             time.Now,
	}
}

func (c *CachingSchedulerClient) LookupNode(
	ctx context.Context,
	req *schedulerv1.LookupNodeRequest,
	opts ...grpc.CallOption,
) (*schedulerv1.LookupNodeResponse, error) {
	sandboxID := req.GetSandboxId()
	if c.disabled || sandboxID == "" {
		return c.SchedulerClient.LookupNode(ctx, req, opts...)
	}

	counted := false
	for {
		entry, ok, startGen, fill, leads := c.lookupOrJoin(sandboxID)
		if ok {
			return entry.answer()
		}
		if !counted {
			// Every caller that did not find an answer missed, whether it pays
			// the round trip or waits for someone else's. The RPC rate beside
			// this counter is what shows how many of those trips were saved.
			recordGatewayBindingCache(bindingCacheResultMiss)
			counted = true
		}
		if leads {
			return c.fill(ctx, req, sandboxID, startGen, fill, opts...)
		}
		select {
		case <-fill.done:
			if fill.abandoned {
				// The next caller through leads a fill of its own.
				continue
			}
			return fill.resp, fill.err
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
}

// fill performs the scheduler round trip for a miss and hands the answer to
// every waiter that joined it.
func (c *CachingSchedulerClient) fill(
	ctx context.Context,
	req *schedulerv1.LookupNodeRequest,
	sandboxID string,
	startGen uint64,
	fl *inflight,
	opts ...grpc.CallOption,
) (*schedulerv1.LookupNodeResponse, error) {
	resp, err := c.SchedulerClient.LookupNode(ctx, req, opts...)

	var entry bindingCacheEntry
	cacheable := false
	switch {
	case err == nil:
		entry = bindingCacheEntry{node: resp.GetNode(), found: true, expiresAt: c.now().Add(c.ttl)}
		cacheable = true
	case status.Code(err) == codes.NotFound:
		entry = bindingCacheEntry{found: false, expiresAt: c.now().Add(c.negativeTTL)}
		cacheable = true
	default:
		// Transport and store failures say nothing about the binding, so
		// caching them would turn a scheduler blip into a routing outage that
		// outlives it.
	}

	c.mu.Lock()
	if cacheable && !c.installLocked(sandboxID, entry, startGen) {
		// A newer write landed while this fill was out. Serving the fill's
		// answer anyway would hand the caller a binding the cache already
		// knows is stale — most sharply, a NotFound for a sandbox whose
		// create this gateway just recorded.
		if newer, ok := c.entries[sandboxID]; ok && newer.expiresAt.After(c.now()) {
			resp, err = newer.answer()
		}
	}
	fl.resp, fl.err = resp, err
	fl.abandoned = err != nil && ctx.Err() != nil
	delete(c.inflight, sandboxID)
	close(fl.done)
	c.mu.Unlock()
	return resp, err
}

// Record installs a binding the gateway learned first-hand from a create
// response, ahead of any lookup.
//
// The node has said which sandbox it just made; there is no fresher truth. A
// Record therefore takes a new generation, so a LookupNode fill that started
// earlier — one that may be carrying the NotFound from before the binding
// existed — can no longer install over it. Without this, a client that
// creates a sandbox and immediately uses it could be told it does not exist
// by the very gateway that created it.
func (c *CachingSchedulerClient) Record(sandboxID string, node *schedulerv1.Node) {
	if c == nil || c.disabled || sandboxID == "" || node == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gen++
	c.installLocked(sandboxID, bindingCacheEntry{
		node:      node,
		found:     true,
		expiresAt: c.now().Add(c.ttl),
	}, c.gen)
}

// Invalidate drops a cached entry. Called when the upstream contradicts the
// cache — the sandbox has moved or is gone — so the next request re-resolves
// rather than waiting out the TTL.
// A nil receiver is a gateway configured without a binding cache, where there
// is nothing to invalidate. Callers on the response path should not each have
// to know that.
func (c *CachingSchedulerClient) Invalidate(sandboxID string) {
	if c == nil || c.disabled || sandboxID == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gen++
	c.invalidatedAt = c.gen
	if _, ok := c.entries[sandboxID]; ok {
		delete(c.entries, sandboxID)
		recordGatewayBindingCache(bindingCacheResultEvict)
	}
}

// lookupOrJoin returns the live entry for sandboxID, or — for a miss — the
// fill to wait on and whether the caller is the one who performs it, along
// with the generation the fill began under.
func (c *CachingSchedulerClient) lookupOrJoin(sandboxID string) (entry bindingCacheEntry, ok bool, startGen uint64, fl *inflight, leads bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[sandboxID]; ok {
		if entry.expiresAt.After(c.now()) {
			if entry.found {
				recordGatewayBindingCache(bindingCacheResultHit)
			} else {
				recordGatewayBindingCache(bindingCacheResultNegativeHit)
			}
			return entry, true, 0, nil, false
		}
		delete(c.entries, sandboxID)
	}
	if fl, ok := c.inflight[sandboxID]; ok {
		return bindingCacheEntry{}, false, 0, fl, false
	}
	fl = &inflight{done: make(chan struct{})}
	c.inflight[sandboxID] = fl
	return bindingCacheEntry{}, false, c.gen, fl, true
}

// installLocked writes an entry produced under gen, and reports whether it
// was installed. It declines when an invalidation happened after gen, or when
// the entry already present was written under a newer generation.
func (c *CachingSchedulerClient) installLocked(sandboxID string, entry bindingCacheEntry, gen uint64) bool {
	if c.invalidatedAt > gen {
		return false
	}
	if existing, ok := c.entries[sandboxID]; ok && existing.gen > gen {
		return false
	}
	if len(c.entries) >= c.maxEntries {
		c.evictExpiredLocked()
		// Still full of live entries: skip caching rather than grow without
		// bound. The cache is an optimization, so declining to add one is
		// always safe.
		if len(c.entries) >= c.maxEntries {
			return false
		}
	}
	entry.gen = gen
	c.entries[sandboxID] = entry
	return true
}

func (c *CachingSchedulerClient) evictExpiredLocked() {
	now := c.now()
	for sandboxID, entry := range c.entries {
		if !entry.expiresAt.After(now) {
			delete(c.entries, sandboxID)
		}
	}
}

func (e bindingCacheEntry) answer() (*schedulerv1.LookupNodeResponse, error) {
	if !e.found {
		return nil, status.Error(codes.NotFound, "sandbox assignment not found")
	}
	return &schedulerv1.LookupNodeResponse{Node: e.node}, nil
}
