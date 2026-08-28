package scheduler

import (
	"context"
	"testing"
	"time"
)

// Eliding a roster is only free while the binding TTL outlives the round the
// scheduler skips when it cannot resolve a digest. The startup check in
// services/shared/config states that relation but is opt-in on a key no
// shipped config sets, so what actually enforces it is the interval each node
// reports — by deciding whether that node is allowed to elide at all.

func elisionService(t *testing.T, bindingTTL time.Duration) *Service {
	t.Helper()
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "10.0.0.1:8000"}}, time.Minute)
	store := NewInMemoryBindingStoreWithGrace(bindingTTL, 0)
	return NewService(nil, registry, NewStrategy("round_robin"), store, WithBindingTTL(bindingTTL))
}

func elisionPermitted(t *testing.T, service *Service, intervalMs uint64) bool {
	t.Helper()
	beat := rosterHeartbeat("node-a", "", nil, true)
	beat.HeartbeatIntervalMs = intervalMs
	response, err := service.Heartbeat(context.Background(), beat)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	return response.GetRosterDigestAccepted()
}

func TestElisionPermissionFollowsTheReportedHeartbeatInterval(t *testing.T) {
	for _, tc := range []struct {
		name       string
		bindingTTL time.Duration
		intervalMs uint64
		want       bool
	}{
		{
			// Two heartbeats of slack: one miss and the retry that follows it
			// both land after the bindings are gone.
			name:       "a ttl that cannot cover a missed round withholds it",
			bindingTTL: 10 * time.Second,
			intervalMs: 5000,
			want:       false,
		},
		{
			name:       "a ttl with room to spare keeps it",
			bindingTTL: 30 * time.Second,
			intervalMs: 5000,
			want:       true,
		},
		{
			// The relation is the same one the config check enforces, so the
			// boundary has to be the same too, inclusive.
			name:       "exactly the minimum is enough",
			bindingTTL: 15 * time.Second,
			intervalMs: 5000,
			want:       true,
		},
		{
			// A node that slows down enough outgrows a TTL that was ample for
			// its old interval.
			name:       "a slower node outgrows the same ttl",
			bindingTTL: 30 * time.Second,
			intervalMs: 20000,
			want:       false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := elisionService(t, tc.bindingTTL)
			if got := elisionPermitted(t, service, tc.intervalMs); got != tc.want {
				t.Fatalf("digest support advertised = %v, want %v", got, tc.want)
			}
		})
	}
}

// A node that reports an interval so large it saturated its own conversion is
// misconfigured, not infinitely patient. The check must answer rather than
// overflow into one.
func TestAnAbsurdReportedIntervalWithholdsElisionInsteadOfWrapping(t *testing.T) {
	service := elisionService(t, time.Hour)
	if elisionPermitted(t, service, ^uint64(0)) {
		t.Fatal("an interval no TTL could cover must withhold elision")
	}
}

// Withholding the permission stops the next heartbeat from eliding; it must
// not touch this one. A heartbeat already in flight carries no ids, and
// refusing to resolve it would skip exactly the reconcile round the whole
// check exists to avoid skipping.
func TestWithheldElisionStillServesAnAlreadyElidedHeartbeat(t *testing.T) {
	service := elisionService(t, 10*time.Second)
	roster := []string{"sandbox-1"}
	digest := RosterDigest(roster)

	seed := rosterHeartbeat("node-a", digest, roster, true)
	seed.HeartbeatIntervalMs = 5000
	if _, err := service.Heartbeat(context.Background(), seed); err != nil {
		t.Fatalf("seed heartbeat: %v", err)
	}

	elided := rosterHeartbeat("node-a", digest, nil, false)
	elided.HeartbeatIntervalMs = 5000
	response, err := service.Heartbeat(context.Background(), elided)
	if err != nil {
		t.Fatalf("elided heartbeat: %v", err)
	}
	if response.GetRosterDigestAccepted() {
		t.Fatal("the permission should still be withheld")
	}
	if response.GetRequestFullRoster() {
		t.Fatal("a resolvable digest must still be served from the cache")
	}
}

// The warning is once per node, and nothing on the graceful path clears it, so
// the departed-node sweep is the only thing between it and a map that grows
// with fleet churn for the life of the process.
func TestTheWarnOnceSetIsPerNodeAndSwept(t *testing.T) {
	warned := newNodeWarnSet()

	if !warned.mark("node-a") {
		t.Fatal("the first warning for a node must be reported")
	}
	if warned.mark("node-a") {
		t.Fatal("a node must not be warned about twice")
	}
	if !warned.mark("node-b") {
		t.Fatal("one node's warning must not silence another's")
	}

	if dropped := warned.retain(func(nodeID string) bool { return nodeID != "node-a" }); dropped != 1 {
		t.Fatalf("the sweep should have dropped one departed node, got %d", dropped)
	}
	if !warned.mark("node-a") {
		t.Fatal("a node that left and came back is new again")
	}
	if warned.mark("node-b") {
		t.Fatal("the sweep must not drop a node the registry still knows")
	}
}

// The warn-once set has to be reclaimed by the same sweep that reclaims every
// other per-node map, or suppressing a repeated log becomes a slow leak keyed
// by node id. TestTheWarnOnceSetIsPerNodeAndSwept exercises nodeWarnSet.retain
// directly, which says nothing about whether pruneDepartedNodes reaches it —
// removing it from the sweep leaves that test green.
func TestTheSweepReclaimsTheWarnSetForDepartedNodes(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "10.0.0.1:8000"},
		{ID: "node-b", Endpoint: "10.0.0.2:8000"},
	}, time.Minute)
	// A one-millisecond binding TTL is misordered against any real reporting
	// interval, so every node that reports one gets warned exactly once and
	// lands in the set.
	store := NewInMemoryBindingStoreWithGrace(time.Millisecond, 0)
	service := NewService(nil, registry, NewStrategy("round_robin"), store, WithBindingTTL(time.Millisecond))

	for _, nodeID := range []string{"node-a", "node-b"} {
		beat := rosterHeartbeat(nodeID, "", nil, true)
		beat.HeartbeatIntervalMs = 5000
		if _, err := service.Heartbeat(context.Background(), beat); err != nil {
			t.Fatalf("heartbeat for %s: %v", nodeID, err)
		}
	}
	if got := len(service.ttlOrderingWarned.nodes); got != 2 {
		t.Fatalf("warn set holds %d nodes, want 2", got)
	}

	// node-b leaves the way a node actually leaves: it stops being discovered,
	// without ever calling UnregisterNode.
	registry.Set([]Node{{ID: "node-a", Endpoint: "10.0.0.1:8000"}}, nil)

	// The sweep is paced, so a test that just sends another heartbeat would
	// race the interval rather than exercise the reclaim.
	service.sweepMu.Lock()
	service.lastSweep = time.Now().Add(-2 * departedSweepInterval)
	service.sweepMu.Unlock()

	beat := rosterHeartbeat("node-a", "", nil, true)
	beat.HeartbeatIntervalMs = 5000
	if _, err := service.Heartbeat(context.Background(), beat); err != nil {
		t.Fatalf("sweeping heartbeat: %v", err)
	}

	if got := len(service.ttlOrderingWarned.nodes); got != 1 {
		t.Fatalf("warn set holds %d nodes after the sweep, want 1", got)
	}
}
