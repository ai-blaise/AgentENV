package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

// Models what a correct node sends, which is why completeness tracks `full`:
// an elided heartbeat carries no ids and so claims no authority over any. See
// the wire contract in src/observability/roster.rs.
func rosterHeartbeat(nodeID, digest string, sandboxIDs []string, full bool) *schedulerv1.HeartbeatRequest {
	return &schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         "cluster",
		ServiceInstanceId: "instance-1",
		SandboxIds:        sandboxIDs,
		RosterComplete:    full,
		RosterDigest:      digest,
		RosterFull:        full,
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}
}

// The reconcile grace period is disabled here. It exists to stop a roster that
// predates a just-recorded binding from deleting it, and these tests write and
// reconcile within the same instant, where it would mask every deletion.
func rosterService(t *testing.T) (*Service, BindingStore) {
	t.Helper()
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "10.0.0.1:8000"}}, time.Minute)
	store := NewInMemoryBindingStoreWithGrace(time.Minute, 0)
	return NewService(nil, registry, NewStrategy("round_robin"), store), store
}

func boundNode(t *testing.T, store BindingStore, sandboxID string) (string, bool) {
	t.Helper()
	node, ok, err := store.Get(sandboxID, time.Now())
	if err != nil {
		t.Fatalf("lookup %s: %v", sandboxID, err)
	}
	return node.ID, ok
}

// The whole point: a node that resends nothing but a digest must still have
// its bindings refreshed, or the elision would quietly expire them.
func TestHeartbeatReconcilesFromTheCachedRosterWhenTheDigestIsUnchanged(t *testing.T) {
	service, store := rosterService(t)
	roster := []string{"sandbox-1", "sandbox-2"}
	digest := RosterDigest(roster)

	first, err := service.Heartbeat(context.Background(), rosterHeartbeat("node-a", digest, roster, true))
	if err != nil {
		t.Fatalf("first heartbeat: %v", err)
	}
	if !first.GetRosterDigestAccepted() {
		t.Fatal("the scheduler must advertise that it understands digests")
	}
	if first.GetRequestFullRoster() {
		t.Fatal("a heartbeat that carried the roster should not ask for it again")
	}

	// Second heartbeat elides the roster entirely.
	second, err := service.Heartbeat(context.Background(), rosterHeartbeat("node-a", digest, nil, false))
	if err != nil {
		t.Fatalf("second heartbeat: %v", err)
	}
	if second.GetRequestFullRoster() {
		t.Fatal("a known digest must not trigger a re-send")
	}

	for _, sandboxID := range roster {
		if node, ok := boundNode(t, store, sandboxID); !ok || node != "node-a" {
			t.Fatalf("%s should still be bound to node-a, got %q ok=%v", sandboxID, node, ok)
		}
	}
}

// An elided roster and an empty one are the same bytes. A scheduler that
// cannot resolve the digest must ask rather than guess, because guessing wrong
// deletes the node's entire data plane.
func TestAnUnknownDigestRequestsTheRosterInsteadOfDeletingBindings(t *testing.T) {
	service, store := rosterService(t)
	roster := []string{"sandbox-1", "sandbox-2"}

	if _, err := service.Heartbeat(context.Background(), rosterHeartbeat("node-a", RosterDigest(roster), roster, true)); err != nil {
		t.Fatalf("seed heartbeat: %v", err)
	}

	// A digest this scheduler has never seen — the node's roster changed, or
	// this is a different scheduler process.
	response, err := service.Heartbeat(context.Background(), rosterHeartbeat("node-a", "0000", nil, false))
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !response.GetRequestFullRoster() {
		t.Fatal("an unresolvable digest must ask for the full roster")
	}

	for _, sandboxID := range roster {
		if _, ok := boundNode(t, store, sandboxID); !ok {
			t.Fatalf("%s must not be unbound on the strength of a digest we cannot read", sandboxID)
		}
	}
}

// A node that never adopted digests must behave exactly as before.
func TestAHeartbeatWithoutADigestIsAuthoritativeAsBefore(t *testing.T) {
	service, store := rosterService(t)

	// A node with no digest has no way to elide, so its roster always comes
	// along — which is what `full` says.
	if _, err := service.Heartbeat(context.Background(), rosterHeartbeat("node-a", "", []string{"sandbox-1"}, true)); err != nil {
		t.Fatalf("seed heartbeat: %v", err)
	}
	if _, ok := boundNode(t, store, "sandbox-1"); !ok {
		t.Fatal("sandbox-1 should be bound")
	}

	// Now the node reports it owns nothing, with no digest. That is a real
	// empty roster and the binding must go.
	if _, err := service.Heartbeat(context.Background(), rosterHeartbeat("node-a", "", nil, true)); err != nil {
		t.Fatalf("empty heartbeat: %v", err)
	}
	if _, ok := boundNode(t, store, "sandbox-1"); ok {
		t.Fatal("an authoritative empty roster must remove the binding")
	}
}

// A node that comes back must be asked for a fresh roster rather than
// reconciled against what it had before it left. It comes back as a new
// process, so with a newer incarnation: the one that unregistered is fenced.
func TestUnregisterForgetsTheCachedRoster(t *testing.T) {
	service, _ := rosterService(t)
	roster := []string{"sandbox-1"}
	digest := RosterDigest(roster)

	if _, err := service.Heartbeat(context.Background(), rosterHeartbeat("node-a", digest, roster, true)); err != nil {
		t.Fatalf("seed heartbeat: %v", err)
	}
	if _, err := service.UnregisterNode(context.Background(), &schedulerv1.UnregisterNodeRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "instance-1",
	}); err != nil {
		t.Fatalf("unregister: %v", err)
	}

	returned := rosterHeartbeat("node-a", digest, nil, false)
	returned.ServiceInstanceId = "instance-2"
	response, err := service.Heartbeat(context.Background(), returned)
	if err != nil {
		t.Fatalf("heartbeat after unregister: %v", err)
	}
	if !response.GetRequestFullRoster() {
		t.Fatal("a returning node must be asked to reintroduce its roster")
	}
}

// The digest describes the set, not the order the ids arrived in. Neither side
// promises an order, and requiring one would make every heartbeat a re-send.
func TestRosterDigestIgnoresOrderAndBlanks(t *testing.T) {
	forward := RosterDigest([]string{"a", "b", "c"})
	reverse := RosterDigest([]string{"c", "b", "a"})
	padded := RosterDigest([]string{" c ", "", "a", "b"})

	if forward != reverse || forward != padded {
		t.Fatalf("digest should ignore order and blanks: %q %q %q", forward, reverse, padded)
	}
	if forward == RosterDigest([]string{"a", "b"}) {
		t.Fatal("a different set must produce a different digest")
	}
}

// The digest is a wire format shared with the Rust node, so it is pinned to
// fixed vectors rather than compared against itself. The same vectors are
// asserted in src/observability/roster.rs; if the two ever disagree, every
// heartbeat silently becomes a re-send and the elision buys nothing.
func TestRosterDigestMatchesTheAgreedWireFormat(t *testing.T) {
	const abc = "880553fca8fcea94e325ee2cfb48e5a985cc797f39a14cc6d3cedecfeb2ae4d2"
	if got := RosterDigest([]string{"c", "a", "b"}); got != abc {
		t.Fatalf("digest drifted from the agreed format: got %q want %q", got, abc)
	}

	// An empty roster is sha256 of nothing, not of the empty string.
	const empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if got := RosterDigest(nil); got != empty {
		t.Fatalf("empty digest drifted: got %q want %q", got, empty)
	}
}

// An incomplete roster cached during startup recovery must be upgraded once
// the node says it has finished, even though the ids did not change: what an
// empty roster means is different on either side of that line.
//
// A current node says so by resending in full, because its own state changed.
// A node old enough to still assert completeness on an elided heartbeat says
// so only through the wire bit, and the scheduler has to keep hearing it —
// hence the explicit flag here. The raise is one-way: the same bit arriving
// false can never undo it.
func TestCompletenessUpgradeIsRememberedForAnUnchangedDigest(t *testing.T) {
	service, _ := rosterService(t)
	roster := []string{"sandbox-1"}
	digest := RosterDigest(roster)

	incomplete := rosterHeartbeat("node-a", digest, roster, true)
	incomplete.RosterComplete = false
	if _, err := service.Heartbeat(context.Background(), incomplete); err != nil {
		t.Fatalf("incomplete heartbeat: %v", err)
	}
	recovered := rosterHeartbeat("node-a", digest, nil, false)
	recovered.RosterComplete = true
	if _, err := service.Heartbeat(context.Background(), recovered); err != nil {
		t.Fatalf("complete heartbeat: %v", err)
	}

	cached, complete, ok := service.rosters.lookup("node-a", digest)
	if !ok || len(cached) != 1 {
		t.Fatalf("roster should be cached, got %v ok=%v", cached, ok)
	}
	if !complete {
		t.Fatal("the cached roster should have been upgraded to complete")
	}
}

// An elided heartbeat says nothing about its own ids, so the authority to
// delete has to come from the cached roster it resolves to. Taking the wire
// bit instead would leave every elided round unauthoritative, and a node that
// has emptied out would keep its dead bindings until the TTL expired them —
// which is the failure the cache exists to avoid.
func TestElidedHeartbeatStillReapsFromTheCachedAuthority(t *testing.T) {
	service, store := rosterService(t)

	// The node empties out and says so authoritatively, in full.
	emptyDigest := RosterDigest(nil)
	if _, err := service.Heartbeat(context.Background(), rosterHeartbeat("node-a", emptyDigest, nil, true)); err != nil {
		t.Fatalf("empty heartbeat: %v", err)
	}

	// A binding appears behind the node's back — a stale write, or one the
	// node has already forgotten. The next elided round must still reap it.
	if err := store.Record("sandbox-1", Node{ID: "node-a", Endpoint: "10.0.0.1:8000"}, time.Now()); err != nil {
		t.Fatalf("record stray binding: %v", err)
	}
	elided, err := service.Heartbeat(context.Background(), rosterHeartbeat("node-a", emptyDigest, nil, false))
	if err != nil {
		t.Fatalf("elided heartbeat: %v", err)
	}
	if elided.GetRequestFullRoster() {
		t.Fatal("a known digest must not trigger a re-send")
	}
	if _, ok := boundNode(t, store, "sandbox-1"); ok {
		t.Fatal("an elided heartbeat must reconcile with the cached roster's authority")
	}
}

// The other direction. A node that was still recovering when it sent its
// roster never claimed authority, and eliding must not manufacture it: an
// empty roster from a node that has not finished replaying its own state says
// nothing about what it holds.
func TestElidedHeartbeatDoesNotPromoteARecoveringRoster(t *testing.T) {
	service, store := rosterService(t)

	emptyDigest := RosterDigest(nil)
	recovering := rosterHeartbeat("node-a", emptyDigest, nil, true)
	recovering.RosterComplete = false
	if _, err := service.Heartbeat(context.Background(), recovering); err != nil {
		t.Fatalf("recovering heartbeat: %v", err)
	}

	if err := store.Record("sandbox-1", Node{ID: "node-a", Endpoint: "10.0.0.1:8000"}, time.Now()); err != nil {
		t.Fatalf("record binding: %v", err)
	}
	if _, err := service.Heartbeat(context.Background(), rosterHeartbeat("node-a", emptyDigest, nil, false)); err != nil {
		t.Fatalf("elided heartbeat: %v", err)
	}
	if _, ok := boundNode(t, store, "sandbox-1"); !ok {
		t.Fatal("a roster the node never claimed was complete must not delete anything")
	}
}

// The cache is keyed by a digest the node computes, so the one thing it cannot
// do is take that digest on trust. remember is its only writer.
func TestRememberRefusesARosterThatDoesNotMatchItsDigest(t *testing.T) {
	cache := newRosterCache()
	roster := []string{"sandbox-1", "sandbox-2"}
	honest := RosterDigest(roster)
	wrong := RosterDigest(nil)

	if !cache.remember("node-a", honest, roster, true) {
		t.Fatal("a roster that matches its digest must be cached")
	}
	if cache.remember("node-a", wrong, roster, true) {
		t.Fatal("a roster that does not match its digest must be refused")
	}
	if _, _, ok := cache.lookup("node-a", wrong); ok {
		t.Fatal("a refused digest must not resolve to anything")
	}
	cached, _, ok := cache.lookup("node-a", honest)
	if !ok || len(cached) != 2 {
		t.Fatalf("the verified entry must survive the refused one, got %v ok=%v", cached, ok)
	}
}

// A heartbeat whose ids and digest describe different rosters contradicts
// itself, and both halves of it are load-bearing: the ids decide what is
// reconciled now, the digest decides what every later elided heartbeat
// resolves to. Believing either one means acting on a roster the node did not
// send — here, dropping a sandbox it still owns and then keeping it dropped
// for as long as the digest stays unchanged.
func TestAHeartbeatThatContradictsItsOwnDigestReconcilesNothing(t *testing.T) {
	service, store := rosterService(t)
	roster := []string{"sandbox-1", "sandbox-2"}
	digest := RosterDigest(roster)

	if _, err := service.Heartbeat(context.Background(), rosterHeartbeat("node-a", digest, roster, true)); err != nil {
		t.Fatalf("seed heartbeat: %v", err)
	}

	// The same digest, but only half the roster arrives with it.
	truncated, err := service.Heartbeat(context.Background(), rosterHeartbeat("node-a", digest, roster[:1], true))
	if err != nil {
		t.Fatalf("truncated heartbeat: %v", err)
	}
	if !truncated.GetRequestFullRoster() {
		t.Fatal("a heartbeat that contradicts its own digest must be sent back for the roster")
	}
	if _, ok := boundNode(t, store, "sandbox-2"); !ok {
		t.Fatal("a roster the node's own digest disowns must not delete bindings")
	}

	// The cache must still hold what was verified, so the node's next elided
	// heartbeat resolves to the roster it really sent.
	elided, err := service.Heartbeat(context.Background(), rosterHeartbeat("node-a", digest, nil, false))
	if err != nil {
		t.Fatalf("elided heartbeat: %v", err)
	}
	if elided.GetRequestFullRoster() {
		t.Fatal("the verified entry should still resolve the digest")
	}
	for _, sandboxID := range roster {
		if node, ok := boundNode(t, store, sandboxID); !ok || node != "node-a" {
			t.Fatalf("%s should still be bound to node-a, got %q ok=%v", sandboxID, node, ok)
		}
	}
}
