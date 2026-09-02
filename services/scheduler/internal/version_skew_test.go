package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

// Nodes and schedulers are upgraded separately, and every wire change here
// added fields that an older peer simply will not send. Absent must therefore
// mean the safe thing in every case, and "safe" is not the same direction for
// every field — so this walks the matrix explicitly rather than trusting that
// each change got it right on its own.
//
// The other half of the matrix, a new node talking to an older scheduler,
// cannot be exercised from here: it is about what the node chooses to send.
// It lives in src/observability/roster.rs, where the rule is that the node
// keeps sending its full roster until a scheduler says it understands digests.

func skewService(t *testing.T) (*Service, BindingStore) {
	t.Helper()
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Minute)
	store := NewInMemoryBindingStoreWithGrace(time.Minute, 0)
	return NewService(nil, registry, NewStrategy("round_robin"), store), store
}

// An older node sends a roster and no digest. The roster must stay
// authoritative, exactly as before digests existed.
func TestSkewOldNodeSendsNoRosterDigest(t *testing.T) {
	service, store := skewService(t)

	seed := readyHeartbeat("node-a")
	seed.SandboxIds = []string{"sandbox-1"}
	if _, err := service.Heartbeat(context.Background(), seed); err != nil {
		t.Fatalf("seed heartbeat: %v", err)
	}
	if _, ok, _ := store.Get("sandbox-1", time.Now()); !ok {
		t.Fatal("an old node's roster must still create bindings")
	}

	// And its empty roster must still delete them: without a digest there is
	// no other way to read it.
	if _, err := service.Heartbeat(context.Background(), readyHeartbeat("node-a")); err != nil {
		t.Fatalf("empty heartbeat: %v", err)
	}
	if _, ok, _ := store.Get("sandbox-1", time.Now()); ok {
		t.Fatal("an old node's empty roster must remove the binding")
	}
}

// An older node sends no roster-completeness flag. Absent must read as "not
// authoritative", so a node still recovering at startup cannot wipe its own
// bindings by reporting an empty roster.
func TestSkewOldNodeSendsNoRosterCompleteness(t *testing.T) {
	service, store := skewService(t)

	seed := readyHeartbeat("node-a")
	seed.SandboxIds = []string{"sandbox-1"}
	if _, err := service.Heartbeat(context.Background(), seed); err != nil {
		t.Fatalf("seed heartbeat: %v", err)
	}

	incomplete := readyHeartbeat("node-a")
	incomplete.RosterComplete = false
	if _, err := service.Heartbeat(context.Background(), incomplete); err != nil {
		t.Fatalf("incomplete heartbeat: %v", err)
	}
	if _, ok, _ := store.Get("sandbox-1", time.Now()); !ok {
		t.Fatal("an unauthoritative empty roster must not delete bindings")
	}
}

// An older node reports no event count. Absent must mean "does not implement
// it", not "lost everything it ever emitted".
func TestSkewOldNodeReportsNoEventCount(t *testing.T) {
	service, _ := skewService(t)

	if _, err := service.ReportSandboxEvent(context.Background(), &schedulerv1.ReportSandboxEventRequest{
		NodeId: "node-a",
		Events: []*schedulerv1.SandboxEvent{{SandboxId: "sandbox-1"}},
	}); err != nil {
		t.Fatalf("report events: %v", err)
	}
	if _, err := service.Heartbeat(context.Background(), readyHeartbeat("node-a")); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if missed := service.eventLoss.observeEmitted("node-a", 0); missed != 0 {
		t.Fatalf("an absent count must not be read as loss, got %d", missed)
	}
}

// An older node reports no heartbeat interval. The scheduler must not validate
// its TTL ordering against a zero it invented — a zero interval would make
// every TTL too short and take the node's permission to elide away for a field
// it does not know exists.
func TestSkewOldNodeReportsNoHeartbeatInterval(t *testing.T) {
	// A TTL far too short for any interval a node could report, so the check
	// would fail if it ran at all.
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Minute)
	store := NewInMemoryBindingStoreWithGrace(time.Millisecond, 0)
	service := NewService(nil, registry, NewStrategy("round_robin"), store, WithBindingTTL(time.Millisecond))

	beat := readyHeartbeat("node-a")
	beat.HeartbeatIntervalMs = 0
	response, err := service.Heartbeat(context.Background(), beat)
	if err != nil {
		t.Fatalf("a heartbeat without an interval must be accepted: %v", err)
	}
	if !response.GetRosterDigestAccepted() {
		t.Fatal("an absent interval must read as \"older node\", not as a violated TTL ordering")
	}
}

// A new scheduler advertises that it understands digests, which is what lets a
// node start eliding. It must do so on the very first heartbeat from a node it
// has never seen, or no node would ever begin. The one thing that withholds it
// is a TTL that cannot cover a skipped round; nothing about the node being new
// does.
func TestSkewNewSchedulerAlwaysAdvertisesDigestSupport(t *testing.T) {
	service, _ := skewService(t)

	response, err := service.Heartbeat(context.Background(), readyHeartbeat("node-a"))
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if !response.GetRosterDigestAccepted() {
		t.Fatal("a new scheduler must advertise digest support unconditionally")
	}
}

// Completeness protects the empty roster and nothing else. A non-empty roster
// is reconciled whatever the flag says, because nodes that predate the flag
// never set it and their departed sandboxes must still be reaped at heartbeat
// latency rather than binding-TTL latency. The consequence, pinned here so a
// change to it is a decision rather than an accident: a node that is still
// discovering what it holds must send an empty roster, never a partial one.
// The proto and RosterIncomplete both say so.
func TestNonEmptyIncompleteRosterStillReapsUnlistedBindings(t *testing.T) {
	service, store := skewService(t)

	seed := readyHeartbeat("node-a")
	seed.SandboxIds = []string{"s1", "s2", "s3", "s4", "s5"}
	if _, err := service.Heartbeat(context.Background(), seed); err != nil {
		t.Fatalf("seed heartbeat: %v", err)
	}

	partial := readyHeartbeat("node-a")
	partial.SandboxIds = []string{"s1", "s2"}
	partial.RosterComplete = false
	if _, err := service.Heartbeat(context.Background(), partial); err != nil {
		t.Fatalf("partial heartbeat: %v", err)
	}
	for _, sandboxID := range []string{"s1", "s2"} {
		if _, ok, _ := store.Get(sandboxID, time.Now()); !ok {
			t.Fatalf("%s was listed and must survive", sandboxID)
		}
	}
	for _, sandboxID := range []string{"s3", "s4", "s5"} {
		if _, ok, _ := store.Get(sandboxID, time.Now()); ok {
			t.Fatalf("%s was omitted from a non-empty roster and must be reaped, completeness notwithstanding", sandboxID)
		}
	}
}
