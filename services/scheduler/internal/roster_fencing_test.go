package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

// The losing node's roster must not take a handed-over sandbox back.
//
// After a paused sandbox moves from origin to destination, the origin keeps
// listing it until its own record is dropped -- and dropping it is explicitly
// allowed to fail: `MigrationSteps::release_origin_state` reports a failure
// there and does not undo the migration, because the sandbox really is live
// elsewhere. Before this, the origin's next heartbeat took the binding back,
// the destination's took it again, and the two alternated for as long as both
// were up, so the sandbox was routable to the wrong node half the time and
// there was no state that would ever converge.
func TestARosterDoesNotTakeBackAHandedOverBinding(t *testing.T) {
	store := NewInMemoryBindingStore(30 * time.Second)
	origin := Node{ID: "node-origin", Endpoint: "http://node-origin"}
	destination := Node{ID: "node-destination", Endpoint: "http://node-destination"}
	now := time.Now()

	// The sandbox starts on the origin and is then deliberately reassigned,
	// which is what a completed handover does.
	if err := store.Record("sbx-moved", origin, now); err != nil {
		t.Fatalf("seed origin binding: %v", err)
	}
	if err := store.Record("sbx-moved", destination, now); err != nil {
		t.Fatalf("record the handover: %v", err)
	}

	// The origin failed to drop its record, so it still reports the sandbox.
	for beat := 1; beat <= 3; beat++ {
		if err := store.ReconcileNodeRoster(
			origin,
			[]string{"sbx-moved"},
			RosterComplete,
			now.Add(time.Duration(beat)*time.Second),
		); err != nil {
			t.Fatalf("origin heartbeat %d: %v", beat, err)
		}
		node, ok, err := store.Get("sbx-moved", now.Add(time.Duration(beat)*time.Second))
		if err != nil || !ok {
			t.Fatalf("binding vanished after origin heartbeat %d (ok=%v err=%v)", beat, ok, err)
		}
		if node.ID != destination.ID {
			t.Fatalf("origin heartbeat %d took the binding back: it names %q, want %q",
				beat, node.ID, destination.ID)
		}
	}
}

// A roster still establishes a binding nothing else has claimed.
//
// Refusing every roster write would be a different bug: a sandbox created
// directly on a node, with no assignment recorded for it, would never become
// routable at all.
func TestARosterStillEstablishesAnUnclaimedBinding(t *testing.T) {
	store := NewInMemoryBindingStore(30 * time.Second)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}
	now := time.Now()

	if err := store.ReconcileNodeRoster(node, []string{"sbx-new"}, RosterComplete, now); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, ok, err := store.Get("sbx-new", now)
	if err != nil || !ok {
		t.Fatalf("a roster must establish an absent binding (ok=%v err=%v)", ok, err)
	}
	if got.ID != node.ID {
		t.Fatalf("binding names %q, want %q", got.ID, node.ID)
	}
}

// A binding whose owner has gone is reclaimable once it lapses.
//
// This is the cost of the rule above: recovery from a departed node is bounded
// by the binding TTL rather than being immediate. Pinned so the bound is a
// decision rather than an accident.
func TestARosterReclaimsALapsedBindingFromADepartedNode(t *testing.T) {
	ttl := 30 * time.Second
	store := NewInMemoryBindingStore(ttl)
	departed := Node{ID: "node-departed", Endpoint: "http://node-departed"}
	survivor := Node{ID: "node-survivor", Endpoint: "http://node-survivor"}
	now := time.Now()

	if err := store.Record("sbx-orphan", departed, now); err != nil {
		t.Fatalf("seed departed binding: %v", err)
	}

	// Inside the TTL the survivor's roster is refused.
	if err := store.ReconcileNodeRoster(
		survivor, []string{"sbx-orphan"}, RosterComplete, now.Add(ttl/2),
	); err != nil {
		t.Fatalf("reconcile inside ttl: %v", err)
	}
	if got, _, _ := store.Get("sbx-orphan", now.Add(ttl/2)); got.ID != departed.ID {
		t.Fatalf("a live binding was taken inside its TTL: %q", got.ID)
	}

	// Past it, the entry is absent and the roster establishes it.
	after := now.Add(ttl + time.Second)
	if err := store.ReconcileNodeRoster(
		survivor, []string{"sbx-orphan"}, RosterComplete, after,
	); err != nil {
		t.Fatalf("reconcile past ttl: %v", err)
	}
	got, ok, err := store.Get("sbx-orphan", after)
	if err != nil || !ok {
		t.Fatalf("a lapsed binding must be reclaimable (ok=%v err=%v)", ok, err)
	}
	if got.ID != survivor.ID {
		t.Fatalf("binding names %q after the TTL lapsed, want %q", got.ID, survivor.ID)
	}
}

// The Redis store answers the same question the same way.
//
// Two replicas reconcile concurrently there, so the refusal has to be inside
// the write; a check outside it is not a check.
func TestRedisARosterDoesNotTakeBackAHandedOverBinding(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 30*time.Second)
	origin := Node{ID: "node-origin", Endpoint: "http://node-origin"}
	destination := Node{ID: "node-destination", Endpoint: "http://node-destination"}
	now := time.Now()

	if err := store.Record("sbx-moved", origin, now); err != nil {
		t.Fatalf("seed origin binding: %v", err)
	}
	if err := store.Record("sbx-moved", destination, now); err != nil {
		t.Fatalf("record the handover: %v", err)
	}

	for beat := 1; beat <= 3; beat++ {
		if err := store.ReconcileNodeRoster(
			origin, []string{"sbx-moved"}, RosterComplete, now,
		); err != nil {
			t.Fatalf("origin heartbeat %d: %v", beat, err)
		}
		node, ok, err := store.Get("sbx-moved", now)
		if err != nil || !ok {
			t.Fatalf("binding vanished after origin heartbeat %d (ok=%v err=%v)", beat, ok, err)
		}
		if node.ID != destination.ID {
			t.Fatalf("origin heartbeat %d took the binding back: it names %q, want %q",
				beat, node.ID, destination.ID)
		}
	}

	// And the destination's own index still claims it, so a later reconcile by
	// the destination does not treat it as departed.
	if err := store.ReconcileNodeRoster(
		destination, []string{"sbx-moved"}, RosterComplete, now,
	); err != nil {
		t.Fatalf("destination heartbeat: %v", err)
	}
	if node, ok, _ := store.Get("sbx-moved", now); !ok || node.ID != destination.ID {
		t.Fatalf("the destination lost its own binding (ok=%v node=%q)", ok, node.ID)
	}
}

// A completed handover routes to the destination and stays there.
//
// This is the whole failure the fencing exists for, end to end. The origin's
// release of its own paused state is explicitly allowed to fail without
// undoing the migration, so the origin keeps listing the sandbox forever.
// Before, the two nodes' heartbeats alternated the binding between them.
// Fencing the roster alone would have been worse, not better: the origin would
// have gone on refreshing its own stale binding and pinned the sandbox to the
// wrong node permanently. The record is what breaks the tie, because it is the
// only thing that knows the handover happened.
func TestACompletedHandoverRoutesToTheDestinationAndStaysThere(t *testing.T) {
	origin := Node{ID: "node-origin", Endpoint: "http://node-origin"}
	destination := Node{ID: "node-destination", Endpoint: "http://node-destination"}
	service := NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry([]Node{origin, destination}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)
	ctx := context.Background()
	now := time.Now()

	if err := service.store.Record("sbx-handover", origin, now); err != nil {
		t.Fatalf("seed the origin binding: %v", err)
	}

	// The destination completes the migration and says so.
	resp, err := service.UpsertMobilityRecord(ctx, &schedulerv1.UpsertMobilityRecordRequest{
		Record: &schedulerv1.MobilityRecord{
			SandboxId:      "sbx-handover",
			OriginNodeId:   origin.ID,
			Generation:     "0193-evacuated",
			State:          schedulerv1.MobilityState_MOBILITY_STATE_EVACUATED,
			HolderNodeId:   destination.ID,
			SnapshotId:     "snap-1",
			PausedAtUnixMs: now.UnixMilli(),
			StateAtUnixMs:  now.UnixMilli(),
		},
	})
	if err != nil {
		t.Fatalf("upsert the evacuated record: %v", err)
	}
	if !resp.GetApplied() {
		t.Fatal("the evacuated record was not applied")
	}

	lookup, err := service.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: "sbx-handover"})
	if err != nil {
		t.Fatalf("lookup after the handover: %v", err)
	}
	if got := lookup.GetNode().GetNodeId(); got != destination.ID {
		t.Fatalf("a completed handover still routes to %q, want %q", got, destination.ID)
	}

	// The origin never released its own copy, so it goes on reporting the
	// sandbox. Its heartbeats must change nothing.
	for beat := 1; beat <= 3; beat++ {
		at := now.Add(time.Duration(beat) * time.Second)
		if err := service.store.ReconcileNodeRoster(
			origin, []string{"sbx-handover"}, RosterComplete, at,
		); err != nil {
			t.Fatalf("origin heartbeat %d: %v", beat, err)
		}
		lookup, err := service.LookupNode(ctx, &schedulerv1.LookupNodeRequest{
			SandboxId: "sbx-handover",
		})
		if err != nil {
			t.Fatalf("lookup after origin heartbeat %d: %v", beat, err)
		}
		if got := lookup.GetNode().GetNodeId(); got != destination.ID {
			t.Fatalf("origin heartbeat %d took the sandbox back: routes to %q, want %q",
				beat, got, destination.ID)
		}
	}
}
