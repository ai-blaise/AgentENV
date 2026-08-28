package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

// Every behaviour added for scale here is gated, and a gate that does nothing
// is worse than no gate: it is a documented rollback that will not roll back.
// This session produced two of exactly that — a prewarm flag the pool ignored,
// and a cache handle the invalidation path never held — both found by accident
// rather than by a test.
//
// So each switch is asserted in both directions. Off must remove the
// behaviour, which catches a dead flag; on must produce it, which catches a
// flag wired to disable something else.

func offSwitchRegistry(t *testing.T, nodes []Node, ttl time.Duration) NodeRegistry {
	t.Helper()
	return NewAtomicNodeRegistry(nodes, ttl)
}

func readyHeartbeat(nodeID string) *schedulerv1.HeartbeatRequest {
	return &schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         "cluster",
		ServiceInstanceId: nodeID + "-instance",
		RosterComplete:    true,
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}
}

// The health gate drops nodes whose heartbeat has gone stale. Off, a stale
// node must be placeable again — that is the whole point of the rollback.
func TestOffSwitchHealthGate(t *testing.T) {
	nodes := []Node{{ID: "fresh", Endpoint: "http://fresh"}, {ID: "stale", Endpoint: "http://stale"}}

	for _, tc := range []struct {
		name               string
		enabled            bool
		wantStalePlaceable bool
	}{
		{name: "on drops the stale node", enabled: true, wantStalePlaceable: false},
		{name: "off restores whole-fleet placement", enabled: false, wantStalePlaceable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := offSwitchRegistry(t, nodes, 50*time.Millisecond)
			service := NewService(nil, registry, NewStrategy("round_robin"), NewInMemoryBindingStore(time.Minute),
				WithHealthGate(tc.enabled), WithReportTTL(50*time.Millisecond))

			// Only "fresh" reports; "stale" never does, so it is as stale as a
			// node gets.
			if _, err := service.Heartbeat(context.Background(), readyHeartbeat("fresh")); err != nil {
				t.Fatalf("heartbeat: %v", err)
			}

			// Excluding the fresh node leaves only the stale one, so whether a
			// placement succeeds is exactly whether the gate is off.
			_, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{
				ExcludeNodeIds: []string{"fresh"},
			})
			placeable := err == nil
			if placeable != tc.wantStalePlaceable {
				t.Fatalf("stale node placeable = %v, want %v (err %v)", placeable, tc.wantStalePlaceable, err)
			}
		})
	}
}

// Candidate sampling bounds how many nodes one placement inspects. Off, every
// node must be eligible again.
func TestOffSwitchCandidateSampling(t *testing.T) {
	const fleet = 200
	nodes := make([]Node, 0, fleet)
	for i := 0; i < fleet; i++ {
		id := nodeIDForIndex(i)
		nodes = append(nodes, Node{ID: id, Endpoint: "http://" + id})
	}

	for _, tc := range []struct {
		name        string
		sampleSize  int
		wantBounded bool
	}{
		{name: "on inspects a bounded sample", sampleSize: 8, wantBounded: true},
		{name: "off inspects the whole fleet", sampleSize: 0, wantBounded: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := offSwitchRegistry(t, nodes, time.Minute)
			service := NewService(nil, registry, NewStrategy("round_robin"), NewInMemoryBindingStore(time.Minute),
				WithCandidateSampleSize(tc.sampleSize), WithHealthGate(false))

			// Excluding all but one node: with sampling on, the sample will
			// usually miss the survivor and the placement fails; with it off,
			// the survivor is always found.
			exclude := make([]string, 0, fleet-1)
			for i := 1; i < fleet; i++ {
				exclude = append(exclude, nodeIDForIndex(i))
			}

			misses := 0
			for attempt := 0; attempt < 20; attempt++ {
				if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{
					ExcludeNodeIds: exclude,
				}); err != nil {
					misses++
				}
			}
			bounded := misses > 0
			if bounded != tc.wantBounded {
				t.Fatalf("sampling bounded = %v (%d/20 misses), want %v", bounded, misses, tc.wantBounded)
			}
		})
	}
}

// The roster digest lets a node elide an unchanged roster. Off — which is what
// a node that sends no digest looks like — the wire must stay authoritative.
func TestOffSwitchRosterDigest(t *testing.T) {
	for _, tc := range []struct {
		name       string
		digest     string
		wantCached bool
	}{
		{name: "on serves a later heartbeat from the cache", digest: RosterDigest([]string{"sandbox-1"}), wantCached: true},
		{name: "off keeps the wire authoritative", digest: "", wantCached: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := offSwitchRegistry(t, []Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Minute)
			store := NewInMemoryBindingStoreWithGrace(time.Minute, 0)
			service := NewService(nil, registry, NewStrategy("round_robin"), store)

			seed := readyHeartbeat("node-a")
			seed.SandboxIds = []string{"sandbox-1"}
			seed.RosterDigest = tc.digest
			seed.RosterFull = true
			if _, err := service.Heartbeat(context.Background(), seed); err != nil {
				t.Fatalf("seed heartbeat: %v", err)
			}

			// Now an elided roster. With digests on it resolves from the
			// cache and the binding survives; with them off the empty roster
			// is authoritative and the binding goes.
			elided := readyHeartbeat("node-a")
			elided.RosterDigest = tc.digest
			elided.RosterFull = false
			if _, err := service.Heartbeat(context.Background(), elided); err != nil {
				t.Fatalf("elided heartbeat: %v", err)
			}

			_, bound, err := store.Get("sandbox-1", time.Now())
			if err != nil {
				t.Fatalf("lookup: %v", err)
			}
			if bound != tc.wantCached {
				t.Fatalf("binding retained = %v, want %v", bound, tc.wantCached)
			}
		})
	}
}

func nodeIDForIndex(i int) string {
	const digits = "0123456789"
	return "node-" + string([]byte{digits[i/100%10], digits[i/10%10], digits[i%10]})
}
