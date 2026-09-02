package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

// invariantGrace is deliberately far larger than the shipped ten seconds. The
// test is about ordering, not about the grace's size — that is pinned by
// TestReconcileKeepsBindingRecordedAfterRoster — and a grace the test could
// outrun under -race on a loaded machine would turn a stall into a false
// failure. It still fails the moment the grace is not applied at all.
const invariantGrace = 10 * time.Minute

// The invariant the fleet simulator exists to check: a sandbox the gateway has
// recorded stays resolvable through LookupNode for at least one heartbeat
// period, whatever the node's heartbeats say in the meantime.
//
// Two defects the scheduler has shipped break it. A node collects its roster
// before it sends the heartbeat, so a sandbox placed in between is bound but
// unlisted; without the reconcile grace that heartbeat deletes it. A fresh
// node's first roster is empty and complete; without the same grace it wipes
// whatever the gateway has just placed there.
//
// The heartbeat period is a round, not a clock. In round r each node
// heartbeats the roster it held at the end of round r-1 — in full, then again
// by digest alone — while the gateway records round r's placements on it,
// concurrently. When both have finished, and only then, everything recorded so
// far must resolve. The interleaving inside a round is the race being
// exercised; what is asserted is decided after the round has closed, on
// channels, never on elapsed time. Every node runs its rounds at once so the
// registry lock and the per-node stripes are contended too.
//
// Two interleavings are run. "ordered" is channel-sequenced: the gateway's
// write completes before the heartbeat whose roster predates it is sent, so
// every round takes the exact path both defects need, and a store without
// the grace fails on round 0 every time rather than when the scheduler
// happens to land the write first. "concurrent" leaves the two in a race,
// which is what -race needs to see the shared state contended; it fails a
// graceless store too, but by scheduling luck rather than by construction.
func TestEveryRecordedSandboxStaysResolvableForOneHeartbeatPeriod(t *testing.T) {
	for _, factory := range benchBackendFactories {
		t.Run("store="+factory.name, func(t *testing.T) {
			t.Run("ordered", func(t *testing.T) {
				backend := factory.build(t, invariantGrace)
				runResolvabilityInvariant(t, backend.store, true)
			})
			t.Run("concurrent", func(t *testing.T) {
				backend := factory.build(t, invariantGrace)
				runResolvabilityInvariant(t, backend.store, false)
			})
		})
	}
}

func runResolvabilityInvariant(t *testing.T, store BindingStore, ordered bool) {
	const (
		nodeCount = 8
		rounds    = 24
		batch     = 16
	)
	nodes, incarnations := benchNodes(nodeCount)
	service := NewService(zap.NewNop(), NewAtomicNodeRegistry(nodes, benchReportTTL),
		NewStrategy("round_robin"), store,
		WithBindingTTL(benchBindingTTL), WithReportTTL(benchReportTTL))

	var wg sync.WaitGroup
	for i := range nodes {
		wg.Add(1)
		go func(sim *simulatedNode) {
			defer wg.Done()
			sim.run(t, rounds, batch, ordered)
		}(&simulatedNode{service: service, node: nodes[i], incarnation: incarnations[i]})
	}
	wg.Wait()
}

// simulatedNode is one node and the gateway traffic aimed at it.
type simulatedNode struct {
	service     *Service
	node        Node
	incarnation string
	// recorded is every sandbox the gateway has placed here, in order.
	recorded []string
	stamp    int64
}

func (s *simulatedNode) run(t *testing.T, rounds int, batch int, ordered bool) {
	ctx := context.Background()
	for round := 0; round < rounds; round++ {
		placed := make([]string, 0, batch)
		for j := 0; j < batch; j++ {
			placed = append(placed, fmt.Sprintf("%s-r%02d-%02d", s.node.ID, round, j))
		}
		// The roster the node collected before this round's placements.
		roster := append([]string(nil), s.recorded...)

		gateway := make(chan error, 1)
		if ordered {
			// The write is in the store before the roster that omits it is
			// applied: on round 0 that roster is empty and complete, which is
			// the fresh-node wipe; afterwards it is merely behind, which is
			// the placed-between-collect-and-send race.
			gateway <- s.record(ctx, round, placed)
		} else {
			go func() { gateway <- s.record(ctx, round, placed) }()
		}
		if err := s.heartbeat(ctx, roster); err != nil {
			t.Errorf("%s round %d: %v", s.node.ID, round, err)
			<-gateway
			return
		}
		if err := <-gateway; err != nil {
			t.Errorf("%s round %d: %v", s.node.ID, round, err)
			return
		}
		s.recorded = append(s.recorded, placed...)

		// One heartbeat period has now passed for round r-1's placements and
		// begun for round r's. Both must resolve; nothing older has been
		// touched since it was last checked and is re-checked at the end.
		recent := s.recorded[max(0, len(s.recorded)-2*batch):]
		if err := s.resolvable(ctx, recent); err != nil {
			t.Errorf("%s round %d: %v", s.node.ID, round, err)
			return
		}
	}
	if err := s.resolvable(ctx, s.recorded); err != nil {
		t.Errorf("%s after %d rounds: %v", s.node.ID, rounds, err)
	}
}

// record binds placed to this node the way the gateway does: the batch RPC on
// even rounds, as after a fork, and one RPC per sandbox on odd rounds, as
// after a create.
func (s *simulatedNode) record(ctx context.Context, round int, placed []string) error {
	if round%2 == 0 {
		req := &schedulerv1.RecordAssignmentsRequest{}
		for _, id := range placed {
			req.Assignments = append(req.Assignments, &schedulerv1.RecordAssignmentRequest{
				SandboxId: id,
				Node:      s.node.ToProto(),
			})
		}
		resp, err := s.service.RecordAssignments(ctx, req)
		if err != nil {
			return fmt.Errorf("RecordAssignments: %w", err)
		}
		for _, result := range resp.GetResults() {
			if result.GetError() != "" {
				return fmt.Errorf("RecordAssignments %s: %s", result.GetSandboxId(), result.GetError())
			}
		}
		return nil
	}
	for _, id := range placed {
		if _, err := s.service.RecordAssignment(ctx, &schedulerv1.RecordAssignmentRequest{
			SandboxId: id,
			Node:      s.node.ToProto(),
		}); err != nil {
			return fmt.Errorf("RecordAssignment %s: %w", id, err)
		}
	}
	return nil
}

// heartbeat reports roster in full, then once more by digest alone, as a node
// whose roster has not changed since does. Both are complete: the node has
// finished recovery and an empty roster from it means "nothing here".
func (s *simulatedNode) heartbeat(ctx context.Context, roster []string) error {
	digest := RosterDigest(roster)
	full := s.request(roster, digest)
	full.RosterFull = true
	full.RosterComplete = true
	if _, err := s.service.Heartbeat(ctx, full); err != nil {
		return fmt.Errorf("full heartbeat: %w", err)
	}

	elided := s.request(nil, digest)
	resp, err := s.service.Heartbeat(ctx, elided)
	if err != nil {
		return fmt.Errorf("elided heartbeat: %w", err)
	}
	if resp.GetRequestFullRoster() {
		return fmt.Errorf("elided heartbeat: scheduler asked for the roster it was just sent")
	}
	return nil
}

func (s *simulatedNode) request(roster []string, digest string) *schedulerv1.HeartbeatRequest {
	s.stamp++
	return &schedulerv1.HeartbeatRequest{
		NodeId:            s.node.ID,
		ClusterId:         benchClusterID,
		ServiceInstanceId: s.incarnation,
		SandboxIds:        roster,
		RosterDigest:      digest,
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:           schedulerv1.NodeStatus_NODE_STATUS_READY,
			SandboxCount:     uint32(len(roster)),
			ReportedAtUnixMs: s.stamp,
		},
	}
}

func (s *simulatedNode) resolvable(ctx context.Context, ids []string) error {
	for _, id := range ids {
		resp, err := s.service.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: id})
		if err != nil {
			return fmt.Errorf("recorded sandbox %s no longer resolves: %w", id, err)
		}
		if got := resp.GetNode().GetNodeId(); got != s.node.ID {
			return fmt.Errorf("recorded sandbox %s resolves to %q, want %q", id, got, s.node.ID)
		}
	}
	return nil
}
