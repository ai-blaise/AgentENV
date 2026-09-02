package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The heartbeat benchmark's numbers mean nothing unless its knobs do what its
// report says: churn is the work a heartbeat has to do, staleness is what it
// can destroy. A harness that quietly listed the arrivals it claimed to omit
// would report a flattering lost_frac of zero at every staleness and nobody
// would know. These pin each knob at a fleet small enough to run under -race
// on every push, against both stores, so the benchmark is trusted for the
// same reason the code under it is.
func TestReconcileFleetChurnAndStalenessKnobs(t *testing.T) {
	const (
		nodeCount  = 4
		sandboxes  = 20
		churn      = 5
		grace      = 30 * time.Second
		fresh      = time.Second
		wellBeyond = grace + time.Minute
	)
	cases := []struct {
		name      string
		churn     int
		staleness time.Duration
		// Per heartbeat, what reconcile must do with the bindings the roster
		// omits, and how many of the omitted arrivals must still resolve.
		deleted, retained int
		lostFraction      float64
	}{
		// Nothing changes, so nothing is omitted and nothing is touched.
		{name: "steady", churn: 0, staleness: 0, deleted: 0, retained: 0},
		// The departures were established long before the grace; the arrivals
		// are listed. Only the departures go.
		{name: "churn", churn: churn, staleness: 0, deleted: churn, retained: 0},
		// The arrivals are omitted but recorded inside the grace: retained,
		// and every one of them still routes.
		{name: "churn+fresh-arrivals", churn: churn, staleness: fresh, deleted: churn, retained: churn, lostFraction: 0},
		// The arrivals are omitted and recorded past the grace: the roster is
		// believed and every one of them is lost. This is the D5 defect the
		// grace exists for, and the row the benchmark's lost_frac reports.
		{name: "churn+stale-arrivals", churn: churn, staleness: wellBeyond, deleted: 2 * churn, retained: 0, lostFraction: 1},
	}
	for _, factory := range benchBackendFactories {
		t.Run("store="+factory.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					fleet := newReconcileFleet(t, factory, nodeCount, sandboxes, grace)
					ctx := context.Background()
					before := fleet.reconcileOutcomes(t)
					sample := newArrivalSample(nodeCount * churn)
					var departed []string
					for i := range fleet.nodes {
						departed = append(departed, fleet.rosters[i].live[:tc.churn]...)
						req := fleet.nextHeartbeat(t, i, tc.churn, tc.staleness, sample)
						if _, err := fleet.service.Heartbeat(ctx, req); err != nil {
							t.Fatalf("Heartbeat %s: %v", req.GetNodeId(), err)
						}
					}
					after := fleet.reconcileOutcomes(t)

					perHeartbeat := func(outcome string) int {
						return int(after[outcome]-before[outcome]) / nodeCount
					}
					if got := perHeartbeat(reconcileOutcomeDeleted); got != tc.deleted {
						t.Errorf("deleted/heartbeat = %d, want %d", got, tc.deleted)
					}
					if got := perHeartbeat(reconcileOutcomeRetained); got != tc.retained {
						t.Errorf("retained/heartbeat = %d, want %d", got, tc.retained)
					}
					if tc.staleness > 0 {
						if got := sample.lostFraction(t, fleet.service); got != tc.lostFraction {
							t.Errorf("lost_frac = %v, want %v", got, tc.lostFraction)
						}
					}
					// Departures are gone whatever the knobs; survivors, listed
					// or retained, still route to the node that reported them.
					for _, id := range departed {
						if _, err := fleet.service.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: id}); status.Code(err) != codes.NotFound {
							t.Errorf("departed %s: LookupNode = %v, want NotFound", id, err)
						}
					}
					for i, node := range fleet.nodes {
						listed := fleet.rosters[i].live
						if tc.staleness > 0 && tc.lostFraction == 1 {
							listed = listed[:len(listed)-tc.churn]
						}
						for _, id := range listed {
							resp, err := fleet.service.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: id})
							if err != nil {
								t.Errorf("live %s: LookupNode: %v", id, err)
								continue
							}
							if got := resp.GetNode().GetNodeId(); got != node.ID {
								t.Errorf("live %s resolves to %q, want %q", id, got, node.ID)
							}
						}
					}
				})
			}
		})
	}
}

// The lookup benchmark checks only that each lookup resolves to some node; a
// seed that bound every sandbox to the wrong node would still post a fast
// number. This pins the seed against the placement it claims to make.
func TestLookupFleetSeedResolvesToTheSeededNode(t *testing.T) {
	const (
		nodeCount = 7
		bindings  = 2_500
	)
	for _, factory := range benchBackendFactories {
		t.Run("store="+factory.name, func(t *testing.T) {
			backend := factory.build(t, *fleetGrace)
			nodes, _ := benchNodes(nodeCount)
			service := NewService(zap.NewNop(), NewAtomicNodeRegistry(nodes, benchReportTTL),
				NewStrategy("round_robin"), backend.store, WithBindingTTL(benchBindingTTL))
			ids := seedLookupBindings(t, service, nodes, bindings)
			if len(ids) != bindings {
				t.Fatalf("seeded %d ids, want %d", len(ids), bindings)
			}
			ctx := context.Background()
			for i, id := range ids {
				resp, err := service.LookupNode(ctx, &schedulerv1.LookupNodeRequest{SandboxId: id})
				if err != nil {
					t.Fatalf("LookupNode %s: %v", id, err)
				}
				if got, want := resp.GetNode().GetNodeId(), nodes[i%nodeCount].ID; got != want {
					t.Fatalf("binding %d resolves to %q, want %q", i, got, want)
				}
			}
		})
	}
}
