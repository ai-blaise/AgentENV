package scheduler

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

// shippedStrategies is every name NewStrategy accepts, once each.
var shippedStrategies = []string{"round_robin", "random", "least_loaded_of_two"}

// maxLoadRatio is the bound the placement design set for the sampled strategy:
// after a burst, the busiest node carries at most this multiple of the mean.
// It is generous on purpose — with feedback the strategy lands near 1.02 —
// so that a machine's timing never decides the outcome, while a strategy that
// ignores load sits near 1.3 and cannot pass it.
// benchMaxReservationDelta is far above anything one run moves a node by,
// so the clamp never truncates the effect being measured.
const benchMaxReservationDelta = 1 << 20

const maxLoadRatio = 1.2

// scheduleFleet is a synthetic fleet arranged for placement: every node is
// observed and ready, and each placement is reported back as the create the
// node would emit. That report is what the reservation ledger folds onto the
// node's last snapshot, so a load-aware strategy sees its own decisions before
// the node's next heartbeat confirms them; without it every placement in a
// burst reads the same numbers and there is nothing to be aware of.
type scheduleFleet struct {
	service      *Service
	nodes        []Node
	incarnations map[string]string
}

// seedScheduleRegistry observes nodeCount nodes with initialLoad(i) sandboxes
// each. The registry is what costs at fleet scale, so it is built once and
// shared by every strategy measured against it.
func seedScheduleRegistry(tb testing.TB, nodeCount int, initialLoad func(i int) uint32) (*AtomicNodeRegistry, []Node, map[string]string) {
	tb.Helper()
	nodes, incarnations := benchNodes(nodeCount)
	registry := NewAtomicNodeRegistry(nodes, benchReportTTL)
	byNode := make(map[string]string, nodeCount)
	now := time.Now()
	for i, node := range nodes {
		load := initialLoad(i)
		byNode[node.ID] = incarnations[i]
		if _, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
			NodeId:            node.ID,
			ClusterId:         benchClusterID,
			ServiceInstanceId: incarnations[i],
			RosterComplete:    true,
			Snapshot: &schedulerv1.NodeSnapshot{
				Status:           schedulerv1.NodeStatus_NODE_STATUS_READY,
				SandboxCount:     load,
				CpuCount:         128,
				AllocatedCpu:     load,
				MemoryTotalBytes: 1 << 40,
				MemoryUsedBytes:  uint64(load) << 30,
			},
		}, now); err != nil {
			tb.Fatalf("seed heartbeat for %s: %v", node.ID, err)
		}
	}
	return registry, nodes, byNode
}

func newScheduleFleet(registry *AtomicNodeRegistry, nodes []Node, incarnations map[string]string, strategy string) *scheduleFleet {
	return &scheduleFleet{
		// Reservations on: the bound below is a claim about placement seeing
		// its own effects within a heartbeat interval, which is exactly what
		// the ledger provides. With the shipped default (off) every placement
		// reads the same heartbeat snapshot, so sampling two nodes from a
		// static view degenerates toward random and measures nothing about
		// the strategy.
		service: NewService(zap.NewNop(), registry, NewStrategy(strategy),
			NewInMemoryBindingStore(benchBindingTTL), WithBindingTTL(benchBindingTTL), WithReportTTL(benchReportTTL),
			WithReservations(true, benchMaxReservationDelta)),
		nodes:        nodes,
		incarnations: incarnations,
	}
}

// place schedules one sandbox and has the chosen node report the create, the
// way a node's event batch would. It returns the node and how long Schedule
// alone took.
func (f *scheduleFleet) place(ctx context.Context) (string, time.Duration, error) {
	start := time.Now()
	resp, err := f.service.Schedule(ctx, &schedulerv1.ScheduleRequest{})
	elapsed := time.Since(start)
	if err != nil {
		return "", elapsed, fmt.Errorf("Schedule: %w", err)
	}
	nodeID := resp.GetNode().GetNodeId()
	if _, err := f.service.ReportSandboxEvent(ctx, &schedulerv1.ReportSandboxEventRequest{
		NodeId:            nodeID,
		ClusterId:         benchClusterID,
		ServiceInstanceId: f.incarnations[nodeID],
		Events: []*schedulerv1.SandboxEvent{{
			SandboxId:            "sbx",
			EventType:            schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE,
			RequestedCpu:         1,
			RequestedMemoryBytes: 1 << 30,
		}},
	}); err != nil {
		return nodeID, elapsed, fmt.Errorf("ReportSandboxEvent %s: %w", nodeID, err)
	}
	return nodeID, elapsed, nil
}

// loadRatio is the busiest node's load over the fleet mean. Nodes that
// received nothing still count: they are capacity the strategy left idle.
func loadRatio(load map[string]uint32, nodeCount int) float64 {
	var total, peak uint32
	for _, l := range load {
		total += l
		if l > peak {
			peak = l
		}
	}
	if total == 0 {
		return 0
	}
	return float64(peak) / (float64(total) / float64(nodeCount))
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p*float64(len(sorted)-1) + 0.5)
	return sorted[idx]
}

// The placement design's acceptance: over a 100-node fleet and 10k placements
// with the ledger fed, the sampled strategy's max/mean load stays under the
// bound, and under round-robin's.
//
// The fleet starts skewed — node i holds i sandboxes — rather than empty. From
// an even start a load-blind strategy scatters uniformly and lands near 1.25,
// which the bound catches most of the time but not always; from a skew it has
// to *correct*, a blind strategy preserves the skew and lands near 1.3 every
// time, while the sampled one fills from the bottom and flattens it. That is
// also the fleet after any uneven burst, which is the case the strategy exists
// for.
func TestScheduleFleetLeastLoadedOfTwoBoundsTheLoadRatio(t *testing.T) {
	const (
		nodeCount  = 100
		placements = 10_000
	)
	ctx := context.Background()
	skewed := func(i int) uint32 { return uint32(i) }

	ratios := make(map[string]float64, len(shippedStrategies))
	for _, strategy := range shippedStrategies {
		registry, nodes, incarnations := seedScheduleRegistry(t, nodeCount, skewed)
		fleet := newScheduleFleet(registry, nodes, incarnations, strategy)

		load := make(map[string]uint32, nodeCount)
		for i, node := range nodes {
			load[node.ID] = skewed(i)
		}
		latencies := make([]time.Duration, 0, placements)
		for i := 0; i < placements; i++ {
			nodeID, elapsed, err := fleet.place(ctx)
			if err != nil {
				t.Fatalf("%s placement %d: %v", strategy, i, err)
			}
			load[nodeID]++
			latencies = append(latencies, elapsed)
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		ratios[strategy] = loadRatio(load, nodeCount)
		t.Logf("%-20s max/mean=%.3f  Schedule p50=%s p99=%s", strategy, ratios[strategy],
			percentile(latencies, 0.50), percentile(latencies, 0.99))
	}

	sampled := ratios["least_loaded_of_two"]
	if sampled > maxLoadRatio {
		t.Fatalf("least_loaded_of_two max/mean load %.3f exceeds the design bound %.2f", sampled, maxLoadRatio)
	}
	if roundRobin := ratios["round_robin"]; sampled >= roundRobin {
		t.Fatalf("least_loaded_of_two max/mean load %.3f is no better than round_robin's %.3f", sampled, roundRobin)
	}
}

// Placement cost per shipped strategy with the ledger fed, at the design's
// 100-node point and at the fleet size the other benchmarks use. The reported
// max/mean is the load ratio the run produced from an even start; the test
// above is what asserts it.
func BenchmarkScheduleFleet(b *testing.B) {
	for _, nodeCount := range []int{100, 10_000} {
		b.Run(fmt.Sprintf("nodes=%d", nodeCount), func(b *testing.B) {
			registry, nodes, incarnations := seedScheduleRegistry(b, nodeCount, func(int) uint32 { return 0 })
			ctx := context.Background()
			for _, strategy := range shippedStrategies {
				b.Run("strategy="+strategy, func(b *testing.B) {
					fleet := newScheduleFleet(registry, nodes, incarnations, strategy)
					load := make(map[string]uint32, nodeCount)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						nodeID, _, err := fleet.place(ctx)
						if err != nil {
							b.Fatalf("placement %d: %v", i, err)
						}
						load[nodeID]++
					}
					b.StopTimer()
					b.ReportMetric(loadRatio(load, nodeCount), "max/mean")
				})
			}
		})
	}
}
