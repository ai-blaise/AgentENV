package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

// A synthetic fleet, so scheduler behaviour at 1k-100k nodes can be measured
// without that many hosts.
//
// Node count, sandboxes per node and heartbeat freshness are set independently,
// because the interesting terms scale differently: placement cost is O(nodes),
// while reconcile cost is O(sandboxes on the reporting node).
type simulatedFleet struct {
	service  *Service
	registry *AtomicNodeRegistry
	nodeIDs  []string
}

func newSimulatedFleet(tb testing.TB, nodeCount int, sandboxesPerNode int, strategy string) *simulatedFleet {
	tb.Helper()

	nodes := make([]Node, 0, nodeCount)
	nodeIDs := make([]string, 0, nodeCount)
	for i := 0; i < nodeCount; i++ {
		id := fmt.Sprintf("node-%06d", i)
		nodeIDs = append(nodeIDs, id)
		nodes = append(nodes, Node{ID: id, Endpoint: "http://" + id})
	}

	registry := NewAtomicNodeRegistry(nodes, defaultObservedReportTTL)
	service := NewService(
		zap.NewNop(),
		registry,
		NewStrategy(strategy),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)

	now := time.Now()
	for i, id := range nodeIDs {
		sandboxIDs := make([]string, 0, sandboxesPerNode)
		for j := 0; j < sandboxesPerNode; j++ {
			sandboxIDs = append(sandboxIDs, fmt.Sprintf("%s-sbx-%05d", id, j))
		}
		if _, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
			NodeId:            id,
			ClusterId:         "cluster-sim",
			ServiceInstanceId: fmt.Sprintf("0199a000-0000-7000-8000-%012d", i),
			SandboxIds:        sandboxIDs,
			RosterComplete:    true,
			Snapshot: &schedulerv1.NodeSnapshot{
				Status:           schedulerv1.NodeStatus_NODE_STATUS_READY,
				SandboxCount:     uint32(sandboxesPerNode),
				CpuCount:         64,
				AllocatedCpu:     uint32(sandboxesPerNode),
				MemoryTotalBytes: 1 << 40,
				MemoryUsedBytes:  uint64(sandboxesPerNode) << 20,
			},
		}, now); err != nil {
			tb.Fatalf("seed heartbeat for %s: %v", id, err)
		}
	}

	return &simulatedFleet{service: service, registry: registry, nodeIDs: nodeIDs}
}

// Placement must not get more expensive as the fleet grows. Schedule copies and
// sorts the discovered node list and clones a snapshot per node on every
// request, so this is the measurement that shows whether that has been fixed.
func BenchmarkSchedulePlacement(b *testing.B) {
	for _, nodeCount := range []int{100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("nodes=%d", nodeCount), func(b *testing.B) {
			fleet := newSimulatedFleet(b, nodeCount, 10, "round_robin")
			ctx := context.Background()
			req := &schedulerv1.ScheduleRequest{}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := fleet.service.Schedule(ctx, req); err != nil {
					b.Fatalf("Schedule: %v", err)
				}
			}
		})
	}
}

// Reconcile cost scales with the reporting node's roster, not the fleet, and it
// runs on every heartbeat from every node.
func BenchmarkHeartbeatReconcile(b *testing.B) {
	for _, sandboxes := range []int{10, 100, 1_000} {
		b.Run(fmt.Sprintf("sandboxes=%d", sandboxes), func(b *testing.B) {
			fleet := newSimulatedFleet(b, 16, sandboxes, "round_robin")
			ctx := context.Background()

			sandboxIDs := make([]string, 0, sandboxes)
			for j := 0; j < sandboxes; j++ {
				sandboxIDs = append(sandboxIDs, fmt.Sprintf("%s-sbx-%05d", fleet.nodeIDs[0], j))
			}
			req := &schedulerv1.HeartbeatRequest{
				NodeId:            fleet.nodeIDs[0],
				ClusterId:         "cluster-sim",
				ServiceInstanceId: "0199a000-0000-7000-8000-000000000000",
				SandboxIds:        sandboxIDs,
				RosterComplete:    true,
				Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := fleet.service.Heartbeat(ctx, req); err != nil {
					b.Fatalf("Heartbeat: %v", err)
				}
			}
		})
	}
}

// A dead node keeps its discovery entry, so without the health gate it stays a
// placement candidate indefinitely. This is the fleet-scale version of the
// SIGSTOP-a-node assertion.
func TestSimulatedFleetExcludesStaleNodes(t *testing.T) {
	const nodeCount = 50
	fleet := newSimulatedFleet(t, nodeCount, 1, "round_robin")

	// Refresh all but one node well after the seed, so at evaluation time
	// exactly one node is stale. Ageing every node instead would trip the
	// deliberate fail-open path and prove nothing.
	fresh := time.Now().Add(2 * defaultObservedReportTTL)
	for _, id := range fleet.nodeIDs[1:] {
		if _, _, err := fleet.registry.Heartbeat(&schedulerv1.HeartbeatRequest{
			NodeId:            id,
			ClusterId:         "cluster-sim",
			ServiceInstanceId: "0199b000-0000-7000-8000-000000000001",
			RosterComplete:    true,
			Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		}, fresh); err != nil {
			t.Fatalf("refresh heartbeat: %v", err)
		}
	}

	rich := make([]RichNode, 0, nodeCount)
	for _, id := range fleet.nodeIDs {
		snapshot, health := fleet.registry.PeekObservedHealth(id)
		rich = append(rich, RichNode{
			Node:     Node{ID: id, Endpoint: "http://" + id},
			Snapshot: snapshot,
			Health:   health,
		})
	}

	eligible, dropped := FilterByHealth(rich, defaultObservedReportTTL, fresh)

	if dropped[HealthFilterReasonStale] == 0 {
		t.Fatal("the node that stopped heartbeating should have been dropped as stale")
	}
	for _, n := range eligible {
		if n.ID == fleet.nodeIDs[0] {
			t.Fatal("a stale node remained a placement candidate")
		}
	}
}
