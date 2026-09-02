package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	dto "github.com/prometheus/client_model/go"
)

func vfyLostTotal(t *testing.T, nodeID string) float64 {
	t.Helper()
	var m dto.Metric
	if err := schedulerSandboxEventsLostTotal.WithLabelValues(nodeID).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

// Verifier probe: a node process crashes (no unregister) and its replacement
// emits a few events, all of which arrive, before its first heartbeat. Nothing
// was lost, so the loss counter must not move.
func TestCrashRestartEventsBeforeFirstHeartbeatAreNotLoss(t *testing.T) {
	const nodeID = "node-vfy-crash"
	registry := NewAtomicNodeRegistry([]Node{{ID: nodeID, Endpoint: "http://" + nodeID}}, time.Minute)
	service := NewService(nil, registry, NewStrategy("round_robin"), NewInMemoryBindingStore(time.Minute))
	ctx := context.Background()
	older := "0199a000-0000-7000-8000-000000000001"
	newer := "0199b000-0000-7000-8000-000000000002"

	report := func(instance string, n int) {
		events := make([]*schedulerv1.SandboxEvent, 0, n)
		for i := 0; i < n; i++ {
			events = append(events, event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 1, 1))
		}
		if _, err := service.ReportSandboxEvent(ctx, &schedulerv1.ReportSandboxEventRequest{
			NodeId: nodeID, ServiceInstanceId: instance, Events: events,
		}); err != nil {
			t.Fatalf("report: %v", err)
		}
	}
	beat := func(instance string, emitted uint64) {
		b := readyHeartbeat(nodeID)
		b.ServiceInstanceId = instance
		b.EmittedEventCount = emitted
		if _, err := service.Heartbeat(ctx, b); err != nil {
			t.Fatalf("heartbeat: %v", err)
		}
	}

	before := vfyLostTotal(t, nodeID)
	report(older, 100)
	beat(older, 100)
	// Crash. The new process emits three events; all three arrive; then it heartbeats.
	report(newer, 3)
	beat(newer, 3)
	beat(newer, 3)
	beat(newer, 3)
	if got := vfyLostTotal(t, nodeID) - before; got != 0 {
		t.Fatalf("no event was lost, but %v were reported lost", got)
	}
}

func TestSuccessorOutEmittingPredecessorLeavesNoCreditBehind(t *testing.T) {
	const nodeID = "node-vfy-credit"
	registry := NewAtomicNodeRegistry([]Node{{ID: nodeID, Endpoint: "http://" + nodeID}}, time.Minute)
	service := NewService(nil, registry, NewStrategy("round_robin"), NewInMemoryBindingStore(time.Minute))
	ctx := context.Background()
	older := "0199a000-0000-7000-8000-000000000001"
	newer := "0199b000-0000-7000-8000-000000000002"
	report := func(instance string, n int) {
		events := make([]*schedulerv1.SandboxEvent, 0, n)
		for i := 0; i < n; i++ {
			events = append(events, event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 1, 1))
		}
		if _, err := service.ReportSandboxEvent(ctx, &schedulerv1.ReportSandboxEventRequest{NodeId: nodeID, ServiceInstanceId: instance, Events: events}); err != nil {
			t.Fatalf("report: %v", err)
		}
	}
	beat := func(instance string, emitted uint64) {
		b := readyHeartbeat(nodeID)
		b.ServiceInstanceId = instance
		b.EmittedEventCount = emitted
		if _, err := service.Heartbeat(ctx, b); err != nil {
			t.Fatalf("heartbeat: %v", err)
		}
	}
	before := vfyLostTotal(t, nodeID)
	report(older, 10)
	beat(older, 10)
	// Crash. Successor emits 150 (> predecessor's 10) before its first heartbeat, all arrive.
	report(newer, 150)
	beat(newer, 150)
	// Then it emits 5 more that never arrive; two heartbeats later that is loss.
	beat(newer, 155)
	beat(newer, 155)
	if got := vfyLostTotal(t, nodeID) - before; got != 5 {
		t.Fatalf("five events were lost, but %v were reported lost", got)
	}
}
