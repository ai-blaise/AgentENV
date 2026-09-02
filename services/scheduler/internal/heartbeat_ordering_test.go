package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The scenario the ordering fence exists for. A fresh node's first heartbeat
// carries an empty, complete roster and times out at the node while it is
// still queued here. The gateway places a sandbox on the node and its next
// heartbeat lists it. Then the abandoned heartbeat is applied: its empty roster
// would delete the binding once past the grace, and its digest would be cached
// so the next elided round misses too. The grace is disabled so the deletion
// would be immediate, which makes the test about the ordering and not the
// timing.
func TestALateHeartbeatFromTheSameIncarnationCannotReconcileAStaleRoster(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Minute)
	store := NewInMemoryBindingStoreWithGrace(time.Minute, 0)
	service := NewService(nil, registry, NewStrategy("round_robin"), store)

	stamped := func(roster []string, reportedAt int64, full bool) *schedulerv1.HeartbeatRequest {
		beat := rosterHeartbeat("node-a", RosterDigest(roster), roster, full)
		if !full {
			beat.SandboxIds = nil
		}
		beat.Snapshot.ReportedAtUnixMs = reportedAt
		return beat
	}

	abandoned := stamped(nil, 1000, true)
	if _, err := service.Heartbeat(context.Background(), abandoned); err != nil {
		t.Fatalf("first heartbeat: %v", err)
	}
	if _, err := service.RecordAssignment(context.Background(), &schedulerv1.RecordAssignmentRequest{
		SandboxId: "sbx-1",
		Node:      (&Node{ID: "node-a", Endpoint: "http://node-a"}).ToProto(),
	}); err != nil {
		t.Fatalf("record assignment: %v", err)
	}
	if _, err := service.Heartbeat(context.Background(), stamped([]string{"sbx-1"}, 2000, true)); err != nil {
		t.Fatalf("second heartbeat: %v", err)
	}

	// The same request, delivered late.
	if _, err := service.Heartbeat(context.Background(), abandoned); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a late heartbeat from the same incarnation was applied: err = %v", err)
	}
	resp, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"})
	if err != nil {
		t.Fatalf("the stale roster deleted a binding the node still holds: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != "node-a" {
		t.Fatalf("sbx-1 bound to %q, want node-a", got)
	}

	// The roster cache still holds the node's current roster, so the next
	// elided heartbeat resolves rather than costing a full resend.
	elided, err := service.Heartbeat(context.Background(), stamped([]string{"sbx-1"}, 3000, false))
	if err != nil {
		t.Fatalf("elided heartbeat: %v", err)
	}
	if elided.GetRequestFullRoster() {
		t.Fatal("the late heartbeat poisoned the roster cache")
	}
}

// A heartbeat whose caller has already given up is dropped before it touches
// anything. The ordering fence is what protects against the late-delivered
// case in general; this only saves applying one that is known to be abandoned.
func TestAnAbandonedHeartbeatIsNotApplied(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Minute)
	service := NewService(nil, registry, NewStrategy("round_robin"), NewInMemoryBindingStoreWithGrace(time.Minute, 0))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	beat := readyHeartbeat("node-a")
	beat.SandboxIds = []string{"sbx-1"}
	if _, err := service.Heartbeat(ctx, beat); status.Code(err) != codes.Canceled {
		t.Fatalf("err = %v, want Canceled", err)
	}
	if _, seen := registry.ObservedIncarnation("node-a"); seen {
		t.Fatal("an abandoned heartbeat registered the node")
	}
	if _, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: "sbx-1"}); status.Code(err) != codes.NotFound {
		t.Fatalf("an abandoned heartbeat wrote a binding: %v", err)
	}
}

// After a node unregisters, a heartbeat from its departed incarnation — the
// process that was replaced, or the departing one's own still in flight — used
// to re-register the node and re-upsert every binding it had just had wiped,
// pulling a sandbox that had since moved back onto a node that is gone.
func TestALateHeartbeatAfterUnregisterCannotResurrectTheNode(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-1", Endpoint: "http://node-1"},
		{ID: "node-2", Endpoint: "http://node-2"},
	}, time.Minute)
	store := NewInMemoryBindingStoreWithGrace(time.Minute, 0)
	service := NewService(nil, registry, NewStrategy("round_robin"), store)
	older := "0199a000-0000-7000-8000-000000000001"
	newer := "0199b000-0000-7000-8000-000000000002"
	newest := "0199c000-0000-7000-8000-000000000003"

	beat := func(instance string) *schedulerv1.HeartbeatRequest {
		return &schedulerv1.HeartbeatRequest{
			NodeId:            "node-1",
			ClusterId:         "cluster",
			ServiceInstanceId: instance,
			SandboxIds:        []string{"sbx-moved", "sbx-gone"},
			RosterComplete:    true,
			Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		}
	}
	lookup := func(sandboxID string) (string, codes.Code) {
		resp, err := service.LookupNode(context.Background(), &schedulerv1.LookupNodeRequest{SandboxId: sandboxID})
		return resp.GetNode().GetNodeId(), status.Code(err)
	}

	if _, err := service.Heartbeat(context.Background(), beat(newer)); err != nil {
		t.Fatalf("live heartbeat: %v", err)
	}
	if _, err := service.UnregisterNode(context.Background(), &schedulerv1.UnregisterNodeRequest{
		NodeId:            "node-1",
		ServiceInstanceId: newer,
	}); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	if _, err := service.RecordAssignment(context.Background(), &schedulerv1.RecordAssignmentRequest{
		SandboxId: "sbx-moved",
		Node:      (&Node{ID: "node-2", Endpoint: "http://node-2"}).ToProto(),
	}); err != nil {
		t.Fatalf("move sbx-moved: %v", err)
	}

	for name, instance := range map[string]string{"superseded": older, "departed": newer} {
		if _, err := service.Heartbeat(context.Background(), beat(instance)); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("a heartbeat from the %s incarnation was accepted after unregister: err = %v", name, err)
		}
	}
	if node, code := lookup("sbx-moved"); code != codes.OK || node != "node-2" {
		t.Fatalf("sbx-moved resolved to (%q, %v), want node-2", node, code)
	}
	if _, code := lookup("sbx-gone"); code != codes.NotFound {
		t.Fatalf("sbx-gone was resurrected: %v", code)
	}
	if _, err := service.GetNode(context.Background(), &schedulerv1.GetNodeRequest{NodeId: "node-1"}); status.Code(err) != codes.NotFound {
		t.Fatalf("a departed node is observed again: %v", err)
	}

	// The node coming back is a new process, and it is welcome.
	if _, err := service.Heartbeat(context.Background(), beat(newest)); err != nil {
		t.Fatalf("a restarted node was locked out: %v", err)
	}
}

// Every RPC that rejects an unknown node returns one exact string, because the
// node's reporter, src/observability/reporter.rs, substring-matches it on a
// rejected heartbeat to log the AENV_NODE_ID remediation at error level. The
// literal is spelled out here rather than read from the constant so that a
// reword on this side fails a test on this side, the way the roster digest is
// pinned to fixed vectors on both sides.
func TestUnknownNodeRejectionCarriesTheWireMessage(t *testing.T) {
	const wire = "node is not in scheduler node list"
	if NodeNotInRegistryMessage != wire {
		t.Fatalf("NodeNotInRegistryMessage = %q; reporter.rs matches %q", NodeNotInRegistryMessage, wire)
	}

	service := NewService(nil,
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Minute),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(time.Minute),
	)
	ctx := context.Background()
	unknown := (&Node{ID: "node-unknown", Endpoint: "http://node-unknown"}).ToProto()

	for name, call := range map[string]func() error{
		"Heartbeat": func() error {
			_, err := service.Heartbeat(ctx, readyHeartbeat("node-unknown"))
			return err
		},
		"RecordAssignment": func() error {
			_, err := service.RecordAssignment(ctx, &schedulerv1.RecordAssignmentRequest{SandboxId: "sbx-1", Node: unknown})
			return err
		},
		"ReportSandboxEvent": func() error {
			_, err := service.ReportSandboxEvent(ctx, &schedulerv1.ReportSandboxEventRequest{
				NodeId: "node-unknown",
				Events: []*schedulerv1.SandboxEvent{event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 1, 1)},
			})
			return err
		},
		"RecordP2PArtifact": func() error {
			_, err := service.RecordP2PArtifact(ctx, &schedulerv1.RecordP2PArtifactRequest{
				ClusterId: "cluster", Backend: "backend", Key: "key", NodeId: "node-unknown",
			})
			return err
		},
	} {
		err := call()
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%s: code = %v, want InvalidArgument", name, status.Code(err))
		}
		if got := status.Convert(err).Message(); got != wire {
			t.Fatalf("%s: message = %q, want %q", name, got, wire)
		}
	}

	// RecordAssignments reports per assignment rather than failing the call,
	// and the same text travels in the result.
	resp, err := service.RecordAssignments(ctx, &schedulerv1.RecordAssignmentsRequest{
		Assignments: []*schedulerv1.RecordAssignmentRequest{{SandboxId: "sbx-1", Node: unknown}},
	})
	if err != nil {
		t.Fatalf("RecordAssignments: %v", err)
	}
	if got := resp.GetResults()[0].GetError(); got != status.Error(codes.InvalidArgument, wire).Error() {
		t.Fatalf("RecordAssignments result error = %q, want the wire message", got)
	}
}

// Placement reads the last heartbeat plus what the node has reported since.
// This pins both halves of that seam through the service: the events must be
// folded in (ReportSandboxEvent -> ledger.Apply) and the fold must be read at
// placement (Schedule -> ledger.ApplyTo), and the next heartbeat must replace
// it rather than add to it. Either call was deletable with the suite green.
func TestReportedEventsCountAgainstPlacementUntilTheNextHeartbeat(t *testing.T) {
	five := uint32(5)
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Minute)
	service := NewService(nil, registry, NewStrategy("round_robin"), NewInMemoryBindingStore(time.Minute),
		WithNodeResourceLimit(&config.NodeResourceLimit{MaxSandboxCount: &five}))
	ctx := context.Background()

	heartbeat := func(count uint32) {
		beat := readyHeartbeat("node-a")
		beat.Snapshot.SandboxCount = count
		if _, err := service.Heartbeat(ctx, beat); err != nil {
			t.Fatalf("heartbeat: %v", err)
		}
	}
	report := func(kind schedulerv1.SandboxEventType, n int) {
		events := make([]*schedulerv1.SandboxEvent, 0, n)
		for i := 0; i < n; i++ {
			events = append(events, event(kind, 1, 1))
		}
		if _, err := service.ReportSandboxEvent(ctx, &schedulerv1.ReportSandboxEventRequest{
			NodeId:            "node-a",
			ServiceInstanceId: "node-a-instance",
			Events:            events,
		}); err != nil {
			t.Fatalf("report events: %v", err)
		}
	}
	placeable := func() bool {
		_, err := service.Schedule(ctx, &schedulerv1.ScheduleRequest{})
		switch status.Code(err) {
		case codes.OK:
			return true
		case codes.Unavailable:
			return false
		default:
			t.Fatalf("schedule: %v", err)
			return false
		}
	}

	heartbeat(3)
	if !placeable() {
		t.Fatal("three of five must be placeable")
	}
	report(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 3)
	if placeable() {
		t.Fatal("three creates on top of three reported must exceed the limit of five")
	}
	report(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_PAUSE, 2)
	if !placeable() {
		t.Fatal("two pauses must release two of the six")
	}
	report(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 2)
	if placeable() {
		t.Fatal("two more creates must exceed the limit again")
	}

	// The heartbeat is the truth and replaces the delta; it must not be
	// added to what the events already said.
	heartbeat(4)
	if !placeable() {
		t.Fatal("a fresh heartbeat at four of five must be placeable again")
	}
}

// A batch from a process that has since been replaced describes sandboxes the
// live process's heartbeat already accounts for. Folding it in would over-count
// the node until the next heartbeat reset the ledger.
func TestReportSandboxEventRejectsASupersededIncarnation(t *testing.T) {
	five := uint32(5)
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Minute)
	service := NewService(nil, registry, NewStrategy("round_robin"), NewInMemoryBindingStore(time.Minute),
		WithNodeResourceLimit(&config.NodeResourceLimit{MaxSandboxCount: &five}))
	ctx := context.Background()
	older := "0199a000-0000-7000-8000-000000000001"
	newer := "0199b000-0000-7000-8000-000000000002"
	newest := "0199c000-0000-7000-8000-000000000003"

	beat := readyHeartbeat("node-a")
	beat.ServiceInstanceId = newer
	beat.Snapshot.SandboxCount = 3
	if _, err := service.Heartbeat(ctx, beat); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	report := func(instance string) error {
		_, err := service.ReportSandboxEvent(ctx, &schedulerv1.ReportSandboxEventRequest{
			NodeId:            "node-a",
			ServiceInstanceId: instance,
			Events: []*schedulerv1.SandboxEvent{
				event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 1, 1),
				event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 1, 1),
				event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 1, 1),
			},
		})
		return err
	}

	if err := report(older); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("a superseded incarnation's batch was accepted: err = %v", err)
	}
	if _, err := service.Schedule(ctx, &schedulerv1.ScheduleRequest{}); err != nil {
		t.Fatalf("a rejected batch moved the placement view: %v", err)
	}

	// The live incarnation's batch counts, and so does one from a process
	// that has restarted but not yet heartbeated: nothing on record outranks
	// it.
	if err := report(newer); err != nil {
		t.Fatalf("the live incarnation's batch was refused: %v", err)
	}
	if _, err := service.Schedule(ctx, &schedulerv1.ScheduleRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("three creates over three must exceed five: %v", err)
	}
	if err := report(newest); err != nil {
		t.Fatalf("a restarted process's batch, ahead of its first heartbeat, was refused: %v", err)
	}
}

// An unknown node's batch is refused rather than answered with success. The
// batch is lost either way, but a success is indistinguishable from delivery
// and leaves nothing for an operator to find.
func TestReportSandboxEventRejectsAnUnknownNode(t *testing.T) {
	service := NewService(nil,
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Minute),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(time.Minute),
	)
	_, err := service.ReportSandboxEvent(context.Background(), &schedulerv1.ReportSandboxEventRequest{
		NodeId: "node-unknown",
		Events: []*schedulerv1.SandboxEvent{event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 1, 1)},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
	if len(service.ledger.byNodeID) != 0 {
		t.Fatal("an unknown node's batch reached the ledger")
	}
	if len(service.eventLoss.nodes) != 0 {
		t.Fatal("an unknown node's batch was counted as received")
	}
}
