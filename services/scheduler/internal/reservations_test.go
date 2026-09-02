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

func reservationService(t *testing.T, enabled bool, maxSandboxes uint32, nodes ...Node) *Service {
	t.Helper()
	if len(nodes) == 0 {
		nodes = []Node{{ID: "node-a", Endpoint: "http://node-a"}}
	}
	registry := NewAtomicNodeRegistry(nodes, time.Minute)
	return NewService(nil, registry, NewStrategy("round_robin"), NewInMemoryBindingStore(time.Minute),
		WithNodeResourceLimit(&config.NodeResourceLimit{MaxSandboxCount: &maxSandboxes}),
		WithReservations(enabled, 0),
	)
}

func heartbeatWithCount(t *testing.T, service *Service, nodeID string, count uint32) {
	t.Helper()
	beat := readyHeartbeat(nodeID)
	beat.Snapshot.SandboxCount = count
	if _, err := service.Heartbeat(context.Background(), beat); err != nil {
		t.Fatalf("heartbeat %s: %v", nodeID, err)
	}
}

// The design's own acceptance test, in the filter's terms. One node with a
// ceiling of one sandbox and no heartbeat between placements: the limit
// excludes a node only once it *exceeds* the ceiling, so the first two
// placements go through (0, then 1 reserved) and the third must see the two
// before it. Without the ledger every placement reads the heartbeat's zero and
// all three succeed — the switch's off position, asserted alongside so a dead
// flag is caught.
func TestOffSwitchReservations(t *testing.T) {
	for _, tc := range []struct {
		enabled         bool
		wantThirdPlaced bool
	}{
		{enabled: true, wantThirdPlaced: false},
		{enabled: false, wantThirdPlaced: true},
	} {
		name := "off reads the heartbeat verbatim"
		if tc.enabled {
			name = "on makes each placement see the ones before it"
		}
		t.Run(name, func(t *testing.T) {
			service := reservationService(t, tc.enabled, 1)
			heartbeatWithCount(t, service, "node-a", 0)
			ctx := context.Background()

			for i := 0; i < 2; i++ {
				if _, err := service.Schedule(ctx, &schedulerv1.ScheduleRequest{}); err != nil {
					t.Fatalf("placement %d onto a node under its ceiling: %v", i, err)
				}
			}
			_, err := service.Schedule(ctx, &schedulerv1.ScheduleRequest{})
			placed := err == nil
			if placed != tc.wantThirdPlaced {
				t.Fatalf("third placement succeeded = %v, want %v (err %v)", placed, tc.wantThirdPlaced, err)
			}
			if !placed && status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("a node full of reservations is exhausted capacity, got %v", err)
			}
		})
	}
}

// The reservation is settled by the create event it stood in for, so once
// the node reports the sandbox it is counted once, not twice; and the next
// heartbeat, which includes it, trims whatever is left.
func TestReservationIsSettledByTheCreateEventAndTrimmedByTheHeartbeat(t *testing.T) {
	service := reservationService(t, true, 2)
	heartbeatWithCount(t, service, "node-a", 0)
	ctx := context.Background()

	place := func() error {
		_, err := service.Schedule(ctx, &schedulerv1.ScheduleRequest{})
		return err
	}
	// The ceiling is exceeded, not reached, at three: 0 -> 1 -> 2 -> 3.
	for i := 0; i < 3; i++ {
		if err := place(); err != nil {
			t.Fatalf("placement %d: %v", i, err)
		}
	}
	if err := place(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("fourth placement must be refused: three reservations put the node over two, got %v", err)
	}

	// The node reports one of the three creates. That replaces one reservation
	// rather than adding to it: still three, still over.
	if _, err := service.ReportSandboxEvent(ctx, &schedulerv1.ReportSandboxEventRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "node-a-instance",
		Events:            []*schedulerv1.SandboxEvent{event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 1, 1)},
	}); err != nil {
		t.Fatalf("report create: %v", err)
	}
	if err := place(); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("a reported create must settle a reservation, not stack on it: %v", err)
	}
	// Placeability alone cannot tell three from four; the count can. Two
	// reservations remain and one reported create: three, not four.
	if got := service.ledger.ApplyTo("node-a", baseSnapshot(0, 0, 0), time.Now()).GetSandboxCount(); got != 3 {
		t.Fatalf("ledger count after one reported create = %d, want 3 (the create replaced its reservation)", got)
	}

	// The heartbeat is the truth: it says one sandbox, and the ledger's view
	// of everything before it goes.
	heartbeatWithCount(t, service, "node-a", 1)
	if err := place(); err != nil {
		t.Fatalf("one of two after the heartbeat must be placeable: %v", err)
	}
}

// A cold create names its resources; the reservation carries them so the
// allocated-CPU and memory terms a load-aware strategy scores move too.
func TestReservationCarriesTheHintedResources(t *testing.T) {
	ledger := NewReservationLedger(time.Minute)
	now := time.Unix(9000, 0)

	ledger.Reserve("node-a", 4, 2<<30, now)
	got := ledger.ApplyTo("node-a", baseSnapshot(10, 8, 1<<30), now)

	if got.GetSandboxCount() != 11 || got.GetSandboxStartingCount() != 1 {
		t.Fatalf("count = %d starting = %d, want 11 and 1", got.GetSandboxCount(), got.GetSandboxStartingCount())
	}
	if got.GetAllocatedCpu() != 12 || got.GetAllocatedMemoryBytes() != 3<<30 {
		t.Fatalf("cpu = %d memory = %d, want 12 and 3 GiB", got.GetAllocatedCpu(), got.GetAllocatedMemoryBytes())
	}

	cpu, memory := hintedResources(&schedulerv1.ScheduleRequestHint{
		Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{NewColdSandbox: &schedulerv1.NewColdSandboxHint{CpuCount: 2, MemoryMb: 512}},
	})
	if cpu != 2 || memory != 512<<20 {
		t.Fatalf("hinted = %d cpu, %d bytes; want 2 and 512 MiB", cpu, memory)
	}
	if cpu, memory := hintedResources(nil); cpu != 0 || memory != 0 {
		t.Fatalf("a warm create hints nothing, got %d, %d", cpu, memory)
	}
}

// Trimming on the heartbeat's own arrival stamp keeps what arrived after it.
// Clearing everything on each heartbeat — what this replaced — lost an event
// that landed while the heartbeat was being applied, and the burst it
// described read as free capacity until the next one.
func TestTrimBeforeKeepsEntriesThatArrivedAfterTheHeartbeat(t *testing.T) {
	ledger := NewReservationLedger(time.Minute)
	base := time.Unix(10_000, 0)

	ledger.Apply("node-a", []*schedulerv1.SandboxEvent{event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 1, 1)}, base)
	ledger.Reserve("node-a", 0, 0, base.Add(time.Millisecond))
	heartbeatAt := base.Add(2 * time.Millisecond)
	// Resumes rather than creates, so the late batch does not settle the
	// reservation and the trim is the only thing that can remove it.
	ledger.Apply("node-a", []*schedulerv1.SandboxEvent{
		event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_RESUME, 1, 1),
		event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_RESUME, 1, 1),
	}, heartbeatAt.Add(time.Millisecond))

	drift := ledger.TrimBefore("node-a", heartbeatAt)

	// The reservation and the first create — both before the heartbeat — are
	// what it overtook. The late batch is not.
	if drift != 2 {
		t.Fatalf("drift = %d, want the 2 sandboxes the heartbeat overtook", drift)
	}
	got := ledger.ApplyTo("node-a", baseSnapshot(5, 0, 0), heartbeatAt.Add(2*time.Millisecond))
	if got.GetSandboxCount() != 7 {
		t.Fatalf("count = %d, want 5 plus the 2 late resumes", got.GetSandboxCount())
	}
	// Exactly the stamp counts as overtaken.
	ledger.TrimBefore("node-a", heartbeatAt.Add(time.Millisecond))
	if got := ledger.ApplyTo("node-a", baseSnapshot(5, 0, 0), heartbeatAt); got.GetSandboxCount() != 5 {
		t.Fatalf("count = %d, want the late batch trimmed by a heartbeat stamped at its instant", got.GetSandboxCount())
	}
}

// Events are lossy and the ledger is a hint; the clamp is what stops a node
// that has gone quiet from carrying unbounded phantom load. An event that
// would cross it is dropped whole, in either direction.
func TestLedgerClampsThePerNodeDelta(t *testing.T) {
	ledger := newReservationLedger(time.Minute, 3)
	now := time.Unix(11_000, 0)
	create := event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 1, 1024)

	ledger.Apply("node-a", []*schedulerv1.SandboxEvent{create, create, create, create, create}, now)
	got := ledger.ApplyTo("node-a", baseSnapshot(0, 0, 0), now)
	if got.GetSandboxCount() != 3 || got.GetAllocatedCpu() != 3 || got.GetAllocatedMemoryBytes() != 3*1024 {
		t.Fatalf("clamped delta = %d/%d/%d, want 3 sandboxes with 3 cpu and 3 KiB", got.GetSandboxCount(), got.GetAllocatedCpu(), got.GetAllocatedMemoryBytes())
	}

	// Reservations count against the same clamp.
	ledger.Reserve("node-a", 1, 1, now)
	if got := ledger.ApplyTo("node-a", baseSnapshot(0, 0, 0), now); got.GetSandboxCount() != 3 {
		t.Fatalf("a reservation past the clamp was kept: %d", got.GetSandboxCount())
	}

	deletes := make([]*schedulerv1.SandboxEvent, 0, 8)
	for i := 0; i < 8; i++ {
		deletes = append(deletes, event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_DELETE, 1, 1024))
	}
	ledger.Apply("node-b", deletes, now)
	if got := ledger.ApplyTo("node-b", baseSnapshot(10, 10, 10*1024), now); got.GetSandboxCount() != 7 {
		t.Fatalf("negative delta = %d from 10, want clamped to -3", 10-int(got.GetSandboxCount()))
	}
}

// Pause and resume move a sandbox between the running and paused sets, as
// the node's own SandboxContribution does; the paused count is what the
// including-paused limits and the load score read.
func TestLedgerMovesPausedSandboxesBetweenSets(t *testing.T) {
	ledger := NewReservationLedger(time.Minute)
	now := time.Unix(12_000, 0)

	ledger.Apply("node-a", []*schedulerv1.SandboxEvent{
		event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_PAUSE, 2, 1024),
		event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_PAUSE, 2, 1024),
		event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_RESUME, 2, 1024),
	}, now)

	snapshot := baseSnapshot(10, 8, 8192)
	snapshot.PausedSandboxCount = 3
	got := ledger.ApplyTo("node-a", snapshot, now)

	if got.GetSandboxCount() != 9 || got.GetPausedSandboxCount() != 4 {
		t.Fatalf("running = %d paused = %d, want 9 and 4", got.GetSandboxCount(), got.GetPausedSandboxCount())
	}
	if got.GetAllocatedCpu() != 6 || got.GetAllocatedMemoryBytes() != 8192-1024 {
		t.Fatalf("cpu = %d memory = %d, want the net of one pause", got.GetAllocatedCpu(), got.GetAllocatedMemoryBytes())
	}
}

// The drift histogram is observed on every heartbeat that trims a live
// ledger, with what the ledger had moved the node by. It is what says whether
// the interval is short enough for the ledger to matter.
func TestHeartbeatObservesReservationDrift(t *testing.T) {
	service := reservationService(t, true, 100)
	heartbeatWithCount(t, service, "node-a", 0)
	ctx := context.Background()

	sumBefore, countBefore := histogramSum(t, schedulerReservationDrift)

	for i := 0; i < 3; i++ {
		if _, err := service.Schedule(ctx, &schedulerv1.ScheduleRequest{}); err != nil {
			t.Fatalf("placement %d: %v", i, err)
		}
	}
	heartbeatWithCount(t, service, "node-a", 3)

	sumAfter, countAfter := histogramSum(t, schedulerReservationDrift)
	if countAfter != countBefore+1 {
		t.Fatalf("drift observations = %d, want one more than %d", countAfter, countBefore)
	}
	if sumAfter-sumBefore != 3 {
		t.Fatalf("observed drift = %v, want the 3 reservations the heartbeat overtook", sumAfter-sumBefore)
	}
}

// Off, the ledger is not written either: a node's events are still counted for
// loss but touch no placement state, so flipping the switch back on starts
// from the next heartbeat rather than from stale arithmetic.
func TestReservationsOffDoesNotRecordEvents(t *testing.T) {
	service := reservationService(t, false, 100)
	heartbeatWithCount(t, service, "node-a", 0)
	ctx := context.Background()

	if _, err := service.Schedule(ctx, &schedulerv1.ScheduleRequest{}); err != nil {
		t.Fatalf("placement: %v", err)
	}
	if _, err := service.ReportSandboxEvent(ctx, &schedulerv1.ReportSandboxEventRequest{
		NodeId:            "node-a",
		ServiceInstanceId: "node-a-instance",
		Events:            []*schedulerv1.SandboxEvent{event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 1, 1)},
	}); err != nil {
		t.Fatalf("report: %v", err)
	}
	if len(service.ledger.byNodeID) != 0 {
		t.Fatal("the ledger recorded state while switched off")
	}
	if service.eventLoss.nodes["node-a"] == nil {
		t.Fatal("event loss accounting must not depend on the reservation switch")
	}
}

// The ledger's expiry follows the report TTL the service was built with, so a
// node that has stopped heartbeating stops carrying phantom load at about the
// moment the health gate stops offering it work.
func TestLedgerValveFollowsTheReportTTL(t *testing.T) {
	service := NewService(nil, nil, NewStrategy("round_robin"), NewInMemoryBindingStore(time.Minute),
		WithReportTTL(7*time.Second), WithReservations(true, 9))
	if service.ledger.ttl != 14*time.Second {
		t.Fatalf("ledger ttl = %s, want twice the report ttl", service.ledger.ttl)
	}
	if service.ledger.maxDelta != 9 {
		t.Fatalf("ledger clamp = %d, want the configured 9", service.ledger.maxDelta)
	}
}
