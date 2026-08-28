package scheduler

import (
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

func event(kind schedulerv1.SandboxEventType, cpu uint32, memoryBytes uint64) *schedulerv1.SandboxEvent {
	return &schedulerv1.SandboxEvent{
		SandboxId:            "sbx",
		EventType:            kind,
		RequestedCpu:         cpu,
		RequestedMemoryBytes: memoryBytes,
	}
}

func baseSnapshot(count, cpu uint32, memoryBytes uint64) *schedulerv1.NodeSnapshot {
	return &schedulerv1.NodeSnapshot{
		SandboxCount:         count,
		AllocatedCpu:         cpu,
		AllocatedMemoryBytes: memoryBytes,
	}
}

// The window this exists to close: a heartbeat is up to an interval stale, so
// without the ledger a burst of creates all read the same numbers.
func TestLedgerAddsCreatesToAStaleSnapshot(t *testing.T) {
	ledger := NewReservationLedger(time.Minute)
	now := time.Unix(1000, 0)

	ledger.Apply("node-a", []*schedulerv1.SandboxEvent{
		event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 2, 1024),
		event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_FORK, 1, 512),
	}, now)

	got := ledger.ApplyTo("node-a", baseSnapshot(10, 4, 4096), now)

	if got.GetSandboxCount() != 12 {
		t.Fatalf("sandbox count = %d, want 12", got.GetSandboxCount())
	}
	if got.GetAllocatedCpu() != 7 {
		t.Fatalf("allocated cpu = %d, want 7", got.GetAllocatedCpu())
	}
	if got.GetAllocatedMemoryBytes() != 5632 {
		t.Fatalf("allocated memory = %d, want 5632", got.GetAllocatedMemoryBytes())
	}
}

// Pause and delete release the active-set resources the snapshot reports.
func TestLedgerSubtractsPausesAndDeletes(t *testing.T) {
	ledger := NewReservationLedger(time.Minute)
	now := time.Unix(2000, 0)

	ledger.Apply("node-a", []*schedulerv1.SandboxEvent{
		event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_PAUSE, 2, 1024),
		event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_DELETE, 1, 512),
	}, now)

	got := ledger.ApplyTo("node-a", baseSnapshot(10, 4, 4096), now)

	if got.GetSandboxCount() != 8 {
		t.Fatalf("sandbox count = %d, want 8", got.GetSandboxCount())
	}
	if got.GetAllocatedCpu() != 1 {
		t.Fatalf("allocated cpu = %d, want 1", got.GetAllocatedCpu())
	}
}

// Events are lossy by construction, so a delta must never drive a counter
// negative and read back as free capacity.
func TestLedgerSaturatesAtZero(t *testing.T) {
	ledger := NewReservationLedger(time.Minute)
	now := time.Unix(3000, 0)

	for i := 0; i < 20; i++ {
		ledger.Apply("node-a", []*schedulerv1.SandboxEvent{
			event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_DELETE, 8, 4096),
		}, now)
	}

	got := ledger.ApplyTo("node-a", baseSnapshot(1, 1, 1024), now)

	if got.GetSandboxCount() != 0 || got.GetAllocatedCpu() != 0 || got.GetAllocatedMemoryBytes() != 0 {
		t.Fatalf("counters must saturate at zero, got %+v", got)
	}
}

// The heartbeat is authoritative: once it lands it already includes the events
// the ledger was carrying, so keeping them would double-count.
func TestLedgerResetOnHeartbeatStopsDoubleCounting(t *testing.T) {
	ledger := NewReservationLedger(time.Minute)
	now := time.Unix(4000, 0)

	ledger.Apply("node-a", []*schedulerv1.SandboxEvent{
		event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 2, 1024),
	}, now)
	ledger.Reset("node-a")

	got := ledger.ApplyTo("node-a", baseSnapshot(11, 6, 5120), now)
	if got.GetSandboxCount() != 11 {
		t.Fatalf("sandbox count = %d, want the heartbeat value unchanged", got.GetSandboxCount())
	}
}

// A dropped heartbeat must not let a delta influence placement forever.
func TestLedgerEntriesExpire(t *testing.T) {
	ledger := NewReservationLedger(5 * time.Second)
	now := time.Unix(5000, 0)

	ledger.Apply("node-a", []*schedulerv1.SandboxEvent{
		event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 2, 1024),
	}, now)

	if got := ledger.ApplyTo("node-a", baseSnapshot(1, 1, 1024), now.Add(time.Second)); got.GetSandboxCount() != 2 {
		t.Fatalf("delta should still apply inside the TTL, got %d", got.GetSandboxCount())
	}
	if got := ledger.ApplyTo("node-a", baseSnapshot(1, 1, 1024), now.Add(time.Minute)); got.GetSandboxCount() != 1 {
		t.Fatalf("delta should have expired, got %d", got.GetSandboxCount())
	}
}

// ApplyTo must not mutate the shared snapshot; the registry hands the same
// pointer to everything else that reads it.
func TestLedgerDoesNotMutateTheSharedSnapshot(t *testing.T) {
	ledger := NewReservationLedger(time.Minute)
	now := time.Unix(6000, 0)
	ledger.Apply("node-a", []*schedulerv1.SandboxEvent{
		event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 2, 1024),
	}, now)

	original := baseSnapshot(10, 4, 4096)
	_ = ledger.ApplyTo("node-a", original, now)

	if original.GetSandboxCount() != 10 || original.GetAllocatedCpu() != 4 {
		t.Fatalf("shared snapshot was mutated: %+v", original)
	}
}

func TestLedgerIgnoresUnknownNodesAndEmptyBatches(t *testing.T) {
	ledger := NewReservationLedger(time.Minute)
	now := time.Unix(7000, 0)

	ledger.Apply("", []*schedulerv1.SandboxEvent{
		event(schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE, 1, 1),
	}, now)
	ledger.Apply("node-a", nil, now)

	got := ledger.ApplyTo("node-b", baseSnapshot(3, 3, 3), now)
	if got.GetSandboxCount() != 3 {
		t.Fatalf("unknown node should get its snapshot unchanged, got %d", got.GetSandboxCount())
	}
}
