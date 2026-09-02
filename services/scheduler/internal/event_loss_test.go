package scheduler

import "testing"

// The whole point is turning silence into a number: a batch that never arrives
// must show up as a gap. It shows up one heartbeat late, because on the first
// heartbeat a gap is indistinguishable from a batch still on its way.
func TestEventLossReportsTheGapOnce(t *testing.T) {
	tracker := newEventLossTracker()
	tracker.observeReceived("node-a", 3)

	if missed := tracker.observeEmitted("node-a", 10); missed != 0 {
		t.Fatalf("a gap seen once may be a batch in flight, got %d", missed)
	}
	if missed := tracker.observeEmitted("node-a", 10); missed != 7 {
		t.Fatalf("expected 7 missing events once the gap persisted, got %d", missed)
	}
	// Reported once, not repeated on every heartbeat until something changes.
	if missed := tracker.observeEmitted("node-a", 10); missed != 0 {
		t.Fatalf("the same gap must not be counted twice, got %d", missed)
	}

	tracker.observeReceived("node-a", 5)
	if missed := tracker.observeEmitted("node-a", 15); missed != 0 {
		t.Fatalf("a healthy interval should report nothing, got %d", missed)
	}
}

// A node that restarts counts from zero again. Its earlier events belong to a
// process that no longer exists and are not loss.
func TestARestartedNodeIsNotReportedAsLoss(t *testing.T) {
	tracker := newEventLossTracker()
	tracker.observeReceived("node-a", 100)
	tracker.observeEmitted("node-a", 100)

	if missed := tracker.observeEmitted("node-a", 4); missed != 0 {
		t.Fatalf("a restart must not read as loss, got %d", missed)
	}
	// And it re-baselines, so the next real gap is measured from the restart.
	tracker.observeReceived("node-a", 1)
	tracker.observeEmitted("node-a", 9)
	if missed := tracker.observeEmitted("node-a", 9); missed != 4 {
		t.Fatalf("expected 4 missing after the restart, got %d", missed)
	}
}

// A node that reports nothing is a node that does not implement the counter.
// It must not be read as having lost everything.
func TestANodeThatReportsNoCountIsNotMeasured(t *testing.T) {
	tracker := newEventLossTracker()
	tracker.observeReceived("node-a", 5)
	if missed := tracker.observeEmitted("node-a", 0); missed != 0 {
		t.Fatalf("an absent count must not be read as loss, got %d", missed)
	}
}

// A returning node must not be credited with the events of the process that
// left, which would show up as a spurious surplus and mask real loss.
func TestForgetClearsANodesCounters(t *testing.T) {
	tracker := newEventLossTracker()
	tracker.observeReceived("node-a", 50)
	tracker.forget("node-a")

	tracker.observeEmitted("node-a", 3)
	if missed := tracker.observeEmitted("node-a", 3); missed != 3 {
		t.Fatalf("a forgotten node starts from zero, got %d", missed)
	}
}

// Heartbeats and event batches are separate RPCs. A batch can land before the
// heartbeat whose total does not yet include it, and a heartbeat that then
// carries the old total must not roll the received count back: the credit is
// real, and the next heartbeat consumes it.
func TestABatchThatOvertakesItsHeartbeatIsNotLoss(t *testing.T) {
	tracker := newEventLossTracker()
	tracker.observeReceived("node-a", 100)
	if missed := tracker.observeEmitted("node-a", 100); missed != 0 {
		t.Fatalf("in step, got %d", missed)
	}

	// The batch of five lands first; the heartbeat that was already in flight
	// still says one hundred.
	tracker.observeReceived("node-a", 5)
	if missed := tracker.observeEmitted("node-a", 100); missed != 0 {
		t.Fatalf("a stale total against a surplus is not loss, got %d", missed)
	}
	// The heartbeat that counts the batch finds it already here.
	if missed := tracker.observeEmitted("node-a", 105); missed != 0 {
		t.Fatalf("zero events were lost, but the tracker reports %d missed", missed)
	}
	if missed := tracker.observeEmitted("node-a", 105); missed != 0 {
		t.Fatalf("still nothing lost, got %d", missed)
	}
}

// The mirror case: the node counts a batch when it drains it, before its RPC
// completes, so a heartbeat built in that window carries a total the scheduler
// has not caught up with. The deficit is held one heartbeat and the batch
// cancels it when it lands. A gap that outlasts the hold is real, and is still
// measured.
func TestABatchStillInFlightAtTheHeartbeatIsNotLoss(t *testing.T) {
	tracker := newEventLossTracker()
	tracker.observeReceived("node-a", 100)
	tracker.observeEmitted("node-a", 100)

	if missed := tracker.observeEmitted("node-a", 105); missed != 0 {
		t.Fatalf("a batch in flight is not loss, got %d", missed)
	}
	tracker.observeReceived("node-a", 5)
	if missed := tracker.observeEmitted("node-a", 105); missed != 0 {
		t.Fatalf("the batch landed, got %d", missed)
	}

	tracker.observeEmitted("node-a", 110)
	if missed := tracker.observeEmitted("node-a", 110); missed != 5 {
		t.Fatalf("a gap that persisted across two heartbeats is loss, got %d", missed)
	}
}

// When a lost batch and an in-flight batch overlap, only the part of the
// deficit that survives the hold is loss.
func TestOnlyThePersistentPartOfADeficitIsReported(t *testing.T) {
	tracker := newEventLossTracker()
	tracker.observeReceived("node-a", 100)
	tracker.observeEmitted("node-a", 100)

	// Five lost, five still on their way.
	if missed := tracker.observeEmitted("node-a", 110); missed != 0 {
		t.Fatalf("first sighting must wait, got %d", missed)
	}
	tracker.observeReceived("node-a", 5)
	if missed := tracker.observeEmitted("node-a", 110); missed != 5 {
		t.Fatalf("expected the five that never arrived, got %d", missed)
	}
	if missed := tracker.observeEmitted("node-a", 110); missed != 0 {
		t.Fatalf("reported once, got %d", missed)
	}
}
