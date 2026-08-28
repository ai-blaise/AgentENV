package scheduler

import "testing"

// The whole point is turning silence into a number: a batch that never arrives
// must show up as a gap on the next heartbeat.
func TestEventLossReportsTheGapOnce(t *testing.T) {
	tracker := newEventLossTracker()
	tracker.observeReceived("node-a", 3)

	if missed := tracker.observeEmitted("node-a", 10); missed != 7 {
		t.Fatalf("expected 7 missing events, got %d", missed)
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

	if missed := tracker.observeEmitted("node-a", 3); missed != 3 {
		t.Fatalf("a forgotten node starts from zero, got %d", missed)
	}
}
