package scheduler

import "sync"

// eventLossTracker compares how many lifecycle events a node says it emitted
// with how many arrived.
//
// Event delivery is best effort and fire-and-forget. A dropped batch skews the
// scheduler's short-term view of a node — its in-flight creates, its pauses —
// and nothing anywhere says it happened. The reservation ledger is reset by
// every heartbeat, so the skew self-corrects within one interval and no
// retransmission is warranted; what was missing was any way to know loss was
// occurring at all, and at what rate.
//
// Heartbeats and event batches are separate RPCs with no ordering between
// them, so at any one heartbeat the two counts can legitimately disagree in
// either direction: a batch may land before the heartbeat whose total does not
// yet include it, or the node may count a batch whose RPC is still in flight
// when the heartbeat is built. Neither is loss, and both resolve by the next
// heartbeat. A deficit is therefore reported only once it has been seen on two
// consecutive heartbeats, and a surplus is kept as credit rather than erased.
//
// Counts are cumulative since node start, so a node restart makes the reported
// total go backwards. That is read from the total's own history, never from a
// comparison against what arrived.
type eventLossTracker struct {
	mu    sync.Mutex
	nodes map[string]*eventLossCounters
}

type eventLossCounters struct {
	// received is how many events have arrived from the current process.
	received uint64
	// lastEmitted is the total the node last reported. A cumulative counter
	// only goes backwards across a restart, which is what this detects.
	lastEmitted uint64
	// pending is a deficit seen on the previous heartbeat and not yet
	// reported, in case the batch it counts is still on its way.
	pending uint64
	// rebaseline marks a new incarnation whose next total is taken as the
	// baseline, as a total that went backwards is.
	rebaseline bool
}

func newEventLossTracker() *eventLossTracker {
	return &eventLossTracker{nodes: make(map[string]*eventLossCounters)}
}

func (t *eventLossTracker) countersLocked(nodeID string) *eventLossCounters {
	counters, ok := t.nodes[nodeID]
	if !ok {
		counters = &eventLossCounters{}
		t.nodes[nodeID] = counters
	}
	return counters
}

// observeReceived adds the events that just arrived from a node.
func (t *eventLossTracker) observeReceived(nodeID string, count int) {
	if nodeID == "" || count <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.countersLocked(nodeID).received += uint64(count)
}

// observeEmitted compares a node's reported total against what arrived, and
// returns how many events are now known to have gone missing.
//
// A deficit is held for one heartbeat before it is reported, and only the part
// of it that persists is counted. The received counter is realigned to the
// reported total when a gap is reported, so each gap is counted once rather
// than on every heartbeat after it.
func (t *eventLossTracker) observeEmitted(nodeID string, emitted uint64) uint64 {
	if nodeID == "" || emitted == 0 {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	counters := t.countersLocked(nodeID)

	// A node that restarted counts from zero again. Its earlier events are not
	// lost, they belong to a process that no longer exists.
	if emitted < counters.lastEmitted || counters.rebaseline {
		counters.rebaseline = false
		counters.received = emitted
		counters.lastEmitted = emitted
		counters.pending = 0
		return 0
	}
	counters.lastEmitted = emitted

	// Everything the node has counted so far has arrived, or more has: a
	// batch the heartbeat's total does not yet include. That surplus is
	// credit for the total that will, and any deficit seen last time was a
	// batch in flight rather than a batch lost.
	if emitted <= counters.received {
		counters.pending = 0
		return 0
	}

	deficit := emitted - counters.received
	if counters.pending == 0 {
		counters.pending = deficit
		return 0
	}
	missed := min(counters.pending, deficit)
	counters.received = emitted
	counters.pending = 0
	return missed
}

// retain drops counters for every node `keep` no longer recognises.
//
// forget() covers the graceful path only. A node removed from discovery never
// calls it, and its counters would otherwise sit here for the process lifetime.
func (t *eventLossTracker) retain(keep func(nodeID string) bool) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	dropped := 0
	for nodeID := range t.nodes {
		if !keep(nodeID) {
			delete(t.nodes, nodeID)
			dropped++
		}
	}
	return dropped
}

// forget drops a node's counters, so a node that returns is not credited with
// the events of the process that left.
func (t *eventLossTracker) forget(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.nodes, nodeID)
}

// restarted records that a new process now reports for the node, so its next
// total is a baseline rather than a comparison against its predecessor's.
func (t *eventLossTracker) restarted(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.countersLocked(nodeID).rebaseline = true
}
