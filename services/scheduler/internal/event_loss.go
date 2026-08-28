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
// Counts are cumulative since node start, so a node restart makes the reported
// count go backwards. That is read as a restart rather than as negative loss.
type eventLossTracker struct {
	mu       sync.Mutex
	received map[string]uint64
}

func newEventLossTracker() *eventLossTracker {
	return &eventLossTracker{received: make(map[string]uint64)}
}

// observeReceived adds the events that just arrived from a node.
func (t *eventLossTracker) observeReceived(nodeID string, count int) {
	if nodeID == "" || count <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.received[nodeID] += uint64(count)
}

// observeEmitted compares a node's reported total against what arrived, and
// returns how many events went missing since the last comparison.
//
// The received counter is realigned to the reported total afterwards, so each
// gap is reported once rather than repeated on every heartbeat.
func (t *eventLossTracker) observeEmitted(nodeID string, emitted uint64) uint64 {
	if nodeID == "" || emitted == 0 {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	received := t.received[nodeID]
	t.received[nodeID] = emitted

	// A node that restarted counts from zero again. Its earlier events are not
	// lost, they belong to a process that no longer exists.
	if emitted < received {
		return 0
	}
	return emitted - received
}

// retain drops counters for every node `keep` no longer recognises.
//
// forget() covers the graceful path only. A node removed from discovery never
// calls it, and its counter would otherwise sit here for the process lifetime.
func (t *eventLossTracker) retain(keep func(nodeID string) bool) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	dropped := 0
	for nodeID := range t.received {
		if !keep(nodeID) {
			delete(t.received, nodeID)
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
	delete(t.received, nodeID)
}
