package scheduler

import (
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

// defaultLedgerEntryTTL bounds how long an unconfirmed delta influences
// placement.
//
// Deltas are a bridge across the interval between a sandbox being created and
// the owning node's next heartbeat reporting it. Past that, the heartbeat is
// the truth and a surviving delta would double-count. Two heartbeat intervals
// of slack covers one lost heartbeat without letting a dropped event linger.
const defaultLedgerEntryTTL = 12 * time.Second

// nodeDelta is the in-flight adjustment to a node's last reported snapshot.
type nodeDelta struct {
	sandboxCount   int64
	allocatedCPU   int64
	allocatedBytes int64
	updatedAt      time.Time
}

// ReservationLedger applies lifecycle events reported by nodes on top of their
// last heartbeat snapshot.
//
// A heartbeat is up to one interval stale, and nothing decrements between
// placement decisions, so a burst of creates all see the same numbers and all
// look placeable. Nodes already emit batched create/delete/pause/resume/fork
// events for exactly this window; the scheduler simply discarded them.
//
// The ledger is deliberately advisory. Events are lossy by construction — a
// bounded broadcast channel that drops when nobody is listening — so it can
// only ever be a hint. The heartbeat remains authoritative and resets the
// delta, and node-side admission remains the actual capacity authority.
type ReservationLedger struct {
	mu       sync.Mutex
	ttl      time.Duration
	byNodeID map[string]*nodeDelta
}

func NewReservationLedger(ttl time.Duration) *ReservationLedger {
	if ttl <= 0 {
		ttl = defaultLedgerEntryTTL
	}
	return &ReservationLedger{ttl: ttl, byNodeID: make(map[string]*nodeDelta)}
}

// Apply folds a batch of events into the node's delta.
func (l *ReservationLedger) Apply(nodeID string, events []*schedulerv1.SandboxEvent, now time.Time) {
	if l == nil || nodeID == "" || len(events) == 0 {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	delta, ok := l.byNodeID[nodeID]
	if !ok {
		delta = &nodeDelta{}
		l.byNodeID[nodeID] = delta
	}
	for _, event := range events {
		sign := int64(0)
		switch event.GetEventType() {
		case schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE,
			schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_FORK,
			schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_RESUME:
			sign = 1
		case schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_DELETE,
			schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_PAUSE:
			// A paused sandbox releases its VM-side CPU and memory, so it
			// leaves the active set the snapshot reports.
			sign = -1
		default:
			continue
		}
		delta.sandboxCount += sign
		delta.allocatedCPU += sign * int64(event.GetRequestedCpu())
		delta.allocatedBytes += sign * int64(event.GetRequestedMemoryBytes())
	}
	delta.updatedAt = now
}

// Reset clears a node's delta, called when a heartbeat supersedes it.
func (l *ReservationLedger) Reset(nodeID string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.byNodeID, nodeID)
}

// Forget drops all state for a node that has left the cluster.
func (l *ReservationLedger) Forget(nodeID string) { l.Reset(nodeID) }

// ApplyTo returns a copy of snapshot with the node's in-flight delta folded in.
//
// The snapshot is copied rather than mutated: it is shared with everything else
// reading the registry, and placement must not rewrite the reported truth.
// Counters saturate at zero — a delta can only ever be a hint, and a lost event
// must not produce a negative occupancy that reads as free capacity.
func (l *ReservationLedger) ApplyTo(
	nodeID string,
	snapshot *schedulerv1.NodeSnapshot,
	now time.Time,
) *schedulerv1.NodeSnapshot {
	if l == nil || snapshot == nil {
		return snapshot
	}

	l.mu.Lock()
	delta, ok := l.byNodeID[nodeID]
	if ok && now.Sub(delta.updatedAt) > l.ttl {
		delete(l.byNodeID, nodeID)
		ok = false
	}
	var applied nodeDelta
	if ok {
		applied = *delta
	}
	l.mu.Unlock()

	if !ok || (applied.sandboxCount == 0 && applied.allocatedCPU == 0 && applied.allocatedBytes == 0) {
		return snapshot
	}

	// proto messages carry internal state that must not be copied by value, so
	// clone rather than dereference.
	adjusted := cloneSnapshot(snapshot)
	if adjusted == nil {
		return snapshot
	}
	adjusted.SandboxCount = addSaturatingU32(snapshot.GetSandboxCount(), applied.sandboxCount)
	adjusted.AllocatedCpu = addSaturatingU32(snapshot.GetAllocatedCpu(), applied.allocatedCPU)
	adjusted.AllocatedMemoryBytes = addSaturatingU64(
		snapshot.GetAllocatedMemoryBytes(),
		applied.allocatedBytes,
	)
	return adjusted
}

func addSaturatingU32(base uint32, delta int64) uint32 {
	sum := int64(base) + delta
	if sum < 0 {
		return 0
	}
	if sum > int64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(sum)
}

func addSaturatingU64(base uint64, delta int64) uint64 {
	if delta < 0 {
		magnitude := uint64(-delta)
		if magnitude > base {
			return 0
		}
		return base - magnitude
	}
	return base + uint64(delta)
}
