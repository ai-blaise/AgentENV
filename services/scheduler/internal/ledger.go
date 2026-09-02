package scheduler

import (
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

// defaultLedgerEntryTTL bounds how long an unconfirmed delta influences
// placement when the ledger is built without a report TTL to derive from.
//
// Deltas are a bridge across the interval between a sandbox being created and
// the owning node's next heartbeat reporting it. Past that, the heartbeat is
// the truth and a surviving delta would double-count. The service derives the
// bound from the report TTL — twice it, so a node that stops heartbeating
// stops carrying phantom load at about the moment the health gate stops
// offering it work — and this is what a bare constructor falls back to.
const defaultLedgerEntryTTL = 12 * time.Second

// defaultMaxReservationDelta is the clamp a bare constructor applies; the
// service passes the configured one. See SchedulerConfig.MaxReservationDelta.
const defaultMaxReservationDelta = 512

// nodeDelta is an adjustment to a node's last reported snapshot, in the same
// terms the node's own SandboxContribution uses so the two accountings cannot
// disagree about what an event means.
type nodeDelta struct {
	sandboxCount   int64
	startingCount  int64
	pausedCount    int64
	allocatedCPU   int64
	allocatedBytes int64
}

func (d *nodeDelta) add(o nodeDelta) {
	d.sandboxCount += o.sandboxCount
	d.startingCount += o.startingCount
	d.pausedCount += o.pausedCount
	d.allocatedCPU += o.allocatedCPU
	d.allocatedBytes += o.allocatedBytes
}

func (d *nodeDelta) sub(o nodeDelta) {
	d.sandboxCount -= o.sandboxCount
	d.startingCount -= o.startingCount
	d.pausedCount -= o.pausedCount
	d.allocatedCPU -= o.allocatedCPU
	d.allocatedBytes -= o.allocatedBytes
}

func (d nodeDelta) isZero() bool {
	return d == nodeDelta{}
}

// ledgerEntry is one applied event batch, stamped when the scheduler applied
// it. The stamp is the scheduler's own clock so a heartbeat — also stamped by
// the scheduler on arrival — can be ordered against it without comparing two
// machines' clocks.
type ledgerEntry struct {
	delta nodeDelta
	at    time.Time
}

// reservation is a placement this scheduler made that no node has reported
// yet: one sandbox, one start in flight, and whatever the hint asked for.
type reservation struct {
	cpu         int64
	memoryBytes int64
	at          time.Time
}

func (r reservation) delta() nodeDelta {
	return nodeDelta{sandboxCount: 1, startingCount: 1, allocatedCPU: r.cpu, allocatedBytes: r.memoryBytes}
}

// nodeLedger holds everything the ledger knows about one node since its last
// heartbeat. entries and reservations are kept in arrival order so a trim can
// drop the prefix that a heartbeat has overtaken and keep the rest.
type nodeLedger struct {
	entries      []ledgerEntry
	reservations []reservation
	// sum is the running total of entries, so applying or clamping does not
	// walk them.
	sum nodeDelta
}

func (n *nodeLedger) empty() bool {
	return len(n.entries) == 0 && len(n.reservations) == 0
}

// total is what the node's snapshot should be adjusted by right now.
func (n *nodeLedger) total() nodeDelta {
	out := n.sum
	for _, r := range n.reservations {
		out.add(r.delta())
	}
	return out
}

// ReservationLedger applies lifecycle events reported by nodes, and placements
// this scheduler has made, on top of each node's last heartbeat snapshot.
//
// A heartbeat is up to one interval stale, and nothing decrements between
// placement decisions, so a burst of creates all see the same numbers and all
// look placeable. Nodes already emit batched create/delete/pause/resume/fork
// events for exactly this window; the scheduler simply discarded them. And the
// node's numbers cannot see a create at all until it has allocated a slot and
// started the VM, so the scheduler's own optimistic reservation is the only
// thing that lets two decisions inside one interval see each other.
//
// The ledger is deliberately advisory. Events are lossy by construction — a
// bounded broadcast channel that drops when nobody is listening — so it can
// only ever be a hint. The heartbeat remains authoritative and trims what it
// has overtaken, and node-side admission remains the actual capacity
// authority. Nodes that have never heartbeated are exempt: there is no
// snapshot to adjust, and synthesising an empty one would make an unknown node
// look idle, which the load-aware strategies deliberately refuse to assume.
type ReservationLedger struct {
	mu       sync.Mutex
	ttl      time.Duration
	maxDelta int64
	byNodeID map[string]*nodeLedger
}

// NewReservationLedger builds a ledger whose entries expire after ttl, with
// the default clamp. Zero uses the default TTL.
func NewReservationLedger(ttl time.Duration) *ReservationLedger {
	return newReservationLedger(ttl, defaultMaxReservationDelta)
}

func newReservationLedger(ttl time.Duration, maxDelta int) *ReservationLedger {
	if ttl <= 0 {
		ttl = defaultLedgerEntryTTL
	}
	if maxDelta <= 0 {
		maxDelta = defaultMaxReservationDelta
	}
	return &ReservationLedger{ttl: ttl, maxDelta: int64(maxDelta), byNodeID: make(map[string]*nodeLedger)}
}

// eventDelta is the ledger's reading of one event, mirroring the node's
// SandboxContribution: a running sandbox holds its CPU and memory, a paused
// one holds neither but still counts as paused. Create and fork add a running
// sandbox; delete removes one; pause moves one from running to paused and
// resume moves it back. Unknown kinds contribute nothing.
func eventDelta(event *schedulerv1.SandboxEvent) (nodeDelta, bool) {
	cpu := int64(event.GetRequestedCpu())
	memory := int64(event.GetRequestedMemoryBytes())
	switch event.GetEventType() {
	case schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE,
		schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_FORK:
		return nodeDelta{sandboxCount: 1, allocatedCPU: cpu, allocatedBytes: memory}, true
	case schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_DELETE:
		return nodeDelta{sandboxCount: -1, allocatedCPU: -cpu, allocatedBytes: -memory}, true
	case schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_PAUSE:
		return nodeDelta{sandboxCount: -1, pausedCount: 1, allocatedCPU: -cpu, allocatedBytes: -memory}, true
	case schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_RESUME:
		return nodeDelta{sandboxCount: 1, pausedCount: -1, allocatedCPU: cpu, allocatedBytes: memory}, true
	default:
		return nodeDelta{}, false
	}
}

// Apply folds a batch of events into the node's ledger.
//
// A create settles the oldest outstanding reservation for the node: the
// reservation was the placeholder for a create nobody had reported yet, and
// this is that report. Fork children are never reserved — a fork is routed to
// its source's node, not placed — so they settle nothing.
//
// An event that would push the node's sandbox delta past the clamp is dropped
// whole rather than partially applied, so the CPU and memory terms never
// describe a sandbox the count does not.
func (l *ReservationLedger) Apply(nodeID string, events []*schedulerv1.SandboxEvent, now time.Time) {
	if l == nil || nodeID == "" || len(events) == 0 {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	node := l.nodeLocked(nodeID)
	var batch nodeDelta
	for _, event := range events {
		delta, ok := eventDelta(event)
		if !ok {
			continue
		}
		if event.GetEventType() == schedulerv1.SandboxEventType_SANDBOX_EVENT_TYPE_CREATE && len(node.reservations) > 0 {
			node.reservations = node.reservations[1:]
		}
		if l.exceedsClampLocked(node, batch, delta) {
			continue
		}
		batch.add(delta)
	}
	if batch.isZero() {
		if node.empty() {
			delete(l.byNodeID, nodeID)
		}
		return
	}
	node.entries = append(node.entries, ledgerEntry{delta: batch, at: now})
	node.sum.add(batch)
}

// exceedsClampLocked reports whether applying delta on top of what the node
// already carries, plus a batch under construction, would move its sandbox
// count past the clamp in either direction.
func (l *ReservationLedger) exceedsClampLocked(node *nodeLedger, pending nodeDelta, delta nodeDelta) bool {
	next := node.total().sandboxCount + pending.sandboxCount + delta.sandboxCount
	return next > l.maxDelta || next < -l.maxDelta
}

// Reserve records that this scheduler has just placed a sandbox on the node:
// one more sandbox, one more start in flight, and the CPU and memory the
// request asked for. It is what lets the next placement inside the same
// heartbeat interval see this one.
func (l *ReservationLedger) Reserve(nodeID string, cpu uint32, memoryBytes uint64, now time.Time) {
	if l == nil || nodeID == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	node := l.nodeLocked(nodeID)
	r := reservation{cpu: int64(cpu), memoryBytes: int64(memoryBytes), at: now}
	if l.exceedsClampLocked(node, nodeDelta{}, r.delta()) {
		if node.empty() {
			delete(l.byNodeID, nodeID)
		}
		return
	}
	node.reservations = append(node.reservations, r)
}

// TrimBefore drops everything the node's ledger recorded at or before cutoff,
// and returns how far, in sandboxes, the dropped state had moved the node from
// its last heartbeat.
//
// cutoff is the scheduler's arrival stamp for the heartbeat that is replacing
// this state — the same value the registry records as LastSeenUnixMs — so
// both sides of the comparison come from one clock. Entries stamped after it
// arrived while the heartbeat was being applied and describe changes the
// heartbeat cannot contain, so they survive. Clearing everything on each
// heartbeat lost them.
func (l *ReservationLedger) TrimBefore(nodeID string, cutoff time.Time) int64 {
	if l == nil || nodeID == "" {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	node, ok := l.byNodeID[nodeID]
	if !ok {
		return 0
	}
	before := node.total().sandboxCount

	kept := 0
	for _, entry := range node.entries {
		if entry.at.After(cutoff) {
			node.entries[kept] = entry
			kept++
		} else {
			node.sum.sub(entry.delta)
		}
	}
	clear(node.entries[kept:])
	node.entries = node.entries[:kept]

	kept = 0
	for _, r := range node.reservations {
		if r.at.After(cutoff) {
			node.reservations[kept] = r
			kept++
		}
	}
	node.reservations = node.reservations[:kept]

	drift := before - node.total().sandboxCount
	if node.empty() {
		delete(l.byNodeID, nodeID)
	}
	if drift < 0 {
		drift = -drift
	}
	return drift
}

// Reset clears a node's ledger outright.
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

// Retain drops every node's ledger that `keep` no longer recognises.
//
// Forget() covers the graceful path only; a node removed from discovery never
// calls it.
func (l *ReservationLedger) Retain(keep func(nodeID string) bool) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	dropped := 0
	for nodeID := range l.byNodeID {
		if !keep(nodeID) {
			delete(l.byNodeID, nodeID)
			dropped++
		}
	}
	return dropped
}

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
	node, ok := l.byNodeID[nodeID]
	var applied nodeDelta
	if ok {
		l.expireLocked(node, now)
		applied = node.total()
		if node.empty() {
			delete(l.byNodeID, nodeID)
		}
	}
	l.mu.Unlock()

	if applied.isZero() {
		return snapshot
	}

	// proto messages carry internal state that must not be copied by value, so
	// clone rather than dereference.
	adjusted := cloneSnapshot(snapshot)
	if adjusted == nil {
		return snapshot
	}
	adjusted.SandboxCount = addSaturatingU32(snapshot.GetSandboxCount(), applied.sandboxCount)
	adjusted.SandboxStartingCount = addSaturatingU32(snapshot.GetSandboxStartingCount(), applied.startingCount)
	adjusted.PausedSandboxCount = addSaturatingU32(snapshot.GetPausedSandboxCount(), applied.pausedCount)
	adjusted.AllocatedCpu = addSaturatingU32(snapshot.GetAllocatedCpu(), applied.allocatedCPU)
	adjusted.AllocatedMemoryBytes = addSaturatingU64(
		snapshot.GetAllocatedMemoryBytes(),
		applied.allocatedBytes,
	)
	return adjusted
}

// expireLocked drops entries and reservations older than the TTL. A node that
// has stopped heartbeating never trims its ledger, and without this its
// phantom load would outlive it.
func (l *ReservationLedger) expireLocked(node *nodeLedger, now time.Time) {
	kept := 0
	for _, entry := range node.entries {
		if now.Sub(entry.at) > l.ttl {
			node.sum.sub(entry.delta)
			continue
		}
		node.entries[kept] = entry
		kept++
	}
	clear(node.entries[kept:])
	node.entries = node.entries[:kept]

	kept = 0
	for _, r := range node.reservations {
		if now.Sub(r.at) > l.ttl {
			continue
		}
		node.reservations[kept] = r
		kept++
	}
	node.reservations = node.reservations[:kept]
}

func (l *ReservationLedger) nodeLocked(nodeID string) *nodeLedger {
	node, ok := l.byNodeID[nodeID]
	if !ok {
		node = &nodeLedger{}
		l.byNodeID[nodeID] = node
	}
	return node
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
