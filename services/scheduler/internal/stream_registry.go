package scheduler

import (
	"context"
	"strings"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// nodeStreamMaxSkew bounds how far a stamp from another replica may sit outside
// this replica's own clock before it is pulled back in.
//
// Replicas share an NTP domain, so a stamp far in the future is a broken clock
// rather than a real observation, and a stamp far in the past is a stream that
// has backed up. Four report TTLs is wide enough that ordinary scheduling
// latency never trips it and narrow enough that a clamped stamp still reads as
// stale to the health gate.
const nodeStreamMaxSkew = 4

// nodeStreamMachineInfoRefresh is how often a node's machine info is republished
// even when nothing about it changed.
//
// The CPU configuration inside it is tens of kilobytes, so it is elided from
// almost every event. But a replica that started after the event carrying it
// fell out of the stream's retention would otherwise never learn that node's
// configuration, and would compute the fleet-wide CPU intersection over a
// smaller set than its peers — the exact divergence the intersection exists to
// prevent. One republish a minute per node bounds that window without putting
// the payload on the steady-state stream.
const nodeStreamMachineInfoRefresh = time.Minute

// StreamFedNodeRegistry is an AtomicNodeRegistry that also hears about the nodes
// it does not serve.
//
// A node's connection to the scheduler is sticky — kube-proxy binds it to one
// replica for the life of the TCP connection — so with N replicas each hears
// from roughly 1/N of the fleet. On its own that is worse than one replica:
// every replica would place all of its traffic onto its own slice of the fleet,
// and a freshly started one would see no healthy node at all and fail open onto
// a fleet it has no capacity data for. The replica that took the heartbeat
// republishes it, and every replica converges on the whole fleet.
//
// Only liveness and capacity travel. Roster reconciliation stays with the
// replica that took the RPC: it has a single writer per node by construction,
// and republishing rosters would make the bus O(sandboxes) — the cost that the
// roster digest exists to avoid paying even once.
type StreamFedNodeRegistry struct {
	*AtomicNodeRegistry
	bus       NodeStreamBus
	replicaID string
	logger    *zap.Logger

	// publishMu guards the refresh bookkeeping alone. It is not the embedded
	// registry's lock: the stamps below are this publisher's own record of what
	// it put on the bus, not part of the fleet view the replicas converge on.
	publishMu              sync.Mutex
	machineInfoPublishedMs map[string]int64
	stampsPrunedMs         int64
}

func NewStreamFedNodeRegistry(base *AtomicNodeRegistry, bus NodeStreamBus, replicaID string, logger *zap.Logger) *StreamFedNodeRegistry {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &StreamFedNodeRegistry{
		AtomicNodeRegistry:     base,
		bus:                    bus,
		replicaID:              replicaID,
		logger:                 logger,
		machineInfoPublishedMs: make(map[string]int64),
	}
}

// Heartbeat applies the report locally exactly as a single scheduler would,
// then tells the other replicas about it.
//
// The publish is best effort in both directions: it never fails the RPC, and a
// node whose event is dropped is stale on the other replicas for one interval,
// because its next heartbeat republishes everything.
func (s *StreamFedNodeRegistry) Heartbeat(req *schedulerv1.HeartbeatRequest, now time.Time) (Node, HeartbeatAck, error) {
	baseline, hadBaseline := s.publishBaseline(req.GetNodeId())

	node, ack, err := s.AtomicNodeRegistry.Heartbeat(req, now)
	if err != nil {
		return node, ack, err
	}

	seenMs := now.UTC().UnixMilli()
	event := &schedulerv1.NodeStateEvent{
		OriginReplicaId: s.replicaID,
		LastSeenUnixMs:  seenMs,
		Heartbeat:       publishableHeartbeat(req),
	}
	// Machine info is republished when the publisher's own copy of it changed,
	// and on a slow refresh so a replica that started late still converges.
	stored, _ := s.publishBaseline(req.GetNodeId())
	changed := !hadBaseline || !proto.Equal(baseline, stored)
	if s.machineInfoDue(req.GetNodeId(), seenMs, changed) {
		event.Heartbeat.MachineInfo = stored
	}

	if err := s.bus.Publish(context.Background(), req.GetNodeId(), event); err != nil {
		s.logger.Debug("scheduler node state publish failed", zap.Error(err))
	}
	return node, ack, nil
}

// machineInfoDue reports whether this event should carry the node's machine
// info, and records the answer.
//
// The interval is measured from the last time this replica published machine
// info for the node, never from the node's previous heartbeat: nodes heartbeat
// every few seconds, so a gap measured between two consecutive heartbeats never
// reaches the refresh window and the republish never fires at all. A replica
// that joined an already-running tier then holds no configuration for the nodes
// pinned to its peers, and because the intersection is computed only when every
// observed node has one, it computes none for the whole fleet.
func (s *StreamFedNodeRegistry) machineInfoDue(nodeID string, nowMs int64, changed bool) bool {
	s.publishMu.Lock()
	defer s.publishMu.Unlock()

	s.pruneMachineInfoStampsLocked(nowMs)
	last, published := s.machineInfoPublishedMs[nodeID]
	if published && !changed && nowMs-last < nodeStreamMachineInfoRefresh.Milliseconds() {
		return false
	}
	s.machineInfoPublishedMs[nodeID] = nowMs
	return true
}

// pruneMachineInfoStampsLocked drops the stamps of nodes that stopped
// heartbeating to this replica, so the map is bounded by the live fleet rather
// than by every node the process ever took a heartbeat from. A node still
// heartbeating here has its stamp rewritten once per refresh window, so anything
// older than two windows moved to another replica or left. The sweep itself runs
// at most once per window, because it is O(fleet) and the heartbeats are not.
func (s *StreamFedNodeRegistry) pruneMachineInfoStampsLocked(nowMs int64) {
	window := nodeStreamMachineInfoRefresh.Milliseconds()
	if nowMs-s.stampsPrunedMs < window {
		return
	}
	s.stampsPrunedMs = nowMs
	for nodeID, stamp := range s.machineInfoPublishedMs {
		if nowMs-stamp >= 2*window {
			delete(s.machineInfoPublishedMs, nodeID)
		}
	}
}

// Close releases the bus. The registry itself holds nothing to release.
func (s *StreamFedNodeRegistry) Close() error { return s.bus.Close() }

// Run consumes the bus until ctx is done. The returned channel closes once the
// retained backlog has been replayed.
func (s *StreamFedNodeRegistry) Run(ctx context.Context) (<-chan struct{}, error) {
	return s.bus.Subscribe(ctx, func(ev *schedulerv1.NodeStateEvent) {
		s.apply(ev, time.Now())
	})
}

// publishableHeartbeat strips everything that belongs to the one replica that
// took the RPC.
//
// The roster and its digest are reconciled by that replica alone, so
// republishing them would put O(sandboxes) on a bus that exists to carry
// O(nodes). The emitted event count is compared against the events this replica
// received, so a replica that received none would read a node's whole
// cumulative count as loss on every heartbeat. Machine info is decided by the
// caller and left off here.
func publishableHeartbeat(req *schedulerv1.HeartbeatRequest) *schedulerv1.HeartbeatRequest {
	return &schedulerv1.HeartbeatRequest{
		NodeId:              req.GetNodeId(),
		ClusterId:           req.GetClusterId(),
		ServiceInstanceId:   req.GetServiceInstanceId(),
		Version:             req.GetVersion(),
		Commit:              req.GetCommit(),
		Snapshot:            cloneSnapshot(req.GetSnapshot()),
		P2PEndpoint:         cloneP2PEndpoint(req.GetP2PEndpoint()),
		HeartbeatIntervalMs: req.GetHeartbeatIntervalMs(),
	}
}

func (s *StreamFedNodeRegistry) apply(ev *schedulerv1.NodeStateEvent, now time.Time) {
	if ev.GetOriginReplicaId() == s.replicaID {
		schedulerNodeStreamAppliedTotal.WithLabelValues("self").Inc()
		return
	}
	outcome, clamped := s.applyRemote(ev, now)
	schedulerNodeStreamAppliedTotal.WithLabelValues(outcome).Inc()
	if clamped {
		schedulerNodeStreamClampedTotal.Inc()
	}
	if outcome == nodeStreamApplied {
		schedulerNodeStreamLagSeconds.Observe(float64(now.UTC().UnixMilli()-ev.GetLastSeenUnixMs()) / 1000)
	}
}

// Outcomes of applying one replicated report. A closed set, so it is safe as a
// metric label.
const (
	nodeStreamApplied     = "applied"
	nodeStreamStale       = "stale"
	nodeStreamUnknownNode = "unknown_node"
	nodeStreamInvalid     = "invalid"
)

// applyRemote stores a report another replica received.
//
// It is not a second call into Heartbeat, because four things must differ:
//
//   - The freshness stamp is the origin replica's, applied as sent. Restamping
//     with the local clock would make a stream that backed up look like a fleet
//     that had just been heard from, and the health gate would stop working
//     entirely. This compares two scheduler clocks in one NTP domain, never a
//     node's clock against a scheduler's.
//   - A report older than the one already applied is dropped. Delivery is
//     at-least-once and unordered, so applying is idempotent only if it refuses
//     to run backwards.
//   - A node that discovery has not yet told this replica about is counted and
//     dropped, not logged as an error: on a replica whose informer has not
//     synced that is expected, and it heals when the next event arrives.
//   - Nothing is published back. The event is already on the bus.
//
// Everything else — the incarnation fence, the CPU-config carry-forward, the
// intersection, the snapshot defaults — runs identically, because converging
// those on every replica is the whole point.
func (r *AtomicNodeRegistry) applyRemote(ev *schedulerv1.NodeStateEvent, now time.Time) (outcome string, clamped bool) {
	beat := ev.GetHeartbeat()
	nodeID := strings.TrimSpace(beat.GetNodeId())
	if nodeID == "" || ev.GetLastSeenUnixMs() <= 0 {
		return nodeStreamInvalid, false
	}

	nowMs := now.UTC().UnixMilli()
	seenMs := ev.GetLastSeenUnixMs()
	skew := int64(nodeStreamMaxSkew) * r.observedTTL.Milliseconds()
	switch {
	case seenMs > nowMs:
		seenMs, clamped = nowMs, true
	case seenMs < nowMs-skew:
		seenMs, clamped = nowMs-skew, true
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	node, ok := r.nodesByID[nodeID]
	if !ok {
		return nodeStreamUnknownNode, clamped
	}

	incoming := Incarnation(strings.TrimSpace(beat.GetServiceInstanceId()))
	prev, existed := r.observed[nodeID]
	if err := r.fenceLocked(nodeID, incoming, prev, existed); err != nil {
		return nodeStreamStale, clamped
	}
	if existed &&
		Incarnation(strings.TrimSpace(prev.node.GetServiceInstanceId())) == incoming &&
		prev.node.GetLastSeenUnixMs() >= seenMs {
		return nodeStreamStale, clamped
	}

	r.storeReportLocked(beat, node, prev, existed, seenMs, observedSourceStream)
	return nodeStreamApplied, clamped
}

// publishBaseline reports the machine info this replica holds for a node, which
// is what it would put on the bus.
func (r *AtomicNodeRegistry) publishBaseline(nodeID string) (*schedulerv1.MachineInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.observed[nodeID]
	if !ok {
		return nil, false
	}
	return cloneMachineInfo(record.node.GetMachineInfo()), true
}

// ObservedSourceCounts splits the observed nodes by how this replica came by
// them. On a converged fleet of F nodes across N replicas, every replica holds
// roughly F/N from its own RPCs and the remainder from the stream, and the two
// sum to F.
func (r *AtomicNodeRegistry) ObservedSourceCounts() (rpc int, stream int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, record := range r.observed {
		if record.source == observedSourceStream {
			stream++
			continue
		}
		rpc++
	}
	return rpc, stream
}
