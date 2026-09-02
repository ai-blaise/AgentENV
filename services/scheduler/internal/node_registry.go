package scheduler

import (
	"errors"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/protobuf/proto"
)

type NodeRegistry interface {
	Snapshot(allowLingering bool) []Node
	Contains(node Node) bool
	Resolve(nodeID string) (Node, bool)
	Heartbeat(req *schedulerv1.HeartbeatRequest, now time.Time) (Node, string, error)
	ListObserved(clusterID string, now time.Time) []*schedulerv1.ObservedNode
	ListP2pPeers(clusterID string, backend string, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer
	FilterP2pPeers(clusterID string, backend string, nodeIDs []string, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer
	GetObserved(nodeID string, clusterID string, now time.Time) (*schedulerv1.ObservedNode, bool)
	// PeekObserved returns the latest heartbeat-reported NodeSnapshot for a node.
	// Unlike GetObserved, it does not derive status from discovery state or TTL,
	// and returns only the raw snapshot suitable for scheduling decisions.
	// Returns nil if the node has never sent a heartbeat.
	PeekObserved(nodeID string) *schedulerv1.NodeSnapshot
	// PeekObservedHealth returns the same snapshot alongside the liveness facts
	// scheduling needs to decide whether the node should receive new work.
	PeekObservedHealth(nodeID string) (*schedulerv1.NodeSnapshot, ObservedHealth)
	// ObservedIncarnation returns the incarnation of the process last heard
	// from for a node, and whether one has been heard from at all.
	ObservedIncarnation(nodeID string) (Incarnation, bool)
	// SampleNodes returns at most `size` schedulable nodes without
	// materializing the whole fleet. Zero means no bound.
	SampleNodes(size int, allowLingering bool) []Node
	UnregisterObserved(nodeID string, serviceInstanceID string) error
}

var (
	ErrServiceInstanceMismatch = errors.New("service instance mismatch")
	// ErrStaleIncarnation rejects a report from a node process that has since
	// been replaced, or that has already unregistered.
	ErrStaleIncarnation = errors.New("node reported a superseded service instance")
	// ErrStaleReport rejects a heartbeat the live node process collected before
	// one the scheduler has already applied.
	ErrStaleReport           = errors.New("node reported a snapshot older than the one already applied")
	ErrNodeNotInRegistry     = errors.New(NodeNotInRegistryMessage)
	defaultObservedReportTTL = 30 * time.Second
)

// NodeNotInRegistryMessage is the wire text, sent with codes.InvalidArgument,
// for a request that names a node id the scheduler's node list does not hold.
//
// It is a cross-language contract rather than a log line. The node's reporter,
// src/observability/reporter.rs, substring-matches it on a rejected heartbeat
// to raise an error-level log that names AENV_NODE_ID as the knob to check;
// any other wording degrades that to a generic warning with nothing failing on
// either side. Every RPC that rejects an unknown node returns this exact
// string, and TestUnknownNodeRejectionCarriesTheWireMessage pins it.
const NodeNotInRegistryMessage = "node is not in scheduler node list"

type observedNodeRecord struct {
	node        *schedulerv1.ObservedNode
	p2pEndpoint *schedulerv1.P2PEndpoint
	reportTTL   time.Duration
	// reportedAtMs is the node's own stamp on the applied snapshot, kept as
	// sent. The view stamps an unstamped snapshot with the scheduler's clock
	// for its readers; ordering reports against that would compare two clocks.
	reportedAtMs int64
}

type AtomicNodeRegistry struct {
	mu           sync.RWMutex
	nodesByID    map[string]Node
	lingeringIDs map[string]bool
	observedTTL  time.Duration
	observed     map[string]observedNodeRecord
	// departed keeps the incarnation each node last unregistered with, so the
	// fence on superseded processes outlives the record it used to live on.
	// An entry is dropped when a strictly newer incarnation registers or when
	// discovery stops listing the node, so the map is bounded by fleet size.
	departed         map[string]Incarnation
	cpuIntersection  map[string]string
	intersectionSent map[string]bool
}

func NewAtomicNodeRegistry(nodes []Node, observedTTL time.Duration) *AtomicNodeRegistry {
	ttl := defaultObservedReportTTL
	if observedTTL > 0 {
		ttl = observedTTL
	}

	registry := &AtomicNodeRegistry{
		nodesByID:        make(map[string]Node),
		lingeringIDs:     make(map[string]bool),
		observedTTL:      ttl,
		observed:         make(map[string]observedNodeRecord),
		departed:         make(map[string]Incarnation),
		cpuIntersection:  make(map[string]string),
		intersectionSent: make(map[string]bool),
	}
	registry.Set(nodes, nil)
	return registry
}

// Snapshot returns discovered nodes filtered by their derived status.
// See NodeStatus in scheduler.proto for the full status derivation table.
func (r *AtomicNodeRegistry) Snapshot(allowLingering bool) []Node {
	r.mu.RLock()
	result := make([]Node, 0, len(r.nodesByID))
	for _, node := range r.nodesByID {
		if r.lingeringIDs[node.ID] && !allowLingering {
			continue
		}
		result = append(result, node)
	}
	r.mu.RUnlock()
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result
}

// SampleNodes returns at most `size` schedulable nodes chosen uniformly at
// random, without materializing or sorting the whole fleet.
//
// Snapshot exists to give callers a stable, sorted view; placement wants
// neither. Going through it meant every request copied and sorted the entire
// node list before discarding all but a handful, which is what kept placement
// linear in fleet size even once per-node snapshot cloning was bounded.
//
// A size of zero means "no bound" and falls back to Snapshot, preserving the
// previous whole-fleet behaviour exactly.
func (r *AtomicNodeRegistry) SampleNodes(size int, allowLingering bool) []Node {
	if size <= 0 {
		return r.Snapshot(allowLingering)
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	sampled := make([]Node, 0, size)
	seen := 0
	for _, node := range r.nodesByID {
		if r.lingeringIDs[node.ID] && !allowLingering {
			continue
		}
		// Reservoir sampling over the map's iteration order. Go randomizes that
		// order per iteration, but relying on it would be relying on an
		// implementation detail, so the reservoir does the work.
		if len(sampled) < size {
			sampled = append(sampled, node)
		} else if j := rand.Intn(seen + 1); j < size {
			sampled[j] = node
		}
		seen++
	}
	return sampled
}

func (r *AtomicNodeRegistry) Contains(node Node) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	known, ok := r.nodesByID[node.ID]
	return ok && known.Endpoint == node.Endpoint
}

func (r *AtomicNodeRegistry) Resolve(nodeID string) (Node, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	node, ok := r.nodesByID[nodeID]
	return node, ok
}

// Set replaces the discovered node list. active nodes are serving and not
// terminating; lingering nodes are serving but terminating (graceful shutdown).
func (r *AtomicNodeRegistry) Set(active []Node, lingering []Node) {
	byID := make(map[string]Node, len(active)+len(lingering))
	for _, node := range active {
		byID[node.ID] = node
	}
	for _, node := range lingering {
		byID[node.ID] = node
	}

	lIDs := make(map[string]bool, len(lingering))
	for _, node := range lingering {
		lIDs[node.ID] = true
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodesByID = byID
	r.lingeringIDs = lIDs
	affectedClusters := make(map[string]struct{})
	for nodeID, record := range r.observed {
		if _, ok := byID[nodeID]; ok {
			continue
		}
		if clusterID := record.node.GetClusterId(); clusterID != "" {
			affectedClusters[clusterID] = struct{}{}
		}
		delete(r.observed, nodeID)
		delete(r.intersectionSent, nodeID)
	}
	for nodeID := range r.departed {
		if _, ok := byID[nodeID]; !ok {
			delete(r.departed, nodeID)
		}
	}
	for clusterID := range affectedClusters {
		r.invalidateIntersectionLocked(clusterID)
	}
}

func (r *AtomicNodeRegistry) Heartbeat(req *schedulerv1.HeartbeatRequest, now time.Time) (Node, string, error) {
	nowMs := now.UTC().UnixMilli()

	machineInfo := cloneMachineInfo(req.GetMachineInfo())

	r.mu.Lock()
	defer r.mu.Unlock()
	node, ok := r.nodesByID[req.GetNodeId()]
	if !ok {
		return Node{}, "", ErrNodeNotInRegistry
	}

	prevCPU, existed := "", false
	incoming := Incarnation(strings.TrimSpace(req.GetServiceInstanceId()))
	if prev, ok := r.observed[req.GetNodeId()]; ok {
		existed = true
		// A node process that has already been replaced must not be able to
		// overwrite the live one's state. Incarnations are time-ordered UUIDv7
		// values minted per process start, so a strictly older one is a report
		// from a dead process — most often an RPC delayed behind a restart.
		//
		// Equal or unknown incarnations pass: a node that does not report one
		// must not be locked out, and re-reporting the same one is the normal
		// case. Within the same incarnation the reports themselves are then
		// ordered, so the normal case cannot run backwards either.
		current := Incarnation(strings.TrimSpace(prev.node.GetServiceInstanceId()))
		if current.Supersedes(incoming) {
			return Node{}, "", ErrStaleIncarnation
		}
		if current == incoming && r.reportPredatesApplied(prev, req.GetSnapshot(), nowMs) {
			return Node{}, "", ErrStaleReport
		}
		prevCPU = prev.node.GetMachineInfo().GetCpuConfigJson()
		if machineInfo != nil && machineInfo.CpuConfigJson == "" {
			machineInfo.CpuConfigJson = prevCPU
		}
	} else if tombstone, ok := r.departed[req.GetNodeId()]; ok && fencedByDeparture(tombstone, incoming) {
		return Node{}, "", ErrStaleIncarnation
	}

	record := observedNodeRecord{
		node: &schedulerv1.ObservedNode{
			NodeId:            req.GetNodeId(),
			Endpoint:          node.Endpoint,
			ClusterId:         req.GetClusterId(),
			ServiceInstanceId: req.GetServiceInstanceId(),
			Version:           req.GetVersion(),
			Commit:            req.GetCommit(),
			MachineInfo:       machineInfo,
			LastSeenUnixMs:    nowMs,
			Snapshot:          cloneSnapshot(req.GetSnapshot()),
		},
		p2pEndpoint:  cloneP2PEndpoint(req.GetP2PEndpoint()),
		reportTTL:    r.observedTTL,
		reportedAtMs: req.GetSnapshot().GetReportedAtUnixMs(),
	}
	if record.node.Snapshot.GetReportedAtUnixMs() == 0 {
		record.node.Snapshot.ReportedAtUnixMs = nowMs
	}
	if record.node.Snapshot.GetStatus() == schedulerv1.NodeStatus_NODE_STATUS_UNSPECIFIED {
		record.node.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_CONNECTING
	}

	r.observed[req.GetNodeId()] = record
	delete(r.departed, req.GetNodeId())

	clusterID := req.GetClusterId()
	if !existed || (machineInfo != nil && machineInfo.GetCpuConfigJson() != prevCPU) {
		r.invalidateIntersectionLocked(clusterID)
	}
	if _, computed := r.cpuIntersection[clusterID]; !computed {
		if r.allConfigsReadyLocked(clusterID) {
			if result := r.computeIntersectionLocked(clusterID); result != "" {
				r.cpuIntersection[clusterID] = result
			}
		}
	}

	// Return the intersection on every heartbeat once it is computed, not only
	// the first time. The scheduler holds the intersection in memory, so after
	// a restart it can only be rebuilt from what nodes keep reporting; sending
	// it once per node per scheduler process left a restarted scheduler unable
	// to deliver it at all, and new sandboxes then booted with node-local CPU
	// features. intersectionSent survives only to suppress repeat logging.
	if intersection, ok := r.cpuIntersection[clusterID]; ok {
		if !r.intersectionSent[req.GetNodeId()] {
			r.intersectionSent[req.GetNodeId()] = true
		}
		return node, intersection, nil
	}
	return node, "", nil
}

// reportPredatesApplied reports whether a heartbeat from the live process was
// collected before the one already applied for it.
//
// Arrival order is not send order. A heartbeat the node gave up on at its
// deadline is still delivered and still executed, by which time the node may
// have sent, and had applied, a newer one. Applying the old one afterwards
// reconciles a roster the node has moved past — deleting every binding it did
// not list once past the grace — and caches that roster under its digest for
// the next elided round. Nothing downstream can tell; only the order here can.
//
// The node stamps reported_at_unix_ms when it collects the snapshot, so two
// reports from one process order by it without comparing clocks across
// machines, which is the one comparison the rest of this file refuses to make.
// Zero on either side is a node that does not stamp, and passes. Equal passes:
// a retried heartbeat is the same report, not an older one.
//
// The fence holds only while the applied record is inside the report TTL. A
// node whose clock stepped backwards would otherwise be refused for as long as
// the step, and its bindings would expire under it; letting the fence lapse
// with the record bounds that to one TTL, after which the next report is taken
// as the new baseline.
func (r *AtomicNodeRegistry) reportPredatesApplied(prev observedNodeRecord, snapshot *schedulerv1.NodeSnapshot, nowMs int64) bool {
	incoming := snapshot.GetReportedAtUnixMs()
	applied := prev.reportedAtMs
	if incoming == 0 || applied == 0 || incoming >= applied {
		return false
	}
	return nowMs-prev.node.GetLastSeenUnixMs() <= r.observedTTL.Milliseconds()
}

// fencedByDeparture reports whether an incarnation is locked out by the
// tombstone its node left on unregister.
//
// Unregister is an incarnation's last word, so its own late heartbeat must not
// resurrect it any more than an older process's may: equal is rejected here
// where the live fence lets it through. Only a strictly newer incarnation comes
// back, which is what a restarted node mints. An unknown incarnation on either
// side cannot be ordered and is never locked out.
func fencedByDeparture(tombstone Incarnation, incoming Incarnation) bool {
	if tombstone == "" || incoming == "" {
		return false
	}
	return !incoming.Supersedes(tombstone)
}

func (r *AtomicNodeRegistry) invalidateIntersectionLocked(clusterID string) {
	delete(r.cpuIntersection, clusterID)
	for nodeID, rec := range r.observed {
		if rec.node.GetClusterId() == clusterID {
			delete(r.intersectionSent, nodeID)
		}
	}
}

func (r *AtomicNodeRegistry) allConfigsReadyLocked(clusterID string) bool {
	total, withConfig := 0, 0
	for _, rec := range r.observed {
		if rec.node.GetClusterId() != clusterID {
			continue
		}
		total++
		if rec.node.GetMachineInfo().GetCpuConfigJson() != "" {
			withConfig++
		}
	}
	return total > 0 && withConfig == total
}

func (r *AtomicNodeRegistry) computeIntersectionLocked(clusterID string) string {
	var jsons []string
	for _, rec := range r.observed {
		if rec.node.GetClusterId() != clusterID {
			continue
		}
		if j := rec.node.GetMachineInfo().GetCpuConfigJson(); j != "" {
			jsons = append(jsons, j)
		}
	}
	result, err := IntersectCpuConfigs(jsons)
	if err != nil {
		return ""
	}
	return result
}

func (r *AtomicNodeRegistry) ListObserved(clusterID string, now time.Time) []*schedulerv1.ObservedNode {
	nowMs := now.UTC().UnixMilli()
	trimmedCluster := strings.TrimSpace(clusterID)

	r.mu.RLock()
	defer r.mu.RUnlock()
	nodes := make([]*schedulerv1.ObservedNode, 0, len(r.observed))
	for _, record := range r.observed {
		if trimmedCluster != "" && record.node.GetClusterId() != trimmedCluster {
			continue
		}
		nodes = append(nodes, r.deriveObservedNodeViewLocked(record, nowMs))
	}

	return nodes
}

func (r *AtomicNodeRegistry) ListP2pPeers(clusterID string, backend string, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer {
	return r.filterP2pPeers(clusterID, backend, nil, excludeNodeID, now)
}

func (r *AtomicNodeRegistry) FilterP2pPeers(clusterID string, backend string, nodeIDs []string, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer {
	allowed := make(map[string]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		allowed[nodeID] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil
	}
	return r.filterP2pPeers(clusterID, backend, allowed, excludeNodeID, now)
}

func (r *AtomicNodeRegistry) filterP2pPeers(clusterID string, backend string, allowed map[string]struct{}, excludeNodeID string, now time.Time) []*schedulerv1.P2PPeer {
	nowMs := now.UTC().UnixMilli()
	trimmedCluster := strings.TrimSpace(clusterID)
	trimmedBackend := strings.TrimSpace(backend)
	trimmedExcludeNodeID := strings.TrimSpace(excludeNodeID)

	r.mu.RLock()
	defer r.mu.RUnlock()
	peers := make([]*schedulerv1.P2PPeer, 0, len(r.observed))
	for _, record := range r.observed {
		if trimmedCluster != "" && record.node.GetClusterId() != trimmedCluster {
			continue
		}
		node := r.deriveObservedNodeViewLocked(record, nowMs)
		if len(allowed) > 0 {
			if _, ok := allowed[node.GetNodeId()]; !ok {
				continue
			}
		}
		if node.GetNodeId() == trimmedExcludeNodeID {
			continue
		}
		if node.GetSnapshot().GetStatus() != schedulerv1.NodeStatus_NODE_STATUS_READY {
			continue
		}
		endpoint := record.p2pEndpoint
		if endpoint.GetBackend() == "" || endpoint.GetAddress() == "" {
			continue
		}
		if trimmedBackend != "" && endpoint.GetBackend() != trimmedBackend {
			continue
		}
		peers = append(peers, &schedulerv1.P2PPeer{
			NodeId:   node.GetNodeId(),
			Endpoint: cloneP2PEndpoint(endpoint),
		})
	}

	return peers
}

func (r *AtomicNodeRegistry) GetObserved(nodeID string, clusterID string, now time.Time) (*schedulerv1.ObservedNode, bool) {
	nowMs := now.UTC().UnixMilli()
	trimmedCluster := strings.TrimSpace(clusterID)

	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.observed[nodeID]
	if !ok {
		return nil, false
	}
	if trimmedCluster != "" && record.node.GetClusterId() != trimmedCluster {
		return nil, false
	}

	return r.deriveObservedNodeViewLocked(record, nowMs), true
}

func (r *AtomicNodeRegistry) PeekObserved(nodeID string) *schedulerv1.NodeSnapshot {
	snapshot, _ := r.PeekObservedHealth(nodeID)
	return snapshot
}

// ObservedHealth is what scheduling needs to know about a node's liveness, as
// distinct from the capacity numbers in its snapshot.
//
// PeekObserved deliberately reports the last snapshot verbatim without applying
// the report TTL, so a caller that only reads the snapshot cannot tell a node
// that heartbeated a second ago from one that stopped hours ago. Placement must
// be able to tell those apart.
type ObservedHealth struct {
	// Seen is false when the node has never heartbeated.
	Seen bool
	// LastSeenUnixMs is the scheduler's arrival timestamp for the most recent
	// heartbeat. It is stamped by the scheduler, never by the node, so it is
	// comparable against the scheduler's own clock.
	LastSeenUnixMs int64
	// Status is the node's self-reported status from that heartbeat.
	Status schedulerv1.NodeStatus
}

// PeekObservedHealth returns the node's last snapshot together with the
// liveness facts scheduling needs. The snapshot is nil when the node has never
// heartbeated or reported no snapshot.
func (r *AtomicNodeRegistry) PeekObservedHealth(nodeID string) (*schedulerv1.NodeSnapshot, ObservedHealth) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.observed[nodeID]
	if !ok || record.node == nil {
		return nil, ObservedHealth{}
	}
	health := ObservedHealth{
		Seen:           true,
		LastSeenUnixMs: record.node.GetLastSeenUnixMs(),
		Status:         record.node.GetSnapshot().GetStatus(),
	}
	snapshot := record.node.GetSnapshot()
	if snapshot == nil {
		return nil, health
	}
	return cloneSnapshot(snapshot), health
}

// ObservedIncarnation returns the incarnation of the process last heard from
// for a node, and whether one has been heard from at all.
func (r *AtomicNodeRegistry) ObservedIncarnation(nodeID string) (Incarnation, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.observed[nodeID]
	if !ok {
		return "", false
	}
	return Incarnation(strings.TrimSpace(record.node.GetServiceInstanceId())), true
}

func (r *AtomicNodeRegistry) UnregisterObserved(nodeID string, serviceInstanceID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	record, ok := r.observed[nodeID]
	if !ok {
		return nil
	}
	if record.node.GetServiceInstanceId() != serviceInstanceID {
		return ErrServiceInstanceMismatch
	}

	clusterID := record.node.GetClusterId()
	delete(r.observed, nodeID)
	// The record goes, the fence stays. Heartbeat only compares incarnations
	// against a record, so deleting the record alone reopened the node to any
	// heartbeat at all — including the departing process's own, still in
	// flight behind this call, which would re-register a node that has just
	// said it is gone and re-upsert every binding it just had wiped.
	if incarnation := Incarnation(strings.TrimSpace(record.node.GetServiceInstanceId())); incarnation != "" {
		r.departed[nodeID] = incarnation
	}
	r.invalidateIntersectionLocked(clusterID)
	return nil
}

// deriveObservedNodeViewLocked builds the external ObservedNode view for a
// heartbeat record, overriding the endpoint and status based on the current
// discovery state. See NodeStatus in scheduler.proto for the full derivation
// table. r.mu must be held by the caller.
func (r *AtomicNodeRegistry) deriveObservedNodeViewLocked(record observedNodeRecord, nowMs int64) *schedulerv1.ObservedNode {
	out := cloneObservedNode(record.node)
	if out.Snapshot == nil {
		out.Snapshot = &schedulerv1.NodeSnapshot{}
	}

	nodeID := out.GetNodeId()

	knownNode, inDiscovery := r.nodesByID[nodeID]
	isLingering := r.lingeringIDs[nodeID]

	if inDiscovery && strings.TrimSpace(knownNode.Endpoint) != "" {
		out.Endpoint = knownNode.Endpoint
	}

	ttl := record.reportTTL
	if ttl <= 0 {
		ttl = defaultObservedReportTTL
	}

	if out.GetLastSeenUnixMs() > 0 && nowMs-out.GetLastSeenUnixMs() > ttl.Milliseconds() {
		out.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY
	} else if !inDiscovery {
		out.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_CONNECTING
	} else if isLingering {
		out.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_LINGERING
	} else {
		// Active — keep the status reported by the node.
		if out.Snapshot.GetStatus() == schedulerv1.NodeStatus_NODE_STATUS_UNSPECIFIED {
			out.Snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_CONNECTING
		}
	}

	return out
}

func cloneObservedNode(node *schedulerv1.ObservedNode) *schedulerv1.ObservedNode {
	if node == nil {
		return &schedulerv1.ObservedNode{}
	}
	cloned, ok := proto.Clone(node).(*schedulerv1.ObservedNode)
	if ok {
		return cloned
	}
	return &schedulerv1.ObservedNode{}
}

func cloneSnapshot(snapshot *schedulerv1.NodeSnapshot) *schedulerv1.NodeSnapshot {
	if snapshot == nil {
		return &schedulerv1.NodeSnapshot{}
	}
	cloned, ok := proto.Clone(snapshot).(*schedulerv1.NodeSnapshot)
	if ok {
		return cloned
	}
	return &schedulerv1.NodeSnapshot{}
}

func cloneMachineInfo(machine *schedulerv1.MachineInfo) *schedulerv1.MachineInfo {
	if machine == nil {
		return nil
	}
	cloned, ok := proto.Clone(machine).(*schedulerv1.MachineInfo)
	if ok {
		return cloned
	}
	return nil
}

func cloneP2PEndpoint(endpoint *schedulerv1.P2PEndpoint) *schedulerv1.P2PEndpoint {
	if endpoint == nil {
		return nil
	}
	cloned, ok := proto.Clone(endpoint).(*schedulerv1.P2PEndpoint)
	if ok {
		return cloned
	}
	return nil
}
