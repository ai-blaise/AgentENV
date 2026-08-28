package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	schedulerv1.UnimplementedSchedulerServer
	logger        *zap.Logger
	nodes         NodeRegistry
	strategy      Strategy
	store         BindingStore
	artifacts     ArtifactStore
	resourceLimit *config.NodeResourceLimit
	// reportTTL bounds how stale a node's last heartbeat may be before it stops
	// being a placement candidate.
	reportTTL time.Duration
	// healthGateEnabled is the killswitch for health-gated placement. It
	// defaults to on; disabling it restores the previous behavior of placing on
	// any discovered node regardless of heartbeat age.
	healthGateEnabled bool
	// ledger folds node-reported lifecycle events onto the last heartbeat
	// snapshot, closing the window in which a burst of creates all read the
	// same stale numbers. Advisory only; see ReservationLedger.
	ledger *ReservationLedger
	// candidateSampleSize bounds how many nodes one placement inspects. Zero
	// disables sampling and restores whole-fleet evaluation.
	candidateSampleSize int
	// rosters lets a node skip resending an unchanged sandbox roster.
	rosters *rosterCache
	// eventLoss turns silently dropped lifecycle event batches into a number.
	eventLoss *eventLossTracker
	// sweepMu guards lastSweep, which paces the departed-node sweep.
	sweepMu   sync.Mutex
	lastSweep time.Time
	// mobility arbitrates paused-sandbox ownership across the fleet. It lives
	// here rather than on each node because a destination and an origin cannot
	// agree through a store on one of their disks.
	mobility MobilityStore
}

func NewService(logger *zap.Logger, nodes NodeRegistry, strategy Strategy, store BindingStore, opts ...ServiceOption) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	if nodes == nil {
		nodes = NewAtomicNodeRegistry(nil, defaultObservedReportTTL)
	}
	s := &Service{
		logger:              logger,
		nodes:               nodes,
		strategy:            strategy,
		reportTTL:           defaultObservedReportTTL,
		healthGateEnabled:   true,
		ledger:              NewReservationLedger(0),
		candidateSampleSize: defaultCandidateSampleSize,
		rosters:             newRosterCache(),
		eventLoss:           newEventLossTracker(),
		mobility:            NewInMemoryMobilityStore(),
		store:               store,
		artifacts:           NewInMemoryArtifactStore(defaultArtifactStoreCapacity, 0),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// ServiceOption configures optional Service behaviour.
type ServiceOption func(*Service)

// WithMobilityStore replaces the in-memory mobility store, so a deployment
// with Redis keeps paused-sandbox records across a scheduler restart.
//
// Losing them costs the ability to migrate until each node re-reports, not
// correctness — but a restart is exactly when a drain is most likely to be
// under way.
func WithMobilityStore(store MobilityStore) ServiceOption {
	return func(s *Service) {
		if store != nil {
			s.mobility = store
		}
	}
}

// WithNodeResourceLimit sets per-node resource thresholds for scheduling.
func WithNodeResourceLimit(limit *config.NodeResourceLimit) ServiceOption {
	return func(s *Service) {
		s.resourceLimit = limit
	}
}

func WithArtifactStore(store ArtifactStore) ServiceOption {
	return func(s *Service) {
		s.artifacts = store
	}
}

// WithReportTTL sets how stale a node's last heartbeat may be before it stops
// being a placement candidate. It should match the registry's observed TTL.
func WithReportTTL(ttl time.Duration) ServiceOption {
	return func(s *Service) {
		if ttl > 0 {
			s.reportTTL = ttl
		}
	}
}

// WithCandidateSampleSize bounds how many nodes one placement inspects. Zero
// evaluates the whole fleet, which is what the pre-sampling behaviour did.
func WithCandidateSampleSize(size int) ServiceOption {
	return func(s *Service) {
		if size >= 0 {
			s.candidateSampleSize = size
		}
	}
}

// WithHealthGate toggles health-gated placement. Disabling it reproduces the
// previous behavior exactly: every discovered node is a candidate, however
// long ago it last heartbeated.
func WithHealthGate(enabled bool) ServiceOption {
	return func(s *Service) {
		s.healthGateEnabled = enabled
	}
}

type QueryOnlyService struct {
	schedulerv1.UnimplementedSchedulerServer
	logger *zap.Logger
	store  BindingStore
}

func NewQueryOnlyService(logger *zap.Logger, store BindingStore) *QueryOnlyService {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &QueryOnlyService{logger: logger, store: store}
}

func (s *QueryOnlyService) LookupNode(_ context.Context, req *schedulerv1.LookupNodeRequest) (*schedulerv1.LookupNodeResponse, error) {
	return lookupNode(s.logger, s.store, req)
}

func (s *Service) Schedule(_ context.Context, req *schedulerv1.ScheduleRequest) (resp *schedulerv1.ScheduleResponse, err error) {
	start := time.Now()
	defer func() {
		recordSchedulerSchedule(s.strategy.Name(), start, err)
	}()

	now := time.Now()
	// Inspect a bounded sample rather than the whole fleet. The decision does
	// not need the full list: it already runs against a view up to a heartbeat
	// interval stale and is corrected by the node's own admission decision.
	// Sampling inside the registry avoids copying and sorting the fleet just to
	// discard almost all of it.
	discovered := s.nodes.SampleNodes(s.candidateSampleSize /* allowLingering */, false)
	rich := make([]RichNode, 0, len(discovered))
	for _, n := range discovered {
		snapshot, health := s.nodes.PeekObservedHealth(n.ID)
		rich = append(rich, RichNode{
			Node: n,
			// Fold in what this node has told us since its last heartbeat, so a
			// burst of creates does not keep reading the same stale numbers.
			Snapshot: s.ledger.ApplyTo(n.ID, snapshot, now),
			Health:   health,
		})
	}

	// Health before resources: a node that is not heartbeating has no
	// trustworthy resource numbers to evaluate in the first place.
	eligible := rich
	if s.healthGateEnabled {
		var dropped map[HealthFilterReason]int
		eligible, dropped = FilterByHealth(rich, s.reportTTL, now)
		for reason, count := range dropped {
			recordSchedulerNodesFiltered(string(reason), count)
		}
	}
	eligible = FilterByResourceLimit(eligible, s.resourceLimit)
	// A node that already refused this sandbox is authoritative about its own
	// capacity, so retrying into it would loop. Excluding every candidate is
	// treated as an exhausted fleet, not as "try them all again".
	eligible = FilterExcludedNodes(eligible, req.GetExcludeNodeIds())

	node, selectErr := s.strategy.Select(eligible, req.GetHint())
	if selectErr != nil {
		s.logger.Debug("scheduler selection failed",
			zap.String("strategy", s.strategy.Name()),
			zap.String("hint", summarizeScheduleHint(req.GetHint())),
			zap.Int("candidate_nodes", len(rich)),
			zap.Int("eligible_nodes", len(eligible)),
			zap.Error(selectErr),
		)
		if errors.Is(selectErr, ErrNoNodes) {
			err = status.Error(codes.Unavailable, "no nodes available")
			return nil, err
		}
		err = status.Error(codes.Internal, selectErr.Error())
		return nil, err
	}
	s.logger.Debug("scheduler selected node",
		zap.String("strategy", s.strategy.Name()),
		zap.String("hint", summarizeScheduleHint(req.GetHint())),
		zap.String("node_id", node.ID),
		zap.String("endpoint", node.Endpoint),
		zap.Int("candidate_nodes", len(rich)),
		zap.Int("eligible_nodes", len(eligible)),
	)
	return &schedulerv1.ScheduleResponse{Node: node.Node.ToProto()}, nil
}

// summarizeScheduleHint renders a compact, log-friendly description of a
// scheduling hint.
func summarizeScheduleHint(hint *schedulerv1.ScheduleRequestHint) string {
	switch k := hint.GetKind().(type) {
	case *schedulerv1.ScheduleRequestHint_NewColdSandbox:
		c := k.NewColdSandbox
		return fmt.Sprintf("new_cold_sandbox cpu=%d memory_mb=%d images=%v", c.GetCpuCount(), c.GetMemoryMb(), c.GetImages())
	case *schedulerv1.ScheduleRequestHint_NewSandbox:
		return "new_sandbox"
	default:
		return "none"
	}
}

func (s *Service) ListNodes(_ context.Context, _ *schedulerv1.ListNodesRequest) (*schedulerv1.ListNodesResponse, error) {
	snapshot := s.nodes.Snapshot( /* allowLingering */ true)
	nodes := make([]*schedulerv1.Node, 0, len(snapshot))
	for _, node := range snapshot {
		nodes = append(nodes, node.ToProto())
	}

	s.logger.Debug("scheduler listed nodes", zap.Int("node_count", len(nodes)))

	return &schedulerv1.ListNodesResponse{Nodes: nodes}, nil
}

func (s *Service) LookupNode(_ context.Context, req *schedulerv1.LookupNodeRequest) (*schedulerv1.LookupNodeResponse, error) {
	return lookupNode(s.logger, s.store, req)
}

func lookupNode(logger *zap.Logger, store BindingStore, req *schedulerv1.LookupNodeRequest) (*schedulerv1.LookupNodeResponse, error) {
	if strings.TrimSpace(req.GetSandboxId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	node, ok, getErr := store.Get(req.GetSandboxId(), time.Now())
	if getErr != nil {
		logger.Warn("scheduler lookup binding store failed", zap.String("sandbox_id", req.GetSandboxId()), zap.Error(getErr))
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	if !ok {
		logger.Debug("scheduler lookup missed sandbox assignment", zap.String("sandbox_id", req.GetSandboxId()))
		return nil, status.Error(codes.NotFound, "sandbox assignment not found")
	}
	logger.Debug("scheduler lookup resolved sandbox assignment",
		zap.String("sandbox_id", req.GetSandboxId()),
		zap.String("node_id", node.ID),
		zap.String("endpoint", node.Endpoint),
	)
	return &schedulerv1.LookupNodeResponse{Node: node.ToProto()}, nil
}

// validateAssignment applies the checks shared by RecordAssignment and
// RecordAssignments so a batched write cannot bypass a validation the single
// write enforces.
func (s *Service) validateAssignment(sandboxID string, protoNode *schedulerv1.Node) (Node, error) {
	if strings.TrimSpace(sandboxID) == "" {
		return Node{}, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	node := NodeFromProto(protoNode)
	if strings.TrimSpace(node.ID) == "" || strings.TrimSpace(node.Endpoint) == "" {
		return Node{}, status.Error(codes.InvalidArgument, "node_id and endpoint are required")
	}
	if !s.isKnownNode(node) {
		s.logger.Warn("scheduler rejected assignment for unknown node",
			zap.String("sandbox_id", sandboxID),
			zap.String("node_id", node.ID),
			zap.String("endpoint", node.Endpoint),
		)
		return Node{}, status.Error(codes.InvalidArgument, "node is not in scheduler node list")
	}
	return node, nil
}

// RecordAssignments records a batch of bindings in one store pass. Individual
// failures are reported positionally rather than failing the batch, because
// the caller has already created the sandboxes these bindings describe and
// cannot undo them.
func (s *Service) RecordAssignments(_ context.Context, req *schedulerv1.RecordAssignmentsRequest) (*schedulerv1.RecordAssignmentsResponse, error) {
	requested := req.GetAssignments()
	if len(requested) == 0 {
		return &schedulerv1.RecordAssignmentsResponse{}, nil
	}

	results := make([]*schedulerv1.RecordAssignmentResult, len(requested))
	assignments := make([]BindingAssignment, len(requested))
	accepted := make([]int, 0, len(requested))
	for i, assignment := range requested {
		results[i] = &schedulerv1.RecordAssignmentResult{SandboxId: assignment.GetSandboxId()}
		node, err := s.validateAssignment(assignment.GetSandboxId(), assignment.GetNode())
		if err != nil {
			results[i].Error = err.Error()
			continue
		}
		assignments[i] = BindingAssignment{SandboxID: assignment.GetSandboxId(), Node: node}
		accepted = append(accepted, i)
	}

	if len(accepted) == 0 {
		return &schedulerv1.RecordAssignmentsResponse{Results: results}, nil
	}

	batch := make([]BindingAssignment, 0, len(accepted))
	for _, i := range accepted {
		batch = append(batch, assignments[i])
	}
	for offset, err := range s.store.RecordBatch(batch, time.Now()) {
		if err == nil {
			continue
		}
		i := accepted[offset]
		s.logger.Warn("scheduler record assignment binding store failed",
			zap.String("sandbox_id", batch[offset].SandboxID),
			zap.String("node_id", batch[offset].Node.ID),
			zap.Error(err),
		)
		results[i].Error = status.Error(codes.Unavailable, "binding store unavailable").Error()
	}
	return &schedulerv1.RecordAssignmentsResponse{Results: results}, nil
}

func (s *Service) RecordAssignment(_ context.Context, req *schedulerv1.RecordAssignmentRequest) (*schedulerv1.RecordAssignmentResponse, error) {
	node, err := s.validateAssignment(req.GetSandboxId(), req.GetNode())
	if err != nil {
		return nil, err
	}
	if err := s.store.Record(req.GetSandboxId(), node, time.Now()); err != nil {
		s.logger.Warn("scheduler record assignment binding store failed",
			zap.String("sandbox_id", req.GetSandboxId()),
			zap.String("node_id", node.ID),
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	s.logger.Debug("scheduler recorded sandbox assignment",
		zap.String("sandbox_id", req.GetSandboxId()),
		zap.String("node_id", node.ID),
		zap.String("endpoint", node.Endpoint),
	)
	return &schedulerv1.RecordAssignmentResponse{}, nil
}

func (s *Service) Heartbeat(_ context.Context, req *schedulerv1.HeartbeatRequest) (*schedulerv1.HeartbeatResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	serviceInstanceID := strings.TrimSpace(req.GetServiceInstanceId())
	if nodeID == "" || serviceInstanceID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and service_instance_id are required")
	}

	now := time.Now()
	node, cpuConfigJSON, err := s.nodes.Heartbeat(req, now)
	if err != nil {
		if errors.Is(err, ErrNodeNotInRegistry) {
			s.logger.Warn("scheduler rejected observed registration for unknown node",
				zap.String("node_id", nodeID),
			)
			return nil, status.Error(codes.InvalidArgument, "node is not in scheduler node list")
		}
		if errors.Is(err, ErrStaleIncarnation) {
			// A live node retrying will send its current incarnation and
			// succeed; only a report from a replaced process lands here.
			s.logger.Warn("scheduler rejected heartbeat from a superseded node process",
				zap.String("node_id", nodeID),
				zap.String("service_instance_id", serviceInstanceID),
			)
			schedulerStaleIncarnationTotal.WithLabelValues(nodeID).Inc()
			return nil, status.Error(codes.FailedPrecondition, "service instance has been superseded")
		}
		return nil, status.Error(codes.Internal, "node registry heartbeat failed")
	}
	// The heartbeat is the authoritative count, so anything the ledger was
	// carrying for this node is now either included in it or lost.
	s.ledger.Reset(nodeID)

	s.pruneDepartedNodes()

	if missed := s.eventLoss.observeEmitted(nodeID, req.GetEmittedEventCount()); missed > 0 {
		s.logger.Warn("scheduler did not receive every sandbox event a node emitted",
			zap.String("node_id", nodeID),
			zap.Uint64("missed_events", missed),
		)
		schedulerSandboxEventsLostTotal.WithLabelValues(nodeID).Add(float64(missed))
	}

	completeness := RosterIncomplete
	if req.GetRosterComplete() {
		completeness = RosterComplete
	}

	roster, requestFullRoster := s.resolveRoster(req, nodeID, completeness)
	if !requestFullRoster {
		if err := s.store.ReconcileNodeRoster(node, roster, completeness, now); err != nil {
			s.logger.Warn("scheduler heartbeat binding reconcile failed",
				zap.String("node_id", nodeID),
				zap.Error(err),
			)
			return nil, status.Error(codes.Unavailable, "binding store unavailable")
		}
	}

	return &schedulerv1.HeartbeatResponse{
		CpuConfigJson:        cpuConfigJSON,
		RequestFullRoster:    requestFullRoster,
		RosterDigestAccepted: true,
	}, nil
}

// resolveRoster returns the roster to reconcile against, or asks for it back.
//
// A node that sent its roster is authoritative and its digest is cached. A
// node that elided it is served from the cache, so bindings are still
// refreshed and only the wire cost is saved. A digest the scheduler cannot
// resolve — it restarted, or the roster changed and the node has not caught up
// — means nothing is reconciled this round and the roster is requested back:
// an elided roster and an empty one look identical on the wire and mean
// opposite things, and guessing wrong deletes a node's entire data plane.
//
// Skipping one round is safe because the binding TTL is required to be several
// heartbeats long, which the registry validates against the node's reported
// interval.
func (s *Service) resolveRoster(
	req *schedulerv1.HeartbeatRequest,
	nodeID string,
	completeness RosterCompleteness,
) (roster []string, requestFullRoster bool) {
	digest := strings.TrimSpace(req.GetRosterDigest())

	// No digest, or the roster came along anyway: the wire is authoritative.
	if digest == "" || req.GetRosterFull() {
		if digest != "" {
			s.rosters.remember(nodeID, digest, req.GetSandboxIds(), completeness == RosterComplete)
		}
		return req.GetSandboxIds(), false
	}

	cached, cachedComplete, ok := s.rosters.lookup(nodeID, digest)
	if !ok {
		s.logger.Debug("scheduler asked a node for its full roster",
			zap.String("node_id", nodeID),
			zap.String("roster_digest", digest),
		)
		schedulerRosterFullRequestTotal.Inc()
		return nil, true
	}

	// A node that has since finished startup recovery upgrades a cached
	// incomplete roster: the ids are the same, but what an empty one means is
	// no longer the same.
	if completeness == RosterComplete && !cachedComplete {
		s.rosters.remember(nodeID, digest, cached, true)
	}
	schedulerRosterCacheHitTotal.Inc()
	return cached, false
}

// departedSweepInterval bounds how often the per-node side maps are swept.
//
// The sweep is O(entries) and the entries only change when the fleet does, so
// doing it on every heartbeat would be pure waste on a busy scheduler.
const departedSweepInterval = 60 * time.Second

// pruneDepartedNodes drops per-node bookkeeping for nodes the registry no
// longer recognises.
//
// The roster cache, the event-loss counters and the reservation ledger are all
// keyed by node and are cleared by UnregisterNode — the graceful path. A node
// that is removed from discovery, renamed, or simply never comes back never
// calls it, so without this each map grows with fleet churn for the lifetime
// of the process. The roster cache is the worst of the three: every entry
// holds a node's full sandbox roster.
//
// Membership is the registry's answer, so a node that is still discovered but
// merely unhealthy keeps its state — this reclaims what has left, not what is
// struggling.
func (s *Service) pruneDepartedNodes() {
	now := time.Now()
	s.sweepMu.Lock()
	if now.Sub(s.lastSweep) < departedSweepInterval {
		s.sweepMu.Unlock()
		return
	}
	s.lastSweep = now
	s.sweepMu.Unlock()

	known := func(nodeID string) bool {
		_, ok := s.nodes.Resolve(nodeID)
		return ok
	}
	dropped := s.rosters.retain(known) + s.eventLoss.retain(known) + s.ledger.Retain(known)
	if dropped > 0 {
		s.logger.Debug("scheduler dropped bookkeeping for departed nodes",
			zap.Int("entries", dropped),
		)
	}
}

func (s *Service) ReportSandboxEvent(_ context.Context, req *schedulerv1.ReportSandboxEventRequest) (*schedulerv1.ReportSandboxEventResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}
	// Only nodes the scheduler knows about may move its placement view.
	if _, known := s.nodes.Resolve(nodeID); !known {
		s.logger.Debug("scheduler ignored sandbox events from unknown node",
			zap.String("node_id", nodeID),
			zap.Int("event_count", len(req.GetEvents())),
		)
		return &schedulerv1.ReportSandboxEventResponse{}, nil
	}

	s.ledger.Apply(nodeID, req.GetEvents(), time.Now())
	s.eventLoss.observeReceived(nodeID, len(req.GetEvents()))
	s.logger.Debug("scheduler applied sandbox event batch",
		zap.String("node_id", nodeID),
		zap.String("service_instance_id", req.GetServiceInstanceId()),
		zap.Int("event_count", len(req.GetEvents())),
	)
	return &schedulerv1.ReportSandboxEventResponse{}, nil
}

func (s *Service) RunObservedNodesMetrics(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	s.refreshObservedNodesMetrics(time.Now())

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.refreshObservedNodesMetrics(now)
		}
	}
}

func (s *Service) refreshObservedNodesMetrics(now time.Time) {
	recordObservedNodes(s.nodes.ListObserved("", now))
}

func (s *Service) ListObservedNodes(_ context.Context, req *schedulerv1.ListObservedNodesRequest) (*schedulerv1.ListObservedNodesResponse, error) {
	nodes := s.nodes.ListObserved(req.GetClusterId(), time.Now())
	return &schedulerv1.ListObservedNodesResponse{
		Nodes: nodes,
	}, nil
}

func (s *Service) ListP2PPeers(_ context.Context, req *schedulerv1.ListP2PPeersRequest) (*schedulerv1.ListP2PPeersResponse, error) {
	peers := s.nodes.ListP2pPeers(
		req.GetClusterId(),
		req.GetBackend(),
		req.GetExcludeNodeId(),
		time.Now(),
	)
	return &schedulerv1.ListP2PPeersResponse{Peers: peers}, nil
}

func (s *Service) RecordP2PArtifact(_ context.Context, req *schedulerv1.RecordP2PArtifactRequest) (*schedulerv1.RecordP2PArtifactResponse, error) {
	if strings.TrimSpace(req.GetClusterId()) == "" || strings.TrimSpace(req.GetBackend()) == "" || strings.TrimSpace(req.GetKey()) == "" || strings.TrimSpace(req.GetNodeId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id, backend, key, and node_id are required")
	}
	if _, ok := s.nodes.Resolve(req.GetNodeId()); !ok {
		return nil, status.Error(codes.InvalidArgument, "node is not in scheduler node list")
	}

	s.artifacts.Record(req.GetClusterId(), req.GetBackend(), req.GetKey(), req.GetNodeId())
	s.logger.Debug("scheduler recorded P2P artifact",
		zap.String("cluster_id", req.GetClusterId()),
		zap.String("backend", req.GetBackend()),
		zap.String("key", req.GetKey()),
		zap.String("node_id", req.GetNodeId()),
	)
	return &schedulerv1.RecordP2PArtifactResponse{}, nil
}

func (s *Service) ForgetP2PArtifact(_ context.Context, req *schedulerv1.ForgetP2PArtifactRequest) (*schedulerv1.ForgetP2PArtifactResponse, error) {
	if strings.TrimSpace(req.GetClusterId()) == "" || strings.TrimSpace(req.GetBackend()) == "" || strings.TrimSpace(req.GetKey()) == "" || strings.TrimSpace(req.GetNodeId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id, backend, key, and node_id are required")
	}

	s.artifacts.Forget(req.GetClusterId(), req.GetBackend(), req.GetKey(), req.GetNodeId())
	s.logger.Debug("scheduler forgot P2P artifact",
		zap.String("cluster_id", req.GetClusterId()),
		zap.String("backend", req.GetBackend()),
		zap.String("key", req.GetKey()),
		zap.String("node_id", req.GetNodeId()),
	)
	return &schedulerv1.ForgetP2PArtifactResponse{}, nil
}

func (s *Service) LookupP2PArtifact(_ context.Context, req *schedulerv1.LookupP2PArtifactRequest) (*schedulerv1.LookupP2PArtifactResponse, error) {
	if strings.TrimSpace(req.GetClusterId()) == "" || strings.TrimSpace(req.GetBackend()) == "" || strings.TrimSpace(req.GetKey()) == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id, backend, and key are required")
	}

	nodeIDs := s.artifacts.Lookup(req.GetClusterId(), req.GetBackend(), req.GetKey())
	peers := s.nodes.FilterP2pPeers(
		req.GetClusterId(),
		req.GetBackend(),
		nodeIDs,
		req.GetExcludeNodeId(),
		time.Now(),
	)
	return &schedulerv1.LookupP2PArtifactResponse{Peers: peers}, nil
}

func (s *Service) GetNode(_ context.Context, req *schedulerv1.GetNodeRequest) (*schedulerv1.GetNodeResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	node, ok := s.nodes.GetObserved(nodeID, req.GetClusterId(), time.Now())
	if !ok {
		return nil, status.Error(codes.NotFound, "observed node not found")
	}

	return &schedulerv1.GetNodeResponse{Node: node}, nil
}

func (s *Service) UnregisterNode(_ context.Context, req *schedulerv1.UnregisterNodeRequest) (*schedulerv1.UnregisterNodeResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	serviceInstanceID := strings.TrimSpace(req.GetServiceInstanceId())
	if nodeID == "" || serviceInstanceID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and service_instance_id are required")
	}

	unregisterErr := s.nodes.UnregisterObserved(nodeID, serviceInstanceID)
	if unregisterErr != nil {
		if errors.Is(unregisterErr, ErrServiceInstanceMismatch) {
			return nil, status.Error(codes.FailedPrecondition, "service instance mismatch")
		}
		return nil, status.Error(codes.Internal, unregisterErr.Error())
	}

	now := time.Now()
	s.ledger.Forget(nodeID)
	// A node that comes back is asked for a fresh roster rather than
	// reconciled against whatever it had before it left, and is not credited
	// with the events of the process that left.
	s.rosters.forget(nodeID)
	s.eventLoss.forget(nodeID)
	if err := s.store.ReconcileNodeRoster(Node{ID: nodeID}, nil, RosterFinal, now); err != nil {
		s.logger.Warn("scheduler unregister binding reconcile failed",
			zap.String("node_id", nodeID),
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	s.artifacts.ForgetNode(nodeID)

	return &schedulerv1.UnregisterNodeResponse{}, nil
}

func (s *Service) isKnownNode(node Node) bool {
	return s.nodes.Contains(node)
}
