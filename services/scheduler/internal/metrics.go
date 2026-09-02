package scheduler

import (
	"context"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/observability"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
)

var (
	schedulerRPCDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_scheduler_rpc_duration_seconds",
			Help:    "Scheduler gRPC duration by RPC and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"rpc", "status"},
	)
	schedulerNodesFilteredTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_nodes_filtered_total",
			Help: "Placement candidates dropped by the health gate, by reason.",
		},
		[]string{"reason"},
	)
	schedulerScheduleDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_scheduler_schedule_duration_seconds",
			Help:    "Scheduler Schedule duration by strategy and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"strategy", "status"},
	)
	schedulerScheduleAssignments = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_schedule_assignments_total",
			Help: "Successful scheduler assignments by strategy.",
		},
		[]string{"strategy"},
	)
	schedulerObservedNodes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_observed_nodes",
			Help: "Observed node count by derived status.",
		},
		[]string{"status"},
	)
	// schedulerScheduleFailOpenTotal counts placements decided over stale
	// nodes because no fresh one existed. A steady rate is a scheduler that
	// cannot hear its fleet; a burst on startup is the deliberate window in
	// which the registry is empty and every node counts as unseen.
	schedulerScheduleFailOpenTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_schedule_failopen_total",
			Help: "Placements that fell open to stale candidates because every node failed the health gate.",
		},
	)
	// schedulerReservationDrift is how far, in sandboxes, the reservation
	// ledger had moved a node from its last heartbeat when the next heartbeat
	// replaced it. Fleet-wide by design: a node label would be unbounded at
	// fleet scale, and the distribution is what says whether the interval is
	// short enough for the ledger to matter and the clamp wide enough not to.
	schedulerReservationDrift = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "agentenv_scheduler_reservation_drift",
			Help:    "Sandboxes the reservation ledger had added to or removed from a node's reported count when a heartbeat replaced it.",
			Buckets: []float64{0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512},
		},
	)
	// The node-state stream: what one replica told the others about the nodes
	// only it heard from. None of these carries a node label — the fleet is the
	// unit here, and a per-node series would be unbounded at fleet scale.
	schedulerNodeStreamPublishedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_node_stream_published_total",
			Help: "Node state events this replica published for other replicas.",
		},
	)
	schedulerNodeStreamDroppedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_node_stream_dropped_total",
			Help: "Node state events lost rather than published or read, by reason.",
		},
		[]string{"reason"},
	)
	// The outcomes are exclusive: every event read counts once. Whether its
	// stamp had to be pulled back into this replica's clock is a separate
	// question about the same event, and has its own counter below.
	schedulerNodeStreamAppliedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_node_stream_applied_total",
			Help: "Replicated node state events by what this replica did with them.",
		},
		[]string{"outcome"},
	)
	// schedulerNodeStreamClampedTotal counts events whose stamp sat outside
	// this replica's own clock by more than the skew window. A steady rate is
	// replicas whose clocks disagree, which makes every freshness decision on
	// this replica wrong by that much.
	schedulerNodeStreamClampedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_node_stream_clamped_total",
			Help: "Replicated node state events whose freshness stamp was pulled back into the local clock's range.",
		},
	)
	// schedulerNodeStreamLagSeconds is how long ago the publishing replica saw
	// the node whose state is being applied. It is the single number that says
	// whether the replicas are converged: a p99 approaching the report TTL means
	// followers are placing on a view the health gate is about to discard.
	schedulerNodeStreamLagSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "agentenv_scheduler_node_stream_lag_seconds",
			Help:    "Age of a replicated node state event when this replica applied it.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60},
		},
	)
	// schedulerNodeStreamWarmupIncomplete is set when a starting replica could
	// not read back a full report TTL of history, so some live node may be
	// missing from its registry until that node's next heartbeat.
	schedulerNodeStreamWarmupIncomplete = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_node_stream_warmup_incomplete",
			Help: "1 when this replica started without replaying a full report TTL of node state.",
		},
	)
	// schedulerRegistryNodes splits the observed fleet by how this replica came
	// by it. On a converged fleet the rpc series is the replica's own share of
	// the nodes and the two series sum to the fleet.
	schedulerRegistryNodes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_registry_nodes",
			Help: "Observed nodes by how this replica learned of them.",
		},
		[]string{"source"},
	)
	// schedulerStoreReachable is what the readiness probe reads. A replica that
	// cannot reach its binding store answers every routing lookup with an
	// error, and gRPC's round-robin balancer keeps sending it a share of them
	// because an application error leaves the subchannel ready.
	schedulerStoreReachable = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_store_reachable",
			Help: "1 when this scheduler's binding store answered its last probe.",
		},
	)
	// schedulerMode reports which mode this process runs in, so a dashboard
	// shows a stray primary running beside the replicas.
	schedulerMode = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_mode",
			Help: "1 for the mode this scheduler process is running in.",
		},
		[]string{"mode"},
	)
	// schedulerP2PArtifactEvictionsTotal counts artifact keys the index
	// dropped for capacity. A non-zero rate means the fleet publishes more
	// distinct artifacts than scheduler.artifact_store_capacity holds, and
	// lookups for the evicted ones fall back to a broad peer poll.
	schedulerP2PArtifactEvictionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_p2p_artifact_evictions_total",
			Help: "P2P artifact index keys evicted for capacity.",
		},
	)
	// schedulerP2PLookupPeers is how many providers a lookup returned after
	// health filtering. Zeros are misses; a mass at the lookup limit means the
	// limit, not the fleet, is what bounds the answer.
	schedulerP2PLookupPeers = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "agentenv_scheduler_p2p_lookup_peers",
			Help:    "Providers returned per P2P artifact lookup.",
			Buckets: []float64{0, 1, 2, 4, 8, 16, 32, 64},
		},
	)
)

func MetricsUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		rpc := schedulerRPCLabel(info.FullMethod)
		if rpc == "" {
			return handler(ctx, req)
		}
		start := time.Now()
		resp, err := handler(ctx, req)
		recordSchedulerRPC(rpc, start, err)
		return resp, err
	}
}

func recordSchedulerRPC(rpc string, start time.Time, err error) {
	status := observability.GRPCStatusLabel(err)
	schedulerRPCDuration.WithLabelValues(rpc, status).Observe(time.Since(start).Seconds())
}

func recordSchedulerSchedule(strategy string, start time.Time, err error) {
	strategy = schedulerStrategyLabel(strategy)
	status := observability.GRPCStatusLabel(err)
	schedulerScheduleDuration.WithLabelValues(strategy, status).Observe(time.Since(start).Seconds())
	if err == nil {
		schedulerScheduleAssignments.WithLabelValues(strategy).Inc()
	}
}

func recordObservedNodes(nodes []*schedulerv1.ObservedNode) {
	counts := map[string]int{
		"ready":       0,
		"connecting":  0,
		"unhealthy":   0,
		"lingering":   0,
		"unspecified": 0,
	}
	for _, node := range nodes {
		counts[schedulerNodeStatusLabel(node.GetSnapshot().GetStatus())]++
	}
	for label, count := range counts {
		schedulerObservedNodes.WithLabelValues(label).Set(float64(count))
	}
}

func schedulerRPCLabel(fullMethod string) string {
	switch fullMethod[strings.LastIndex(fullMethod, "/")+1:] {
	case "Schedule":
		return "Schedule"
	case "ListNodes":
		return "ListNodes"
	case "LookupNode":
		return "LookupNode"
	case "RecordAssignment":
		return "RecordAssignment"
	case "RecordAssignments":
		return "RecordAssignments"
	case "Heartbeat":
		return "Heartbeat"
	case "ListObservedNodes":
		return "ListObservedNodes"
	case "ReportSandboxEvent":
		return "ReportSandboxEvent"
	case "GetNode":
		return "GetNode"
	case "UnregisterNode":
		return "UnregisterNode"
	case "ListP2pPeers":
		return "ListP2pPeers"
	case "RecordP2pArtifact":
		return "RecordP2pArtifact"
	case "ForgetP2pArtifact":
		return "ForgetP2pArtifact"
	case "LookupP2pArtifact":
		return "LookupP2pArtifact"
	case "UpsertMobilityRecord":
		return "UpsertMobilityRecord"
	case "GetMobilityRecord":
		return "GetMobilityRecord"
	case "ListMobilityRecords":
		return "ListMobilityRecords"
	case "RemoveMobilityRecord":
		return "RemoveMobilityRecord"
	default:
		return ""
	}
}

func schedulerStrategyLabel(strategy string) string {
	switch strings.ToLower(strings.TrimSpace(strategy)) {
	case "round_robin":
		return "round_robin"
	case "random":
		return "random"
	case "least_loaded_of_two", "p2c":
		return "least_loaded_of_two"
	case "bin_pack":
		return "bin_pack"
	default:
		return "unknown"
	}
}

func schedulerNodeStatusLabel(status schedulerv1.NodeStatus) string {
	switch status {
	case schedulerv1.NodeStatus_NODE_STATUS_READY:
		return "ready"
	case schedulerv1.NodeStatus_NODE_STATUS_CONNECTING:
		return "connecting"
	case schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY:
		return "unhealthy"
	case schedulerv1.NodeStatus_NODE_STATUS_LINGERING:
		return "lingering"
	default:
		return "unspecified"
	}
}

// recordSchedulerNodesFiltered counts placement candidates dropped by the
// health gate, by reason. Without this the fail-open path is invisible: a
// cluster-wide stall and a healthy fleet both look like "everything was
// eligible".
func recordSchedulerNodesFiltered(reason string, count int) {
	if count <= 0 {
		return
	}
	schedulerNodesFilteredTotal.WithLabelValues(reason).Add(float64(count))
}

// schedulerStaleIncarnationTotal counts reports rejected as coming from a
// replaced node process. A persistently non-zero rate for one node means its
// incarnation is pinned or duplicated, which defeats the guard.
var schedulerStaleIncarnationTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "agentenv_scheduler_stale_incarnation_total",
		Help: "Heartbeats rejected as originating from a superseded node process.",
	},
	[]string{"node_id"},
)

// Roster-digest outcomes. Without these the elision is invisible: a fleet
// where every heartbeat still carries a full roster and one where none do look
// the same from outside.
var (
	schedulerRosterCacheHitTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_roster_cache_hit_total",
			Help: "Heartbeats reconciled from a cached roster instead of the wire.",
		},
	)
	schedulerRosterFullRequestTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_roster_full_request_total",
			Help: "Heartbeats whose roster digest could not be resolved, so the full roster was requested.",
		},
	)
)

// schedulerStaleReportTotal counts heartbeats rejected because the same node
// process had already had a newer one applied. The usual source is a heartbeat
// the node gave up on at its deadline that was delivered anyway; a steady rate
// on one node means its heartbeats regularly outlive that deadline.
var schedulerStaleReportTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "agentenv_scheduler_stale_report_total",
		Help: "Heartbeats rejected because a newer report from the same node process had already been applied.",
	},
	[]string{"node_id"},
)

// schedulerSandboxEventsLostTotal counts lifecycle events a node reported
// emitting that never arrived. A persistently non-zero rate means the
// scheduler's short-term view of that node is running behind reality.
var schedulerSandboxEventsLostTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "agentenv_scheduler_sandbox_events_lost_total",
		Help: "Sandbox lifecycle events a node emitted that the scheduler never received.",
	},
	[]string{"node_id"},
)

// schedulerMobilityLookupTotal counts lookups a binding miss did not end.
//
// The three ways it can end differently are worth separating. `holder` means
// another node is mid-handover and the caller was sent there; `origin` means
// the paused state was never committed anywhere the cluster can read, so only
// the node that wrote it can serve it; `placed` means the state is committed
// and this lookup chose a node for it. A rising `origin` rate says paused
// sandboxes are not reaching the repository, which is the thing that makes
// them unmovable -- and it is invisible from the binding metrics alone.
var schedulerMobilityLookupTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "agentenv_scheduler_mobility_lookup_total",
		Help: "Sandbox lookups resolved from a mobility record after a binding miss.",
	},
	[]string{"via"},
)

func recordSchedulerMobilityLookup(via string) {
	schedulerMobilityLookupTotal.WithLabelValues(via).Inc()
}
