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
