package scheduler

import (
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"
)

// HealthFilterReason explains why FilterByHealth dropped a node. It is a closed
// set so it is safe to use as a metric label.
type HealthFilterReason string

const (
	HealthFilterReasonNeverSeen   HealthFilterReason = "never_seen"
	HealthFilterReasonStale       HealthFilterReason = "stale"
	HealthFilterReasonUnhealthy   HealthFilterReason = "unhealthy"
	HealthFilterReasonTerminating HealthFilterReason = "terminating"
)

// FilterByHealth removes nodes that are not currently fit to receive new
// sandboxes: never heartbeated, heartbeat older than reportTTL, or
// self-reported as unhealthy or draining.
//
// Placement previously read the last snapshot verbatim and kept nodes with no
// snapshot at all, so under static discovery a node that had been dead for
// hours still took its share of every create.
//
// The filter fails *open* when it would otherwise reject every candidate. A
// cluster-wide heartbeat stall — a scheduler restart, a network partition on
// the scheduler side — is far more likely than every node in the fleet dying
// at once, and refusing all placement in that case turns a recoverable blip
// into a total outage. Partial staleness still fails closed, which is the case
// this filter exists for. The returned reasons describe the dropped nodes even
// when the fail-open path returns them all, so the decision is observable.
func FilterByHealth(nodes []RichNode, reportTTL time.Duration, now time.Time) ([]RichNode, map[HealthFilterReason]int) {
	if len(nodes) == 0 {
		return nodes, nil
	}

	healthy := make([]RichNode, 0, len(nodes))
	var dropped map[HealthFilterReason]int
	drop := func(reason HealthFilterReason) {
		if dropped == nil {
			dropped = make(map[HealthFilterReason]int, 4)
		}
		dropped[reason]++
	}

	nowMs := now.UTC().UnixMilli()
	for _, n := range nodes {
		switch {
		case !n.Health.Seen:
			drop(HealthFilterReasonNeverSeen)
		case reportTTL > 0 && nowMs-n.Health.LastSeenUnixMs > reportTTL.Milliseconds():
			drop(HealthFilterReasonStale)
		case n.Health.Status == schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY:
			drop(HealthFilterReasonUnhealthy)
		case n.Health.Status == schedulerv1.NodeStatus_NODE_STATUS_LINGERING:
			drop(HealthFilterReasonTerminating)
		default:
			healthy = append(healthy, n)
		}
	}

	if len(healthy) == 0 {
		return nodes, dropped
	}
	return healthy, dropped
}

// FilterByResourceLimit removes nodes that exceed any configured resource
// threshold. Nodes without a heartbeat snapshot are always kept (they have no
// metrics to evaluate). A nil limit disables all filtering.
func FilterByResourceLimit(nodes []RichNode, limit *config.NodeResourceLimit) []RichNode {
	if limit == nil {
		return nodes
	}

	result := make([]RichNode, 0, len(nodes))
	for _, n := range nodes {
		if n.Snapshot == nil {
			// No heartbeat yet — cannot evaluate limits; keep the node.
			result = append(result, n)
			continue
		}
		if !withinLimit(n, limit) {
			continue
		}
		result = append(result, n)
	}
	return result
}

func withinLimit(n RichNode, limit *config.NodeResourceLimit) bool {
	s := n.Snapshot

	if limit.MaxSandboxCount != nil && s.GetSandboxCount() > *limit.MaxSandboxCount {
		return false
	}
	if limit.MaxSandboxStartingCount != nil && s.GetSandboxStartingCount() > *limit.MaxSandboxStartingCount {
		return false
	}
	if limit.MaxCPUUsedPercent != nil && s.GetCpuPercent() > *limit.MaxCPUUsedPercent {
		return false
	}
	if limit.MaxCPUAllocatedPercent != nil {
		if s.GetCpuCount() > 0 {
			allocatedPercent := s.GetAllocatedCpu() * 100 / s.GetCpuCount()
			if allocatedPercent > *limit.MaxCPUAllocatedPercent {
				return false
			}
		}
	}
	if limit.MaxMemoryUsedPercent != nil {
		if s.GetMemoryTotalBytes() > 0 {
			usedPercent := uint32(s.GetMemoryUsedBytes() * 100 / s.GetMemoryTotalBytes())
			if usedPercent > *limit.MaxMemoryUsedPercent {
				return false
			}
		}
	}
	if limit.MaxMemoryAllocatedPercent != nil {
		if s.GetMemoryTotalBytes() > 0 {
			allocatedPercent := uint32(s.GetAllocatedMemoryBytes() * 100 / s.GetMemoryTotalBytes())
			if allocatedPercent > *limit.MaxMemoryAllocatedPercent {
				return false
			}
		}
	}

	// "Including paused" ceilings sum the active running set with the paused
	// reservations reported in the snapshot. A node exceeding any of these is
	// dropped from scheduling candidates regardless of whether the active-only
	// counters are within limits.
	if limit.MaxSandboxCountIncludingPaused != nil {
		total := s.GetSandboxCount() + s.GetPausedSandboxCount()
		if total > *limit.MaxSandboxCountIncludingPaused {
			return false
		}
	}
	if limit.MaxAllocatedCPUIncludingPaused != nil {
		total := s.GetAllocatedCpu() + s.GetPausedAllocatedCpu()
		if total > *limit.MaxAllocatedCPUIncludingPaused {
			return false
		}
	}
	if limit.MaxAllocatedMemoryBytesIncludingPaused != nil {
		total := s.GetAllocatedMemoryBytes() + s.GetPausedAllocatedMemoryBytes()
		if total > *limit.MaxAllocatedMemoryBytesIncludingPaused {
			return false
		}
	}
	return true
}


// FilterExcludedNodes removes nodes the caller has already tried.
//
// Unlike FilterByHealth this does not fail open: the exclusions are facts
// reported by the nodes themselves ("I refused this sandbox"), not inferences
// from stale state, so returning an excluded node would put the caller into a
// retry loop against a node that has already said no.
func FilterExcludedNodes(nodes []RichNode, excludeNodeIDs []string) []RichNode {
	if len(excludeNodeIDs) == 0 || len(nodes) == 0 {
		return nodes
	}

	excluded := make(map[string]struct{}, len(excludeNodeIDs))
	for _, nodeID := range excludeNodeIDs {
		nodeID = strings.TrimSpace(nodeID)
		if nodeID == "" {
			continue
		}
		excluded[nodeID] = struct{}{}
	}
	if len(excluded) == 0 {
		return nodes
	}

	result := make([]RichNode, 0, len(nodes))
	for _, n := range nodes {
		if _, skip := excluded[n.ID]; skip {
			continue
		}
		result = append(result, n)
	}
	return result
}
