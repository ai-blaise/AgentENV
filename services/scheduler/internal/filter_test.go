package scheduler

import (
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"
)

func uint32Ptr(v uint32) *uint32 { return &v }
func uint64Ptr(v uint64) *uint64 { return &v }

func TestFilterNilLimitKeepsAll(t *testing.T) {
	nodes := []RichNode{
		{Node: Node{ID: "a"}},
		{Node: Node{ID: "b"}},
	}
	result := FilterByResourceLimit(nodes, nil)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
}

func TestFilterKeepsNodesWithoutSnapshot(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxSandboxCount: uint32Ptr(5)}
	nodes := []RichNode{
		{Node: Node{ID: "no-heartbeat"}, Snapshot: nil},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 {
		t.Fatalf("expected node without snapshot to be kept, got %d", len(result))
	}
}

func TestFilterMaxSandboxCount(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxSandboxCount: uint32Ptr(10)}
	nodes := []RichNode{
		{Node: Node{ID: "ok"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 5}},
		{Node: Node{ID: "at-limit"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 10}},
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 11}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].ID != "ok" || result[1].ID != "at-limit" {
		t.Fatalf("unexpected nodes: %s, %s", result[0].ID, result[1].ID)
	}
}

func TestFilterMaxSandboxStartingCount(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxSandboxStartingCount: uint32Ptr(2)}
	nodes := []RichNode{
		{Node: Node{ID: "ok"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxStartingCount: 1}},
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxStartingCount: 3}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 || result[0].ID != "ok" {
		t.Fatalf("expected [ok], got %v", result)
	}
}

func TestFilterMaxCPUUsedPercent(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxCPUUsedPercent: uint32Ptr(80)}
	nodes := []RichNode{
		{Node: Node{ID: "ok"}, Snapshot: &schedulerv1.NodeSnapshot{CpuPercent: 50}},
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{CpuPercent: 95}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 || result[0].ID != "ok" {
		t.Fatalf("expected [ok], got %v", result)
	}
}

func TestFilterMaxCPUAllocatedPercent(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxCPUAllocatedPercent: uint32Ptr(50)}
	nodes := []RichNode{
		{Node: Node{ID: "ok"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedCpu: 2, CpuCount: 8}},       // 25%
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedCpu: 6, CpuCount: 8}},     // 75%
		{Node: Node{ID: "zero-cpu"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedCpu: 5, CpuCount: 0}}, // skip check
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].ID != "ok" || result[1].ID != "zero-cpu" {
		t.Fatalf("unexpected: %s, %s", result[0].ID, result[1].ID)
	}
}

func TestFilterMaxMemoryUsedPercent(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxMemoryUsedPercent: uint32Ptr(70)}
	nodes := []RichNode{
		{Node: Node{ID: "ok"}, Snapshot: &schedulerv1.NodeSnapshot{MemoryUsedBytes: 60, MemoryTotalBytes: 100}},   // 60%
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{MemoryUsedBytes: 80, MemoryTotalBytes: 100}}, // 80%
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 || result[0].ID != "ok" {
		t.Fatalf("expected [ok], got %v", result)
	}
}

func TestFilterMaxMemoryAllocatedPercent(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxMemoryAllocatedPercent: uint32Ptr(50)}
	nodes := []RichNode{
		{Node: Node{ID: "ok"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedMemoryBytes: 40, MemoryTotalBytes: 100}},   // 40%
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedMemoryBytes: 60, MemoryTotalBytes: 100}}, // 60%
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 || result[0].ID != "ok" {
		t.Fatalf("expected [ok], got %v", result)
	}
}

func TestFilterMultipleLimits(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxSandboxCount:   uint32Ptr(10),
		MaxCPUUsedPercent: uint32Ptr(80),
	}
	nodes := []RichNode{
		{Node: Node{ID: "both-ok"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 5, CpuPercent: 50}},
		{Node: Node{ID: "sandbox-over"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 15, CpuPercent: 50}},
		{Node: Node{ID: "cpu-over"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 5, CpuPercent: 90}},
		{Node: Node{ID: "both-over"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 15, CpuPercent: 90}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 || result[0].ID != "both-ok" {
		t.Fatalf("expected [both-ok], got %v", result)
	}
}

func TestFilterAllExcludedReturnsEmpty(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxSandboxCount: uint32Ptr(0)}
	nodes := []RichNode{
		{Node: Node{ID: "a"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 1}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 0 {
		t.Fatalf("expected empty, got %d", len(result))
	}
}

func TestFilterMaxSandboxCountIncludingPaused(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxSandboxCountIncludingPaused: uint32Ptr(5)}
	nodes := []RichNode{
		// 2 running + 3 paused = 5, at limit, kept.
		{Node: Node{ID: "at-limit"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 2, PausedSandboxCount: 3}},
		// 3 running + 3 paused = 6, over.
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 3, PausedSandboxCount: 3}},
		// 0 running + 0 paused, kept.
		{Node: Node{ID: "empty"}, Snapshot: &schedulerv1.NodeSnapshot{}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].ID != "at-limit" || result[1].ID != "empty" {
		t.Fatalf("unexpected nodes: %s, %s", result[0].ID, result[1].ID)
	}
}

func TestFilterMaxAllocatedCPUIncludingPaused(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxAllocatedCPUIncludingPaused: uint32Ptr(8)}
	nodes := []RichNode{
		// 4 active + 4 paused = 8, at limit.
		{Node: Node{ID: "at-limit"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedCpu: 4, PausedAllocatedCpu: 4}},
		// 4 active + 5 paused = 9, over.
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedCpu: 4, PausedAllocatedCpu: 5}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 || result[0].ID != "at-limit" {
		t.Fatalf("expected [at-limit], got %v", result)
	}
}

func TestFilterMaxAllocatedMemoryBytesIncludingPaused(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxAllocatedMemoryBytesIncludingPaused: uint64Ptr(1000)}
	nodes := []RichNode{
		// 400 + 600 = 1000, at limit.
		{Node: Node{ID: "at-limit"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedMemoryBytes: 400, PausedAllocatedMemoryBytes: 600}},
		// 500 + 600 = 1100, over.
		{Node: Node{ID: "over"}, Snapshot: &schedulerv1.NodeSnapshot{AllocatedMemoryBytes: 500, PausedAllocatedMemoryBytes: 600}},
		// 0 + 0 = 0, kept.
		{Node: Node{ID: "empty"}, Snapshot: &schedulerv1.NodeSnapshot{}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].ID != "at-limit" || result[1].ID != "empty" {
		t.Fatalf("unexpected nodes: %s, %s", result[0].ID, result[1].ID)
	}
}

// A node that fits active limits but exceeds an "including paused" limit must
// still be excluded.
func TestFilterIncludingPausedExcludesNodeWithinActiveLimits(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxSandboxCount:                uint32Ptr(10),
		MaxSandboxCountIncludingPaused: uint32Ptr(5),
	}
	nodes := []RichNode{
		// Within active ceiling (2 <= 10) but over including-paused (2 + 4 = 6 > 5).
		{Node: Node{ID: "over-paused"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 2, PausedSandboxCount: 4}},
		// Within both.
		{Node: Node{ID: "ok"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 2, PausedSandboxCount: 2}},
	}
	result := FilterByResourceLimit(nodes, limit)
	if len(result) != 1 || result[0].ID != "ok" {
		t.Fatalf("expected [ok], got %v", result)
	}
}

func healthyNode(id string, lastSeen time.Time) RichNode {
	return RichNode{
		Node: Node{ID: id, Endpoint: "http://" + id},
		Health: ObservedHealth{
			Seen:           true,
			LastSeenUnixMs: lastSeen.UTC().UnixMilli(),
			Status:         schedulerv1.NodeStatus_NODE_STATUS_READY,
		},
	}
}

func TestFilterByHealthDropsStaleNode(t *testing.T) {
	now := time.Now()
	fresh := healthyNode("fresh", now)
	stale := healthyNode("stale", now.Add(-5*time.Minute))

	got, dropped, failedOpen := FilterByHealth([]RichNode{fresh, stale}, 30*time.Second, now)

	if failedOpen {
		t.Fatal("a fleet with a fresh node must not fail open")
	}
	if len(got) != 1 || got[0].ID != "fresh" {
		t.Fatalf("FilterByHealth kept %v, want only the fresh node", nodeIDsOf(got))
	}
	if dropped[HealthFilterReasonStale] != 1 {
		t.Fatalf("dropped = %v, want one stale", dropped)
	}
}

func TestFilterByHealthDropsNeverSeenAndUnhealthyAndDraining(t *testing.T) {
	now := time.Now()
	fresh := healthyNode("fresh", now)

	neverSeen := RichNode{Node: Node{ID: "never", Endpoint: "http://never"}}

	unhealthy := healthyNode("unhealthy", now)
	unhealthy.Health.Status = schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY

	draining := healthyNode("draining", now)
	draining.Health.Status = schedulerv1.NodeStatus_NODE_STATUS_LINGERING

	got, dropped, _ := FilterByHealth([]RichNode{fresh, neverSeen, unhealthy, draining}, 30*time.Second, now)

	if len(got) != 1 || got[0].ID != "fresh" {
		t.Fatalf("FilterByHealth kept %v, want only the fresh node", nodeIDsOf(got))
	}
	for _, reason := range []HealthFilterReason{
		HealthFilterReasonNeverSeen,
		HealthFilterReasonUnhealthy,
		HealthFilterReasonTerminating,
	} {
		if dropped[reason] != 1 {
			t.Fatalf("dropped[%s] = %d, want 1 (all: %v)", reason, dropped[reason], dropped)
		}
	}
}

// TestFilterByHealthFailsOpenWhenEveryNodeIsStale pins the deliberate
// asymmetry: a fleet-wide heartbeat stall is far more likely to be a scheduler
// problem than every node dying at once, so refusing all placement would turn
// a recoverable blip into a total outage. Partial staleness still fails closed.
func TestFilterByHealthFailsOpenWhenEveryNodeIsStale(t *testing.T) {
	now := time.Now()
	nodes := []RichNode{
		healthyNode("a", now.Add(-5*time.Minute)),
		healthyNode("b", now.Add(-6*time.Minute)),
	}

	got, dropped, failedOpen := FilterByHealth(nodes, 30*time.Second, now)

	if len(got) != 2 {
		t.Fatalf("FilterByHealth kept %v, want fail-open with both nodes", nodeIDsOf(got))
	}
	if !failedOpen {
		t.Fatal("returning every stale node must be reported as failing open")
	}
	// The drop reasons are still reported so the fail-open path is observable
	// rather than looking identical to a healthy fleet.
	if dropped[HealthFilterReasonStale] != 2 {
		t.Fatalf("dropped = %v, want both reported stale", dropped)
	}
}

func TestFilterByHealthDisabledTTLKeepsEverything(t *testing.T) {
	now := time.Now()
	nodes := []RichNode{healthyNode("a", now.Add(-time.Hour))}

	got, dropped, _ := FilterByHealth(nodes, 0, now)

	if len(got) != 1 {
		t.Fatalf("FilterByHealth kept %v, want the node when TTL is disabled", nodeIDsOf(got))
	}
	if len(dropped) != 0 {
		t.Fatalf("dropped = %v, want none", dropped)
	}
}

func nodeIDsOf(nodes []RichNode) []string {
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.ID)
	}
	return ids
}

// Placement and the node view GET /nodes renders must call the same node stale
// at the same instant. They used to compute it separately, and agreed only by
// coincidence; this drives both from one registry state across the boundary
// and asserts they never disagree.
func TestPlacementAndNodeViewAgreeOnStaleness(t *testing.T) {
	const ttl = 10 * time.Second
	start := time.Unix(1_700_000_000, 0)

	for _, tc := range []struct {
		name      string
		elapsed   time.Duration
		wantStale bool
	}{
		{name: "fresh", elapsed: time.Second, wantStale: false},
		{name: "exactly the ttl", elapsed: ttl, wantStale: false},
		{name: "one millisecond past", elapsed: ttl + time.Millisecond, wantStale: true},
		{name: "long past", elapsed: time.Hour, wantStale: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, ttl)
			if _, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
				NodeId:            "node-a",
				ClusterId:         "cluster",
				ServiceInstanceId: "svc-a",
				Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
			}, start); err != nil {
				t.Fatalf("heartbeat: %v", err)
			}
			now := start.Add(tc.elapsed)

			snapshot, health := registry.PeekObservedHealth("node-a")
			_, dropped, _ := FilterByHealth([]RichNode{{Node: Node{ID: "node-a"}, Snapshot: snapshot, Health: health}}, ttl, now)
			placementStale := dropped[HealthFilterReasonStale] == 1

			view, ok := registry.GetObserved("node-a", "cluster", now)
			if !ok {
				t.Fatal("observed node missing")
			}
			viewStale := view.GetSnapshot().GetStatus() == schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY

			if placementStale != tc.wantStale {
				t.Fatalf("placement stale = %v, want %v", placementStale, tc.wantStale)
			}
			if viewStale != placementStale {
				t.Fatalf("node view stale = %v but placement stale = %v; the two must agree", viewStale, placementStale)
			}
		})
	}
}
