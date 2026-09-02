package scheduler

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
)

func TestListObservedFiltersByCluster(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}, {ID: "node-b", Endpoint: "http://node-b"}}, 30*time.Second)
	now := time.Unix(100, 0)

	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, now)
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-b",
		ClusterId:         "cluster-b",
		ServiceInstanceId: "svc-b",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, now)

	nodes := registry.ListObserved("cluster-a", now)
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if got := nodes[0].GetNodeId(); got != "node-a" {
		t.Fatalf("expected node-a, got %s", got)
	}
}

func TestNodeRegistryHeartbeatRejectsUnknownNode(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)

	_, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
	}, time.Unix(100, 0))
	if !errors.Is(err, ErrNodeNotInRegistry) {
		t.Fatalf("expected ErrNodeNotInRegistry, got %v", err)
	}
}

func TestObservedNodeBecomesUnhealthyAfterTTL(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Second)
	start := time.Unix(100, 0)

	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, start)

	node, ok := registry.GetObserved("node-a", "cluster-a", start.Add(2*time.Second))
	if !ok {
		t.Fatal("expected observed node")
	}
	if got := node.GetSnapshot().GetStatus(); got != schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY {
		t.Fatalf("expected status %v, got %v", schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY, got)
	}
}

func TestObservedNodeUsesLatestKnownEndpoint(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	now := time.Unix(100, 0)

	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, now)

	registry.Set([]Node{{ID: "node-a", Endpoint: "http://node-a-new"}}, nil)

	node, ok := registry.GetObserved("node-a", "cluster-a", now)
	if !ok {
		t.Fatal("expected observed node")
	}
	if got := node.GetEndpoint(); got != "http://node-a-new" {
		t.Fatalf("expected updated endpoint %q, got %q", "http://node-a-new", got)
	}
}

func TestHeartbeatStoresP2PEndpointForPeerListing(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	now := time.Unix(100, 0)
	endpoint := &schedulerv1.P2PEndpoint{
		Backend: "iroh",
		Address: `{"id":"node-id"}`,
	}

	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		P2PEndpoint:       endpoint,
	}, now)

	peers := registry.ListP2pPeers("cluster-a", "iroh", "", now)
	if len(peers) != 1 {
		t.Fatalf("expected one P2P peer, got %d", len(peers))
	}
	got := peers[0].GetEndpoint()
	if got.GetBackend() != endpoint.GetBackend() || got.GetAddress() != endpoint.GetAddress() {
		t.Fatalf("unexpected P2P endpoint: %+v", got)
	}

	node, ok := registry.GetObserved("node-a", "cluster-a", now)
	if !ok {
		t.Fatal("expected observed node")
	}
	if node.ProtoReflect().Descriptor().Fields().ByName("p2p_endpoint") != nil {
		t.Fatal("ObservedNode must not expose p2p_endpoint")
	}
}

func TestUnregisterRemovesP2PEndpointPeer(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	now := time.Unix(100, 0)

	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		P2PEndpoint:       &schedulerv1.P2PEndpoint{Backend: "iroh", Address: `{"id":"node-id"}`},
	}, now)
	if err := registry.UnregisterObserved("node-a", "svc-a"); err != nil {
		t.Fatalf("unregister: %v", err)
	}

	if nodes := registry.ListP2pPeers("cluster-a", "iroh", "", now); len(nodes) != 0 {
		t.Fatalf("expected no observed P2P peers after unregister, got %d", len(nodes))
	}
}

func TestListP2pPeersReturnsOnlyReadyMatchingPeers(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
		{ID: "node-c", Endpoint: "http://node-c"},
	}, defaultObservedReportTTL)
	now := time.Unix(100, 0)

	for _, nodeID := range []string{"node-a", "node-b"} {
		registry.Heartbeat(&schedulerv1.HeartbeatRequest{
			NodeId:            nodeID,
			ClusterId:         "cluster-1",
			ServiceInstanceId: "svc-" + nodeID,
			Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
			P2PEndpoint: &schedulerv1.P2PEndpoint{
				Backend: "iroh",
				Address: nodeID + "-iroh-endpoint",
			},
		}, now)
	}
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-c",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-node-c",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		P2PEndpoint:       &schedulerv1.P2PEndpoint{Backend: "other", Address: "node-c-other-endpoint"},
	}, now)

	peers := registry.ListP2pPeers("cluster-1", "iroh", "node-a", now)
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
	if got := peers[0].GetNodeId(); got != "node-b" {
		t.Fatalf("expected node-b, got %s", got)
	}
	if got := peers[0].GetEndpoint().GetAddress(); got != "node-b-iroh-endpoint" {
		t.Fatalf("unexpected endpoint: %s", got)
	}
}

func TestListP2pPeersDropsExpiredAndUnregisteredNodes(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, time.Second)
	start := time.Unix(100, 0)
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		P2PEndpoint:       &schedulerv1.P2PEndpoint{Backend: "iroh", Address: "node-a-iroh-endpoint"},
	}, start)
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-b",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-b",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		P2PEndpoint:       &schedulerv1.P2PEndpoint{Backend: "iroh", Address: "node-b-iroh-endpoint"},
	}, start)
	if err := registry.UnregisterObserved("node-b", "svc-b"); err != nil {
		t.Fatalf("unregister node-b: %v", err)
	}

	peers := registry.ListP2pPeers("cluster-1", "iroh", "", start.Add(2*time.Second))
	if len(peers) != 0 {
		t.Fatalf("expected no peers after ttl/unregister filtering, got %d", len(peers))
	}
}

func TestListObservedReturnsEmptySliceWhenNoNodes(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, 30*time.Second)
	nodes := registry.ListObserved("", time.Now())
	if len(nodes) != 0 {
		t.Fatalf("expected empty slice, got %d nodes", len(nodes))
	}
}

func TestListObservedReturnsEmptySliceWhenClusterFilterMatchesNothing(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	now := time.Unix(100, 0)

	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, now)

	nodes := registry.ListObserved("cluster-z", now)
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes for unknown cluster, got %d", len(nodes))
	}
}

func TestListObservedBecomesUnhealthyAfterTTL(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, time.Second)
	start := time.Unix(100, 0)

	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, start)

	nodes := registry.ListObserved("", start.Add(2*time.Second))
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if got := nodes[0].GetSnapshot().GetStatus(); got != schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY {
		t.Fatalf("expected UNHEALTHY via ListObserved, got %v", got)
	}
}

func TestLingeringNodeBecomesUnhealthyAfterTTL(t *testing.T) {
	registry := NewAtomicNodeRegistry(nil, time.Second)
	start := time.Unix(100, 0)

	registry.Set(nil, []Node{{ID: "node-a", Endpoint: "http://node-a"}})
	registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-a",
		ServiceInstanceId: "svc-a",
		Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}, start)

	// Within TTL: no_schedule
	node, ok := registry.GetObserved("node-a", "", start)
	if !ok {
		t.Fatal("expected observed node")
	}
	if got := node.GetSnapshot().GetStatus(); got != schedulerv1.NodeStatus_NODE_STATUS_LINGERING {
		t.Fatalf("expected NO_SCHEDULE within TTL, got %v", got)
	}

	// After TTL: unhealthy overrides no_schedule
	node, ok = registry.GetObserved("node-a", "", start.Add(2*time.Second))
	if !ok {
		t.Fatal("expected observed node")
	}
	if got := node.GetSnapshot().GetStatus(); got != schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY {
		t.Fatalf("expected UNHEALTHY after TTL, got %v", got)
	}
}

func heartbeatWithConfig(t *testing.T, registry *AtomicNodeRegistry, nodeID, clusterID, svcID, cpuJSON string) string {
	t.Helper()
	var mi *schedulerv1.MachineInfo
	if cpuJSON != "" {
		mi = &schedulerv1.MachineInfo{CpuConfigJson: cpuJSON}
	}
	_, cpuConfigJSON, err := registry.Heartbeat(
		&schedulerv1.HeartbeatRequest{
			NodeId:            nodeID,
			ClusterId:         clusterID,
			ServiceInstanceId: svcID,
			MachineInfo:       mi,
		},
		time.Unix(100, 0),
	)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	return cpuConfigJSON
}

func TestHeartbeatReturnsCpuIntersectionForSingleNode(t *testing.T) {
	cfg := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xFF)}},
	}}, nil)

	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)

	result := heartbeatWithConfig(t, registry, "node-a", "cluster-1", "svc-a", cfg)
	if result == "" {
		t.Fatal("single-node cluster: expected intersection in response, got empty")
	}
	// Intersection of one config is the config itself.
	var got cpuConfig
	if err := json.Unmarshal([]byte(result), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.CpuidModifiers[0].Modifiers[0].Bitmap != bm32(0xFF) {
		t.Errorf("want %s, got %s", bm32(0xFF), got.CpuidModifiers[0].Modifiers[0].Bitmap)
	}
}

func TestHeartbeatWithholdsCpuIntersectionUntilAllNodesReady(t *testing.T) {
	cfg := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xFF)}},
	}}, nil)

	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, 30*time.Second)

	// Both nodes observed without CPU config so the cluster size is known.
	heartbeatWithConfig(t, registry, "node-a", "cluster-1", "svc-a", "")
	heartbeatWithConfig(t, registry, "node-b", "cluster-1", "svc-b", "")

	// node-a reports config but node-b has none yet: intersection not computable.
	result := heartbeatWithConfig(t, registry, "node-a", "cluster-1", "svc-a", cfg)
	if result != "" {
		t.Errorf("heartbeat before all observed nodes have configs: expected empty, got non-empty")
	}
}

func TestHeartbeatDeliversIntersectionExactlyOncePerNode(t *testing.T) {
	cfgA := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xFF)}},
	}}, nil)
	cfgB := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0x0F)}},
	}}, nil)

	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, 30*time.Second)

	// Seed both nodes without CPU config so the cluster size is known.
	heartbeatWithConfig(t, registry, "node-a", "cluster-1", "svc-a", "")
	heartbeatWithConfig(t, registry, "node-b", "cluster-1", "svc-b", "")

	// node-a reports config: node-b still has none, intersection not computable.
	if r := heartbeatWithConfig(t, registry, "node-a", "cluster-1", "svc-a", cfgA); r != "" {
		t.Errorf("node-a 1st heartbeat: expected empty, got non-empty")
	}

	// node-b reports: triggers intersection, node-b gets it immediately.
	resultB := heartbeatWithConfig(t, registry, "node-b", "cluster-1", "svc-b", cfgB)
	if resultB == "" {
		t.Fatal("node-b heartbeat: expected intersection, got empty")
	}

	// node-a second heartbeat (no config): should now receive the intersection.
	resultA2 := heartbeatWithConfig(t, registry, "node-a", "cluster-1", "svc-a", "")
	if resultA2 == "" {
		t.Fatal("node-a 2nd heartbeat: expected intersection, got empty")
	}

	// Once computed, the intersection is returned on every heartbeat, not just
	// the first. The scheduler holds it only in memory, so a restarted
	// scheduler can rebuild and redeliver it from what nodes keep reporting.
	if r := heartbeatWithConfig(t, registry, "node-a", "cluster-1", "svc-a", ""); r != resultA2 {
		t.Errorf("node-a 3rd heartbeat: intersection must be redelivered, got %q", r)
	}
	if r := heartbeatWithConfig(t, registry, "node-b", "cluster-1", "svc-b", ""); r != resultA2 {
		t.Errorf("node-b 2nd heartbeat: intersection must be redelivered, got %q", r)
	}

	// Verify the intersection value is the bitwise AND of both configs.
	var result cpuConfig
	if err := json.Unmarshal([]byte(resultA2), &result); err != nil {
		t.Fatalf("unmarshal intersection: %v", err)
	}
	want := bm32(0xFF & 0x0F)
	got := result.CpuidModifiers[0].Modifiers[0].Bitmap
	if got != want {
		t.Errorf("intersection eax: want %s, got %s", want, got)
	}
}

func TestMultiClusterCpuIntersectionsAreIndependent(t *testing.T) {
	// cluster-x has nodes with bitmaps 0xFF and 0x0F → intersection 0x0F.
	// cluster-y has a single node with bitmap 0xF0 → intersection 0xF0.
	// Neither cluster's intersection should bleed into the other.
	cfgX1 := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xFF)}},
	}}, nil)
	cfgX2 := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0x0F)}},
	}}, nil)
	cfgY := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xF0)}},
	}}, nil)

	registry := NewAtomicNodeRegistry([]Node{
		{ID: "x1", Endpoint: "http://x1"},
		{ID: "x2", Endpoint: "http://x2"},
		{ID: "y1", Endpoint: "http://y1"},
	}, 30*time.Second)

	// Seed without configs so cluster sizes are known before any config arrives.
	heartbeatWithConfig(t, registry, "x1", "cluster-x", "svc-x1", "")
	heartbeatWithConfig(t, registry, "x2", "cluster-x", "svc-x2", "")
	heartbeatWithConfig(t, registry, "y1", "cluster-y", "svc-y1", "")

	// x1 and x2 report configs; x2's heartbeat triggers the cluster-x intersection.
	heartbeatWithConfig(t, registry, "x1", "cluster-x", "svc-x1", cfgX1)
	heartbeatWithConfig(t, registry, "x2", "cluster-x", "svc-x2", cfgX2)

	// y1 reports its config; single-node cluster, gets its own config back.
	resultY := heartbeatWithConfig(t, registry, "y1", "cluster-y", "svc-y1", cfgY)
	if resultY == "" {
		t.Fatal("cluster-y single node: expected intersection, got empty")
	}

	// Collect the cluster-x intersection (delivered on the next heartbeat for x1).
	resultX := heartbeatWithConfig(t, registry, "x1", "cluster-x", "svc-x1", "")
	if resultX == "" {
		t.Fatal("cluster-x x1: expected intersection, got empty")
	}

	// cluster-x intersection must be 0xFF & 0x0F = 0x0F.
	var ix cpuConfig
	if err := json.Unmarshal([]byte(resultX), &ix); err != nil {
		t.Fatalf("unmarshal cluster-x intersection: %v", err)
	}
	if got, want := ix.CpuidModifiers[0].Modifiers[0].Bitmap, bm32(0xFF&0x0F); got != want {
		t.Errorf("cluster-x eax: want %s, got %s", want, got)
	}

	// cluster-y intersection must be 0xF0 (unaffected by cluster-x nodes).
	var iy cpuConfig
	if err := json.Unmarshal([]byte(resultY), &iy); err != nil {
		t.Fatalf("unmarshal cluster-y intersection: %v", err)
	}
	if got, want := iy.CpuidModifiers[0].Modifiers[0].Bitmap, bm32(0xF0); got != want {
		t.Errorf("cluster-y eax: want %s, got %s", want, got)
	}

	// Redelivery stays scoped to the node's own cluster: cluster-x nodes keep
	// receiving cluster-x's intersection and never cluster-y's.
	if r := heartbeatWithConfig(t, registry, "x2", "cluster-x", "svc-x2", ""); r != resultX {
		t.Errorf("cluster-x x2: want cluster-x intersection redelivered, got %q", r)
	}
	if r := heartbeatWithConfig(t, registry, "y1", "cluster-y", "svc-y1", ""); r != resultY {
		t.Errorf("cluster-y y1: want cluster-y intersection redelivered, got %q", r)
	}
}

// TestIntersectionRecomputedByFreshRegistry is the scheduler-restart case. The
// registry is in-memory, so a restarted scheduler starts with no observed
// nodes and no intersection. It must be able to rebuild and deliver one from
// the heartbeats that keep arriving; previously delivery was gated to once per
// node per process, so a restarted scheduler could never deliver again and new
// sandboxes booted with node-local CPU features.
func TestIntersectionRecomputedByFreshRegistry(t *testing.T) {
	nodes := []Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}
	cfgA := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0xFF)}},
	}}, nil)
	cfgB := buildConfig(nil, []cpuidModifier{{
		Leaf: "0x1", Subleaf: "0x0", Flags: 0,
		Modifiers: []registerMod{{Register: "eax", Bitmap: bm32(0x0F)}},
	}}, nil)

	original := NewAtomicNodeRegistry(nodes, defaultObservedReportTTL)
	heartbeatWithConfig(t, original, "node-a", "cluster-1", "svc-a", cfgA)
	want := heartbeatWithConfig(t, original, "node-b", "cluster-1", "svc-b", cfgB)
	if want == "" {
		t.Fatal("original registry produced no intersection")
	}

	// A fresh registry stands in for a restarted scheduler process.
	restarted := NewAtomicNodeRegistry(nodes, defaultObservedReportTTL)
	heartbeatWithConfig(t, restarted, "node-a", "cluster-1", "svc-a", cfgA)
	got := heartbeatWithConfig(t, restarted, "node-b", "cluster-1", "svc-b", cfgB)
	if got != want {
		t.Fatalf("restarted registry intersection = %q, want %q", got, want)
	}

	// And it keeps delivering on later heartbeats.
	if again := heartbeatWithConfig(t, restarted, "node-a", "cluster-1", "svc-a", cfgA); again != want {
		t.Fatalf("restarted registry stopped delivering: %q", again)
	}
}

// A node process that has been replaced must not be able to overwrite the live
// one's state. The usual cause is an RPC delayed behind a restart.
func TestHeartbeatRejectsSupersededIncarnation(t *testing.T) {
	registry := NewAtomicNodeRegistry(
		[]Node{{ID: "node-a", Endpoint: "http://node-a"}},
		defaultObservedReportTTL,
	)

	// UUIDv7 sorts in time order, so the second is the later process.
	older := "0199a000-0000-7000-8000-000000000001"
	newer := "0199b000-0000-7000-8000-000000000002"

	heartbeatWithConfig(t, registry, "node-a", "cluster-1", newer, "")

	_, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-1",
		ServiceInstanceId: older,
		Snapshot:          &schedulerv1.NodeSnapshot{SandboxCount: 999},
	}, time.Now())

	if !errors.Is(err, ErrStaleIncarnation) {
		t.Fatalf("err = %v, want ErrStaleIncarnation", err)
	}

	observed, ok := registry.GetObserved("node-a", "cluster-1", time.Now())
	if !ok {
		t.Fatal("node should still be observed")
	}
	if observed.GetServiceInstanceId() != newer {
		t.Fatalf("service instance = %q, want the live one %q", observed.GetServiceInstanceId(), newer)
	}
	if observed.GetSnapshot().GetSandboxCount() == 999 {
		t.Fatal("a superseded process overwrote the live snapshot")
	}
}

// The live process keeps working, and a genuine restart to a newer incarnation
// takes over. Locking a node out on restart would be worse than the race.
func TestHeartbeatAcceptsSameAndNewerIncarnations(t *testing.T) {
	registry := NewAtomicNodeRegistry(
		[]Node{{ID: "node-a", Endpoint: "http://node-a"}},
		defaultObservedReportTTL,
	)
	first := "0199a000-0000-7000-8000-000000000001"
	second := "0199b000-0000-7000-8000-000000000002"

	heartbeatWithConfig(t, registry, "node-a", "cluster-1", first, "")
	heartbeatWithConfig(t, registry, "node-a", "cluster-1", first, "")
	heartbeatWithConfig(t, registry, "node-a", "cluster-1", second, "")

	observed, ok := registry.GetObserved("node-a", "cluster-1", time.Now())
	if !ok || observed.GetServiceInstanceId() != second {
		t.Fatalf("restart should take over; got %q", observed.GetServiceInstanceId())
	}
}

// An empty incarnation means "unknown" and must neither displace a live one nor
// lock the node out.
func TestIncarnationSupersedesTreatsEmptyAsUnknown(t *testing.T) {
	newer := Incarnation("0199b000-0000-7000-8000-000000000002")
	older := Incarnation("0199a000-0000-7000-8000-000000000001")

	if !newer.Supersedes(older) {
		t.Fatal("a later UUIDv7 must supersede an earlier one")
	}
	if older.Supersedes(newer) {
		t.Fatal("an earlier UUIDv7 must not supersede a later one")
	}
	if newer.Supersedes("") || Incarnation("").Supersedes(newer) {
		t.Fatal("an unknown incarnation must not order against anything")
	}
	if newer.Supersedes(newer) {
		t.Fatal("an incarnation must not supersede itself")
	}
}

// Within one incarnation, reports are ordered by the stamp the node put on
// them. Arrival order is not send order: a heartbeat the node gave up on at its
// deadline is still delivered, by which time a newer one may have been applied,
// and applying the old one afterwards would reconcile a roster the node has
// moved past.
func TestHeartbeatRejectsAnOlderReportFromTheSameIncarnation(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	instance := "0199a000-0000-7000-8000-000000000001"
	now := time.Unix(100, 0)

	beat := func(instance string, reportedAt int64, count uint32, at time.Time) error {
		_, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
			NodeId:            "node-a",
			ClusterId:         "cluster-1",
			ServiceInstanceId: instance,
			Snapshot:          &schedulerv1.NodeSnapshot{ReportedAtUnixMs: reportedAt, SandboxCount: count},
		}, at)
		return err
	}
	count := func() uint32 {
		return registry.PeekObserved("node-a").GetSandboxCount()
	}

	if err := beat(instance, 2000, 2, now); err != nil {
		t.Fatalf("first report: %v", err)
	}
	if err := beat(instance, 1000, 999, now); !errors.Is(err, ErrStaleReport) {
		t.Fatalf("an older report was applied: err = %v", err)
	}
	if got := count(); got != 2 {
		t.Fatalf("the older report overwrote the applied snapshot: count = %d", got)
	}

	// A retried heartbeat is the same report, not an older one.
	if err := beat(instance, 2000, 3, now); err != nil {
		t.Fatalf("a repeated report was refused: %v", err)
	}
	if err := beat(instance, 2500, 4, now); err != nil {
		t.Fatalf("a newer report was refused: %v", err)
	}
	if got := count(); got != 4 {
		t.Fatalf("count = %d, want the newest report's 4", got)
	}

	// A restart is a new incarnation, and its stamps are not ordered against
	// the old one's.
	if err := beat("0199b000-0000-7000-8000-000000000002", 100, 5, now); err != nil {
		t.Fatalf("a newer incarnation with an older stamp was refused: %v", err)
	}
	if got := count(); got != 5 {
		t.Fatalf("count = %d, want the new incarnation's 5", got)
	}
}

// A node that does not stamp its snapshots has nothing to order by and is never
// refused. The scheduler stamps the view it serves of such a snapshot with its
// own clock, and that stamp must not be what a later report is measured against.
func TestHeartbeatOrderingIgnoresUnstampedReports(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, 30*time.Second)
	now := time.Unix(100, 0)
	beat := func(reportedAt int64) error {
		_, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
			NodeId:            "node-a",
			ClusterId:         "cluster-1",
			ServiceInstanceId: "svc-a",
			Snapshot:          &schedulerv1.NodeSnapshot{ReportedAtUnixMs: reportedAt},
		}, now)
		return err
	}

	if err := beat(0); err != nil {
		t.Fatalf("unstamped: %v", err)
	}
	if err := beat(0); err != nil {
		t.Fatalf("unstamped again: %v", err)
	}
	// The served view carries the scheduler's stamp, which is far later than
	// this node-side value; it must not be compared against it.
	if err := beat(1000); err != nil {
		t.Fatalf("a stamped report after unstamped ones was refused: %v", err)
	}
	if err := beat(0); err != nil {
		t.Fatalf("an unstamped report after a stamped one was refused: %v", err)
	}
}

// The ordering fence lapses with the report TTL. A node whose clock stepped
// backwards would otherwise be refused for as long as the step, and its
// bindings would expire under it; once its applied record has gone stale the
// next report is taken as the new baseline.
func TestHeartbeatOrderingLapsesWithTheReportTTL(t *testing.T) {
	const ttl = 30 * time.Second
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, ttl)
	start := time.Unix(100, 0)
	beat := func(reportedAt int64, at time.Time) error {
		_, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
			NodeId:            "node-a",
			ClusterId:         "cluster-1",
			ServiceInstanceId: "svc-a",
			Snapshot:          &schedulerv1.NodeSnapshot{ReportedAtUnixMs: reportedAt},
		}, at)
		return err
	}

	if err := beat(2000, start); err != nil {
		t.Fatalf("first report: %v", err)
	}
	if err := beat(1000, start.Add(ttl)); !errors.Is(err, ErrStaleReport) {
		t.Fatalf("an older report inside the ttl was applied: err = %v", err)
	}
	if err := beat(1000, start.Add(ttl+time.Millisecond)); err != nil {
		t.Fatalf("an older report after the ttl lapsed was still refused: %v", err)
	}
}

// Unregister is an incarnation's last word. Deleting its record used to delete
// the fence with it, so any heartbeat at all re-registered the node — including
// the departing process's own, still in flight behind the unregister.
func TestUnregisterFencesTheDepartedIncarnation(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	older := "0199a000-0000-7000-8000-000000000001"
	newer := "0199b000-0000-7000-8000-000000000002"
	newest := "0199c000-0000-7000-8000-000000000003"
	beat := func(instance string) error {
		_, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
			NodeId:            "node-a",
			ClusterId:         "cluster-1",
			ServiceInstanceId: instance,
			Snapshot:          &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		}, time.Now())
		return err
	}

	if err := beat(newer); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := registry.UnregisterObserved("node-a", newer); err != nil {
		t.Fatalf("unregister: %v", err)
	}

	if err := beat(older); !errors.Is(err, ErrStaleIncarnation) {
		t.Fatalf("a superseded incarnation re-registered after unregister: err = %v", err)
	}
	if err := beat(newer); !errors.Is(err, ErrStaleIncarnation) {
		t.Fatalf("the departed incarnation's own late heartbeat re-registered it: err = %v", err)
	}
	if _, health := registry.PeekObservedHealth("node-a"); health.Seen {
		t.Fatal("a fenced heartbeat left the node observed")
	}

	// A restart mints a newer incarnation, and that one comes back.
	if err := beat(newest); err != nil {
		t.Fatalf("a newer incarnation was locked out after unregister: %v", err)
	}
	if got, ok := registry.ObservedIncarnation("node-a"); !ok || got != Incarnation(newest) {
		t.Fatalf("observed incarnation = %q, %v; want %q", got, ok, newest)
	}
	if _, ok := registry.departed["node-a"]; ok {
		t.Fatal("the fence outlived the incarnation that cleared it")
	}
}

// The fence is bounded by fleet size: a node that leaves discovery takes its
// fence with it, exactly as its observed record goes.
func TestDiscoveryDroppingANodeDropsItsDepartureFence(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL)
	older := "0199a000-0000-7000-8000-000000000001"
	newer := "0199b000-0000-7000-8000-000000000002"
	beat := func(instance string) error {
		_, _, err := registry.Heartbeat(&schedulerv1.HeartbeatRequest{
			NodeId:            "node-a",
			ClusterId:         "cluster-1",
			ServiceInstanceId: instance,
		}, time.Now())
		return err
	}

	if err := beat(newer); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := registry.UnregisterObserved("node-a", newer); err != nil {
		t.Fatalf("unregister: %v", err)
	}
	registry.Set(nil, nil)
	if _, ok := registry.departed["node-a"]; ok {
		t.Fatal("a node discovery dropped kept its fence")
	}

	registry.Set([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, nil)
	if err := beat(older); err != nil {
		t.Fatalf("a node re-added to discovery was still fenced: %v", err)
	}
}
