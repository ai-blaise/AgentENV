package scheduler

import (
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func TestBindingStoreExpiresBindingsOnLookup(t *testing.T) {
	store := NewInMemoryBindingStore(time.Second)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}
	base := time.Unix(100, 0)

	store.Record("sbx-1", node, base)

	if got, ok, err := store.Get("sbx-1", base.Add(500*time.Millisecond)); err != nil || !ok || got.ID != node.ID {
		t.Fatalf("expected binding before ttl expiry, got (%+v, %v, %v)", got, ok, err)
	}
	if _, ok, err := store.Get("sbx-1", base.Add(time.Second)); err != nil || ok {
		t.Fatalf("expected binding to expire at ttl boundary, got ok=%v err=%v", ok, err)
	}
	if _, ok, err := store.Get("sbx-1", base.Add(2*time.Second)); err != nil || ok {
		t.Fatalf("expected expired binding to be removed, got ok=%v err=%v", ok, err)
	}
}

func TestBindingStoreReconcileNodeRefreshesAndRemovesBindings(t *testing.T) {
	store := NewInMemoryBindingStoreWithGrace(10*time.Second, 0)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}
	base := time.Unix(200, 0)

	store.Record("sbx-1", node, base)
	store.Record("sbx-2", node, base)

	store.ReconcileNode(node, []string{"sbx-2", "sbx-3", "sbx-3", ""}, base.Add(time.Second))

	if _, ok, err := store.Get("sbx-1", base.Add(2*time.Second)); err != nil || ok {
		t.Fatalf("expected reconcile to remove sandbox missing from heartbeat roster, got ok=%v err=%v", ok, err)
	}
	for _, sandboxID := range []string{"sbx-2", "sbx-3"} {
		got, ok, err := store.Get(sandboxID, base.Add(2*time.Second))
		if err != nil || !ok || got.ID != node.ID {
			t.Fatalf("expected %s to resolve to node-a, got (%+v, %v, %v)", sandboxID, got, ok, err)
		}
	}
	if _, ok, err := store.Get("sbx-2", base.Add(11*time.Second)); err != nil || ok {
		t.Fatalf("expected reconcile to refresh ttl from latest heartbeat time, got ok=%v err=%v", ok, err)
	}
}

func TestBindingStoreMovesBindingBetweenNodes(t *testing.T) {
	store := NewInMemoryBindingStore(10 * time.Second)
	base := time.Unix(300, 0)

	store.Record("sbx-1", Node{ID: "node-a", Endpoint: "http://node-a"}, base)
	store.Record("sbx-1", Node{ID: "node-b", Endpoint: "http://node-b"}, base.Add(time.Second))
	store.ReconcileNode(Node{ID: "node-a", Endpoint: "http://node-a"}, nil, base.Add(2*time.Second))

	got, ok, err := store.Get("sbx-1", base.Add(3*time.Second))
	if err != nil || !ok {
		t.Fatalf("expected moved binding to remain available, got ok=%v err=%v", ok, err)
	}
	if got.ID != "node-b" {
		t.Fatalf("expected binding to move to node-b, got %q", got.ID)
	}
}

func TestArtifactStoreRecordsForgetsAndRemovesNode(t *testing.T) {
	store := NewInMemoryArtifactStore(10, 0)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-a", "node-b")
	store.Record("cluster-1", "other", "artifact-a", "node-c")

	nodes := store.Lookup("cluster-1", "backend", "artifact-a")
	if len(nodes) != 2 {
		t.Fatalf("expected 2 iroh providers, got %v", nodes)
	}

	store.Forget("cluster-1", "backend", "artifact-a", "node-a")
	nodes = store.Lookup("cluster-1", "backend", "artifact-a")
	if len(nodes) != 1 || nodes[0] != "node-b" {
		t.Fatalf("expected only node-b after forget, got %v", nodes)
	}

	store.ForgetNode("node-b")
	if nodes := store.Lookup("cluster-1", "backend", "artifact-a"); len(nodes) != 0 {
		t.Fatalf("expected node removal to clear artifact, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "other", "artifact-a"); len(nodes) != 1 || nodes[0] != "node-c" {
		t.Fatalf("expected other backend provider to remain, got %v", nodes)
	}
}

func TestArtifactStoreCapacityCountsArtifactKeys(t *testing.T) {
	store := NewInMemoryArtifactStore(2, 0)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-a", "node-b")
	store.Record("cluster-1", "backend", "artifact-b", "node-c")

	nodes := store.Lookup("cluster-1", "backend", "artifact-a")
	if len(nodes) != 2 {
		t.Fatalf("expected artifact-a providers to share one capacity entry, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "backend", "artifact-b"); len(nodes) != 1 || nodes[0] != "node-c" {
		t.Fatalf("expected artifact-b to remain, got %v", nodes)
	}
}

func TestArtifactStoreEvictsLeastRecentlyUsedArtifactKey(t *testing.T) {
	store := NewInMemoryArtifactStore(2, 0)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-a", "node-b")
	store.Record("cluster-1", "backend", "artifact-b", "node-c")
	store.Record("cluster-1", "backend", "artifact-c", "node-d")

	if nodes := store.Lookup("cluster-1", "backend", "artifact-a"); len(nodes) != 0 {
		t.Fatalf("expected artifact-a to be evicted with all providers, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "backend", "artifact-b"); len(nodes) != 1 || nodes[0] != "node-c" {
		t.Fatalf("expected artifact-b to remain, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "backend", "artifact-c"); len(nodes) != 1 || nodes[0] != "node-d" {
		t.Fatalf("expected artifact-c to remain, got %v", nodes)
	}

	store.ForgetNode("node-a")
	if nodes := store.Lookup("cluster-1", "backend", "artifact-b"); len(nodes) != 1 || nodes[0] != "node-c" {
		t.Fatalf("expected evicted node reverse index to be cleaned, got %v", nodes)
	}
}

func TestArtifactStoreLookupRefreshesLRU(t *testing.T) {
	store := NewInMemoryArtifactStore(2, 0)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-b", "node-b")
	if nodes := store.Lookup("cluster-1", "backend", "artifact-a"); len(nodes) != 1 || nodes[0] != "node-a" {
		t.Fatalf("expected artifact-a before eviction, got %v", nodes)
	}
	store.Record("cluster-1", "backend", "artifact-c", "node-c")

	if nodes := store.Lookup("cluster-1", "backend", "artifact-a"); len(nodes) != 1 || nodes[0] != "node-a" {
		t.Fatalf("expected lookup to refresh artifact-a, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "backend", "artifact-b"); len(nodes) != 0 {
		t.Fatalf("expected artifact-b to be evicted, got %v", nodes)
	}
}

func TestArtifactStoreRecordRefreshesLRU(t *testing.T) {
	store := NewInMemoryArtifactStore(2, 0)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-b", "node-b")
	store.Record("cluster-1", "backend", "artifact-a", "node-c")
	store.Record("cluster-1", "backend", "artifact-c", "node-d")

	nodes := store.Lookup("cluster-1", "backend", "artifact-a")
	if len(nodes) != 2 {
		t.Fatalf("expected record to refresh artifact-a, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "backend", "artifact-b"); len(nodes) != 0 {
		t.Fatalf("expected artifact-b to be evicted, got %v", nodes)
	}
}

func TestArtifactStoreForgetRemovesKeysFromLRU(t *testing.T) {
	store := NewInMemoryArtifactStore(2, 0)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-b", "node-b")
	store.Forget("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-c", "node-c")

	if nodes := store.Lookup("cluster-1", "backend", "artifact-b"); len(nodes) != 1 || nodes[0] != "node-b" {
		t.Fatalf("expected artifact-b to remain after forgetting artifact-a, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "backend", "artifact-c"); len(nodes) != 1 || nodes[0] != "node-c" {
		t.Fatalf("expected artifact-c to remain, got %v", nodes)
	}
}

func TestArtifactStoreForgetNodeRemovesKeysFromLRU(t *testing.T) {
	store := NewInMemoryArtifactStore(2, 0)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-b", "node-b")
	store.ForgetNode("node-a")
	store.Record("cluster-1", "backend", "artifact-c", "node-c")

	if nodes := store.Lookup("cluster-1", "backend", "artifact-b"); len(nodes) != 1 || nodes[0] != "node-b" {
		t.Fatalf("expected artifact-b to remain after forgetting node-a, got %v", nodes)
	}
	if nodes := store.Lookup("cluster-1", "backend", "artifact-c"); len(nodes) != 1 || nodes[0] != "node-c" {
		t.Fatalf("expected artifact-c to remain, got %v", nodes)
	}
}

func TestArtifactStoreLookupLimitsReturnedNodes(t *testing.T) {
	store := NewInMemoryArtifactStore(10, 2)

	store.Record("cluster-1", "backend", "artifact-a", "node-a")
	store.Record("cluster-1", "backend", "artifact-a", "node-b")
	store.Record("cluster-1", "backend", "artifact-a", "node-c")

	nodes := store.Lookup("cluster-1", "backend", "artifact-a")
	if len(nodes) != 2 {
		t.Fatalf("expected lookup to return 2 nodes, got %v", nodes)
	}
	for _, nodeID := range nodes {
		if nodeID != "node-a" && nodeID != "node-b" && nodeID != "node-c" {
			t.Fatalf("lookup returned unexpected node %q", nodeID)
		}
	}
}

func TestArtifactStoreLookupReturnsAllNodesWhenLimitIsNonPositive(t *testing.T) {
	for _, limit := range []int{0, -1} {
		store := NewInMemoryArtifactStore(10, limit)

		store.Record("cluster-1", "backend", "artifact-a", "node-a")
		store.Record("cluster-1", "backend", "artifact-a", "node-b")
		store.Record("cluster-1", "backend", "artifact-a", "node-c")

		if nodes := store.Lookup("cluster-1", "backend", "artifact-a"); len(nodes) != 3 {
			t.Fatalf("expected limit %d to return all nodes, got %v", limit, nodes)
		}
	}
}

func TestInMemoryBindingStoreRecordBatch(t *testing.T) {
	store := NewInMemoryBindingStore(time.Minute)
	now := time.Now()
	nodeA := Node{ID: "node-a", Endpoint: "http://a"}

	errs := store.RecordBatch([]BindingAssignment{
		{SandboxID: "sbx-1", Node: nodeA},
		{SandboxID: "  ", Node: nodeA},
		{SandboxID: "sbx-2", Node: nodeA},
	}, now)

	if len(errs) != 3 {
		t.Fatalf("RecordBatch returned %d results, want 3", len(errs))
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("errs[%d] = %v, want nil", i, err)
		}
	}
	for _, sandboxID := range []string{"sbx-1", "sbx-2"} {
		node, ok, err := store.Get(sandboxID, now)
		if err != nil || !ok {
			t.Fatalf("Get(%q) = (%v, %v, %v), want the recorded node", sandboxID, node, ok, err)
		}
		if node.ID != nodeA.ID {
			t.Fatalf("Get(%q) node = %q, want %q", sandboxID, node.ID, nodeA.ID)
		}
	}
	if _, ok, _ := store.Get("  ", now); ok {
		t.Fatal("blank sandbox id must not be recorded")
	}
}

func TestInMemoryBindingStoreRecordBatchEmpty(t *testing.T) {
	store := NewInMemoryBindingStore(time.Minute)
	if errs := store.RecordBatch(nil, time.Now()); errs != nil {
		t.Fatalf("RecordBatch(nil) = %v, want nil", errs)
	}
}

// TestInMemoryBindingStoreRecordBatchMovesNode covers the same node-change
// bookkeeping Record performs, so the batch path cannot leave a stale entry in
// the previous node's reverse index.
func TestInMemoryBindingStoreRecordBatchMovesNode(t *testing.T) {
	store := NewInMemoryBindingStore(time.Minute)
	now := time.Now()
	nodeA := Node{ID: "node-a", Endpoint: "http://a"}
	nodeB := Node{ID: "node-b", Endpoint: "http://b"}

	store.RecordBatch([]BindingAssignment{{SandboxID: "sbx-1", Node: nodeA}}, now)
	store.RecordBatch([]BindingAssignment{{SandboxID: "sbx-1", Node: nodeB}}, now)

	node, ok, err := store.Get("sbx-1", now)
	if err != nil || !ok || node.ID != nodeB.ID {
		t.Fatalf("Get after move = (%v, %v, %v), want node-b", node, ok, err)
	}
	// Reconciling the old node with an empty roster must not delete a binding
	// that now belongs to another node.
	if err := store.ReconcileNode(nodeA, nil, now); err != nil {
		t.Fatalf("ReconcileNode: %v", err)
	}
	if _, ok, _ := store.Get("sbx-1", now); !ok {
		t.Fatal("binding owned by node-b was deleted by node-a reconcile")
	}
}

// TestReconcileKeepsBindingRecordedAfterRoster covers the race between a
// binding being written and the heartbeat that omits it. Building a node
// snapshot walks every sandbox twice and reads /proc, so a sandbox created
// after the roster was collected but before the heartbeat lands is bound and
// absent from the roster at the same time. Deleting it there hands the client
// an id the scheduler has already forgotten.
func TestReconcileKeepsBindingRecordedAfterRoster(t *testing.T) {
	store := NewInMemoryBindingStore(time.Minute)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}
	base := time.Unix(400, 0)

	store.Record("sbx-established", node, base)
	// Recorded while the reporting node was already building its roster.
	store.Record("sbx-inflight", node, base.Add(defaultReconcileGracePeriod))

	store.ReconcileNode(node, []string{"sbx-established"}, base.Add(defaultReconcileGracePeriod+time.Second))

	if _, ok, _ := store.Get("sbx-inflight", base.Add(defaultReconcileGracePeriod+time.Second)); !ok {
		t.Fatal("binding recorded inside the grace window was deleted by a roster that could not have seen it")
	}

	// Once the grace has elapsed the same omission is authoritative.
	store.ReconcileNode(node, []string{"sbx-established"}, base.Add(3*defaultReconcileGracePeriod))
	if _, ok, _ := store.Get("sbx-inflight", base.Add(3*defaultReconcileGracePeriod)); ok {
		t.Fatal("binding omitted from an authoritative roster past the grace window was not deleted")
	}
}

// TestReconcileGraceDoesNotRestampOnRefresh pins that recordedAt marks when a
// binding was established, not when it was last refreshed. Restamping on every
// heartbeat would extend the grace to every deletion, indefinitely.
func TestReconcileGraceDoesNotRestampOnRefresh(t *testing.T) {
	store := NewInMemoryBindingStore(time.Minute)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}
	base := time.Unix(500, 0)

	store.Record("sbx-1", node, base)
	// Repeated heartbeats keep the binding alive and in the roster.
	for i := 1; i <= 5; i++ {
		store.ReconcileNode(node, []string{"sbx-1"}, base.Add(time.Duration(i)*time.Second))
	}

	// The binding was established well outside the grace window, so the first
	// roster that omits it is authoritative even though it was just refreshed.
	store.ReconcileNode(node, nil, base.Add(defaultReconcileGracePeriod+6*time.Second))
	if _, ok, _ := store.Get("sbx-1", base.Add(defaultReconcileGracePeriod+6*time.Second)); ok {
		t.Fatal("refresh restamped recordedAt, so the grace window never expires")
	}
}

func TestReconcileIncompleteRosterNeverWipesNodeBindings(t *testing.T) {
	store := NewInMemoryBindingStore(time.Minute)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}
	base := time.Unix(600, 0)

	store.Record("sbx-1", node, base)
	store.Record("sbx-2", node, base)

	// A node still restoring persisted paused sandboxes reports an empty
	// roster it does not consider authoritative.
	if err := store.ReconcileNodeRoster(node, nil, RosterIncomplete, base.Add(time.Hour)); err != nil {
		t.Fatalf("ReconcileNodeRoster: %v", err)
	}
	for _, sandboxID := range []string{"sbx-1", "sbx-2"} {
		if _, ok, _ := store.Get(sandboxID, base.Add(time.Second)); !ok {
			t.Fatalf("%s was wiped by an incomplete empty roster", sandboxID)
		}
	}

	// The same empty roster from a node that has finished recovery is
	// authoritative and clears them.
	if err := store.ReconcileNodeRoster(node, nil, RosterComplete, base.Add(time.Hour)); err != nil {
		t.Fatalf("ReconcileNodeRoster: %v", err)
	}
	for _, sandboxID := range []string{"sbx-1", "sbx-2"} {
		if _, ok, _ := store.Get(sandboxID, base.Add(time.Second)); ok {
			t.Fatalf("%s survived an authoritative empty roster", sandboxID)
		}
	}
}

// TestReconcileFinalRosterIgnoresGrace covers explicit unregister: the node is
// gone, so there is no live node left to observe a binding it never saw.
func TestReconcileFinalRosterIgnoresGrace(t *testing.T) {
	store := NewInMemoryBindingStore(time.Minute)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}
	base := time.Unix(700, 0)

	store.Record("sbx-fresh", node, base)

	if err := store.ReconcileNodeRoster(node, nil, RosterFinal, base); err != nil {
		t.Fatalf("ReconcileNodeRoster: %v", err)
	}
	if _, ok, _ := store.Get("sbx-fresh", base); ok {
		t.Fatal("unregister left a freshly recorded binding pointing at a departed node")
	}
}

// reconcileOutcomeCount reads one node's counter for one outcome. The counters
// are process-global, so the tests below compare deltas and use node ids no
// other test touches.
func reconcileOutcomeCount(t *testing.T, nodeID string, outcome string) float64 {
	t.Helper()
	counter, err := schedulerReconcileBindingsTotal.GetMetricWithLabelValues(nodeID, outcome)
	if err != nil {
		t.Fatalf("read reconcile counter for %s/%s failed: %v", nodeID, outcome, err)
	}
	metric := &dto.Metric{}
	if err := counter.Write(metric); err != nil {
		t.Fatalf("write reconcile counter for %s/%s failed: %v", nodeID, outcome, err)
	}
	return metric.GetCounter().GetValue()
}

// TestReconcileCountsDeletesAndRetentions pins that reconcile reports what it
// did with the bindings a roster omitted.
//
// A delete that beat the grace window is indistinguishable, from inside the
// process, from a sandbox that legitimately went away: both leave no binding,
// no error and no log line. The per-node split between deletes and retentions
// is the only thing that separates a fleet whose grace covers its roster
// latency from one whose grace is quietly too short.
func TestReconcileCountsDeletesAndRetentions(t *testing.T) {
	store := NewInMemoryBindingStore(time.Minute)
	node := Node{ID: "node-reconcile-counts", Endpoint: "http://node-a"}
	base := time.Unix(800, 0)
	now := base.Add(2 * defaultReconcileGracePeriod)

	// Two deletable against one retained. With one of each the labels are
	// interchangeable and swapping them at the recording sites stays green,
	// which makes the metric worse than absent: it would report deletes as
	// retentions.
	store.Record("sbx-old", node, base)
	store.Record("sbx-older", node, base)
	store.Record("sbx-fresh", node, now)

	deletedBefore := reconcileOutcomeCount(t, node.ID, reconcileOutcomeDeleted)
	retainedBefore := reconcileOutcomeCount(t, node.ID, reconcileOutcomeRetained)

	if err := store.ReconcileNode(node, []string{"sbx-reported"}, now); err != nil {
		t.Fatalf("ReconcileNode: %v", err)
	}

	if got := reconcileOutcomeCount(t, node.ID, reconcileOutcomeDeleted) - deletedBefore; got != 2 {
		t.Fatalf("delete count rose by %v, want 2", got)
	}
	if got := reconcileOutcomeCount(t, node.ID, reconcileOutcomeRetained) - retainedBefore; got != 1 {
		t.Fatalf("retention count rose by %v, want 1", got)
	}
}

// TestReconcileCountsDeletesFromEmptyRoster covers the branch an authoritative
// empty roster takes, which deletes without ever consulting the desired set and
// so counts at a different site.
func TestReconcileCountsDeletesFromEmptyRoster(t *testing.T) {
	store := NewInMemoryBindingStore(time.Minute)
	node := Node{ID: "node-empty-roster-counts", Endpoint: "http://node-a"}
	base := time.Unix(900, 0)
	now := base.Add(2 * defaultReconcileGracePeriod)

	// Distinct counts per label, for the reason given above.
	store.Record("sbx-old", node, base)
	store.Record("sbx-older", node, base)
	store.Record("sbx-fresh", node, now)

	deletedBefore := reconcileOutcomeCount(t, node.ID, reconcileOutcomeDeleted)
	retainedBefore := reconcileOutcomeCount(t, node.ID, reconcileOutcomeRetained)

	if err := store.ReconcileNodeRoster(node, nil, RosterComplete, now); err != nil {
		t.Fatalf("ReconcileNodeRoster: %v", err)
	}

	if got := reconcileOutcomeCount(t, node.ID, reconcileOutcomeDeleted) - deletedBefore; got != 2 {
		t.Fatalf("delete count rose by %v, want 2", got)
	}
	if got := reconcileOutcomeCount(t, node.ID, reconcileOutcomeRetained) - retainedBefore; got != 1 {
		t.Fatalf("retention count rose by %v, want 1", got)
	}
}

// TestValidateReconcileGraceRejectsGraceAtOrPastBindingTTL pins the far end of
// the range. Equal is not a harmless boundary: a grace that reaches the TTL
// protects a binding for longer than the TTL itself, so a sandbox created and
// deleted inside one TTL can only be reaped by expiry. It is a narrower claim
// than "reconcile can never delete anything" — recordedAt is not restamped on
// refresh, so a long-lived binding does age out of the grace and reconcile can
// still reap it.
func TestValidateReconcileGraceRejectsGraceAtOrPastBindingTTL(t *testing.T) {
	const ttl = 30 * time.Second

	if err := ValidateReconcileGrace(ttl, ttl, 0); err == nil {
		t.Fatal("a grace equal to the binding ttl was accepted")
	}
	if err := ValidateReconcileGrace(ttl, ttl+time.Second, 0); err == nil {
		t.Fatal("a grace longer than the binding ttl was accepted")
	}
	if err := ValidateReconcileGrace(ttl, ttl-time.Second, 0); err != nil {
		t.Fatalf("a grace shorter than the binding ttl was rejected: %v", err)
	}
}

// TestValidateReconcileGraceRejectsGraceShorterThanReportingWindow pins the
// near end. The grace has to outlast the reporting cycle it is protecting
// against, including one retry of the heartbeat that carries the roster.
func TestValidateReconcileGraceRejectsGraceShorterThanReportingWindow(t *testing.T) {
	const interval = 5 * time.Second
	// Written out rather than derived from minReconcileGraceHeartbeats. Deriving
	// it makes the test agree with whatever the constant says, so weakening the
	// constant — the direction that removes the protection — keeps it green.
	const minimum = 10 * time.Second

	if err := ValidateReconcileGrace(time.Minute, minimum-time.Millisecond, interval); err == nil {
		t.Fatalf("a grace of %s was accepted against a %s reporting interval", minimum-time.Millisecond, interval)
	}
	if err := ValidateReconcileGrace(time.Minute, minimum, interval); err != nil {
		t.Fatalf("a grace of exactly %s was rejected: %v", minimum, err)
	}
	// With no configured interval there is nothing to check the grace against,
	// which is the shipped case: scheduler.heartbeat_interval is optional.
	if err := ValidateReconcileGrace(time.Minute, time.Millisecond, 0); err != nil {
		t.Fatalf("an unconfigured reporting interval rejected a short grace: %v", err)
	}
}

// TestBindingStoreOptionsAcceptShippedDefaults pins that the ordering check does
// not reject the configuration the service ships with: a 30s binding TTL, the
// default grace, and the 5s interval nodes report at. A check that a correct
// deployment fails is a check that gets deleted, not a deployment that gets
// fixed.
func TestBindingStoreOptionsAcceptShippedDefaults(t *testing.T) {
	store, err := NewInMemoryBindingStoreWithOptions(BindingStoreOptions{
		BindingTTL:        30 * time.Second,
		HeartbeatInterval: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("shipped defaults were rejected: %v", err)
	}
	if store.bindingTTL != 30*time.Second {
		t.Fatalf("binding ttl = %s, want 30s", store.bindingTTL)
	}
	if store.reconcileGrace != defaultReconcileGracePeriod {
		t.Fatalf("reconcile grace = %s, want %s", store.reconcileGrace, defaultReconcileGracePeriod)
	}
}

// TestNewInMemoryBindingStoreWithOptionsRejectsUnusableGrace covers a caller
// that lowers the binding TTL and leaves the grace alone, which is how the two
// end up crossed in practice.
func TestNewInMemoryBindingStoreWithOptionsRejectsUnusableGrace(t *testing.T) {
	store, err := NewInMemoryBindingStoreWithOptions(BindingStoreOptions{BindingTTL: 5 * time.Second})
	if err == nil {
		t.Fatalf("a 5s binding ttl with the %s default grace was accepted", defaultReconcileGracePeriod)
	}
	if store != nil {
		t.Fatal("a rejected configuration still returned a store")
	}
}
