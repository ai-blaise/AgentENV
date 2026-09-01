package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisBindingStoreGetCases(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 5*time.Second)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}

	if got, ok, err := store.Get("missing", time.Now()); err != nil || ok || got != (Node{}) {
		t.Fatalf("expected missing binding to return zero/miss, got (%+v, %v, %v)", got, ok, err)
	}
	if got, ok, err := store.Get("   ", time.Now()); err != nil || ok || got != (Node{}) {
		t.Fatalf("expected blank sandbox id to return zero/miss, got (%+v, %v, %v)", got, ok, err)
	}

	writeRawRedisBinding(t, store, "valid", node)
	assertRedisBinding(t, store, " valid ", node)

	// Malformed values should behave like a cache miss but must not be deleted by Get.
	malformedKey := store.bindingKey("malformed")
	if err := store.client.Set(context.Background(), malformedKey, "not-json", time.Minute).Err(); err != nil {
		t.Fatalf("write malformed binding failed: %v", err)
	}
	if got, ok, err := store.Get("malformed", time.Now()); err != nil || ok || got != (Node{}) {
		t.Fatalf("expected malformed binding to return zero/miss, got (%+v, %v, %v)", got, ok, err)
	}
	if exists := redisExists(t, store, malformedKey); !exists {
		t.Fatal("expected malformed binding key to remain after Get")
	}

	invalidNodeKey := store.bindingKey("invalid-node")
	if err := store.client.Set(context.Background(), invalidNodeKey, `{"node":{"node_id":"node-a","endpoint":""}}`, time.Minute).Err(); err != nil {
		t.Fatalf("write invalid-node binding failed: %v", err)
	}
	if got, ok, err := store.Get("invalid-node", time.Now()); err != nil || ok || got != (Node{}) {
		t.Fatalf("expected invalid-node binding to return zero/miss, got (%+v, %v, %v)", got, ok, err)
	}
	if exists := redisExists(t, store, invalidNodeKey); !exists {
		t.Fatal("expected invalid-node binding key to remain after Get")
	}
}

func TestRedisBindingStoreRecordCases(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 5*time.Second)
	nodeA := Node{ID: "node-a", Endpoint: "http://node-a"}
	nodeATrimmed := Node{ID: " node-a ", Endpoint: " http://node-a "}
	nodeB := Node{ID: "node-b", Endpoint: "http://node-b"}

	store.Record("  sbx-1  ", nodeATrimmed, time.Now())
	assertRedisBinding(t, store, "sbx-1", nodeA)
	assertRedisSetEqual(t, store, store.nodeKey("node-a"), []string{"sbx-1"})
	assertRedisBindingJSON(t, store, "sbx-1", nodeA)
	assertRedisBindingHasPositiveTTL(t, store, "sbx-1")
	assertRedisKeyHasPositiveTTL(t, store, store.nodeKey("node-a"))

	store.Record("sbx-1", nodeB, time.Now())
	assertRedisBinding(t, store, "sbx-1", nodeB)
	assertRedisSetEqual(t, store, store.nodeKey("node-a"), nil)
	assertRedisSetEqual(t, store, store.nodeKey("node-b"), []string{"sbx-1"})
	assertRedisKeyHasPositiveTTL(t, store, store.nodeKey("node-b"))

	store.Record("", Node{ID: "node-c", Endpoint: "http://node-c"}, time.Now())
	store.Record("sbx-blank-node", Node{Endpoint: "http://node-c"}, time.Now())
	store.Record("sbx-blank-endpoint", Node{ID: "node-c"}, time.Now())
	assertRedisMissing(t, store, "sbx-blank-node")
	assertRedisMissing(t, store, "sbx-blank-endpoint")
	assertRedisSetEqual(t, store, store.nodeKey("node-c"), nil)

	// Existing malformed JSON should not block a Record; the new binding is written.
	if err := store.client.Set(context.Background(), store.bindingKey("malformed"), "not-json", time.Minute).Err(); err != nil {
		t.Fatalf("write malformed binding failed: %v", err)
	}
	store.Record("malformed", nodeA, time.Now())
	assertRedisBinding(t, store, "malformed", nodeA)
	assertRedisSetEqual(t, store, store.nodeKey("node-a"), []string{"malformed"})
}

func TestRedisBindingStoreReconcileCases(t *testing.T) {
	store := newRedisBindingStoreForTest(t, 5*time.Second)
	nodeA := Node{ID: "node-a", Endpoint: "http://node-a"}
	nodeB := Node{ID: "node-b", Endpoint: "http://node-b"}
	nodeC := Node{ID: "node-c", Endpoint: "http://node-c"}

	// Build initial state with a stale binding for node-a, two active node-b bindings,
	// and one node-c binding that will be moved by ReconcileNode.
	store.Record("stale-a", nodeA, time.Now())
	store.Record("keep-b", nodeB, time.Now())
	store.Record("drop-b", nodeB, time.Now())
	store.Record("move-c-to-b", nodeC, time.Now())

	store.ReconcileNode(Node{ID: " node-b ", Endpoint: " http://node-b "}, []string{"keep-b", "new-b", "move-c-to-b", "keep-b", ""}, time.Now())

	assertRedisBinding(t, store, "keep-b", nodeB)
	assertRedisBinding(t, store, "new-b", nodeB)
	assertRedisBinding(t, store, "move-c-to-b", nodeB)
	assertRedisMissing(t, store, "drop-b")
	assertRedisBinding(t, store, "stale-a", nodeA)
	assertRedisSetEqual(t, store, store.nodeKey("node-b"), []string{"keep-b", "new-b", "move-c-to-b"})
	assertRedisSetEqual(t, store, store.nodeKey("node-c"), nil)
	assertRedisSetEqual(t, store, store.nodeKey("node-a"), []string{"stale-a"})
	for _, sandboxID := range []string{"keep-b", "new-b", "move-c-to-b"} {
		assertRedisBindingHasPositiveTTL(t, store, sandboxID)
	}
	assertRedisKeyHasPositiveTTL(t, store, store.nodeKey("node-b"))

	// If a node reverse index has a stale sandbox whose binding now points to another node,
	// empty reconcile should remove only the reverse-index entry and must not delete that binding.
	store.Record("foreign", nodeB, time.Now())
	if err := store.client.SAdd(context.Background(), store.nodeKey("node-a"), "foreign").Err(); err != nil {
		t.Fatalf("inject stale reverse-index entry failed: %v", err)
	}
	store.ReconcileNode(nodeA, nil, time.Now())
	assertRedisMissing(t, store, "stale-a")
	assertRedisBinding(t, store, "foreign", nodeB)
	assertRedisSetEqual(t, store, store.nodeKey("node-a"), nil)
	assertRedisSetEqual(t, store, store.nodeKey("node-b"), []string{"keep-b", "new-b", "move-c-to-b", "foreign"})

	// Invalid node inputs should be ignored.
	store.ReconcileNode(Node{Endpoint: "http://missing-id"}, []string{"ignored"}, time.Now())
	assertRedisMissing(t, store, "ignored")
	store.ReconcileNode(Node{ID: "node-no-endpoint"}, []string{"ignored-no-endpoint"}, time.Now())
	assertRedisMissing(t, store, "ignored-no-endpoint")
	if redisExists(t, store, store.bindingKey("ignored-no-endpoint")) {
		t.Fatal("expected reconcile with non-empty desired list and empty endpoint not to write a phantom binding key")
	}
}

func newRedisBindingStoreForTest(t *testing.T, ttl time.Duration) *RedisBindingStore {
	t.Helper()
	// Reconcile-semantics tests write a binding and immediately reconcile it
	// away, which the grace window is designed to prevent. Disable it here and
	// cover it explicitly in TestRedisReconcileGraceAndRosterCompleteness.
	return newRedisBindingStoreForTestWithGrace(t, ttl, 0)
}

func newRedisBindingStoreForTestWithGrace(t *testing.T, ttl time.Duration, grace time.Duration) *RedisBindingStore {
	t.Helper()
	addr := startRedisServerForTest(t)
	store, err := NewRedisBindingStore(addr, ttl)
	if err != nil {
		t.Fatalf("create redis binding store failed: %v", err)
	}
	store.reconcileGrace = grace
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func startRedisServerForTest(t *testing.T) string {
	t.Helper()

	bin := strings.TrimSpace(os.Getenv("REDIS_SERVER_BIN"))
	if bin == "" {
		var err error
		bin, err = exec.LookPath("redis-server")
		if err != nil {
			t.Skip("redis-server not found; set REDIS_SERVER_BIN or install redis-server to run RedisBindingStore integration test")
		}
	}

	// A free port is found by binding and releasing it, which leaves a window
	// in which another test process on the same host takes it first. Under
	// -count=2 with the package's other redis fixtures that window is hit,
	// so a bind failure is retried on a fresh port rather than failing the
	// test. Only startup is retried: a server that came up and then died is
	// reported as such.
	const attempts = 5
	var lastOutput string
	for attempt := 1; attempt <= attempts; attempt++ {
		addr, output, ok := tryStartRedisServer(t, bin)
		if ok {
			return addr
		}
		lastOutput = output
	}
	t.Fatalf("redis-server did not start on %d attempts; last output: %s", attempts, lastOutput)
	return ""
}

// tryStartRedisServer starts one redis-server on a freshly chosen port and
// waits for it to answer PING. It reports false when the process exited
// before readiness, which is what a lost port race looks like.
func tryStartRedisServer(t *testing.T, bin string) (addr string, output string, ok bool) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate redis test port failed: %v", err)
	}
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		t.Fatalf("parse redis test listener addr failed: %v", err)
	}
	_ = listener.Close()

	var buf bytes.Buffer
	cmd := exec.Command(
		bin,
		"--bind", "127.0.0.1",
		"--port", port,
		"--save", "",
		"--appendonly", "no",
		"--dir", t.TempDir(),
		"--loglevel", "warning",
	)
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis-server failed: %v", err)
	}

	// ProcessState is only populated by Wait, so polling it from the
	// readiness loop below could never observe an exit: the old check was
	// dead code and a bind failure burned the whole deadline. Reap in the
	// background and let the loop select on the result instead.
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()
	reap := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		<-waitCh
	}

	addr = net.JoinHostPort("127.0.0.1", port)
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-waitCh:
			// Exited before answering: almost always the port race. The
			// caller picks another port; the output is returned in case the
			// last attempt has to be reported.
			return "", buf.String(), false
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err := client.Ping(ctx).Err()
		cancel()
		if err == nil {
			t.Cleanup(reap)
			return addr, "", true
		}
		time.Sleep(25 * time.Millisecond)
	}
	reap()
	pid := 0
	if cmd.Process != nil {
		pid = cmd.Process.Pid
	}
	t.Fatalf("redis-server pid %s did not become ready; output: %s", strconv.Itoa(pid), buf.String())
	return "", "", false
}

func writeRawRedisBinding(t *testing.T, store *RedisBindingStore, sandboxID string, node Node) {
	t.Helper()
	value, err := json.Marshal(redisBindingRecord{Node: node})
	if err != nil {
		t.Fatalf("marshal binding record failed: %v", err)
	}
	if err := store.client.Set(context.Background(), store.bindingKey(sandboxID), value, time.Minute).Err(); err != nil {
		t.Fatalf("write raw redis binding failed: %v", err)
	}
}

func assertRedisBinding(t *testing.T, store *RedisBindingStore, sandboxID string, want Node) {
	t.Helper()
	got, ok, err := store.Get(sandboxID, time.Now())
	if err != nil || !ok || got != want {
		t.Fatalf("expected %s to resolve to %+v, got (%+v, %v, %v)", strings.TrimSpace(sandboxID), want, got, ok, err)
	}
}

func assertRedisMissing(t *testing.T, store *RedisBindingStore, sandboxID string) {
	t.Helper()
	if got, ok, err := store.Get(sandboxID, time.Now()); err != nil || ok {
		t.Fatalf("expected %s to be missing, got %+v ok=%v err=%v", sandboxID, got, ok, err)
	}
}

func assertRedisBindingJSON(t *testing.T, store *RedisBindingStore, sandboxID string, want Node) {
	t.Helper()
	raw, err := store.client.Get(context.Background(), store.bindingKey(sandboxID)).Bytes()
	if err != nil {
		t.Fatalf("read raw redis binding %s failed: %v", sandboxID, err)
	}
	var record redisBindingRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatalf("binding value for %s is not JSON: %v; raw=%q", sandboxID, err, raw)
	}
	if record.Node != want {
		t.Fatalf("unexpected JSON binding value for %s: got %+v want %+v", sandboxID, record.Node, want)
	}
}

func assertRedisBindingHasPositiveTTL(t *testing.T, store *RedisBindingStore, sandboxID string) {
	t.Helper()
	assertRedisKeyHasPositiveTTL(t, store, store.bindingKey(sandboxID))
}

func assertRedisKeyHasPositiveTTL(t *testing.T, store *RedisBindingStore, key string) {
	t.Helper()
	ttl, err := store.client.TTL(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("read ttl for %s failed: %v", key, err)
	}
	if ttl <= 0 {
		t.Fatalf("expected %s to have positive ttl, got %s", key, ttl)
	}
}

func assertRedisSetEqual(t *testing.T, store *RedisBindingStore, key string, want []string) {
	t.Helper()
	got, err := store.client.SMembers(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("read redis set %s failed: %v", key, err)
	}
	sort.Strings(got)
	want = append([]string{}, want...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("redis set %s mismatch: got %v want %v", key, got, want)
	}
}

func redisExists(t *testing.T, store *RedisBindingStore, key string) bool {
	t.Helper()
	exists, err := store.client.Exists(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("check redis key %s exists failed: %v", key, err)
	}
	return exists > 0
}

// TestRedisReconcileGraceAndRosterCompleteness mirrors the in-memory coverage
// against a real Redis so the two stores cannot drift. The stamps and the
// comparison both happen inside Lua via redis.call("TIME"), so this also
// exercises that the write and the reconcile agree on one clock.
func TestRedisReconcileGraceAndRosterCompleteness(t *testing.T) {
	store := newRedisBindingStoreForTestWithGrace(t, time.Minute, time.Hour)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}

	store.Record("sbx-established", node, time.Now())
	store.Record("sbx-inflight", node, time.Now())

	// A roster that omits a binding written inside the grace window cannot have
	// seen it, so the binding survives.
	if err := store.ReconcileNode(node, []string{"sbx-established"}, time.Now()); err != nil {
		t.Fatalf("ReconcileNode: %v", err)
	}
	assertRedisBinding(t, store, "sbx-inflight", node)

	// With no grace, the same omission is authoritative.
	store.reconcileGrace = 0
	if err := store.ReconcileNode(node, []string{"sbx-established"}, time.Now()); err != nil {
		t.Fatalf("ReconcileNode: %v", err)
	}
	assertRedisMissing(t, store, "sbx-inflight")

	// An empty roster from a node still in startup recovery is not grounds for
	// deleting anything.
	if err := store.ReconcileNodeRoster(node, nil, RosterIncomplete, time.Now()); err != nil {
		t.Fatalf("ReconcileNodeRoster incomplete: %v", err)
	}
	assertRedisBinding(t, store, "sbx-established", node)

	// The same empty roster, once authoritative, clears the node.
	if err := store.ReconcileNodeRoster(node, nil, RosterComplete, time.Now()); err != nil {
		t.Fatalf("ReconcileNodeRoster complete: %v", err)
	}
	assertRedisMissing(t, store, "sbx-established")
}

// TestRedisReconcileFinalIgnoresGrace covers explicit unregister: the node is
// gone, so a freshly written binding pointing at it must not be retained.
func TestRedisReconcileFinalIgnoresGrace(t *testing.T) {
	store := newRedisBindingStoreForTestWithGrace(t, time.Minute, time.Hour)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}

	store.Record("sbx-fresh", node, time.Now())
	if err := store.ReconcileNodeRoster(node, nil, RosterFinal, time.Now()); err != nil {
		t.Fatalf("ReconcileNodeRoster final: %v", err)
	}
	assertRedisMissing(t, store, "sbx-fresh")
}

// TestRedisReconcileCountsOutcomes mirrors the in-memory counters against a
// real Redis, and covers the two outcomes only this store can produce: a
// binding that expired underneath the index, and one that moved to another node
// between the roster being collected and the delete being attempted.
//
// Bindings written through writeRawRedisBinding carry no recorded_at stamp, so
// the grace check cannot apply to them and the outcome does not depend on how
// long the test took to run.
func TestRedisReconcileCountsOutcomes(t *testing.T) {
	store := newRedisBindingStoreForTestWithGrace(t, time.Minute, time.Hour)
	node := Node{ID: "node-redis-outcome-counts", Endpoint: "http://node-a"}
	other := Node{ID: "node-redis-outcome-other", Endpoint: "http://node-b"}

	// A distinct number of each, so no two labels can be swapped at their
	// recording sites and leave this green. One of each would make the labels
	// interchangeable, and a metric that reports deletes as retentions is
	// worse than no metric.
	want := map[string]float64{
		reconcileOutcomeDeleted:  3,
		reconcileOutcomeRetained: 1,
		reconcileOutcomeMoved:    2,
		reconcileOutcomeAbsent:   4,
	}

	// Stamped by the server on write, so the hour-long grace covers it.
	store.Record("sbx-retained", node, time.Now())
	// Unstamped, still owned by this node: deleted.
	deleted := []string{"sbx-deleted", "sbx-deleted-2", "sbx-deleted-3"}
	for _, sandboxID := range deleted {
		writeRawRedisBinding(t, store, sandboxID, node)
	}
	// Unstamped, owned by someone else: not this node's to delete.
	moved := []string{"sbx-moved", "sbx-moved-2"}
	for _, sandboxID := range moved {
		writeRawRedisBinding(t, store, sandboxID, other)
	}
	// In the index with no binding behind it at all.
	absent := []string{"sbx-absent", "sbx-absent-2", "sbx-absent-3", "sbx-absent-4"}
	indexed := append(append(append([]string{}, deleted...), moved...), absent...)
	for _, sandboxID := range indexed {
		if err := store.client.SAdd(context.Background(), store.nodeKey(node.ID), sandboxID).Err(); err != nil {
			t.Fatalf("seed node index with %s failed: %v", sandboxID, err)
		}
	}

	before := map[string]float64{}
	for outcome := range want {
		before[outcome] = reconcileOutcomeCount(t, node.ID, outcome)
	}

	if err := store.ReconcileNodeRoster(node, nil, RosterComplete, time.Now()); err != nil {
		t.Fatalf("ReconcileNodeRoster: %v", err)
	}

	for outcome, expected := range want {
		if got := reconcileOutcomeCount(t, node.ID, outcome) - before[outcome]; got != expected {
			t.Fatalf("%s count rose by %v, want %v", outcome, got, expected)
		}
	}

	assertRedisMissing(t, store, "sbx-deleted")
	assertRedisBinding(t, store, "sbx-retained", node)
	assertRedisBinding(t, store, "sbx-moved", other)
}

// TestRedisNodeIndexTTLTracksBindingTTL pins the index expiry to the binding
// TTL at both sites that set it.
//
// The index is a hint that may name a node no longer holding the sandbox:
// queueIndexMove can only drop the old entry when the SET script could name the
// old owner, so an entry whose binding had already expired is never removed by
// anything but expiry. It used to expire after an hour against a thirty-second
// binding TTL. The write path and the reconcile path have drifted apart before,
// so both are checked here.
func TestRedisNodeIndexTTLTracksBindingTTL(t *testing.T) {
	const bindingTTL = 5 * time.Second
	store := newRedisBindingStoreForTest(t, bindingTTL)
	node := Node{ID: "node-a", Endpoint: "http://node-a"}
	want := nodeIndexTTLMultiple * bindingTTL

	store.Record("sbx-1", node, time.Now())
	assertRedisIndexTTLWithin(t, store, store.nodeKey(node.ID), bindingTTL, want)

	if err := store.ReconcileNode(node, []string{"sbx-1"}, time.Now()); err != nil {
		t.Fatalf("ReconcileNode: %v", err)
	}
	assertRedisIndexTTLWithin(t, store, store.nodeKey(node.ID), bindingTTL, want)
}

// assertRedisIndexTTLWithin checks a node index expires later than the bindings
// it names but not by an unbounded margin. Below the binding TTL it would drop
// entries that are still live; far above it, a stale entry outlives its binding
// for as long as the larger value.
func assertRedisIndexTTLWithin(t *testing.T, store *RedisBindingStore, key string, low time.Duration, high time.Duration) {
	t.Helper()
	ttl, err := store.client.PTTL(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("read pttl for %s failed: %v", key, err)
	}
	if ttl <= low || ttl > high {
		t.Fatalf("index ttl for %s is %s, want within (%s, %s]", key, ttl, low, high)
	}
}

// TestNewRedisBindingStoreWithOptionsValidatesGrace covers the constructor a
// caller reaches for when it has the configured TTL and reporting interval to
// hand, rather than only a TTL.
func TestNewRedisBindingStoreWithOptionsValidatesGrace(t *testing.T) {
	addr := startRedisServerForTest(t)

	if _, err := NewRedisBindingStoreWithOptions(addr, BindingStoreOptions{BindingTTL: 5 * time.Second}); err == nil {
		t.Fatalf("a 5s binding ttl with the %s default grace was accepted", defaultReconcileGracePeriod)
	}

	store, err := NewRedisBindingStoreWithOptions(addr, BindingStoreOptions{
		BindingTTL:        time.Minute,
		ReconcileGrace:    20 * time.Second,
		HeartbeatInterval: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("a valid configuration was rejected: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	if store.reconcileGrace != 20*time.Second {
		t.Fatalf("reconcile grace = %s, want 20s", store.reconcileGrace)
	}
}
