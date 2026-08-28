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

	var output bytes.Buffer
	cmd := exec.Command(
		bin,
		"--bind", "127.0.0.1",
		"--port", port,
		"--save", "",
		"--appendonly", "no",
		"--dir", t.TempDir(),
		"--loglevel", "warning",
	)
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start redis-server failed: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	})

	addr := net.JoinHostPort("127.0.0.1", port)
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err := client.Ping(ctx).Err()
		cancel()
		if err == nil {
			return addr
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			t.Fatalf("redis-server exited before readiness: %s", output.String())
		}
		time.Sleep(25 * time.Millisecond)
	}
	pid := 0
	if cmd.Process != nil {
		pid = cmd.Process.Pid
	}
	t.Fatalf("redis-server pid %s did not become ready; output: %s", strconv.Itoa(pid), output.String())
	return ""
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
