package scheduler

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis Cluster is the case the key layout exists for, and it cannot be
// simulated: a single instance accepts cross-slot access happily, so a layout
// that would be rejected by a cluster passes every single-instance test. These
// tests run against a real three-master cluster.
func TestRedisClusterBindingStore(t *testing.T) {
	addrs := startRedisClusterForTest(t, 3)
	store, err := NewRedisBindingStore(strings.Join(addrs, ","), 5*time.Second)
	if err != nil {
		t.Fatalf("create cluster binding store: %v", err)
	}
	store.reconcileGrace = 0
	t.Cleanup(func() { _ = store.Close() })

	if _, ok := store.client.(*redis.ClusterClient); !ok {
		t.Fatalf("a cluster address must produce a cluster client, got %T", store.client)
	}

	nodeA := Node{ID: "node-a", Endpoint: "http://node-a"}
	nodeB := Node{ID: "node-b", Endpoint: "http://node-b"}

	// Enough sandboxes that they cannot all land in one slot, which is what
	// makes this a real test of the layout rather than an accident.
	roster := make([]string, 0, 24)
	for i := 0; i < 24; i++ {
		roster = append(roster, fmt.Sprintf("sandbox-%02d", i))
	}
	if slots := distinctSlots(roster); slots < 2 {
		t.Fatalf("the roster should span several slots, got %d", slots)
	}

	assignments := make([]BindingAssignment, 0, len(roster))
	for _, sandboxID := range roster {
		assignments = append(assignments, BindingAssignment{SandboxID: sandboxID, Node: nodeA})
	}
	for i, err := range store.RecordBatch(assignments, time.Now()) {
		if err != nil {
			t.Fatalf("record %s across the cluster: %v", roster[i], err)
		}
	}
	for _, sandboxID := range roster {
		if got, ok, err := store.Get(sandboxID, time.Now()); err != nil || !ok || got != nodeA {
			t.Fatalf("%s should resolve to node-a, got (%+v, %v, %v)", sandboxID, got, ok, err)
		}
	}

	// Reconciling to a shorter roster must delete exactly the departed
	// bindings, each of which lives in its own slot.
	kept := roster[:8]
	if err := store.ReconcileNode(nodeA, kept, time.Now()); err != nil {
		t.Fatalf("reconcile across the cluster: %v", err)
	}
	for _, sandboxID := range kept {
		if _, ok, err := store.Get(sandboxID, time.Now()); err != nil || !ok {
			t.Fatalf("%s should have been kept, got ok=%v err=%v", sandboxID, ok, err)
		}
	}
	for _, sandboxID := range roster[8:] {
		if _, ok, err := store.Get(sandboxID, time.Now()); err != nil || ok {
			t.Fatalf("%s should have been reconciled away, got ok=%v err=%v", sandboxID, ok, err)
		}
	}

	// A sandbox that moves must leave its old node's index, or the old node
	// would keep reconciling a binding it no longer holds.
	moved := kept[0]
	if err := store.Record(moved, nodeB, time.Now()); err != nil {
		t.Fatalf("move %s to node-b: %v", moved, err)
	}
	if got, ok, _ := store.Get(moved, time.Now()); !ok || got != nodeB {
		t.Fatalf("%s should now resolve to node-b, got (%+v, %v)", moved, got, ok)
	}
	if members := clusterIndexMembers(t, store, nodeA.ID); contains(members, moved) {
		t.Fatalf("%s should have left node-a's index, got %v", moved, members)
	}

	// And node-a reconciling without it must not delete the binding node-b now
	// owns. This is the guard that replaces the atomicity the single-slot
	// script used to provide.
	if err := store.ReconcileNode(nodeA, kept[1:], time.Now()); err != nil {
		t.Fatalf("reconcile after the move: %v", err)
	}
	if got, ok, _ := store.Get(moved, time.Now()); !ok || got != nodeB {
		t.Fatalf("a moved binding must survive its old node's reconcile, got (%+v, %v)", got, ok)
	}
}

// A cluster caches scripts per server, so a client that loaded them on one
// shard finds them missing the moment a key hashes elsewhere. And inside a
// pipeline EVALSHA cannot fall back to EVAL, so this would fail quietly.
func TestRedisClusterSurvivesScriptFlush(t *testing.T) {
	addrs := startRedisClusterForTest(t, 3)
	store, err := NewRedisBindingStore(strings.Join(addrs, ","), 5*time.Second)
	if err != nil {
		t.Fatalf("create cluster binding store: %v", err)
	}
	store.reconcileGrace = 0
	t.Cleanup(func() { _ = store.Close() })

	node := Node{ID: "node-a", Endpoint: "http://node-a"}
	if err := store.Record("sandbox-1", node, time.Now()); err != nil {
		t.Fatalf("seed record: %v", err)
	}

	cluster := store.client.(*redis.ClusterClient)
	if err := cluster.ForEachMaster(context.Background(), func(ctx context.Context, shard *redis.Client) error {
		return shard.ScriptFlush(ctx).Err()
	}); err != nil {
		t.Fatalf("flush scripts: %v", err)
	}

	if err := store.Record("sandbox-2", node, time.Now()); err != nil {
		t.Fatalf("record after a script flush must recover, got %v", err)
	}
	if _, ok, _ := store.Get("sandbox-2", time.Now()); !ok {
		t.Fatal("the binding written after a script flush should be readable")
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func clusterIndexMembers(t *testing.T, store *RedisBindingStore, nodeID string) []string {
	t.Helper()
	members, err := store.client.SMembers(context.Background(), store.nodeKey(nodeID)).Result()
	if err != nil {
		t.Fatalf("read node index: %v", err)
	}
	sort.Strings(members)
	return members
}

// distinctSlots counts how many hash slots a set of sandbox ids maps to under
// the store's key layout.
func distinctSlots(sandboxIDs []string) int {
	slots := make(map[uint16]struct{}, len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		// The tag is what the slot is computed over, so hashing it directly
		// matches what Redis will do with the full key.
		slots[crc16Slot(sandboxID)] = struct{}{}
	}
	return len(slots)
}

// crc16Slot is Redis's key-to-slot function over an already-extracted tag.
func crc16Slot(tag string) uint16 {
	var crc uint16
	for i := 0; i < len(tag); i++ {
		crc ^= uint16(tag[i]) << 8
		for bit := 0; bit < 8; bit++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc % 16384
}

// startRedisClusterForTest brings up `masters` cluster-enabled servers, assigns
// the slot range across them, and waits for the cluster to report itself ready.
func startRedisClusterForTest(t *testing.T, masters int) []string {
	t.Helper()

	bin := strings.TrimSpace(os.Getenv("REDIS_SERVER_BIN"))
	if bin == "" {
		var err error
		bin, err = exec.LookPath("redis-server")
		if err != nil {
			t.Skip("redis-server not found; set REDIS_SERVER_BIN to run the Redis Cluster integration test")
		}
	}

	addrs := make([]string, 0, masters)
	clients := make([]*redis.Client, 0, masters)
	for i := 0; i < masters; i++ {
		port := freeClusterPort(t)
		dir := t.TempDir()
		var output bytes.Buffer
		cmd := exec.Command(bin,
			"--bind", "127.0.0.1",
			"--port", port,
			"--cluster-enabled", "yes",
			"--cluster-config-file", fmt.Sprintf("nodes-%s.conf", port),
			"--cluster-node-timeout", "2000",
			"--save", "",
			"--appendonly", "no",
			"--dir", dir,
			"--loglevel", "warning",
		)
		cmd.Stdout = &output
		cmd.Stderr = &output
		if err := cmd.Start(); err != nil {
			t.Fatalf("start cluster redis-server: %v", err)
		}
		t.Cleanup(func() {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			_ = cmd.Wait()
		})

		addr := net.JoinHostPort("127.0.0.1", port)
		client := redis.NewClient(&redis.Options{Addr: addr})
		t.Cleanup(func() { _ = client.Close() })
		waitForRedis(t, client, cmd, &output)
		addrs = append(addrs, addr)
		clients = append(clients, client)
	}

	ctx := context.Background()
	// Slots are divided evenly; the last master absorbs the remainder.
	const totalSlots = 16384
	per := totalSlots / masters
	for i, client := range clients {
		start := i * per
		end := start + per - 1
		if i == masters-1 {
			end = totalSlots - 1
		}
		slots := make([]int, 0, end-start+1)
		for slot := start; slot <= end; slot++ {
			slots = append(slots, slot)
		}
		if err := client.ClusterAddSlots(ctx, slots...).Err(); err != nil {
			t.Fatalf("assign slots to master %d: %v", i, err)
		}
	}
	for i, client := range clients {
		for j, other := range addrs {
			if i == j {
				continue
			}
			host, port, err := net.SplitHostPort(other)
			if err != nil {
				t.Fatalf("split %s: %v", other, err)
			}
			if err := client.ClusterMeet(ctx, host, port).Err(); err != nil {
				t.Fatalf("cluster meet: %v", err)
			}
		}
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		ready := true
		for _, client := range clients {
			info, err := client.ClusterInfo(ctx).Result()
			if err != nil || !strings.Contains(info, "cluster_state:ok") {
				ready = false
				break
			}
		}
		if ready {
			return addrs
		}
		if time.Now().After(deadline) {
			t.Fatal("redis cluster did not converge")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// clusterBusPortOffset is the fixed distance Redis puts between a cluster
// node's client port and its bus port.
const clusterBusPortOffset = 10000

// freeClusterPort picks a client port whose bus port also fits below 65535
// and is also free.
//
// Asking the kernel for any free port does not work here: on macOS the
// ephemeral range starts at 49152, so most of what it hands out puts the bus
// port past the top of the port space and redis-server refuses to start. The
// port is drawn from a range with room above it and both ends are probed,
// which leaves the usual bind-and-release window; the caller's readiness wait
// reports a lost race as a startup failure with the server's own output.
func freeClusterPort(t *testing.T) string {
	t.Helper()
	const low, high = 20000, 40000
	for attempt := 0; attempt < 50; attempt++ {
		port := low + rand.Intn(high-low)
		client, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err != nil {
			continue
		}
		bus, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port+clusterBusPortOffset)))
		_ = client.Close()
		if err != nil {
			continue
		}
		_ = bus.Close()
		return strconv.Itoa(port)
	}
	t.Fatal("no free cluster port pair found")
	return ""
}

// waitForRedis blocks until the server answers PING. On failure the process is
// reaped before its output is read: exec copies stdout and stderr into the
// buffer from its own goroutine until Wait returns, so reading earlier races
// that copy.
func waitForRedis(t *testing.T, client *redis.Client, cmd *exec.Cmd, output *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		err := client.Ping(ctx).Err()
		cancel()
		if err == nil {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
	_ = cmd.Wait()
	t.Fatalf("redis-server did not become ready; output: %s", output.String())
}
