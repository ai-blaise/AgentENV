package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
)

// newRedisStreamReplica builds a replica against a real Redis, so the encoding,
// the shard hash and the blocking read are exercised rather than stubbed.
func newRedisStreamReplica(t *testing.T, ctx context.Context, addr string, prefix string, replicaID string, nodes []Node) *StreamFedNodeRegistry {
	t.Helper()

	bus, err := NewRedisNodeStream(ctx, addr, NodeStreamOptions{
		KeyPrefix: prefix,
		MaxLen:    1000,
		ReportTTL: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("connect node stream: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	registry := NewStreamFedNodeRegistry(NewAtomicNodeRegistry(nodes, 30*time.Second), bus, replicaID, nil)
	if _, err := registry.Run(ctx); err != nil {
		t.Fatalf("subscribe node stream: %v", err)
	}
	return registry
}

func waitForObserved(t *testing.T, registry *StreamFedNodeRegistry, nodeID string) ObservedHealth {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, health := registry.PeekObservedHealth(nodeID); health.Seen {
			return health
		}
		if time.Now().After(deadline) {
			t.Fatalf("replica never learned of %s", nodeID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRedisNodeStreamConverges(t *testing.T) {
	addr := startRedisServerForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := streamNodes()
	first := newRedisStreamReplica(t, ctx, addr, "test:converge", "replica-1", nodes)
	second := newRedisStreamReplica(t, ctx, addr, "test:converge", "replica-2", nodes)

	now := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	if _, _, err := first.Heartbeat(readyHeartbeat("node-a"), now); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	health := waitForObserved(t, second, "node-a")
	if health.LastSeenUnixMs != now.UTC().UnixMilli() {
		t.Fatalf("last seen = %d, want the publishing replica's stamp %d", health.LastSeenUnixMs, now.UTC().UnixMilli())
	}
	if health.Status != schedulerv1.NodeStatus_NODE_STATUS_READY {
		t.Fatalf("status = %v, want READY", health.Status)
	}
}

// A replica that starts into a running fleet cannot wait for every node's next
// heartbeat before it places: the retained tail is what it has, and reading it
// is the whole reason readiness waits on the warm-up.
func TestRedisNodeStreamWarmUpReadsTheRetainedTail(t *testing.T) {
	addr := startRedisServerForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes := streamNodes()
	publisher := newRedisStreamReplica(t, ctx, addr, "test:warmup", "replica-1", nodes)

	now := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	for _, nodeID := range []string{"node-a", "node-b"} {
		if _, _, err := publisher.Heartbeat(readyHeartbeat(nodeID), now); err != nil {
			t.Fatalf("heartbeat %s: %v", nodeID, err)
		}
	}

	// The joiner is started after every heartbeat has been sent, so anything it
	// knows came from the replay rather than from live traffic.
	joiner := newRedisStreamReplica(t, ctx, addr, "test:warmup", "replica-3", nodes)
	for _, nodeID := range []string{"node-a", "node-b"} {
		waitForObserved(t, joiner, nodeID)
	}
}

// Sixteen shards, hashed by node id. The keys carry no hash tag, so they spread
// across slots in a cluster; a reader that covered fewer than all of them would
// lose whole nodes with nothing to say so.
func TestNodeStreamShardsCoverTheFleetDeterministically(t *testing.T) {
	seen := map[uint32]bool{}
	for i := 0; i < 500; i++ {
		nodeID := "node-" + time.Duration(i).String()
		shard := nodeStreamShard(nodeID)
		if shard >= nodeStreamShards {
			t.Fatalf("node %s hashed to shard %d, outside the %d shards read", nodeID, shard, nodeStreamShards)
		}
		if again := nodeStreamShard(nodeID); again != shard {
			t.Fatalf("node %s hashed to %d then %d", nodeID, shard, again)
		}
		seen[shard] = true
	}
	if len(seen) != nodeStreamShards {
		t.Fatalf("500 nodes landed on %d of %d shards", len(seen), nodeStreamShards)
	}
}

// A reader that followed from the live tail would miss anything written between
// its replay and its first blocking read, and that node would stay invisible to
// this replica until its next heartbeat — which is one lost heartbeat interval
// of capacity data on a replica that has just started serving. The follow point
// is the replay horizon instead, so the two reads overlap.
func TestNodeStreamFollowsFromTheHorizonNotTheLiveTail(t *testing.T) {
	addr := startRedisServerForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus, err := NewRedisNodeStream(ctx, addr, NodeStreamOptions{KeyPrefix: "test:horizon", ReportTTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("connect node stream: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	before := time.Now().Add(-2 * 30 * time.Second).UnixMilli()
	follow := bus.warmUpShard(ctx, 0, func(*schedulerv1.NodeStateEvent) {})
	after := time.Now().UnixMilli()

	if follow == "$" {
		t.Fatal("an empty shard is followed from the live tail, which drops whatever lands during the handover")
	}
	var followMs, seq int64
	if _, err := fmt.Sscanf(follow, "%d-%d", &followMs, &seq); err != nil {
		t.Fatalf("follow id %q is not a stream id: %v", follow, err)
	}
	if followMs < before || followMs > after {
		t.Fatalf("follow id %q is outside the replay window [%d, %d]", follow, before, after)
	}
}

// Retention shorter than the replay window must be reported, and healthy
// retention must not be.
//
// `schedulerNodeStreamWarmupIncomplete` and its WARN are the only signal that a
// starting replica's view of the fleet is missing nodes. The check compares the
// stream's oldest entry against the first entry at or after the horizon: equal
// means nothing older than the horizon survived, so the tail was trimmed inside
// the window. Written the other way round it fired on every healthy replica and
// stayed silent on the trimmed one, which is worse than having no check --
// operators are told to raise node_stream_maxlen on a stream that is fine, and
// told nothing when it is not.
func TestNodeStreamWarmupFlagsShortRetentionAndNotHealthyRetention(t *testing.T) {
	addr := startRedisServerForTest(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reportTTL := 30 * time.Second
	bus, err := NewRedisNodeStream(ctx, addr, NodeStreamOptions{KeyPrefix: "test:retention", ReportTTL: reportTTL})
	if err != nil {
		t.Fatalf("connect node stream: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })

	key := bus.key(0)
	horizonMs := time.Now().Add(-2 * reportTTL).UnixMilli()

	// Healthy: an entry from well before the horizon is still retained, so the
	// replay covers the whole window.
	addEntry := func(atMs int64) {
		if err := bus.client.XAdd(ctx, &redis.XAddArgs{
			Stream: key,
			ID:     fmt.Sprintf("%d-0", atMs),
			Values: map[string]any{"payload": "x"},
		}).Err(); err != nil {
			t.Fatalf("seed stream entry at %d: %v", atMs, err)
		}
	}
	addEntry(horizonMs - 60_000)
	addEntry(horizonMs + 1_000)

	// The gauge is process-global and this test asserts on transitions, so it
	// is cleared rather than assumed clear -- under `-count=2` the second run
	// would otherwise inherit the first run's trimmed-case value.
	schedulerNodeStreamWarmupIncomplete.Set(0)
	bus.warmUpShard(ctx, 0, func(*schedulerv1.NodeStateEvent) {})
	if incomplete := readWarmupIncompleteGauge(t); incomplete != 0 {
		t.Fatalf("healthy retention reported as incomplete (gauge=%v); operators are told to raise "+
			"node_stream_maxlen on a stream that is fine", incomplete)
	}

	// Trimmed: drop everything older than the horizon, so the oldest entry
	// retained is itself inside the replay window.
	if err := bus.client.Del(ctx, key).Err(); err != nil {
		t.Fatalf("clear stream: %v", err)
	}
	addEntry(horizonMs + 1_000)
	addEntry(horizonMs + 2_000)

	bus.warmUpShard(ctx, 0, func(*schedulerv1.NodeStateEvent) {})
	if incomplete := readWarmupIncompleteGauge(t); incomplete != 1 {
		t.Fatalf("retention trimmed inside the replay window went unreported (gauge=%v); a replica "+
			"starts with part of the fleet missing and nothing says so", incomplete)
	}
}

func readWarmupIncompleteGauge(t *testing.T) float64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := schedulerNodeStreamWarmupIncomplete.Write(metric); err != nil {
		t.Fatalf("read warmup gauge: %v", err)
	}
	return metric.GetGauge().GetValue()
}
