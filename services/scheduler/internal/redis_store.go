package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultRedisOperationTimeout = 2 * time.Second
const defaultRedisBindingKeyPrefix = "agentenv:scheduler:bindings"

// nodeIndexTTLMultiple bounds how far a node index may outlive the bindings it
// names, as a multiple of the binding TTL.
//
// The index accumulates entries no binding backs: queueIndexMove can only drop
// a sandbox from its old node's index when the SET script could name that node,
// and a binding key that had already expired names nobody. Expiry is therefore
// the only thing that ever removes those entries. A flat hour against a
// thirty-second binding TTL let one survive its binding by two orders of
// magnitude; deriving it keeps the two in step whatever the TTL is configured
// to. Four leaves slack for a node that misses several reports and still holds
// bindings, since every write and every reconcile pushes the expiry back out.
const nodeIndexTTLMultiple = 4

type redisBindingRecord struct {
	Node Node `json:"node"`
	// RecordedAtMs is stamped inside Lua from redis.call("TIME") so every
	// binding, whichever scheduler replica wrote it, is timed by the one clock
	// both the write and the reconcile can see.
	RecordedAtMs int64 `json:"recorded_at_ms,omitempty"`
}

// RedisBindingStore keeps sandbox-to-node bindings in Redis, on a single
// instance or across a cluster.
//
// # Why no operation spans two keys
//
// Redis Cluster shards by key, and a script may only touch keys in one slot.
// The obvious layout — a binding key and a per-node index set, mutated
// together — cannot satisfy that: a node's sandboxes hash all over the
// keyspace. Pinning everything into one slot with a shared hash tag would make
// it legal and pointless, since every binding in the fleet would land on one
// shard.
//
// So each key stands alone. A binding is tagged by its sandbox id, a node
// index by its node id, and every script touches exactly one of them. What
// used to be one atomic script is now a sequence of single-key operations, and
// the guards below are what make that safe.
//
// # Why losing atomicity is affordable here
//
// Reconciliation is a convergence loop, not a transaction: it runs again on
// the next heartbeat. The two ways a sequence can be interrupted both heal.
//
//   - An index entry whose binding is gone is skipped, then dropped from the
//     index on the pass that notices.
//   - A binding missing from its node's index is untouched by reconciliation
//     and re-added the next time the node reports it in a roster.
//
// What must not happen is deleting a binding that has moved on, and that is
// prevented per key rather than by a lock: a delete only fires when the
// binding still names the node being reconciled, and never inside the grace
// window.
//
// The grace is the one guard here that is a bet rather than a proof. It holds
// only while it is longer than the time from a node collecting its roster to
// this delete running, and nothing in the request path can observe that time.
// So a caller that knows the reporting interval has the value checked against
// it (NewRedisBindingStoreWithOptions), and every outcome below is counted per
// node.
type RedisBindingStore struct {
	client           redis.UniversalClient
	bindingTTL       time.Duration
	reconcileGrace   time.Duration
	keyPrefix        string
	operationTimeout time.Duration
}

// NewRedisBindingStore connects to `addr`, which may be a comma-separated list
// of cluster seeds.
//
// Whether to speak the cluster protocol is asked of the server rather than
// inferred from the address. A single-seed cluster is ordinary, and a client
// that guessed "one address means one instance" would fail on the first MOVED
// it received — at some arbitrary later moment rather than at startup.
func NewRedisBindingStore(addr string, bindingTTL time.Duration) (*RedisBindingStore, error) {
	addrs, opts, err := parseRedisAddress(addr)
	if err != nil {
		return nil, err
	}
	if bindingTTL <= 0 {
		bindingTTL = defaultBindingTTL
	}

	store := &RedisBindingStore{
		bindingTTL:       bindingTTL,
		reconcileGrace:   defaultReconcileGracePeriod,
		keyPrefix:        defaultRedisBindingKeyPrefix,
		operationTimeout: defaultRedisOperationTimeout,
	}

	client, err := store.connect(addrs, opts)
	if err != nil {
		return nil, err
	}
	store.client = client

	ctx, cancel := store.context()
	defer cancel()
	if err := store.loadScripts(ctx); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("load redis scripts: %w", err)
	}
	return store, nil
}

// NewRedisBindingStoreWithOptions is NewRedisBindingStore with the grace period
// checked against the binding TTL and the interval nodes report at, for callers
// that have both to hand.
func NewRedisBindingStoreWithOptions(addr string, opts BindingStoreOptions) (*RedisBindingStore, error) {
	resolved, err := opts.resolve()
	if err != nil {
		return nil, err
	}
	store, err := NewRedisBindingStore(addr, resolved.BindingTTL)
	if err != nil {
		return nil, err
	}
	store.reconcileGrace = resolved.ReconcileGrace
	return store, nil
}

// parseRedisAddress splits a configured address into seeds plus any options a
// URL form carried.
func parseRedisAddress(addr string) ([]string, *redis.Options, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, nil, fmt.Errorf("redis address is required")
	}

	if strings.Contains(addr, "://") {
		parsed, err := redis.ParseURL(addr)
		if err != nil {
			return nil, nil, fmt.Errorf("parse redis address: %w", err)
		}
		return []string{parsed.Addr}, parsed, nil
	}

	seeds := make([]string, 0, 1)
	for _, seed := range strings.Split(addr, ",") {
		seed = strings.TrimSpace(seed)
		if seed != "" {
			seeds = append(seeds, seed)
		}
	}
	if len(seeds) == 0 {
		return nil, nil, fmt.Errorf("redis address is required")
	}
	return seeds, nil, nil
}

func (s *RedisBindingStore) connect(addrs []string, opts *redis.Options) (redis.UniversalClient, error) {
	return connectRedis(addrs, opts, s.operationTimeout, 0)
}

// connectRedis dials seeds and speaks whichever protocol the server is running.
//
// Whether this is a cluster is asked of the server rather than inferred from
// the address: a single-seed cluster is ordinary, and a client that guessed
// "one address means one instance" would fail on the first MOVED it received,
// at some arbitrary later moment rather than at startup.
//
// poolSize overrides the client's connection pool, for a caller that holds
// connections open indefinitely — blocking reads — and would otherwise starve
// everything sharing the pool.
func connectRedis(addrs []string, opts *redis.Options, timeout time.Duration, poolSize int) (redis.UniversalClient, error) {
	if timeout <= 0 {
		timeout = defaultRedisOperationTimeout
	}
	withTimeout := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), timeout)
	}

	singleOpts := singleOptions(addrs[0], opts)
	if poolSize > 0 {
		singleOpts.PoolSize = poolSize
	}
	single := redis.NewClient(singleOpts)
	ctx, cancel := withTimeout()
	defer cancel()

	// `INFO cluster`, not `CLUSTER INFO`: only the former reports whether
	// cluster mode is enabled at all. `CLUSTER INFO` describes the state of a
	// cluster and says nothing about whether this server is in one.
	info, err := single.Info(ctx, "cluster").Result()
	if err == nil && strings.Contains(info, "cluster_enabled:1") {
		_ = single.Close()
		clusterOpts := clusterOptions(addrs, opts)
		if poolSize > 0 {
			clusterOpts.PoolSize = poolSize
		}
		cluster := redis.NewClusterClient(clusterOpts)
		ctx, cancel := withTimeout()
		defer cancel()
		if err := cluster.Ping(ctx).Err(); err != nil {
			_ = cluster.Close()
			return nil, fmt.Errorf("connect redis cluster: %w", err)
		}
		return cluster, nil
	}
	// A server with cluster support compiled out answers with an error rather
	// than with cluster_enabled:0, so an error here is not itself a failure.
	if err := single.Ping(ctx).Err(); err != nil {
		_ = single.Close()
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	return single, nil
}

func singleOptions(addr string, opts *redis.Options) *redis.Options {
	if opts == nil {
		return &redis.Options{Addr: addr}
	}
	clone := *opts
	clone.Addr = addr
	return &clone
}

func clusterOptions(addrs []string, opts *redis.Options) *redis.ClusterOptions {
	cluster := &redis.ClusterOptions{Addrs: addrs}
	if opts != nil {
		cluster.Username = opts.Username
		cluster.Password = opts.Password
		cluster.TLSConfig = opts.TLSConfig
	}
	return cluster
}

// loadScripts primes every server with the scripts, so the common path can use
// EVALSHA.
//
// On a cluster this has to reach every master: a script is cached per server,
// and a client that loaded it on one shard would find it missing the moment a
// key hashed elsewhere.
func (s *RedisBindingStore) loadScripts(ctx context.Context) error {
	load := func(ctx context.Context, target redis.Scripter) error {
		for _, script := range []*redis.Script{
			redisSetBindingScript,
			redisRefreshBindingScript,
			redisDeleteBindingScript,
		} {
			if err := script.Load(ctx, target).Err(); err != nil {
				return err
			}
		}
		return nil
	}
	if cluster, ok := s.client.(*redis.ClusterClient); ok {
		return cluster.ForEachMaster(ctx, func(ctx context.Context, shard *redis.Client) error {
			return load(ctx, shard)
		})
	}
	return load(ctx, s.client)
}

// runPipeline executes a batch of scripted commands, reloading the scripts and
// retrying once if the server has forgotten them.
//
// `Script.Run` normally falls back from EVALSHA to EVAL on NOSCRIPT, but
// inside a pipeline it cannot: the error only becomes visible after Exec, by
// which point the batch is already sent. A Redis restart or a SCRIPT FLUSH
// would otherwise fail every binding write until the process was restarted —
// and, as the caller reports per-assignment errors rather than one, would fail
// them quietly.
func (s *RedisBindingStore) runPipeline(
	ctx context.Context,
	build func(redis.Pipeliner) []*redis.Cmd,
) ([]*redis.Cmd, error) {
	pipe := s.client.Pipeline()
	commands := build(pipe)
	_, err := pipe.Exec(ctx)
	if err != nil && isNoScriptError(err) {
		if loadErr := s.loadScripts(ctx); loadErr != nil {
			return commands, fmt.Errorf("reload redis scripts: %w", loadErr)
		}
		pipe = s.client.Pipeline()
		commands = build(pipe)
		_, err = pipe.Exec(ctx)
	}
	if err != nil && !errors.Is(err, redis.Nil) {
		return commands, err
	}
	return commands, nil
}

func isNoScriptError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOSCRIPT")
}

func (s *RedisBindingStore) Close() error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Close()
}

func (s *RedisBindingStore) Get(sandboxID string, _ time.Time) (Node, bool, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return Node{}, false, nil
	}

	ctx, cancel := s.context()
	defer cancel()
	raw, err := s.client.Get(ctx, s.bindingKey(sandboxID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return Node{}, false, nil
		}
		return Node{}, false, fmt.Errorf("redis get binding: %w", err)
	}
	node, ok := parseRedisBindingBytes(raw)
	return node, ok, nil
}

func (s *RedisBindingStore) Record(sandboxID string, node Node, now time.Time) error {
	errs := s.RecordBatch([]BindingAssignment{{SandboxID: sandboxID, Node: node}}, now)
	if len(errs) == 0 {
		return nil
	}
	return errs[0]
}

func (s *RedisBindingStore) RecordBatch(assignments []BindingAssignment, _ time.Time) []error {
	if len(assignments) == 0 {
		return nil
	}
	errs := make([]error, len(assignments))

	ctx, cancel := s.context()
	defer cancel()

	// Two pipelines rather than two round trips per assignment. A 100-way fork
	// would otherwise pay 100 sequential round trips inside the caller's
	// deadline; on a cluster go-redis splits each pipeline per shard, so the
	// cost is one round trip per shard rather than per binding.
	nodes := make([]Node, len(assignments))
	values := make([]string, len(assignments))
	for i, assignment := range assignments {
		sandboxID := strings.TrimSpace(assignment.SandboxID)
		node, ok := normalizeBindingNode(assignment.Node)
		if sandboxID == "" || !ok {
			continue
		}
		value, err := marshalRedisNode(node)
		if err != nil {
			errs[i] = err
			continue
		}
		nodes[i] = node
		values[i] = value
	}

	writes, err := s.runPipeline(ctx, func(pipe redis.Pipeliner) []*redis.Cmd {
		commands := make([]*redis.Cmd, len(assignments))
		for i, assignment := range assignments {
			if values[i] == "" || errs[i] != nil {
				continue
			}
			commands[i] = redisSetBindingScript.Run(ctx, pipe,
				[]string{s.bindingKey(strings.TrimSpace(assignment.SandboxID))},
				values[i],
				nodes[i].ID,
				int64(s.bindingTTL/time.Millisecond),
			)
		}
		return commands
	})
	if err != nil {
		for i := range errs {
			if errs[i] == nil && values[i] != "" {
				errs[i] = fmt.Errorf("redis record binding: %w", err)
			}
		}
	}

	// Second pass: move each sandbox into its new node's index, and out of the
	// index of whichever node used to own it. Index membership is a hint used
	// only by reconciliation, so a failure here costs a slower convergence
	// rather than a wrong binding, and is not reported as a record failure.
	index := s.client.Pipeline()
	for i, cmd := range writes {
		if cmd == nil || errs[i] != nil {
			continue
		}
		if err := cmd.Err(); err != nil && !errors.Is(err, redis.Nil) {
			errs[i] = fmt.Errorf("redis record binding: %w", err)
			continue
		}
		sandboxID := strings.TrimSpace(assignments[i].SandboxID)
		previous := commandString(cmd)
		s.queueIndexMove(ctx, index, sandboxID, previous, nodes[i].ID)
	}
	if _, err := index.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		// Deliberately not an error for the caller. See above.
		_ = err
	}
	return errs
}

func (s *RedisBindingStore) ReconcileNode(node Node, sandboxIDs []string, now time.Time) error {
	return s.ReconcileNodeRoster(node, sandboxIDs, RosterComplete, now)
}

func (s *RedisBindingStore) ReconcileNodeRoster(node Node, sandboxIDs []string, completeness RosterCompleteness, _ time.Time) error {
	node.ID = strings.TrimSpace(node.ID)
	node.Endpoint = strings.TrimSpace(node.Endpoint)
	if node.ID == "" {
		return nil
	}

	desired := normalizeSandboxIDs(sandboxIDs)
	desiredSet := make(map[string]struct{}, len(desired))
	for _, sandboxID := range desired {
		desiredSet[sandboxID] = struct{}{}
	}

	value := ""
	if len(desired) > 0 {
		if node.Endpoint == "" {
			return nil
		}
		var err error
		value, err = marshalRedisNode(node)
		if err != nil {
			return err
		}
	}

	// An empty roster from a node that has not finished startup recovery says
	// nothing about what it owns, so it is not grounds for deleting anything.
	if len(desired) == 0 && completeness == RosterIncomplete {
		return nil
	}

	grace := s.reconcileGrace
	if completeness == RosterFinal {
		// The node is gone; nothing is left to observe a binding it never saw.
		grace = 0
	}

	ctx, cancel := s.context()
	defer cancel()

	nodeKey := s.nodeKey(node.ID)
	current, err := s.client.SMembers(ctx, nodeKey).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("redis read node index: %w", err)
	}

	removed, retained, err := s.deleteDeparted(ctx, node.ID, current, desiredSet, grace)
	if err != nil {
		return err
	}

	written, refused, err := s.refreshRoster(ctx, desired, value, node.ID)
	if err != nil {
		return err
	}
	recordReconcileOutcome(node.ID, reconcileOutcomeRefused, len(refused))

	// Only what was actually written joins this node's index. A refused entry
	// belongs to whoever the binding names, and adding it here would leave two
	// nodes' indexes claiming it with neither having written the binding.
	return s.updateNodeIndex(ctx, nodeKey, written, removed, retained)
}

// deleteDeparted removes bindings the node no longer reports.
//
// Each delete is guarded on the binding still naming this node, so a sandbox
// that moved elsewhere between the roster being collected and this running is
// left alone rather than deleted out from under its new owner.
//
// Every outcome the script reports is counted against the node, deletes
// included. A delete that beats the grace window by a millisecond leaves a
// running sandbox with no binding, and produces no error and no log line; the
// per-node delete rate is where it shows.
func (s *RedisBindingStore) deleteDeparted(
	ctx context.Context,
	nodeID string,
	current []string,
	desired map[string]struct{},
	grace time.Duration,
) (removed []string, retained int, err error) {
	departed := make([]string, 0, len(current))
	for _, sandboxID := range current {
		if _, ok := desired[sandboxID]; !ok {
			departed = append(departed, sandboxID)
		}
	}
	if len(departed) == 0 {
		return nil, 0, nil
	}

	commands, execErr := s.runPipeline(ctx, func(pipe redis.Pipeliner) []*redis.Cmd {
		commands := make([]*redis.Cmd, len(departed))
		for i, sandboxID := range departed {
			commands[i] = redisDeleteBindingScript.Run(ctx, pipe,
				[]string{s.bindingKey(sandboxID)},
				nodeID,
				int64(grace/time.Millisecond),
			)
		}
		return commands
	})
	if execErr != nil {
		return nil, 0, fmt.Errorf("redis delete departed bindings: %w", execErr)
	}

	removed = make([]string, 0, len(departed))
	outcomes := make(map[string]int, 4)
	for i, cmd := range commands {
		outcome := reconcileOutcomeLabel(commandString(cmd))
		outcomes[outcome]++
		if outcome == reconcileOutcomeRetained {
			// Written after the node collected its roster; the next pass sees it.
			retained++
			continue
		}
		// Deleted, absent, or owned by someone else — either way it does not
		// belong in this node's index.
		removed = append(removed, departed[i])
	}
	for outcome, count := range outcomes {
		recordReconcileOutcome(nodeID, outcome, count)
	}
	return removed, retained, nil
}

// reconcileOutcomeLabel maps a delete-script result onto a metric label. An
// unrecognised result folds into one bucket rather than letting server output
// grow the label set.
func reconcileOutcomeLabel(result string) string {
	switch result {
	case reconcileOutcomeDeleted, reconcileOutcomeRetained, reconcileOutcomeMoved, reconcileOutcomeAbsent:
		return result
	default:
		return reconcileOutcomeUnknown
	}
}

// refreshRoster writes every binding the node reports and returns, per
// sandbox, whichever node previously owned it.
// refreshRoster writes the bindings a roster claims, and reports which of them
// another node still holds.
//
// The refusal is the point: see InMemoryBindingStore.refreshFromRosterLocked
// for why a roster may establish and refresh a binding but not move one.
func (s *RedisBindingStore) refreshRoster(
	ctx context.Context,
	desired []string,
	value string,
	nodeID string,
) (written []string, refused []string, err error) {
	if len(desired) == 0 {
		return nil, nil, nil
	}

	commands, err := s.runPipeline(ctx, func(pipe redis.Pipeliner) []*redis.Cmd {
		commands := make([]*redis.Cmd, len(desired))
		for i, sandboxID := range desired {
			commands[i] = redisRefreshBindingScript.Run(ctx, pipe,
				[]string{s.bindingKey(sandboxID)},
				value,
				nodeID,
				int64(s.bindingTTL/time.Millisecond),
			)
		}
		return commands
	})
	if err != nil {
		return nil, nil, fmt.Errorf("redis refresh node bindings: %w", err)
	}

	written = make([]string, 0, len(desired))
	for i, cmd := range commands {
		// The script returns the current owner when it declined to write, and
		// an empty string when it wrote.
		if owner := commandString(cmd); owner != "" && owner != nodeID {
			refused = append(refused, desired[i])
			continue
		}
		written = append(written, desired[i])
	}
	return written, refused, nil
}

// updateNodeIndex brings the reverse index in line with what was just written.
func (s *RedisBindingStore) updateNodeIndex(
	ctx context.Context,
	nodeKey string,
	desired []string,
	removed []string,
	retained int,
) error {
	pipe := s.client.Pipeline()
	if len(removed) > 0 {
		pipe.SRem(ctx, nodeKey, toAny(removed)...)
	}
	if len(desired) > 0 {
		pipe.SAdd(ctx, nodeKey, toAny(desired)...)
	}
	if len(desired) > 0 || retained > 0 {
		pipe.PExpire(ctx, nodeKey, s.nodeIndexTTL())
	} else {
		pipe.Del(ctx, nodeKey)
	}

	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("redis update node index: %w", err)
	}
	return nil
}

// queueIndexMove adds the sandbox to its new node's index and removes it from
// the old one's, when the SET script was able to say who the old one was.
//
// Often it cannot. The previous owner is read out of the binding key, so a
// sandbox rebound after its binding expired names nobody, and the node that
// used to hold it keeps an index entry for it. Two node indexes then list the
// same sandbox, and that is tolerated rather than fixed.
//
// It is tolerable because the index is a hint, not an answer. It names nodes
// that may no longer hold a sandbox, and every consumer has to treat it that
// way: reconciliation re-reads the binding key for each entry and deletes only
// while the binding still names the reconciling node, so an entry in the wrong
// index costs one wasted read. Nothing else reads it, and nothing may start
// reading it as an ownership record. What a stale entry must not do is outlive
// its binding indefinitely, which is what the derived TTL bounds.
func (s *RedisBindingStore) queueIndexMove(
	ctx context.Context,
	pipe redis.Pipeliner,
	sandboxID string,
	previousNodeID string,
	nodeID string,
) {
	if previousNodeID != "" && previousNodeID != nodeID {
		pipe.SRem(ctx, s.nodeKey(previousNodeID), sandboxID)
	}
	pipe.SAdd(ctx, s.nodeKey(nodeID), sandboxID)
	pipe.PExpire(ctx, s.nodeKey(nodeID), s.nodeIndexTTL())
}

// nodeIndexTTL is read by every site that refreshes an index key, so the write
// path and the reconcile path cannot drift apart.
func (s *RedisBindingStore) nodeIndexTTL() time.Duration {
	return nodeIndexTTLMultiple * s.bindingTTL
}

func (s *RedisBindingStore) context() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.operationTimeout)
}

// bindingKey tags on the sandbox id so every binding shards independently and
// the read path — one key, on every proxied request — never leaves its slot.
func (s *RedisBindingStore) bindingKey(sandboxID string) string {
	return s.keyPrefix + ":sandbox:{" + sandboxID + "}"
}

// nodeKey tags on the node id so a node's index is one key in one slot.
func (s *RedisBindingStore) nodeKey(nodeID string) string {
	return s.keyPrefix + ":node:{" + nodeID + "}"
}

func toAny(values []string) []any {
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = value
	}
	return out
}

// commandString reads a script result that returns a string, treating any
// other shape — including an error already reported elsewhere — as empty.
func commandString(cmd *redis.Cmd) string {
	if cmd == nil || cmd.Err() != nil {
		return ""
	}
	value, err := cmd.Text()
	if err != nil {
		return ""
	}
	return value
}

func normalizeSandboxIDs(sandboxIDs []string) []string {
	seen := make(map[string]struct{}, len(sandboxIDs))
	result := make([]string, 0, len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		sandboxID = strings.TrimSpace(sandboxID)
		if sandboxID == "" {
			continue
		}
		if _, ok := seen[sandboxID]; ok {
			continue
		}
		seen[sandboxID] = struct{}{}
		result = append(result, sandboxID)
	}
	return result
}

// marshalRedisNode emits the node object alone. The Lua scripts splice it into
// the stored record together with a server-stamped recorded_at, so the stamp
// cannot come from a caller's clock.
func marshalRedisNode(node Node) (string, error) {
	data, err := json.Marshal(node)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func parseRedisBindingBytes(raw []byte) (Node, bool) {
	var record redisBindingRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return Node{}, false
	}
	node := Node{
		ID:       strings.TrimSpace(record.Node.ID),
		Endpoint: strings.TrimSpace(record.Node.Endpoint),
	}
	if node.ID == "" || node.Endpoint == "" {
		return Node{}, false
	}
	return node, true
}

const redisLuaHelpers = `
local function parse_node_id(raw)
  if not raw then
    return nil
  end
  local ok, decoded = pcall(cjson.decode, raw)
  if not ok or not decoded or not decoded["node"] then
    return nil
  end
  local node_id = decoded["node"]["node_id"]
  if not node_id or node_id == "" then
    return nil
  end
  return node_id
end

local function parse_recorded_at(raw)
  if not raw then
    return nil
  end
  local ok, decoded = pcall(cjson.decode, raw)
  if not ok or not decoded then
    return nil
  end
  return tonumber(decoded["recorded_at_ms"])
end

-- Redis server time in milliseconds. Both the binding write and the reconcile
-- that may delete it are stamped here, so the comparison never spans clocks.
local function now_ms()
  local t = redis.call("TIME")
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- build_value splices the recorded-at stamp into the caller-supplied node JSON.
local function build_value(node_json, recorded_at)
  return '{"node":' .. node_json .. ',"recorded_at_ms":' .. recorded_at .. '}'
end
`

// redisSetBindingScript writes one binding and reports who held it before.
//
// The previous owner is returned rather than acted on, because that owner's
// index lives in another slot and this script may not touch it. The caller
// moves the index entry afterwards.
const redisSetBindingScriptSource = redisLuaHelpers + `
-- KEYS[1]: sandbox binding key
-- ARGV[1]: node object JSON ({"node_id":"...","endpoint":"..."})
-- ARGV[2]: target node ID
-- ARGV[3]: binding TTL in milliseconds
local raw = redis.call("GET", KEYS[1])
local old_node_id = parse_node_id(raw)

-- recorded_at marks when this sandbox->node binding was established, so a
-- refresh by the same owner keeps the original stamp and the reconcile grace
-- window does not restart on every heartbeat.
local recorded_at = now_ms()
if old_node_id == ARGV[2] then
  recorded_at = parse_recorded_at(raw) or recorded_at
end

redis.call("SET", KEYS[1], build_value(ARGV[1], recorded_at), "PX", ARGV[3])
return old_node_id or ""
`

// redisDeleteBindingScript removes a binding the reporting node no longer
// claims, if it is still that node's to remove.
const redisDeleteBindingScriptSource = redisLuaHelpers + `
-- KEYS[1]: sandbox binding key
-- ARGV[1]: reconciling node ID
-- ARGV[2]: reconcile grace period in milliseconds
local raw = redis.call("GET", KEYS[1])
if not raw then
  return "absent"
end

-- Written too recently to have been visible when the node collected its
-- roster, so its absence from that roster says nothing.
local recorded_at = parse_recorded_at(raw)
local grace_ms = tonumber(ARGV[2]) or 0
if recorded_at and (now_ms() - recorded_at) < grace_ms then
  return "retained"
end

-- Owned by someone else now. Not ours to delete, and not ours to index.
if parse_node_id(raw) ~= ARGV[1] then
  return "moved"
end

redis.call("DEL", KEYS[1])
return "deleted"
`

// redisRefreshBindingScript is redisSetBindingScript for a roster refresh: it
// writes only when nothing is stored or the reporting node already owns the
// entry.
//
// The reasoning is with InMemoryBindingStore.refreshFromRosterLocked; the two
// stores answer the same question and have to answer it the same way. Redis
// needs its own script rather than a read-then-write because two replicas
// reconcile concurrently, and a check outside the write is not a check.
const redisRefreshBindingScriptSource = redisLuaHelpers + `
-- KEYS[1]: sandbox binding key
-- ARGV[1]: node object JSON ({"node_id":"...","endpoint":"..."})
-- ARGV[2]: reporting node ID
-- ARGV[3]: binding TTL in milliseconds
local raw = redis.call("GET", KEYS[1])
local old_node_id = parse_node_id(raw)

if old_node_id and old_node_id ~= ARGV[2] then
  -- Another node holds it. Say who, so the caller can count the refusal and
  -- leave the reverse index alone.
  return old_node_id
end

local recorded_at = now_ms()
if old_node_id == ARGV[2] then
  recorded_at = parse_recorded_at(raw) or recorded_at
end

redis.call("SET", KEYS[1], build_value(ARGV[1], recorded_at), "PX", ARGV[3])
return ""
`

var redisSetBindingScript = redis.NewScript(redisSetBindingScriptSource)
var redisRefreshBindingScript = redis.NewScript(redisRefreshBindingScriptSource)
var redisDeleteBindingScript = redis.NewScript(redisDeleteBindingScriptSource)
