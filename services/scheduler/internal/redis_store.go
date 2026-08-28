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
const defaultRedisNodeIndexTTL = time.Hour

type redisBindingRecord struct {
	Node Node `json:"node"`
	// RecordedAtMs is stamped inside Lua from redis.call("TIME") so every
	// binding, whichever scheduler replica wrote it, is timed by the one clock
	// both the write and the reconcile can see.
	RecordedAtMs int64 `json:"recorded_at_ms,omitempty"`
}

type RedisBindingStore struct {
	client           *redis.Client
	bindingTTL       time.Duration
	reconcileGrace   time.Duration
	keyPrefix        string
	operationTimeout time.Duration
}

func NewRedisBindingStore(addr string, bindingTTL time.Duration) (*RedisBindingStore, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, fmt.Errorf("redis address is required")
	}
	if bindingTTL <= 0 {
		bindingTTL = defaultBindingTTL
	}

	var opts *redis.Options
	if strings.Contains(addr, "://") {
		parsed, err := redis.ParseURL(addr)
		if err != nil {
			return nil, fmt.Errorf("parse redis address: %w", err)
		}
		opts = parsed
	} else {
		opts = &redis.Options{Addr: addr}
	}

	store := &RedisBindingStore{
		client:           redis.NewClient(opts),
		bindingTTL:       bindingTTL,
		reconcileGrace:   defaultReconcileGracePeriod,
		keyPrefix:        defaultRedisBindingKeyPrefix,
		operationTimeout: defaultRedisOperationTimeout,
	}
	ctx, cancel := store.context()
	defer cancel()
	if err := store.client.Ping(ctx).Err(); err != nil {
		_ = store.client.Close()
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	return store, nil
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
		if err == redis.Nil {
			return Node{}, false, nil
		}
		return Node{}, false, fmt.Errorf("redis get binding: %w", err)
	}
	node, ok := parseRedisBindingBytes(raw)
	return node, ok, nil
}

func (s *RedisBindingStore) Record(sandboxID string, node Node, _ time.Time) error {
	sandboxID = strings.TrimSpace(sandboxID)
	node.ID = strings.TrimSpace(node.ID)
	node.Endpoint = strings.TrimSpace(node.Endpoint)
	if sandboxID == "" || node.ID == "" || node.Endpoint == "" {
		return nil
	}
	value, err := marshalRedisNode(node)
	if err != nil {
		return err
	}

	ctx, cancel := s.context()
	defer cancel()
	if err := redisRecordBindingScript.Run(ctx, s.client,
		[]string{s.bindingKey(sandboxID), s.nodeKey(node.ID)},
		value,
		node.ID,
		sandboxID,
		int64(s.bindingTTL/time.Millisecond),
		s.keyPrefix,
		s.nodeIndexTTLMillis(),
	).Err(); err != nil {
		return fmt.Errorf("redis record binding: %w", err)
	}
	return nil
}

func (s *RedisBindingStore) RecordBatch(assignments []BindingAssignment, _ time.Time) []error {
	if len(assignments) == 0 {
		return nil
	}

	errs := make([]error, len(assignments))

	ctx, cancel := s.context()
	defer cancel()

	// One pipeline for the whole batch: a 100-way fork otherwise pays 100
	// sequential round trips inside the caller's deadline.
	pipe := s.client.Pipeline()
	commands := make([]*redis.Cmd, len(assignments))
	for i, assignment := range assignments {
		sandboxID := strings.TrimSpace(assignment.SandboxID)
		node := assignment.Node
		node.ID = strings.TrimSpace(node.ID)
		node.Endpoint = strings.TrimSpace(node.Endpoint)
		if sandboxID == "" || node.ID == "" || node.Endpoint == "" {
			continue
		}
		value, err := marshalRedisNode(node)
		if err != nil {
			errs[i] = err
			continue
		}
		commands[i] = redisRecordBindingScript.Run(ctx, pipe,
			[]string{s.bindingKey(sandboxID), s.nodeKey(node.ID)},
			value,
			node.ID,
			sandboxID,
			int64(s.bindingTTL/time.Millisecond),
			s.keyPrefix,
			s.nodeIndexTTLMillis(),
		)
	}

	// Exec reports the first command error; per-command results are read below,
	// so a single failure does not mask the outcome of its siblings.
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		for i := range errs {
			if errs[i] == nil && commands[i] != nil {
				errs[i] = fmt.Errorf("redis record binding: %w", err)
			}
		}
	}
	for i, cmd := range commands {
		if cmd == nil || errs[i] != nil {
			continue
		}
		if err := cmd.Err(); err != nil && !errors.Is(err, redis.Nil) {
			errs[i] = fmt.Errorf("redis record binding: %w", err)
		}
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

	rosterComplete := "0"
	grace := int64(s.reconcileGrace / time.Millisecond)
	switch completeness {
	case RosterComplete:
		rosterComplete = "1"
	case RosterFinal:
		// The node is gone; nothing is left to observe a binding it never saw.
		rosterComplete = "1"
		grace = 0
	}

	args := make([]any, 0, 8+len(desired))
	args = append(args,
		node.ID,
		value,
		int64(s.bindingTTL/time.Millisecond),
		s.keyPrefix,
		s.nodeIndexTTLMillis(),
		len(desired),
		rosterComplete,
		grace,
	)
	for _, sandboxID := range desired {
		args = append(args, sandboxID)
	}

	ctx, cancel := s.context()
	defer cancel()
	if err := redisReconcileNodeScript.Run(ctx, s.client, []string{s.nodeKey(node.ID)}, args...).Err(); err != nil {
		return fmt.Errorf("redis reconcile node bindings: %w", err)
	}
	return nil
}

func (s *RedisBindingStore) context() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.operationTimeout)
}

func (s *RedisBindingStore) bindingKey(sandboxID string) string {
	return s.keyPrefix + ":sandbox:" + sandboxID
}

func (s *RedisBindingStore) nodeKey(nodeID string) string {
	return s.keyPrefix + ":node:" + nodeID
}

func (s *RedisBindingStore) nodeIndexTTLMillis() int64 {
	return int64(defaultRedisNodeIndexTTL / time.Millisecond)
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

const redisRecordBindingScriptSource = redisLuaHelpers + `
-- KEYS[1]: sandbox binding key, e.g. {prefix}:sandbox:{sandbox_id}
-- KEYS[2]: target node reverse-index set key, e.g. {prefix}:node:{node_id}
-- ARGV[1]: node object JSON ({"node_id":"...","endpoint":"..."})
-- ARGV[2]: target node ID
-- ARGV[3]: sandbox ID
-- ARGV[4]: binding TTL in milliseconds
-- ARGV[5]: redis binding key prefix, used to remove stale reverse-index entries from old nodes
-- ARGV[6]: node reverse-index set TTL in milliseconds
local raw = redis.call("GET", KEYS[1])
local old_node_id = parse_node_id(raw)
if old_node_id and old_node_id ~= ARGV[2] then
  redis.call("SREM", ARGV[5] .. ":node:" .. old_node_id, ARGV[3])
end
-- recorded_at marks when this sandbox->node binding was established, so a
-- refresh of the same owner keeps the original stamp.
local recorded_at = now_ms()
if old_node_id == ARGV[2] then
  recorded_at = parse_recorded_at(raw) or recorded_at
end
redis.call("SET", KEYS[1], build_value(ARGV[1], recorded_at), "PX", ARGV[4])
redis.call("SADD", KEYS[2], ARGV[3])
redis.call("PEXPIRE", KEYS[2], ARGV[6])
return 1
`

const redisReconcileNodeScriptSource = redisLuaHelpers + `
-- KEYS[1]: reconciled node reverse-index set key, e.g. {prefix}:node:{node_id}
-- ARGV[1]: reconciled node ID
-- ARGV[2]: node object JSON for this node ({"node_id":"...","endpoint":"..."}); empty when desired_count is 0
-- ARGV[3]: binding TTL in milliseconds
-- ARGV[4]: redis binding key prefix, used to build sandbox binding keys and other node reverse-index keys
-- ARGV[5]: node reverse-index set TTL in milliseconds
-- ARGV[6]: desired sandbox ID count (N)
-- ARGV[7]: 1 when the reporting node considers its roster authoritative, else 0
-- ARGV[8]: reconcile grace period in milliseconds
-- ARGV[9..8+N]: desired sandbox IDs reported by the node heartbeat
local node_key = KEYS[1]
local node_id = ARGV[1]
local value = ARGV[2]
local ttl_ms = ARGV[3]
local key_prefix = ARGV[4]
local node_index_ttl_ms = ARGV[5]
local desired_count = tonumber(ARGV[6]) or 0
local roster_complete = ARGV[7] == "1"
local grace_ms = tonumber(ARGV[8]) or 0
local reconcile_now = now_ms()

-- within_grace reports whether a binding was written too recently to have been
-- visible when the reporting node collected its roster.
local function within_grace(raw)
  local recorded_at = parse_recorded_at(raw)
  if not recorded_at then
    return false
  end
  return (reconcile_now - recorded_at) < grace_ms
end

local function binding_key(sandbox_id)
  return key_prefix .. ":sandbox:" .. sandbox_id
end

local function node_key_for(id)
  return key_prefix .. ":node:" .. id
end

-- An empty roster from a node that has not finished startup recovery says
-- nothing about what it owns, so it is not grounds for deleting anything.
if desired_count == 0 and not roster_complete then
  return 1
end

local desired = {}
for i = 1, desired_count do
  local sandbox_id = ARGV[8 + i]
  desired[sandbox_id] = true
end

local retained = 0
local current = redis.call("SMEMBERS", node_key)
for _, sandbox_id in ipairs(current) do
  if not desired[sandbox_id] then
    local raw = redis.call("GET", binding_key(sandbox_id))
    if within_grace(raw) then
      retained = retained + 1
    else
      local old_node_id = parse_node_id(raw)
      if old_node_id == node_id then
        redis.call("DEL", binding_key(sandbox_id))
      end
      redis.call("SREM", node_key, sandbox_id)
    end
  end
end

for sandbox_id, _ in pairs(desired) do
  local raw = redis.call("GET", binding_key(sandbox_id))
  local old_node_id = parse_node_id(raw)
  if old_node_id and old_node_id ~= node_id then
    redis.call("SREM", node_key_for(old_node_id), sandbox_id)
  end
  local recorded_at = reconcile_now
  if old_node_id == node_id then
    recorded_at = parse_recorded_at(raw) or recorded_at
  end
  redis.call("SET", binding_key(sandbox_id), build_value(value, recorded_at), "PX", ttl_ms)
  redis.call("SADD", node_key, sandbox_id)
end

if desired_count > 0 or retained > 0 then
  redis.call("PEXPIRE", node_key, node_index_ttl_ms)
else
  redis.call("DEL", node_key)
end
return 1
`

var redisRecordBindingScript = redis.NewScript(redisRecordBindingScriptSource)
var redisReconcileNodeScript = redis.NewScript(redisReconcileNodeScriptSource)
