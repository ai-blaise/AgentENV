package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// MobilityRecord is what a node knows about one of its paused sandboxes.
//
// The scheduler holds these because the claim protocol they support is
// inherently cross-node: a destination takes a sandbox from an origin, and two
// nodes cannot arbitrate through a store that lives on one of their disks.
// Records were originally node-local, which meant a destination's claim was
// written somewhere the origin never read and the origin's resume fence could
// never fire.
//
// # Why the fingerprint is opaque
//
// `Fingerprint` is the node's compatibility description — kernel, Firecracker
// build, CPU template, page geometry. The scheduler never interprets it and
// never compares it; only a candidate node can say whether it matches its own.
// Keeping it opaque means the fingerprint can gain fields without a scheduler
// release, which matters because it is the part most likely to grow.
type MobilityRecord struct {
	SandboxID    string `json:"sandbox_id"`
	OriginNodeID string `json:"origin_node_id"`
	// Generation is a UUIDv7 in hyphenated hex. Its string order is its
	// numeric order — the 48-bit timestamp leads, big-endian, and every value
	// is the same length — so a lexicographic compare is a valid ordering and
	// Lua can do it without parsing.
	Generation    string          `json:"generation"`
	Fingerprint   json.RawMessage `json:"fingerprint,omitempty"`
	ArtifactReach string          `json:"artifact_reach"`
	CPUCount      uint32          `json:"cpu_count"`
	MemoryMiB     uint32          `json:"memory_mib"`
	// SnapshotID is empty until the paused state is committed somewhere the
	// cluster can read. An empty value is what makes a sandbox unmovable.
	SnapshotID   string `json:"snapshot_id,omitempty"`
	PausedAtMs   int64  `json:"paused_at_ms"`
	State        string `json:"state"`
	HolderNodeID string `json:"holder_node_id,omitempty"`
	StateAtMs    int64  `json:"state_at_ms,omitempty"`
}

// Mobility record states, mirroring the node-side enum.
const (
	MobilityParked    = "parked"
	MobilityClaimed   = "claimed"
	MobilityEvacuated = "evacuated"
)

func (r MobilityRecord) valid() error {
	if strings.TrimSpace(r.SandboxID) == "" {
		return errors.New("mobility record requires a sandbox id")
	}
	if strings.TrimSpace(r.OriginNodeID) == "" {
		return errors.New("mobility record requires an origin node id")
	}
	if strings.TrimSpace(r.Generation) == "" {
		return errors.New("mobility record requires a generation")
	}
	switch r.State {
	case MobilityParked, MobilityClaimed, MobilityEvacuated:
	default:
		return fmt.Errorf("unknown mobility state %q", r.State)
	}
	return nil
}

// MobilityStore arbitrates paused-sandbox ownership across the fleet.
type MobilityStore interface {
	// Upsert writes the record unless a generation at least as new is already
	// stored, and reports whether it was applied.
	//
	// This is a compare-and-set, not a read followed by a write. That is the
	// whole reason the store moved here: the node-local version could not be
	// atomic even between two threads in one process, and the claim protocol
	// depends on exactly one writer winning.
	Upsert(ctx context.Context, record MobilityRecord) (bool, error)
	// CompareAndSet writes the record only if the stored generation is exactly
	// `expected`; an empty `expected` means "only if nothing is stored".
	//
	// This is what turns a claim into an arbitration. Upsert's generation
	// ordering cannot do it: every claimant mints a newer generation, so every
	// claimant's write supersedes what it read and they are all told they won.
	CompareAndSet(ctx context.Context, expected string, record MobilityRecord) (bool, error)
	Get(ctx context.Context, sandboxID string) (MobilityRecord, bool, error)
	// ListByOrigin returns the records a node is holding, which is what a
	// drain of that node plans over.
	ListByOrigin(ctx context.Context, nodeID string) ([]MobilityRecord, error)
	Remove(ctx context.Context, sandboxID string) error
}

// InMemoryMobilityStore is the default, for a deployment with one scheduler
// and no Redis. Records are lost on restart, which costs the ability to
// migrate until each node re-reports — not correctness.
type InMemoryMobilityStore struct {
	mu      sync.RWMutex
	records map[string]MobilityRecord
}

func NewInMemoryMobilityStore() *InMemoryMobilityStore {
	return &InMemoryMobilityStore{records: make(map[string]MobilityRecord)}
}

func (s *InMemoryMobilityStore) Upsert(_ context.Context, record MobilityRecord) (bool, error) {
	if err := record.valid(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.records[record.SandboxID]; ok {
		// Equal generations do not supersede: allowing a rewrite under the
		// same generation would make the ordering decide nothing.
		if record.Generation <= existing.Generation {
			return false, nil
		}
	}
	s.records[record.SandboxID] = record
	return true, nil
}

func (s *InMemoryMobilityStore) CompareAndSet(
	_ context.Context,
	expected string,
	record MobilityRecord,
) (bool, error) {
	if err := record.valid(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := ""
	if existing, ok := s.records[record.SandboxID]; ok {
		current = existing.Generation
	}
	if current != expected {
		return false, nil
	}
	s.records[record.SandboxID] = record
	return true, nil
}

func (s *InMemoryMobilityStore) Get(_ context.Context, sandboxID string) (MobilityRecord, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.records[sandboxID]
	return record, ok, nil
}

func (s *InMemoryMobilityStore) ListByOrigin(_ context.Context, nodeID string) ([]MobilityRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]MobilityRecord, 0)
	for _, record := range s.records {
		if record.OriginNodeID == nodeID {
			out = append(out, record)
		}
	}
	sortRecords(out)
	return out, nil
}

func (s *InMemoryMobilityStore) Remove(_ context.Context, sandboxID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, sandboxID)
	return nil
}

// RedisMobilityStore keeps records where a restarted scheduler and a
// query-only replica can both see them.
//
// Keys follow the binding store's layout and for the same reason: each record
// is tagged by its sandbox id so it shards independently, each origin index by
// its node id, and no script touches more than one key. See RedisBindingStore
// for why that constraint exists and what replaces the atomicity it costs.
type RedisMobilityStore struct {
	client           redis.UniversalClient
	keyPrefix        string
	operationTimeout time.Duration
	indexTTL         time.Duration
}

const defaultMobilityKeyPrefix = "agentenv:scheduler:mobility"

// defaultMobilityIndexTTL bounds an origin index whose node never comes back.
//
// Records themselves do not expire: a paused sandbox does not stop existing
// because nobody asked about it, and expiring one would silently make a
// sandbox unmovable. The index is a cache of which sandboxes belong to a node
// and is rebuilt from the records the node re-reports.
const defaultMobilityIndexTTL = 24 * time.Hour

// MobilityStoreFor returns the mobility store that matches a binding store:
// Redis-backed on the binding store's own client when bindings are in Redis,
// in-memory otherwise.
//
// The two are decided together because they answer the same question — is
// there a store outside this process — and a deployment that put bindings in
// Redis so a restart or a second replica could see them needs its mobility
// records there for the same reason. Sharing the client is deliberate: every
// mobility operation is request-scoped and bounded by the same timeout, so it
// costs the pool nothing a binding write does not.
func MobilityStoreFor(bindings BindingStore) MobilityStore {
	if redisStore, ok := bindings.(*RedisBindingStore); ok && redisStore != nil && redisStore.client != nil {
		return NewRedisMobilityStore(redisStore.client)
	}
	return NewInMemoryMobilityStore()
}

func NewRedisMobilityStore(client redis.UniversalClient) *RedisMobilityStore {
	return &RedisMobilityStore{
		client:           client,
		keyPrefix:        defaultMobilityKeyPrefix,
		operationTimeout: defaultRedisOperationTimeout,
		indexTTL:         defaultMobilityIndexTTL,
	}
}

func (s *RedisMobilityStore) recordKey(sandboxID string) string {
	return s.keyPrefix + ":sandbox:{" + sandboxID + "}"
}

func (s *RedisMobilityStore) originKey(nodeID string) string {
	return s.keyPrefix + ":origin:{" + nodeID + "}"
}

func (s *RedisMobilityStore) context(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.operationTimeout)
}

func (s *RedisMobilityStore) Upsert(ctx context.Context, record MobilityRecord) (bool, error) {
	if err := record.valid(); err != nil {
		return false, err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return false, fmt.Errorf("encode mobility record: %w", err)
	}

	ctx, cancel := s.context(ctx)
	defer cancel()

	applied, err := s.runUpsert(ctx, record, encoded)
	if err != nil {
		return false, err
	}
	if !applied {
		return false, nil
	}

	// The origin index is a hint used only by ListByOrigin, so a failure here
	// costs a slower drain rather than a wrong record, and is not reported as
	// a write failure. It self-heals: the node re-reports the record and the
	// index entry is added again.
	pipe := s.client.Pipeline()
	pipe.SAdd(ctx, s.originKey(record.OriginNodeID), record.SandboxID)
	pipe.PExpire(ctx, s.originKey(record.OriginNodeID), s.indexTTL)
	_, _ = pipe.Exec(ctx)
	return true, nil
}

func (s *RedisMobilityStore) runUpsert(ctx context.Context, record MobilityRecord, encoded []byte) (bool, error) {
	run := func() (any, error) {
		return redisMobilityUpsertScript.Run(ctx, s.client,
			[]string{s.recordKey(record.SandboxID)},
			string(encoded),
			record.Generation,
		).Result()
	}
	result, err := run()
	if isNoScriptError(err) {
		if loadErr := redisMobilityUpsertScript.Load(ctx, s.client).Err(); loadErr != nil {
			return false, fmt.Errorf("reload mobility script: %w", loadErr)
		}
		result, err = run()
	}
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, fmt.Errorf("redis upsert mobility record: %w", err)
	}
	applied, _ := result.(int64)
	return applied == 1, nil
}

// CompareAndSet is one script on one key, so it is atomic and legal on a
// cluster. That atomicity is the point: the node-local store could not offer
// it even between two threads, which is why the records moved here.
func (s *RedisMobilityStore) CompareAndSet(
	ctx context.Context,
	expected string,
	record MobilityRecord,
) (bool, error) {
	if err := record.valid(); err != nil {
		return false, err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return false, fmt.Errorf("encode mobility record: %w", err)
	}

	ctx, cancel := s.context(ctx)
	defer cancel()

	run := func() (any, error) {
		return redisMobilityCompareAndSetScript.Run(ctx, s.client,
			[]string{s.recordKey(record.SandboxID)},
			string(encoded),
			expected,
		).Result()
	}
	result, err := run()
	if isNoScriptError(err) {
		if loadErr := redisMobilityCompareAndSetScript.Load(ctx, s.client).Err(); loadErr != nil {
			return false, fmt.Errorf("reload mobility script: %w", loadErr)
		}
		result, err = run()
	}
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, fmt.Errorf("redis compare-and-set mobility record: %w", err)
	}
	applied, _ := result.(int64)
	if applied != 1 {
		return false, nil
	}

	pipe := s.client.Pipeline()
	pipe.SAdd(ctx, s.originKey(record.OriginNodeID), record.SandboxID)
	pipe.PExpire(ctx, s.originKey(record.OriginNodeID), s.indexTTL)
	_, _ = pipe.Exec(ctx)
	return true, nil
}

func (s *RedisMobilityStore) Get(ctx context.Context, sandboxID string) (MobilityRecord, bool, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return MobilityRecord{}, false, nil
	}
	ctx, cancel := s.context(ctx)
	defer cancel()

	raw, err := s.client.Get(ctx, s.recordKey(sandboxID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return MobilityRecord{}, false, nil
		}
		return MobilityRecord{}, false, fmt.Errorf("redis get mobility record: %w", err)
	}
	var record MobilityRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		// A record we cannot read is not a record. Reporting it as absent
		// would let a claim proceed against state nobody can evaluate, so
		// this surfaces as an error instead.
		return MobilityRecord{}, false, fmt.Errorf("decode mobility record %s: %w", sandboxID, err)
	}
	return record, true, nil
}

func (s *RedisMobilityStore) ListByOrigin(ctx context.Context, nodeID string) ([]MobilityRecord, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return nil, nil
	}
	ctx, cancel := s.context(ctx)
	defer cancel()

	ids, err := s.client.SMembers(ctx, s.originKey(nodeID)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("redis list mobility origin index: %w", err)
	}

	// Each record is its own key in its own slot, so this is a pipeline of
	// single-key reads rather than an MGET, which a cluster would refuse.
	pipe := s.client.Pipeline()
	gets := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		gets[i] = pipe.Get(ctx, s.recordKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("redis read mobility records: %w", err)
	}

	records := make([]MobilityRecord, 0, len(ids))
	stale := make([]any, 0)
	for i, get := range gets {
		raw, err := get.Bytes()
		if errors.Is(err, redis.Nil) {
			// Indexed but gone: the record was removed and the index has not
			// caught up. Drop the entry rather than carrying it forever.
			stale = append(stale, ids[i])
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("redis read mobility record %s: %w", ids[i], err)
		}
		var record MobilityRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, fmt.Errorf("decode mobility record %s: %w", ids[i], err)
		}
		// The index is a hint; the record is the truth about who owns it.
		if record.OriginNodeID != nodeID {
			stale = append(stale, ids[i])
			continue
		}
		records = append(records, record)
	}
	if len(stale) > 0 {
		_ = s.client.SRem(ctx, s.originKey(nodeID), stale...).Err()
	}

	sortRecords(records)
	return records, nil
}

func (s *RedisMobilityStore) Remove(ctx context.Context, sandboxID string) error {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return nil
	}
	ctx, cancel := s.context(ctx)
	defer cancel()

	// Read first so the origin index entry can be removed too. A failure
	// between the two leaves an index entry pointing at nothing, which
	// ListByOrigin drops on sight.
	record, found, err := s.Get(ctx, sandboxID)
	if err != nil {
		// Undecodable: still remove the record, but there is no origin to
		// clean up. The index entry will be dropped by the next list.
		record = MobilityRecord{}
		found = false
	}
	if err := s.client.Del(ctx, s.recordKey(sandboxID)).Err(); err != nil {
		return fmt.Errorf("redis remove mobility record: %w", err)
	}
	if found && record.OriginNodeID != "" {
		_ = s.client.SRem(ctx, s.originKey(record.OriginNodeID), sandboxID).Err()
	}
	return nil
}

// sortRecords gives every listing a stable order, so a plan built from one is
// reproducible and can be reviewed before it is run.
func sortRecords(records []MobilityRecord) {
	sort.Slice(records, func(i, j int) bool {
		return records[i].SandboxID < records[j].SandboxID
	})
}

// redisMobilityUpsertScript is the compare-and-set the claim protocol needs.
//
// One key, so it is legal on a cluster. String comparison is a valid ordering
// for UUIDv7 in hyphenated hex: the timestamp leads, big-endian, and every
// value is the same length.
const redisMobilityUpsertScriptSource = `
-- KEYS[1]: mobility record key
-- ARGV[1]: encoded record
-- ARGV[2]: the record's generation
local raw = redis.call("GET", KEYS[1])
if raw then
  local ok, decoded = pcall(cjson.decode, raw)
  if ok and decoded and decoded["generation"] then
    -- Equal generations do not supersede. Allowing a rewrite under the same
    -- generation would make the ordering decide nothing.
    if ARGV[2] <= decoded["generation"] then
      return 0
    end
  end
end
redis.call("SET", KEYS[1], ARGV[1])
return 1
`

var redisMobilityUpsertScript = redis.NewScript(redisMobilityUpsertScriptSource)

// redisMobilityCompareAndSetScript writes only when the stored generation is
// exactly what the caller last read.
//
// An empty ARGV[2] means "only if absent", which the empty string cannot
// otherwise express — "expect nothing" and "do not check" are opposite
// instructions and must not collapse into one.
const redisMobilityCompareAndSetScriptSource = `
-- KEYS[1]: mobility record key
-- ARGV[1]: encoded record
-- ARGV[2]: expected generation, or "" for "must not exist"
local raw = redis.call("GET", KEYS[1])
local current = ""
if raw then
  local ok, decoded = pcall(cjson.decode, raw)
  if ok and decoded and decoded["generation"] then
    current = decoded["generation"]
  end
end
if current ~= ARGV[2] then
  return 0
end
redis.call("SET", KEYS[1], ARGV[1])
return 1
`

var redisMobilityCompareAndSetScript = redis.NewScript(redisMobilityCompareAndSetScriptSource)
