package scheduler

import (
	"strings"
	"sync"
	"time"

	"agentenv/services/shared/config"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const defaultBindingTTL = 30 * time.Second

// defaultReconcileGracePeriod is the grace a store built without one uses, when
// the binding TTL leaves room for it. The value and the reasoning behind it
// live with the config, which checks the same relations at load time.
const defaultReconcileGracePeriod = config.DefaultReconcileGrace

// Outcomes of considering one binding a node's roster omitted. The in-memory
// store can only delete or retain; the Redis store also sees bindings that
// expired underneath it or moved to another node between the roster being
// collected and the delete being attempted.
const (
	reconcileOutcomeDeleted  = "deleted"
	reconcileOutcomeRetained = "retained"
	reconcileOutcomeMoved    = "moved"
	reconcileOutcomeAbsent   = "absent"
	reconcileOutcomeUnknown  = "unknown"
	// A roster named a sandbox the scheduler believes another node still holds.
	// Counted rather than logged per occurrence: during a handover it is the
	// expected outcome for one heartbeat, and it is only a problem if it does
	// not stop.
	reconcileOutcomeRefused = "refused"
)

// schedulerReconcileBindingsTotal counts what reconcile did with each binding a
// node's roster omitted.
//
// Reconcile deleting a live binding is silent by construction: the sandbox
// keeps running on the node, the client keeps an id the scheduler has
// forgotten, and nothing fails until the next request for that sandbox. The
// delete rate per node is the only place that shows up early. A node whose
// deletes track its genuine teardowns is healthy; one that deletes steadily
// while its retention count sits at zero is running with a grace period too
// short for its roster latency.
var schedulerReconcileBindingsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "agentenv_scheduler_reconcile_bindings_total",
		Help: "Bindings omitted from a node's heartbeat roster, by node and by what reconcile did with them.",
	},
	[]string{"node_id", "outcome"},
)

func recordReconcileOutcome(nodeID string, outcome string, count int) {
	if nodeID == "" || count <= 0 {
		return
	}
	schedulerReconcileBindingsTotal.WithLabelValues(nodeID, outcome).Add(float64(count))
}

// ValidateReconcileGrace checks the timing relations the grace period depends on
// but that nothing states or measures. It is config.ValidateReconcileGrace,
// kept here so the store's own tests and any caller building options by hand
// check exactly what the config loader checks.
func ValidateReconcileGrace(bindingTTL time.Duration, reconcileGrace time.Duration, heartbeatInterval time.Duration) error {
	return config.ValidateReconcileGrace(bindingTTL, reconcileGrace, heartbeatInterval)
}

// BindingStoreOptions carries the timings a binding store needs, so a caller
// can pass the configured binding TTL and reporting interval together and have
// the store reject a combination it cannot honour. The grace period is only
// meaningful relative to those two, and validating it here keeps that check
// next to the code that uses it.
type BindingStoreOptions struct {
	// BindingTTL is how long a binding survives without a refresh. Zero uses
	// the default.
	BindingTTL time.Duration
	// ReconcileGrace is how recently a binding must have been written for
	// reconcile to leave it alone. Zero derives one from the binding TTL, see
	// config.ResolveReconcileGrace. There is no way to ask for no grace at all
	// through here: that is a test affordance, and it goes through
	// NewInMemoryBindingStoreWithGrace.
	ReconcileGrace time.Duration
	// HeartbeatInterval is the interval nodes are expected to report at, from
	// scheduler.heartbeat_interval. Zero leaves the grace unchecked against it.
	HeartbeatInterval time.Duration
}

func (o BindingStoreOptions) resolve() (BindingStoreOptions, error) {
	if o.BindingTTL <= 0 {
		o.BindingTTL = defaultBindingTTL
	}
	grace, err := config.CheckReconcileGrace(o.BindingTTL, o.ReconcileGrace, o.HeartbeatInterval)
	if err != nil {
		return BindingStoreOptions{}, err
	}
	o.ReconcileGrace = grace
	return o, nil
}

// RosterCompleteness states whether a heartbeat's sandbox roster is the node's
// authoritative view of what it owns.
type RosterCompleteness uint8

const (
	// RosterIncomplete means the node had not finished startup recovery, so an
	// empty roster must not be read as "this node owns nothing".
	//
	// That is the whole of what it protects. A non-empty incomplete roster is
	// reconciled exactly as a complete one: bindings it omits are deleted once
	// past the grace. Nodes that predate roster_complete never claim it, and
	// honouring it for their non-empty rosters would leave every departed
	// sandbox of theirs to the binding TTL. A node still discovering what it
	// holds must therefore report nothing, not part of it; scheduler.proto
	// states the same rule on the wire field, and
	// TestNonEmptyIncompleteRosterStillReapsUnlistedBindings pins it.
	RosterIncomplete RosterCompleteness = iota
	// RosterComplete means the roster is the node's authoritative view. Bindings
	// it omits are deleted, except those written inside the reconcile grace
	// period, which the node cannot have seen when it collected the roster.
	RosterComplete
	// RosterFinal means the node is gone for good, as on explicit unregister.
	// Everything it owned is cleared immediately: the grace period exists to
	// protect bindings a live node has not observed yet, and there is no live
	// node left to observe them.
	RosterFinal
)

const defaultArtifactStoreCapacity = 1_000_000

// defaultArtifactLookupNodeLimit bounds how many providers one lookup names.
// The reasoning is with config.defaultSchedulerArtifactLookupNodeLimit, which
// this mirrors for a Service built without options.
const defaultArtifactLookupNodeLimit = 8

// forgetNodeChunkSize bounds how many of a departing node's keys ForgetNode
// removes under one hold of the write lock.
//
// ForgetNode is the one operation on this store that is not O(1): it walks
// every key the node held, and a node that published a large image cache
// holds tens of thousands. Holding the write lock for the whole walk stalls
// every lookup on every other key for its duration, on the request path of
// every peer trying to fetch a layer. Releasing between chunks lets those
// through; the walk itself takes no longer.
const forgetNodeChunkSize = 256

// BindingAssignment is one sandbox-to-node binding in a batch write.
type BindingAssignment struct {
	SandboxID string
	Node      Node
}

// Incarnation identifies one run of a node process.
//
// Nodes generate a time-ordered UUIDv7 at startup and carry it on Heartbeat,
// ReportSandboxEvent and UnregisterNode. Comparing incarnations lets the
// scheduler reject a write from a process that has since been replaced: a
// restarted node cannot resurrect bindings it lost, and an RPC delayed behind
// a restart cannot overwrite the live incarnation's view.
//
// This is the closest thing to a fencing token available without a resource
// that validates tokens. It is deliberately *not* the sandbox access token,
// which is a deterministic non-expiring HMAC identical on every node holding
// the seed and so incapable of ordering anything.
type Incarnation string

// Supersedes reports whether i replaces other.
//
// UUIDv7 sorts lexicographically in time order, so a plain comparison is
// enough. An empty incarnation is treated as "unknown" and never supersedes:
// an older node that does not report one must not be able to displace a newer
// one, and must not be locked out either.
func (i Incarnation) Supersedes(other Incarnation) bool {
	if i == "" || other == "" {
		return false
	}
	return i > other
}

type BindingStore interface {
	Get(sandboxID string, now time.Time) (Node, bool, error)
	Record(sandboxID string, node Node, now time.Time) error
	// RecordBatch records every assignment and returns a slice of errors
	// positionally aligned with assignments: entry i reports assignments[i],
	// and a nil entry means that binding was recorded. A failure that prevents
	// the whole batch from being attempted fills every entry with that error.
	//
	// Fork creates up to 100 children in one response, so recording them one
	// at a time serializes that many lock acquisitions or round trips inside
	// the caller's deadline.
	RecordBatch(assignments []BindingAssignment, now time.Time) []error
	ReconcileNode(node Node, sandboxIDs []string, now time.Time) error
	// ReconcileNodeRoster is ReconcileNode with the node's own statement about
	// whether its roster is authoritative. An empty roster is only grounds for
	// deleting a node's bindings when the node says the roster is complete.
	ReconcileNodeRoster(node Node, sandboxIDs []string, completeness RosterCompleteness, now time.Time) error
}

type ArtifactStore interface {
	Record(clusterID string, backend string, key string, nodeID string)
	Forget(clusterID string, backend string, key string, nodeID string)
	Lookup(clusterID string, backend string, key string) []string
	ForgetNode(nodeID string)
}

type bindingRecord struct {
	node      Node
	expiresAt time.Time
	// recordedAt is stamped from the scheduler's own clock when the binding is
	// written. Reconcile uses it to avoid deleting a binding that was recorded
	// after the reporting node collected its roster.
	//
	// It is deliberately scheduler-stamped rather than node-stamped: comparing
	// a node's clock against the scheduler's is the class of bug this exists to
	// prevent, not one to introduce here.
	recordedAt time.Time
}

type InMemoryBindingStore struct {
	mu             sync.Mutex
	bindingTTL     time.Duration
	reconcileGrace time.Duration
	bindings       map[string]bindingRecord
	nodeBinding    map[string]map[string]struct{}
}

func NewInMemoryBindingStore(bindingTTL time.Duration) *InMemoryBindingStore {
	return NewInMemoryBindingStoreWithGrace(bindingTTL, defaultReconcileGracePeriod)
}

// NewInMemoryBindingStoreWithOptions builds a store whose grace period has been
// checked against the binding TTL and the reporting interval.
func NewInMemoryBindingStoreWithOptions(opts BindingStoreOptions) (*InMemoryBindingStore, error) {
	resolved, err := opts.resolve()
	if err != nil {
		return nil, err
	}
	return NewInMemoryBindingStoreWithGrace(resolved.BindingTTL, resolved.ReconcileGrace), nil
}

// NewInMemoryBindingStoreWithGrace builds a store with an explicit reconcile
// grace period. A zero grace makes reconcile delete every binding a complete
// roster omits, however recently it was written.
//
// Nothing here is validated against the binding TTL: a zero grace is a valid
// instruction, and the relations only bind a caller that means the grace to
// protect anything. Those callers go through NewInMemoryBindingStoreWithOptions.
func NewInMemoryBindingStoreWithGrace(bindingTTL time.Duration, reconcileGrace time.Duration) *InMemoryBindingStore {
	if bindingTTL <= 0 {
		bindingTTL = defaultBindingTTL
	}
	if reconcileGrace < 0 {
		reconcileGrace = 0
	}
	return &InMemoryBindingStore{
		bindingTTL:     bindingTTL,
		reconcileGrace: reconcileGrace,
		bindings:       make(map[string]bindingRecord),
		nodeBinding:    make(map[string]map[string]struct{}),
	}
}

// normalizeBindingNode trims a node's fields and reports whether both survive.
// It is the one input contract both stores apply to a write: a node missing
// either field cannot be routed to, so recording it would only teach the
// gateway to dial "".
func normalizeBindingNode(node Node) (Node, bool) {
	node.ID = strings.TrimSpace(node.ID)
	node.Endpoint = strings.TrimSpace(node.Endpoint)
	return node, node.ID != "" && node.Endpoint != ""
}

func (s *InMemoryBindingStore) Get(sandboxID string, now time.Time) (Node, bool, error) {
	sandboxID = strings.TrimSpace(sandboxID)

	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.bindings[sandboxID]
	if !ok {
		return Node{}, false, nil
	}
	if !record.expiresAt.After(now) {
		s.deleteLocked(sandboxID)
		return Node{}, false, nil
	}
	return record.node, true, nil
}

// Record and RecordBatch drop an unroutable assignment silently, as the Redis
// store does: the caller has already created the sandbox and cannot undo it, so
// an error here has nothing to act on it. validateAssignment refuses such
// nodes before they reach either store; this is the contract for callers that
// compose a Node by hand.
func (s *InMemoryBindingStore) Record(sandboxID string, node Node, now time.Time) error {
	sandboxID = strings.TrimSpace(sandboxID)
	node, ok := normalizeBindingNode(node)
	if sandboxID == "" || !ok {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.upsertLocked(sandboxID, node, now)
	return nil
}

func (s *InMemoryBindingStore) RecordBatch(assignments []BindingAssignment, now time.Time) []error {
	if len(assignments) == 0 {
		return nil
	}

	errs := make([]error, len(assignments))

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, assignment := range assignments {
		sandboxID := strings.TrimSpace(assignment.SandboxID)
		node, ok := normalizeBindingNode(assignment.Node)
		if sandboxID == "" || !ok {
			continue
		}
		s.upsertLocked(sandboxID, node, now)
	}
	return errs
}

func (s *InMemoryBindingStore) ReconcileNode(node Node, sandboxIDs []string, now time.Time) error {
	return s.ReconcileNodeRoster(node, sandboxIDs, RosterComplete, now)
}

func (s *InMemoryBindingStore) ReconcileNodeRoster(node Node, sandboxIDs []string, completeness RosterCompleteness, now time.Time) error {
	node.ID = strings.TrimSpace(node.ID)
	node.Endpoint = strings.TrimSpace(node.Endpoint)
	if node.ID == "" {
		return nil
	}

	normalized := make(map[string]struct{}, len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		sandboxID = strings.TrimSpace(sandboxID)
		if sandboxID == "" {
			continue
		}
		normalized[sandboxID] = struct{}{}
	}
	// A roster can only be written under a node that can be routed to. An
	// empty roster writes nothing, so an endpoint-less node may still clear
	// its bindings: unregister names the node by id alone.
	if len(normalized) > 0 && node.Endpoint == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(normalized) == 0 {
		// An empty roster from a node that has not finished startup recovery
		// says nothing about what it owns. Deleting on it would wipe the whole
		// node's data plane on the strength of a report the node itself does
		// not consider authoritative; the TTL still reaps genuine departures.
		if completeness == RosterIncomplete {
			return nil
		}
		if bindings, ok := s.nodeBinding[node.ID]; ok {
			deleted, retained := 0, 0
			for sandboxID := range bindings {
				if completeness == RosterComplete && s.recordedWithinGraceLocked(sandboxID, now) {
					retained++
					continue
				}
				s.deleteLocked(sandboxID)
				deleted++
			}
			recordReconcileOutcome(node.ID, reconcileOutcomeDeleted, deleted)
			recordReconcileOutcome(node.ID, reconcileOutcomeRetained, retained)
		}
		return nil
	}

	expiresAt := now.Add(s.bindingTTL)
	stolen := 0
	for sandboxID := range normalized {
		if !s.refreshFromRosterLocked(sandboxID, node, expiresAt, now) {
			stolen++
		}
	}
	recordReconcileOutcome(node.ID, reconcileOutcomeRefused, stolen)

	current := s.nodeBinding[node.ID]
	if len(current) == 0 {
		return nil
	}

	deleted, retained := 0, 0
	for sandboxID := range current {
		if _, ok := normalized[sandboxID]; ok {
			continue
		}
		// A final roster leaves no live node to observe a binding it never
		// saw, so the grace protects nothing; the empty-roster branch above
		// and the Redis store already read it that way.
		if completeness != RosterFinal && s.recordedWithinGraceLocked(sandboxID, now) {
			retained++
			continue
		}
		s.deleteLocked(sandboxID)
		deleted++
	}
	recordReconcileOutcome(node.ID, reconcileOutcomeDeleted, deleted)
	recordReconcileOutcome(node.ID, reconcileOutcomeRetained, retained)
	return nil
}

// recordedWithinGraceLocked reports whether a binding was written too recently
// to have been visible when the reporting node collected its roster.
//
// Building a node snapshot walks every sandbox twice and reads /proc, so the
// gap between "roster collected" and "heartbeat applied" is wide enough that a
// sandbox created inside it is absent from the roster while already bound.
// Without this, that binding is deleted and the client holds an id the
// scheduler has just forgotten.
//
// The comparison is one-sided and unforgiving on the far side: a binding a
// microsecond older than the grace is deleted exactly as if it had been there
// for an hour. Widening it to <= would move that cliff by a microsecond and
// leave it a cliff. What keeps the cliff away from live bindings is the size of
// the grace relative to the reporting interval, checked once at construction,
// and the per-node delete counter that makes a wrong choice visible.
//
// Expiry here is lazy — only Get removes a lapsed record — so a record whose
// TTL has passed is treated as the absent key Redis would see, and gets no
// grace.
func (s *InMemoryBindingStore) recordedWithinGraceLocked(sandboxID string, now time.Time) bool {
	record, ok := s.bindings[sandboxID]
	if !ok || record.recordedAt.IsZero() || !record.expiresAt.After(now) {
		return false
	}
	return now.Sub(record.recordedAt) < s.reconcileGrace
}

func (s *InMemoryBindingStore) upsertLocked(sandboxID string, node Node, now time.Time) {
	s.upsertLockedWithExpiry(sandboxID, node, now.Add(s.bindingTTL), now)
}

// refreshFromRosterLocked writes a binding a node's roster claims, unless the
// scheduler already believes another node holds it.
//
// A roster says "I am running these", and for a sandbox that has never moved
// that is the last word. It stops being the last word the moment a paused
// sandbox can be handed to another node: after a handover, the origin keeps
// listing the sandbox until its own record is dropped, and dropping it is
// explicitly allowed to fail -- `MigrationSteps::release_origin_state` reports
// a failure there and does not undo the migration, because the sandbox really
// is live elsewhere. An unfenced roster then takes the binding back on the
// origin's next heartbeat, the destination's next heartbeat takes it again,
// and the two alternate for as long as both are up.
//
// So a roster may establish a binding that is absent and refresh one it already
// owns, but it may not move one. Moving is left to the deliberate acts that
// have a reason to: recording an assignment, and placing from a mobility
// record, which is the arbiter of who owns a paused sandbox in the first place.
//
// A binding pointing at a departed node is not stranded by this: bindings carry
// a TTL, and once it lapses the entry is absent and the next roster establishes
// it. Recovery is bounded by the TTL instead of being immediate, which is the
// price of not letting a stale roster overrule a live handover.
//
// Reports whether the write happened.
func (s *InMemoryBindingStore) refreshFromRosterLocked(
	sandboxID string,
	node Node,
	expiresAt time.Time,
	now time.Time,
) bool {
	if existing, ok := s.bindings[sandboxID]; ok &&
		existing.node.ID != node.ID &&
		existing.expiresAt.After(now) {
		return false
	}
	s.upsertLockedWithExpiry(sandboxID, node, expiresAt, now)
	return true
}

func (s *InMemoryBindingStore) upsertLockedWithExpiry(sandboxID string, node Node, expiresAt time.Time, recordedAt time.Time) {
	if existing, ok := s.bindings[sandboxID]; ok {
		if existing.node.ID != node.ID {
			s.removeNodeBindingLocked(existing.node.ID, sandboxID)
		} else if !existing.recordedAt.IsZero() && existing.expiresAt.After(recordedAt) {
			// A heartbeat refresh must not restamp recordedAt. It marks when
			// this sandbox->node binding was established, so that a binding
			// written after the reporting node collected its roster survives
			// one reconcile. Restamping on every refresh would instead extend
			// the grace to every deletion, indefinitely.
			//
			// A record whose TTL has lapsed is not refreshed but re-established:
			// in Redis the key is physically gone by then and the write stamps
			// afresh, and the grace clock restarts with it here too.
			recordedAt = existing.recordedAt
		}
	}

	if _, ok := s.nodeBinding[node.ID]; !ok {
		s.nodeBinding[node.ID] = make(map[string]struct{})
	}
	s.nodeBinding[node.ID][sandboxID] = struct{}{}
	s.bindings[sandboxID] = bindingRecord{
		node:       node,
		expiresAt:  expiresAt,
		recordedAt: recordedAt,
	}
}

func (s *InMemoryBindingStore) deleteLocked(sandboxID string) {
	record, ok := s.bindings[sandboxID]
	if !ok {
		return
	}
	delete(s.bindings, sandboxID)
	s.removeNodeBindingLocked(record.node.ID, sandboxID)
}

func (s *InMemoryBindingStore) removeNodeBindingLocked(nodeID string, sandboxID string) {
	bindings, ok := s.nodeBinding[nodeID]
	if !ok {
		return
	}
	delete(bindings, sandboxID)
	if len(bindings) == 0 {
		delete(s.nodeBinding, nodeID)
	}
}

type artifactIndexKey struct {
	clusterID string
	backend   string
	key       string
}

type InMemoryArtifactStore struct {
	mu              sync.RWMutex
	entries         map[artifactIndexKey]map[string]struct{}
	nodeKeys        map[string]map[artifactIndexKey]struct{}
	lru             *lru.Cache[artifactIndexKey, struct{}]
	lookupNodeLimit int
	// betweenForgetChunks, when set, runs each time ForgetNode has released
	// the lock between two chunks. It exists so a test can prove the lock is
	// released mid-walk; production leaves it nil.
	betweenForgetChunks func()
}

func NewInMemoryArtifactStore(capacity int, lookupNodeLimit int) *InMemoryArtifactStore {
	if capacity <= 0 {
		capacity = defaultArtifactStoreCapacity
	}

	store := &InMemoryArtifactStore{
		entries:         make(map[artifactIndexKey]map[string]struct{}),
		nodeKeys:        make(map[string]map[artifactIndexKey]struct{}),
		lookupNodeLimit: lookupNodeLimit,
	}
	cache, err := lru.NewWithEvict(capacity, store.evictLocked)
	if err != nil {
		panic(err)
	}
	store.lru = cache
	return store
}

func (s *InMemoryArtifactStore) Record(clusterID string, backend string, key string, nodeID string) {
	indexKey, ok := normalizeArtifactIndexKey(clusterID, backend, key)
	nodeID = strings.TrimSpace(nodeID)
	if !ok || nodeID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.entries[indexKey]; !ok {
		s.entries[indexKey] = make(map[string]struct{})
	}
	s.entries[indexKey][nodeID] = struct{}{}

	if _, ok := s.nodeKeys[nodeID]; !ok {
		s.nodeKeys[nodeID] = make(map[artifactIndexKey]struct{})
	}
	s.nodeKeys[nodeID][indexKey] = struct{}{}
	s.lru.Add(indexKey, struct{}{})
}

func (s *InMemoryArtifactStore) Forget(clusterID string, backend string, key string, nodeID string) {
	indexKey, ok := normalizeArtifactIndexKey(clusterID, backend, key)
	nodeID = strings.TrimSpace(nodeID)
	if !ok || nodeID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.forgetLocked(indexKey, nodeID)
}

func (s *InMemoryArtifactStore) Lookup(clusterID string, backend string, key string) []string {
	indexKey, ok := normalizeArtifactIndexKey(clusterID, backend, key)
	if !ok {
		return nil
	}

	s.mu.RLock()
	nodes := s.entries[indexKey]
	if len(nodes) == 0 {
		s.mu.RUnlock()
		return nil
	}
	resultCapacity := len(nodes)
	if s.lookupNodeLimit > 0 && s.lookupNodeLimit < resultCapacity {
		resultCapacity = s.lookupNodeLimit
	}
	result := make([]string, 0, resultCapacity)
	// A prefix of the map's iteration order, which Go randomises per
	// iteration: with more providers than the limit, successive lookups name
	// different subsets, spreading fetches across them for free. A sorted
	// prefix would send every peer to the same few nodes.
	for nodeID := range nodes {
		result = append(result, nodeID)
		if s.lookupNodeLimit > 0 && len(result) >= s.lookupNodeLimit {
			break
		}
	}
	s.mu.RUnlock()

	// The recency touch happens outside s.mu on purpose. The LRU has its own
	// lock and Get never fires the eviction callback, so nothing here needs
	// the store lock — and running library code under it would make the
	// store's consistency depend on that library never calling back from Get,
	// which an expiring or TTL-based LRU would break. See evictLocked.
	s.lru.Get(indexKey)

	return result
}

// ForgetNode removes a node from every key it provided, in chunks so lookups
// on the rest of the index are not held behind the whole walk.
//
// The keys are snapshotted once. A Record for the same node that lands between
// chunks adds to both maps under its own lock hold and is left alone: it is a
// later statement about the node than this forget, and removing it would make
// the index disagree with what the node just said. The reverse index is only
// dropped once it is empty, so such a key stays reachable from both sides.
func (s *InMemoryArtifactStore) ForgetNode(nodeID string) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return
	}

	s.mu.Lock()
	keys := make([]artifactIndexKey, 0, len(s.nodeKeys[nodeID]))
	for indexKey := range s.nodeKeys[nodeID] {
		keys = append(keys, indexKey)
	}
	s.forgetKeysLocked(nodeID, keys[:min(len(keys), forgetNodeChunkSize)])
	s.mu.Unlock()

	for start := forgetNodeChunkSize; start < len(keys); start += forgetNodeChunkSize {
		if s.betweenForgetChunks != nil {
			s.betweenForgetChunks()
		}
		s.mu.Lock()
		s.forgetKeysLocked(nodeID, keys[start:min(start+forgetNodeChunkSize, len(keys))])
		s.mu.Unlock()
	}
}

// forgetKeysLocked removes nodeID from each key and each key from nodeID's
// reverse index, dropping the reverse index once nothing is left in it.
func (s *InMemoryArtifactStore) forgetKeysLocked(nodeID string, keys []artifactIndexKey) {
	for _, indexKey := range keys {
		s.forgetLocked(indexKey, nodeID)
	}
	if held := s.nodeKeys[nodeID]; held != nil && len(held) == 0 {
		delete(s.nodeKeys, nodeID)
	}
}

func (s *InMemoryArtifactStore) forgetLocked(indexKey artifactIndexKey, nodeID string) {
	if nodes := s.entries[indexKey]; len(nodes) > 0 {
		delete(nodes, nodeID)
		if len(nodes) == 0 {
			s.removeArtifactKeyLocked(indexKey)
		}
	}

	if keys := s.nodeKeys[nodeID]; len(keys) > 0 {
		delete(keys, indexKey)
		if len(keys) == 0 {
			delete(s.nodeKeys, nodeID)
		}
	}
}

func (s *InMemoryArtifactStore) removeArtifactKeyLocked(indexKey artifactIndexKey) {
	delete(s.entries, indexKey)
	s.lru.Remove(indexKey)
}

// evictLocked keeps entries and nodeKeys consistent when the LRU drops a key
// for capacity.
//
// It mutates both maps without taking s.mu, so it is only correct if the LRU
// invokes it while the caller already holds the write lock. That is a
// requirement on the LRU implementation, not an observation about this one:
// the callback must fire only from Add, Remove and Resize — every call to
// which is made under s.mu.Lock here — and never from Get, Peek, Contains or
// a background goroutine. golang-lru/v2's plain Cache satisfies it; its
// expirable variant does not, because its expiry goroutine evicts on a timer
// with no lock of ours held. Swapping the cache means re-establishing this.
func (s *InMemoryArtifactStore) evictLocked(indexKey artifactIndexKey, _ struct{}) {
	schedulerP2PArtifactEvictionsTotal.Inc()
	nodes := s.entries[indexKey]
	delete(s.entries, indexKey)
	for nodeID := range nodes {
		if keys := s.nodeKeys[nodeID]; len(keys) > 0 {
			delete(keys, indexKey)
			if len(keys) == 0 {
				delete(s.nodeKeys, nodeID)
			}
		}
	}
}

func normalizeArtifactIndexKey(clusterID string, backend string, key string) (artifactIndexKey, bool) {
	indexKey := artifactIndexKey{
		clusterID: strings.TrimSpace(clusterID),
		backend:   strings.TrimSpace(backend),
		key:       strings.TrimSpace(key),
	}
	return indexKey, indexKey.clusterID != "" && indexKey.backend != "" && indexKey.key != ""
}
