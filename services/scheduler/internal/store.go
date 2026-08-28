package scheduler

import (
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

const defaultBindingTTL = 30 * time.Second

// defaultReconcileGracePeriod is how recently a binding must have been written
// for reconcile to leave it alone even when the reporting node's roster omits
// it. It only has to cover the interval between a node collecting its roster
// and the scheduler applying that heartbeat.
const defaultReconcileGracePeriod = 10 * time.Second

// RosterCompleteness states whether a heartbeat's sandbox roster is the node's
// authoritative view of what it owns.
type RosterCompleteness uint8

const (
	// RosterIncomplete means the node had not finished startup recovery, so an
	// empty roster must not be read as "this node owns nothing".
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

// NewInMemoryBindingStoreWithGrace builds a store with an explicit reconcile
// grace period. A zero grace makes reconcile delete every binding a complete
// roster omits, however recently it was written.
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

func (s *InMemoryBindingStore) Get(sandboxID string, now time.Time) (Node, bool, error) {
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

func (s *InMemoryBindingStore) Record(sandboxID string, node Node, now time.Time) error {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
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
		if sandboxID == "" {
			continue
		}
		s.upsertLocked(sandboxID, assignment.Node, now)
	}
	return errs
}

func (s *InMemoryBindingStore) ReconcileNode(node Node, sandboxIDs []string, now time.Time) error {
	return s.ReconcileNodeRoster(node, sandboxIDs, RosterComplete, now)
}

func (s *InMemoryBindingStore) ReconcileNodeRoster(node Node, sandboxIDs []string, completeness RosterCompleteness, now time.Time) error {
	normalized := make(map[string]struct{}, len(sandboxIDs))
	for _, sandboxID := range sandboxIDs {
		sandboxID = strings.TrimSpace(sandboxID)
		if sandboxID == "" {
			continue
		}
		normalized[sandboxID] = struct{}{}
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
			for sandboxID := range bindings {
				if completeness == RosterComplete && s.recordedWithinGraceLocked(sandboxID, now) {
					continue
				}
				s.deleteLocked(sandboxID)
			}
		}
		return nil
	}

	expiresAt := now.Add(s.bindingTTL)
	for sandboxID := range normalized {
		s.upsertLockedWithExpiry(sandboxID, node, expiresAt, now)
	}

	current := s.nodeBinding[node.ID]
	if len(current) == 0 {
		return nil
	}

	for sandboxID := range current {
		if _, ok := normalized[sandboxID]; ok {
			continue
		}
		if s.recordedWithinGraceLocked(sandboxID, now) {
			continue
		}
		s.deleteLocked(sandboxID)
	}
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
func (s *InMemoryBindingStore) recordedWithinGraceLocked(sandboxID string, now time.Time) bool {
	record, ok := s.bindings[sandboxID]
	if !ok || record.recordedAt.IsZero() {
		return false
	}
	return now.Sub(record.recordedAt) < s.reconcileGrace
}

func (s *InMemoryBindingStore) upsertLocked(sandboxID string, node Node, now time.Time) {
	s.upsertLockedWithExpiry(sandboxID, node, now.Add(s.bindingTTL), now)
}

func (s *InMemoryBindingStore) upsertLockedWithExpiry(sandboxID string, node Node, expiresAt time.Time, recordedAt time.Time) {
	if existing, ok := s.bindings[sandboxID]; ok {
		if existing.node.ID != node.ID {
			s.removeNodeBindingLocked(existing.node.ID, sandboxID)
		} else if !existing.recordedAt.IsZero() {
			// A heartbeat refresh must not restamp recordedAt. It marks when
			// this sandbox->node binding was established, so that a binding
			// written after the reporting node collected its roster survives
			// one reconcile. Restamping on every refresh would instead extend
			// the grace to every deletion, indefinitely.
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
	s.lru.Get(indexKey)
	resultCapacity := len(nodes)
	if s.lookupNodeLimit > 0 && s.lookupNodeLimit < resultCapacity {
		resultCapacity = s.lookupNodeLimit
	}
	result := make([]string, 0, resultCapacity)
	for nodeID := range nodes {
		result = append(result, nodeID)
		if s.lookupNodeLimit > 0 && len(result) >= s.lookupNodeLimit {
			break
		}
	}
	s.mu.RUnlock()

	return result
}

func (s *InMemoryArtifactStore) ForgetNode(nodeID string) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	keys := s.nodeKeys[nodeID]
	for indexKey := range keys {
		if nodes := s.entries[indexKey]; len(nodes) > 0 {
			delete(nodes, nodeID)
			if len(nodes) == 0 {
				s.removeArtifactKeyLocked(indexKey)
			}
		}
	}
	delete(s.nodeKeys, nodeID)
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

// evictLocked runs from LRU callbacks while callers hold s.mu's write lock.
func (s *InMemoryArtifactStore) evictLocked(indexKey artifactIndexKey, _ struct{}) {
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
