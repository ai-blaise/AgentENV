package scheduler

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
)

// rosterCache remembers each node's last reconciled roster, keyed by the
// digest the node computed over it.
//
// Rosters barely change: a node's set of sandboxes is stable between creates
// and deletes, while heartbeats arrive every few seconds. Resending the whole
// set each time is the largest part of a heartbeat's wire cost — a node with
// two hundred sandboxes spends several kilobytes of UUIDs per heartbeat, and a
// fleet of ten thousand such nodes spends tens of megabytes a second saying
// nothing changed.
//
// The cache is what lets that be elided without losing anything. Bindings are
// still refreshed on every heartbeat, from the cached roster rather than from
// the wire — skipping the refresh instead would let the binding TTL expire
// every roster the node stopped resending, which is the opposite of the
// intent.
type rosterCache struct {
	mu      sync.RWMutex
	entries map[string]rosterEntry
}

type rosterEntry struct {
	digest      string
	sandboxIDs  []string
	rosterFinal bool
}

func newRosterCache() *rosterCache {
	return &rosterCache{entries: make(map[string]rosterEntry)}
}

// remember stores the roster a node just sent under its digest.
func (c *rosterCache) remember(nodeID, digest string, sandboxIDs []string, complete bool) {
	if nodeID == "" || digest == "" {
		return
	}
	stored := make([]string, len(sandboxIDs))
	copy(stored, sandboxIDs)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[nodeID] = rosterEntry{
		digest:      digest,
		sandboxIDs:  stored,
		rosterFinal: complete,
	}
}

// lookup returns the cached roster for a node when the digest matches.
//
// A mismatch returns false rather than the stale roster: reconciling against a
// roster the node has already moved on from would delete bindings it still
// owns, which is worse than waiting one heartbeat for the real thing.
func (c *rosterCache) lookup(nodeID, digest string) ([]string, bool, bool) {
	if nodeID == "" || digest == "" {
		return nil, false, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[nodeID]
	if !ok || entry.digest != digest {
		return nil, false, false
	}
	return entry.sandboxIDs, entry.rosterFinal, true
}

// forget drops a node's cached roster, so a node that comes back is asked for
// a fresh one rather than reconciled against what it had before it left.
func (c *rosterCache) forget(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, nodeID)
}

func (c *rosterCache) len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// RosterDigest hashes a roster the same way a node does.
//
// Order-independent by construction, because the two sides build the list from
// different structures and neither should have to promise an order. Used by
// the scheduler only to verify a node's digest in tests; in production the
// node's value is taken as given, since a wrong digest costs a re-send and
// nothing else.
func RosterDigest(sandboxIDs []string) string {
	normalized := make([]string, 0, len(sandboxIDs))
	for _, id := range sandboxIDs {
		id = strings.TrimSpace(id)
		if id != "" {
			normalized = append(normalized, id)
		}
	}
	sort.Strings(normalized)

	hash := sha256.New()
	for _, id := range normalized {
		hash.Write([]byte(id))
		hash.Write([]byte{'\n'})
	}
	return hex.EncodeToString(hash.Sum(nil))
}
