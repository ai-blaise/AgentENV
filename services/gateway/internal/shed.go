package gateway

import (
	"net/http"
	"strconv"
	"sync/atomic"
)

// Reasons a create was refused, sent as a header so a client can tell them
// apart. All are 503, but they call for different responses: back off, retry
// later, or slow down. The node speaks the same header and vocabulary, so one
// name covers a refusal wherever it originated.
//
// Retry-After is the other half of the signal, and its presence is deliberate
// per reason: it is sent when waiting can change the answer, and withheld when
// only an operator can. A client that retries on Retry-After alone and gives
// up otherwise behaves correctly without reading the reason at all.
const (
	headerRefusalReason = "x-agentenv-refusal-reason"

	// refusalFleetExhausted means the scheduler saw nodes and none would take
	// the sandbox. Capacity frees up as sandboxes end, so a later retry can
	// succeed; Retry-After says when.
	refusalFleetExhausted = "fleet_exhausted"
	// refusalNoNodes means the scheduler could offer no node at all: none
	// discovered, or the scheduler itself unreachable. Nothing a client waits
	// out fixes that, so it carries no Retry-After.
	refusalNoNodes = "no_nodes"
	// refusalGatewayShed means the gateway declined before trying, because it
	// already has more creates in flight than it will carry. The fleet may be
	// fine; the client should slow down.
	refusalGatewayShed = "gateway_shed"
	// refusalNodeAtCapacity is the node's, not the gateway's: its admission
	// gate refused this create. It is the one refusal the gateway answers by
	// placing elsewhere, so a client sees it only when the gateway was
	// configured not to retry.
	refusalNodeAtCapacity = "node_at_capacity"
	// refusalRetriesExhausted means every node the gateway offered the create
	// to refused it for capacity, up to gateway.max_schedule_retries.
	refusalRetriesExhausted = "retries_exhausted"
	// refusalBodyNotReplayable means a node refused the create for capacity
	// and the gateway could not offer it elsewhere, because the request body
	// was too large to hold for a second attempt.
	refusalBodyNotReplayable = "body_not_replayable"

	// fleetExhaustedRetryAfterSecs is how long a client is told to wait after
	// a capacity refusal the gateway itself issued. Two seconds is on the order
	// of a heartbeat interval, which is how quickly the scheduler's view of a
	// freed node changes.
	fleetExhaustedRetryAfterSecs = 2
	gatewayShedRetryAfterSecs    = 1
)

// defaultMaxInFlightCreates bounds concurrent create placements per gateway.
//
// Creates are the only requests that can fan out across several nodes, so an
// unbounded burst multiplies into scheduler load exactly when the fleet is
// least able to absorb it. Shedding early is cheaper for everyone than
// admitting work that will be refused a few hops later, and it keeps the
// gateway responsive for the data-plane traffic sharing it.
const defaultMaxInFlightCreates = 512

// createLimiter caps concurrent create placements.
type createLimiter struct {
	inFlight atomic.Int64
	limit    int64
}

func newCreateLimiter(limit int) *createLimiter {
	if limit <= 0 {
		limit = defaultMaxInFlightCreates
	}
	return &createLimiter{limit: int64(limit)}
}

// acquire reserves a slot, returning false when the gateway is already at its
// limit. The release function must be called when the create finishes.
func (l *createLimiter) acquire() (release func(), ok bool) {
	if l == nil {
		return func() {}, true
	}
	if l.inFlight.Add(1) > l.limit {
		l.inFlight.Add(-1)
		return nil, false
	}
	var released atomic.Bool
	return func() {
		if released.CompareAndSwap(false, true) {
			l.inFlight.Add(-1)
		}
	}, true
}

func (l *createLimiter) currentInFlight() int64 {
	if l == nil {
		return 0
	}
	return l.inFlight.Load()
}

// writeRefusal answers a refused create with a reason a client can act on. A
// non-positive retryAfterSecs sends no Retry-After, which is itself the signal
// that waiting will not help.
func writeRefusal(w http.ResponseWriter, reason string, retryAfterSecs int) {
	w.Header().Set(headerRefusalReason, reason)
	if retryAfterSecs > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfterSecs))
	}
	http.Error(w, reason, http.StatusServiceUnavailable)
}
