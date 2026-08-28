package scheduler

import (
	"errors"
	"math"
	"math/rand"
	"sync/atomic"

	schedulerv1 "agentenv/services/api/proto"
)

var ErrNoNodes = errors.New("no nodes available")

type Strategy interface {
	Select(nodes []RichNode, hint *schedulerv1.ScheduleRequestHint) (RichNode, error)
	Name() string
}

type RoundRobinStrategy struct {
	next uint64
}

func (s *RoundRobinStrategy) Select(nodes []RichNode, _ *schedulerv1.ScheduleRequestHint) (RichNode, error) {
	if len(nodes) == 0 {
		return RichNode{}, ErrNoNodes
	}
	idx := atomic.AddUint64(&s.next, 1)
	return nodes[(idx-1)%uint64(len(nodes))], nil
}

func (s *RoundRobinStrategy) Name() string {
	return "round_robin"
}

type RandomStrategy struct{}

func NewRandomStrategy() *RandomStrategy {
	return &RandomStrategy{}
}

func (s *RandomStrategy) Select(nodes []RichNode, _ *schedulerv1.ScheduleRequestHint) (RichNode, error) {
	if len(nodes) == 0 {
		return RichNode{}, ErrNoNodes
	}
	return nodes[rand.Intn(len(nodes))], nil
}

func (s *RandomStrategy) Name() string {
	return "random"
}

func NewStrategy(name string) Strategy {
	switch name {
	case "least_loaded_of_two", "p2c":
		return NewLeastLoadedOfTwoStrategy()
	case "random":
		return NewRandomStrategy()
	case "round_robin":
		fallthrough
	default:
		return &RoundRobinStrategy{}
	}
}

// LeastLoadedOfTwoStrategy samples two candidates and picks the less loaded.
//
// Round-robin and random both ignore the node snapshot entirely, which is the
// worst available option for this regime: placement runs against a view that
// can be a full heartbeat interval stale, and nothing decrements between
// decisions, so a burst distributes by position rather than by capacity.
//
// Sampling two and taking the better one bounds the maximum load far more
// tightly than round-robin under exactly that staleness, without the herding
// that picking the single least-loaded node causes — with a stale view, "least
// loaded" is the same answer for every request in a burst, so they all pile
// onto one node. Two samples also keeps selection O(1) in fleet size.
type LeastLoadedOfTwoStrategy struct{}

func NewLeastLoadedOfTwoStrategy() *LeastLoadedOfTwoStrategy {
	return &LeastLoadedOfTwoStrategy{}
}

func (s *LeastLoadedOfTwoStrategy) Select(nodes []RichNode, _ *schedulerv1.ScheduleRequestHint) (RichNode, error) {
	switch len(nodes) {
	case 0:
		return RichNode{}, ErrNoNodes
	case 1:
		return nodes[0], nil
	}

	first := rand.Intn(len(nodes))
	second := rand.Intn(len(nodes) - 1)
	if second >= first {
		second++
	}
	if nodeLoadScore(nodes[second]) < nodeLoadScore(nodes[first]) {
		return nodes[second], nil
	}
	return nodes[first], nil
}

func (s *LeastLoadedOfTwoStrategy) Name() string {
	return "least_loaded_of_two"
}

// nodeLoadScore ranks a node's current occupancy. Lower is more available.
//
// A node that has never reported is scored as maximally loaded rather than
// empty: an absent snapshot means "unknown", and treating unknown as idle
// would send every burst at whichever node just joined.
//
// Starting sandboxes count double because they are the expensive part of a
// create — slot setup, image work, device acquisition — and a node with many
// in flight is closer to saturation than its running count suggests. Paused
// sandboxes count, at a discount: they hold persisted state and can be
// resumed, but have released their VM-side CPU and memory.
func nodeLoadScore(n RichNode) float64 {
	s := n.Snapshot
	if s == nil {
		return math.MaxFloat64
	}

	score := float64(s.GetSandboxCount())
	score += 2 * float64(s.GetSandboxStartingCount())
	score += 0.25 * float64(s.GetPausedSandboxCount())

	// Fold in proportional pressure so heterogeneous nodes compare sensibly:
	// a count alone would treat a large node and a small one as equal.
	if total := s.GetMemoryTotalBytes(); total > 0 {
		score += 100 * float64(s.GetMemoryUsedBytes()) / float64(total)
	}
	if cpus := s.GetCpuCount(); cpus > 0 {
		score += 100 * float64(s.GetAllocatedCpu()) / float64(cpus)
	}
	return score
}
