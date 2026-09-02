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
	// NeedsStableOrder reports whether Select's answer depends on the order
	// candidates arrive in. Round-robin does — its cycle is only a cycle over
	// a list that stays put — and pays for a sort on every placement to get
	// it. Strategies that draw or score have no use for the order, and at
	// fleet scale the sort was most of what a placement cost.
	NeedsStableOrder() bool
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

func (s *RoundRobinStrategy) NeedsStableOrder() bool { return true }

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

func (s *RandomStrategy) NeedsStableOrder() bool { return false }

func NewStrategy(name string) Strategy {
	switch name {
	case "least_loaded_of_two", "p2c":
		return NewLeastLoadedOfTwoStrategy()
	case "bin_pack":
		return NewBinPackStrategy()
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

func (s *LeastLoadedOfTwoStrategy) NeedsStableOrder() bool { return false }

// BinPackStrategy fills the most loaded candidate that is still eligible.
//
// Every node handed to Select has already passed the resource limits, so
// "most loaded" is "closest to its ceiling without being over it". That is
// what a drain or a consolidation wants: the fleet's occupied set stays as
// small as possible and the rest can be emptied and removed.
//
// It is the wrong strategy for tail latency, and is off by default for that
// reason. A burst of creates all score the same node highest and all land on
// it, where each start contends with the others for the node's network-slot
// and iptables locks — the per-node serialisation that makes the slowest
// create in a burst slow. It is also only bounded by the configured resource
// limits: with none, it fills one node indefinitely.
type BinPackStrategy struct{}

func NewBinPackStrategy() *BinPackStrategy {
	return &BinPackStrategy{}
}

func (s *BinPackStrategy) Select(nodes []RichNode, _ *schedulerv1.ScheduleRequestHint) (RichNode, error) {
	if len(nodes) == 0 {
		return RichNode{}, ErrNoNodes
	}

	// A node with no snapshot scores as unknown, which nodeLoadScore renders as
	// maximally loaded — the right answer for "avoid it" but the wrong one for
	// "pack into it". It is only chosen when nothing has reported.
	best, found := RichNode{}, false
	bestScore := 0.0
	for _, n := range nodes {
		if n.Snapshot == nil {
			continue
		}
		score := nodeLoadScore(n)
		// Ties go to the lower ID so two equal nodes do not split a burst by
		// whatever order they were handed in.
		if !found || score > bestScore || (score == bestScore && n.ID < best.ID) {
			best, bestScore, found = n, score, true
		}
	}
	if !found {
		return nodes[0], nil
	}
	return best, nil
}

func (s *BinPackStrategy) Name() string {
	return "bin_pack"
}

func (s *BinPackStrategy) NeedsStableOrder() bool { return false }

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
