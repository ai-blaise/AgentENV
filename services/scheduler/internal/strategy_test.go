package scheduler

import (
	"fmt"
	"testing"

	schedulerv1 "agentenv/services/api/proto"
)

func TestRoundRobin(t *testing.T) {
	s := &RoundRobinStrategy{}
	nodes := []RichNode{{Node: Node{ID: "a"}}, {Node: Node{ID: "b"}}, {Node: Node{ID: "c"}}}

	got1, _ := s.Select(nodes, nil)
	got2, _ := s.Select(nodes, nil)
	got3, _ := s.Select(nodes, nil)
	got4, _ := s.Select(nodes, nil)

	if got1.ID != "a" || got2.ID != "b" || got3.ID != "c" || got4.ID != "a" {
		t.Fatalf("unexpected order: %s %s %s %s", got1.ID, got2.ID, got3.ID, got4.ID)
	}
}

func TestRandomNoNodes(t *testing.T) {
	s := NewRandomStrategy()
	_, err := s.Select(nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func snapshotNode(id string, running, starting, paused uint32) RichNode {
	return RichNode{
		Node: Node{ID: id, Endpoint: "http://" + id},
		Snapshot: &schedulerv1.NodeSnapshot{
			SandboxCount:         running,
			SandboxStartingCount: starting,
			PausedSandboxCount:   paused,
		},
		Health: ObservedHealth{Seen: true, Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
	}
}

// With only two candidates the sampling is exhaustive, so the less loaded node
// must win every time.
func TestLeastLoadedOfTwoPrefersTheLessLoadedNode(t *testing.T) {
	strategy := NewLeastLoadedOfTwoStrategy()
	nodes := []RichNode{
		snapshotNode("busy", 50, 0, 0),
		snapshotNode("idle", 1, 0, 0),
	}

	for i := 0; i < 50; i++ {
		got, err := strategy.Select(nodes, nil)
		if err != nil {
			t.Fatalf("Select: %v", err)
		}
		if got.ID != "idle" {
			t.Fatalf("selected %q, want the less loaded node", got.ID)
		}
	}
}

// A node that has never reported is unknown, not empty. Scoring it as idle
// would send every burst at whichever node most recently joined.
func TestLeastLoadedOfTwoAvoidsNodesWithNoSnapshot(t *testing.T) {
	strategy := NewLeastLoadedOfTwoStrategy()
	nodes := []RichNode{
		{Node: Node{ID: "unreported", Endpoint: "http://unreported"}},
		snapshotNode("known", 100, 0, 0),
	}

	for i := 0; i < 50; i++ {
		got, err := strategy.Select(nodes, nil)
		if err != nil {
			t.Fatalf("Select: %v", err)
		}
		if got.ID != "known" {
			t.Fatalf("selected %q, want the node that has actually reported", got.ID)
		}
	}
}

// Starting sandboxes are the expensive part of a create — slot setup, image
// work, device acquisition — so a node with many in flight is closer to
// saturation than the same count of already-running sandboxes.
func TestLeastLoadedOfTwoWeightsStartingSandboxes(t *testing.T) {
	starting := nodeLoadScore(snapshotNode("starting", 0, 10, 0))
	running := nodeLoadScore(snapshotNode("running", 10, 0, 0))
	if starting <= running {
		t.Fatalf("10 starting (%v) must score as more loaded than 10 running (%v)", starting, running)
	}

	if nodeLoadScore(snapshotNode("a", 0, 10, 0)) <= nodeLoadScore(snapshotNode("b", 0, 5, 0)) {
		t.Fatal("more starting sandboxes must score as more loaded")
	}

	// Paused sandboxes have released their VM-side CPU and memory, so they
	// weigh less than a running one rather than the same.
	paused := nodeLoadScore(snapshotNode("paused", 0, 0, 10))
	if paused >= nodeLoadScore(snapshotNode("running", 10, 0, 0)) {
		t.Fatalf("10 paused (%v) must score below 10 running", paused)
	}
	if paused <= nodeLoadScore(snapshotNode("empty", 0, 0, 0)) {
		t.Fatal("paused sandboxes must still count for something")
	}
}

// Round-robin distributes by position, so it keeps feeding a saturated node.
// This is the regression the strategy exists to fix, stated as a comparison.
func TestLeastLoadedOfTwoBoundsMaxLoadBetterThanRoundRobin(t *testing.T) {
	const nodeCount = 8
	const creates = 400

	build := func() []RichNode {
		nodes := make([]RichNode, 0, nodeCount)
		for i := 0; i < nodeCount; i++ {
			nodes = append(nodes, snapshotNode(fmt.Sprintf("node-%d", i), 0, 0, 0))
		}
		// One node starts hot, as it would after an uneven earlier burst.
		nodes[0].Snapshot.SandboxCount = 100
		return nodes
	}

	run := func(strategy Strategy) uint32 {
		nodes := build()
		for i := 0; i < creates; i++ {
			got, err := strategy.Select(nodes, nil)
			if err != nil {
				t.Fatalf("Select: %v", err)
			}
			for j := range nodes {
				if nodes[j].ID == got.ID {
					nodes[j].Snapshot.SandboxCount++
					break
				}
			}
		}
		var max uint32
		for _, n := range nodes {
			if c := n.Snapshot.GetSandboxCount(); c > max {
				max = c
			}
		}
		return max
	}

	roundRobinMax := run(&RoundRobinStrategy{})
	leastLoadedMax := run(NewLeastLoadedOfTwoStrategy())

	if leastLoadedMax >= roundRobinMax {
		t.Fatalf("max load: least_loaded_of_two=%d, round_robin=%d; want the sampled strategy to bound it lower",
			leastLoadedMax, roundRobinMax)
	}
}
