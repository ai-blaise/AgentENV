package scheduler

import (
	"fmt"
	"testing"
)

func fleetOf(n int) []Node {
	nodes := make([]Node, 0, n)
	for i := 0; i < n; i++ {
		nodes = append(nodes, Node{ID: fmt.Sprintf("node-%04d", i)})
	}
	return nodes
}

// Small fleets must keep exact behaviour and pay nothing for the sampler.
func TestSampleNodesReturnsSmallFleetsUntouched(t *testing.T) {
	nodes := fleetOf(8)
	got := sampleNodes(nodes, 32)
	if len(got) != len(nodes) {
		t.Fatalf("sampled %d nodes, want all %d", len(got), len(nodes))
	}
	if &got[0] != &nodes[0] {
		t.Fatal("a fleet that already fits should be returned as-is, not copied")
	}
}

func TestSampleNodesBoundsTheSample(t *testing.T) {
	got := sampleNodes(fleetOf(10_000), 32)
	if len(got) != 32 {
		t.Fatalf("sampled %d nodes, want 32", len(got))
	}
}

// Sampling must not mutate the registry's slice, which is shared state.
func TestSampleNodesDoesNotMutateItsInput(t *testing.T) {
	nodes := fleetOf(1_000)
	before := make([]Node, len(nodes))
	copy(before, nodes)

	sampleNodes(nodes, 16)

	for i := range nodes {
		if nodes[i].ID != before[i].ID {
			t.Fatalf("input mutated at %d: %q != %q", i, nodes[i].ID, before[i].ID)
		}
	}
}

// A prefix would make placement follow the registry's sort order, so every
// replica would inspect the same nodes and the fleet's tail would never be
// considered. Over many draws the sampler must reach deep into the list.
func TestSampleNodesReachesTheWholeFleet(t *testing.T) {
	const fleetSize = 1_000
	nodes := fleetOf(fleetSize)
	seen := make(map[string]struct{})

	for i := 0; i < 500; i++ {
		for _, n := range sampleNodes(nodes, 32) {
			seen[n.ID] = struct{}{}
		}
	}

	if len(seen) < fleetSize/2 {
		t.Fatalf("sampling reached only %d of %d nodes; it is not uniform", len(seen), fleetSize)
	}
	// Specifically check the tail, which a prefix-based sampler never reaches.
	if _, ok := seen[fmt.Sprintf("node-%04d", fleetSize-1)]; !ok {
		t.Fatal("the last node in the fleet was never sampled")
	}
}

func TestSampleNodesDisabledEvaluatesWholeFleet(t *testing.T) {
	nodes := fleetOf(500)
	if got := sampleNodes(nodes, 0); len(got) != 500 {
		t.Fatalf("sampled %d nodes, want the whole fleet when disabled", len(got))
	}
}
