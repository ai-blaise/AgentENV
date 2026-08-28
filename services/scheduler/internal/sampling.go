package scheduler

import "math/rand"

// defaultCandidateSampleSize bounds how many nodes one placement inspects.
//
// Placement previously copied and sorted the whole discovered node list and
// cloned a NodeSnapshot per node on every request, which is linear in fleet
// size: ~38 us at 100 nodes but ~5.2 ms and 3.2 MB at 10,000, or roughly 190
// placements per second per core.
//
// The algorithm does not need the whole fleet. It runs against a view that is
// already up to a heartbeat interval stale and is corrected downstream by the
// node's own admission decision, so a random sample is as good a basis for the
// decision as the full list — and sampling is exactly what the
// least-loaded-of-two strategy assumes.
//
// The sample is sized well above that strategy's two picks so the health and
// resource filters still have candidates left to reject, rather than making a
// single unhealthy draw fail the placement.
const defaultCandidateSampleSize = 32

// sampleNodes returns at most `size` nodes chosen uniformly at random.
//
// Uniform rather than positional: taking a prefix would make placement follow
// the registry's sort order, so every scheduler replica would inspect the same
// nodes and the fleet's tail would never be considered.
//
// Returns the input untouched when it already fits, so small fleets keep exact
// behaviour and pay nothing.
func sampleNodes(nodes []Node, size int) []Node {
	if size <= 0 || len(nodes) <= size {
		return nodes
	}

	sampled := make([]Node, size)
	// Reservoir sampling: one pass, no allocation beyond the result, and no
	// mutation of the caller's slice, which is shared registry state.
	copy(sampled, nodes[:size])
	for i := size; i < len(nodes); i++ {
		j := rand.Intn(i + 1)
		if j < size {
			sampled[j] = nodes[i]
		}
	}
	return sampled
}
