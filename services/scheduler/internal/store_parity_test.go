package scheduler

import (
	"testing"
	"time"
)

// The in-memory and Redis stores are one HA knob apart and are meant to be
// interchangeable, so the contract is written once here and run against both.
// Each case ran green on Redis before it ran green in memory: where the two
// disagreed, Redis had the documented semantics and the in-memory store was
// brought to it. The Redis half skips without a redis-server, so a divergence
// can only be signed off with one present.
//
// Everything is wall-clock, because the Redis store stamps and compares inside
// Lua from the server's clock and cannot be handed a time.
type bindingStoreContractCase struct {
	name  string
	ttl   time.Duration
	grace time.Duration
	run   func(t *testing.T, store BindingStore)
}

var bindingStoreContractCases = []bindingStoreContractCase{
	{
		// Final means no live node is left to observe a binding it never saw,
		// so the grace protects nothing. The in-memory store applied it to a
		// non-empty final roster anyway and kept the departed binding.
		name:  "a final roster deletes departed bindings inside the grace",
		ttl:   30 * time.Second,
		grace: 10 * time.Second,
		run: func(t *testing.T, store BindingStore) {
			node := Node{ID: "node-a", Endpoint: "http://node-a"}
			mustRecord(t, store, "sbx-survivor", node)
			mustRecord(t, store, "sbx-departed", node)

			if err := store.ReconcileNodeRoster(node, []string{"sbx-survivor"}, RosterFinal, time.Now()); err != nil {
				t.Fatalf("ReconcileNodeRoster: %v", err)
			}
			assertBound(t, store, "sbx-survivor", node)
			assertUnbound(t, store, "sbx-departed")
		},
	},
	{
		// A binding whose TTL lapsed and is then named by a roster again is a
		// new establishment, not a refresh. In Redis the key is gone and the
		// write stamps afresh; in memory the lapsed record still sat in the
		// map and kept its original stamp, so the next omission deleted it.
		name:  "a lapsed binding re-established by a roster starts a new grace",
		ttl:   400 * time.Millisecond,
		grace: 300 * time.Millisecond,
		run: func(t *testing.T, store BindingStore) {
			node := Node{ID: "node-a", Endpoint: "http://node-a"}
			mustRecord(t, store, "sbx-1", node)
			time.Sleep(500 * time.Millisecond)

			if err := store.ReconcileNode(node, []string{"sbx-1"}, time.Now()); err != nil {
				t.Fatalf("re-establishing reconcile: %v", err)
			}
			if err := store.ReconcileNode(node, nil, time.Now()); err != nil {
				t.Fatalf("omitting reconcile: %v", err)
			}
			assertBound(t, store, "sbx-1", node)
		},
	},
	{
		// Completeness protects the empty roster and nothing else; see
		// RosterIncomplete. Both stores reap what a non-empty roster omits.
		name:  "a non-empty incomplete roster still reaps what it omits",
		ttl:   time.Minute,
		grace: 0,
		run: func(t *testing.T, store BindingStore) {
			node := Node{ID: "node-a", Endpoint: "http://node-a"}
			for _, sandboxID := range []string{"s1", "s2", "s3"} {
				mustRecord(t, store, sandboxID, node)
			}
			if err := store.ReconcileNodeRoster(node, nil, RosterIncomplete, time.Now()); err != nil {
				t.Fatalf("empty incomplete reconcile: %v", err)
			}
			for _, sandboxID := range []string{"s1", "s2", "s3"} {
				assertBound(t, store, sandboxID, node)
			}

			if err := store.ReconcileNodeRoster(node, []string{"s1"}, RosterIncomplete, time.Now()); err != nil {
				t.Fatalf("partial incomplete reconcile: %v", err)
			}
			assertBound(t, store, "s1", node)
			assertUnbound(t, store, "s2")
			assertUnbound(t, store, "s3")
		},
	},
	{
		name:  "a node without an endpoint is not recorded",
		ttl:   time.Minute,
		grace: 0,
		run: func(t *testing.T, store BindingStore) {
			mustRecord(t, store, "sbx-1", Node{ID: "node-c"})
			assertUnbound(t, store, "sbx-1")
		},
	},
	{
		name:  "a node without an id is not recorded",
		ttl:   time.Minute,
		grace: 0,
		run: func(t *testing.T, store BindingStore) {
			mustRecord(t, store, "sbx-1", Node{Endpoint: "http://node-c"})
			assertUnbound(t, store, "sbx-1")
		},
	},
	{
		name:  "a batch drops unroutable nodes and records the rest",
		ttl:   time.Minute,
		grace: 0,
		run: func(t *testing.T, store BindingStore) {
			node := Node{ID: "node-a", Endpoint: "http://node-a"}
			errs := store.RecordBatch([]BindingAssignment{
				{SandboxID: "sbx-ok", Node: node},
				{SandboxID: "sbx-no-endpoint", Node: Node{ID: "node-a"}},
				{SandboxID: "sbx-no-id", Node: Node{Endpoint: "http://node-a"}},
			}, time.Now())
			for i, err := range errs {
				if err != nil {
					t.Fatalf("errs[%d] = %v, want nil: an unroutable node is dropped, not reported", i, err)
				}
			}
			assertBound(t, store, "sbx-ok", node)
			assertUnbound(t, store, "sbx-no-endpoint")
			assertUnbound(t, store, "sbx-no-id")
		},
	},
	{
		name:  "a roster under an empty node id writes nothing",
		ttl:   time.Minute,
		grace: 0,
		run: func(t *testing.T, store BindingStore) {
			if err := store.ReconcileNode(Node{Endpoint: "http://missing-id"}, []string{"sbx-1"}, time.Now()); err != nil {
				t.Fatalf("ReconcileNode: %v", err)
			}
			assertUnbound(t, store, "sbx-1")
		},
	},
	{
		name:  "a roster under an endpoint-less node writes nothing",
		ttl:   time.Minute,
		grace: 0,
		run: func(t *testing.T, store BindingStore) {
			if err := store.ReconcileNode(Node{ID: "node-no-endpoint"}, []string{"sbx-1"}, time.Now()); err != nil {
				t.Fatalf("ReconcileNode: %v", err)
			}
			assertUnbound(t, store, "sbx-1")
		},
	},
	{
		// Unregister names the node by id alone with an empty roster, and
		// that has to keep clearing bindings on both backends.
		name:  "an empty roster under an endpoint-less node still clears the node",
		ttl:   time.Minute,
		grace: 0,
		run: func(t *testing.T, store BindingStore) {
			node := Node{ID: "node-a", Endpoint: "http://node-a"}
			mustRecord(t, store, "sbx-1", node)
			if err := store.ReconcileNodeRoster(Node{ID: "node-a"}, nil, RosterFinal, time.Now()); err != nil {
				t.Fatalf("ReconcileNodeRoster: %v", err)
			}
			assertUnbound(t, store, "sbx-1")
		},
	},
	{
		// Padding around an id used to split the in-memory per-node index, so
		// the padded node's binding survived the trimmed node's roster.
		name:  "node fields are trimmed before indexing",
		ttl:   time.Minute,
		grace: 0,
		run: func(t *testing.T, store BindingStore) {
			trimmed := Node{ID: "node-a", Endpoint: "http://node-a"}
			mustRecord(t, store, "sbx-1", Node{ID: " node-a ", Endpoint: " http://node-a "})
			assertBound(t, store, "sbx-1", trimmed)

			if err := store.ReconcileNode(trimmed, nil, time.Now()); err != nil {
				t.Fatalf("ReconcileNode: %v", err)
			}
			assertUnbound(t, store, "sbx-1")
		},
	},
	{
		name:  "sandbox ids are trimmed on lookup",
		ttl:   time.Minute,
		grace: 0,
		run: func(t *testing.T, store BindingStore) {
			node := Node{ID: "node-a", Endpoint: "http://node-a"}
			mustRecord(t, store, "sbx-1", node)
			assertBound(t, store, "  sbx-1  ", node)
		},
	},
}

func TestBindingStoreContractHoldsOnBothBackends(t *testing.T) {
	for _, tc := range bindingStoreContractCases {
		t.Run("memory/"+tc.name, func(t *testing.T) {
			tc.run(t, NewInMemoryBindingStoreWithGrace(tc.ttl, tc.grace))
		})
		t.Run("redis/"+tc.name, func(t *testing.T) {
			tc.run(t, newRedisBindingStoreForTestWithGrace(t, tc.ttl, tc.grace))
		})
	}
}

func mustRecord(t *testing.T, store BindingStore, sandboxID string, node Node) {
	t.Helper()
	if err := store.Record(sandboxID, node, time.Now()); err != nil {
		t.Fatalf("Record(%q): %v", sandboxID, err)
	}
}

func assertBound(t *testing.T, store BindingStore, sandboxID string, want Node) {
	t.Helper()
	got, ok, err := store.Get(sandboxID, time.Now())
	if err != nil || !ok || got != want {
		t.Fatalf("Get(%q) = (%+v, %v, %v), want %+v", sandboxID, got, ok, err, want)
	}
}

func assertUnbound(t *testing.T, store BindingStore, sandboxID string) {
	t.Helper()
	if got, ok, err := store.Get(sandboxID, time.Now()); err != nil || ok {
		t.Fatalf("Get(%q) = (%+v, %v, %v), want no binding", sandboxID, got, ok, err)
	}
}
