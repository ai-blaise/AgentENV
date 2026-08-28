package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// generation returns a UUIDv7-shaped value whose string order is its time
// order, which is the property the store's compare-and-set relies on.
func generation(seq int) string {
	return fmt.Sprintf("01a04861-ed74-7%03x-a5d9-0b71d25cb896", seq)
}

func mobilityRecord(sandboxID, origin string, seq int) MobilityRecord {
	return MobilityRecord{
		SandboxID:     sandboxID,
		OriginNodeID:  origin,
		Generation:    generation(seq),
		Fingerprint:   json.RawMessage(`{"kernel_version":"vmlinux-6.1.175"}`),
		ArtifactReach: "cluster_shared",
		CPUCount:      2,
		MemoryMiB:     2048,
		PausedAtMs:    1700000000000,
		State:         MobilityParked,
	}
}

// mobilityStores returns every implementation, so the semantics are asserted
// against all of them rather than against whichever one is convenient. A
// divergence between the in-memory and Redis stores is exactly the kind of bug
// that only shows up in the deployment that uses the other one.
func mobilityStores(t *testing.T) map[string]MobilityStore {
	t.Helper()
	stores := map[string]MobilityStore{"memory": NewInMemoryMobilityStore()}

	addr := startRedisServerForTest(t)
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	stores["redis"] = NewRedisMobilityStore(client)
	return stores
}

// The claim protocol depends on exactly one writer winning. A later generation
// must land and an earlier one must not, or a superseded actor can resurrect
// stale state — re-parking a sandbox another node is already restoring.
func TestMobilityUpsertIsACompareAndSet(t *testing.T) {
	for name, store := range mobilityStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			first := mobilityRecord("sandbox-1", "node-a", 1)

			applied, err := store.Upsert(ctx, first)
			if err != nil || !applied {
				t.Fatalf("first write should apply, got applied=%v err=%v", applied, err)
			}

			newer := first
			newer.Generation = generation(2)
			newer.State = MobilityClaimed
			newer.HolderNodeID = "node-b"
			if applied, err := store.Upsert(ctx, newer); err != nil || !applied {
				t.Fatalf("a newer generation must apply, got applied=%v err=%v", applied, err)
			}

			// The late write from a superseded actor.
			if applied, err := store.Upsert(ctx, first); err != nil || applied {
				t.Fatalf("an older generation must not apply, got applied=%v err=%v", applied, err)
			}
			// And a rewrite under the same generation, which would make the
			// ordering decide nothing.
			if applied, err := store.Upsert(ctx, newer); err != nil || applied {
				t.Fatalf("an equal generation must not apply, got applied=%v err=%v", applied, err)
			}

			got, found, err := store.Get(ctx, "sandbox-1")
			if err != nil || !found {
				t.Fatalf("record should exist, got found=%v err=%v", found, err)
			}
			if got.State != MobilityClaimed || got.HolderNodeID != "node-b" {
				t.Fatalf("the newer state must survive, got %+v", got)
			}
		})
	}
}

// The reason the store moved to the scheduler at all: the node-local version
// was a read followed by a write and could not be atomic even between two
// threads. Under concurrency exactly one claimant must win.
func TestMobilityConcurrentClaimsProduceOneWinner(t *testing.T) {
	for name, store := range mobilityStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if _, err := store.Upsert(ctx, mobilityRecord("sandbox-1", "node-a", 1)); err != nil {
				t.Fatalf("seed: %v", err)
			}

			const claimants = 16
			var wg sync.WaitGroup
			applied := make([]bool, claimants)
			for i := 0; i < claimants; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					claim := mobilityRecord("sandbox-1", "node-a", 2)
					claim.State = MobilityClaimed
					claim.HolderNodeID = fmt.Sprintf("node-%d", i)
					ok, err := store.Upsert(ctx, claim)
					if err != nil {
						t.Errorf("claim %d: %v", i, err)
					}
					applied[i] = ok
				}(i)
			}
			wg.Wait()

			winners := 0
			for _, ok := range applied {
				if ok {
					winners++
				}
			}
			// Every claimant used the same generation, so exactly one write
			// may land: the rest are equal-generation rewrites.
			if winners != 1 {
				t.Fatalf("expected exactly one winner among equal generations, got %d", winners)
			}
		})
	}
}

// A drain plans over the records a node holds, so the listing has to be
// complete, scoped to that node, and stable enough that a plan can be reviewed
// before it is run.
func TestMobilityListByOriginIsScopedAndStable(t *testing.T) {
	for name, store := range mobilityStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			for i := 0; i < 5; i++ {
				if _, err := store.Upsert(ctx, mobilityRecord(fmt.Sprintf("a-%d", i), "node-a", 1)); err != nil {
					t.Fatalf("seed a-%d: %v", i, err)
				}
			}
			for i := 0; i < 3; i++ {
				if _, err := store.Upsert(ctx, mobilityRecord(fmt.Sprintf("b-%d", i), "node-b", 1)); err != nil {
					t.Fatalf("seed b-%d: %v", i, err)
				}
			}

			listed, err := store.ListByOrigin(ctx, "node-a")
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			if len(listed) != 5 {
				t.Fatalf("expected node-a's five records, got %d", len(listed))
			}
			for _, record := range listed {
				if record.OriginNodeID != "node-a" || !strings.HasPrefix(record.SandboxID, "a-") {
					t.Fatalf("listing leaked another node's record: %+v", record)
				}
			}

			again, err := store.ListByOrigin(ctx, "node-a")
			if err != nil {
				t.Fatalf("list twice: %v", err)
			}
			for i := range listed {
				if listed[i].SandboxID != again[i].SandboxID {
					t.Fatal("listing order must be stable so a plan is reproducible")
				}
			}
		})
	}
}

// A removed sandbox must leave the listing, or a drain keeps trying to place
// something that no longer exists.
func TestMobilityRemoveLeavesTheListing(t *testing.T) {
	for name, store := range mobilityStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if _, err := store.Upsert(ctx, mobilityRecord("sandbox-1", "node-a", 1)); err != nil {
				t.Fatalf("seed: %v", err)
			}
			if err := store.Remove(ctx, "sandbox-1"); err != nil {
				t.Fatalf("remove: %v", err)
			}

			if _, found, _ := store.Get(ctx, "sandbox-1"); found {
				t.Fatal("a removed record must not be readable")
			}
			listed, err := store.ListByOrigin(ctx, "node-a")
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			if len(listed) != 0 {
				t.Fatalf("a removed record must leave the listing, got %+v", listed)
			}
			// Removing what is not there is how cleanup paths behave.
			if err := store.Remove(ctx, "sandbox-1"); err != nil {
				t.Fatalf("remove must be idempotent: %v", err)
			}
		})
	}
}

// A record whose origin moved must not still be listed under the old node: the
// index is a hint and the record is the truth about who holds it.
func TestMobilityIndexFollowsTheRecordsOrigin(t *testing.T) {
	for name, store := range mobilityStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if _, err := store.Upsert(ctx, mobilityRecord("sandbox-1", "node-a", 1)); err != nil {
				t.Fatalf("seed: %v", err)
			}

			moved := mobilityRecord("sandbox-1", "node-b", 2)
			if applied, err := store.Upsert(ctx, moved); err != nil || !applied {
				t.Fatalf("move: applied=%v err=%v", applied, err)
			}

			fromA, err := store.ListByOrigin(ctx, "node-a")
			if err != nil {
				t.Fatalf("list a: %v", err)
			}
			if len(fromA) != 0 {
				t.Fatalf("node-a must not still list a record it no longer owns, got %+v", fromA)
			}
			fromB, err := store.ListByOrigin(ctx, "node-b")
			if err != nil {
				t.Fatalf("list b: %v", err)
			}
			if len(fromB) != 1 {
				t.Fatalf("node-b should list it, got %+v", fromB)
			}
		})
	}
}

// The scheduler never interprets the fingerprint, so it must survive a round
// trip untouched — a field added by a newer node must not be dropped by an
// older scheduler.
func TestMobilityFingerprintIsOpaqueAndPreserved(t *testing.T) {
	for name, store := range mobilityStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			record := mobilityRecord("sandbox-1", "node-a", 1)
			record.Fingerprint = json.RawMessage(
				`{"kernel_version":"vmlinux-6.1.175","a_field_this_scheduler_has_never_heard_of":42}`,
			)
			if _, err := store.Upsert(ctx, record); err != nil {
				t.Fatalf("upsert: %v", err)
			}

			got, found, err := store.Get(ctx, "sandbox-1")
			if err != nil || !found {
				t.Fatalf("get: found=%v err=%v", found, err)
			}
			var decoded map[string]any
			if err := json.Unmarshal(got.Fingerprint, &decoded); err != nil {
				t.Fatalf("fingerprint should still be valid json: %v", err)
			}
			if decoded["a_field_this_scheduler_has_never_heard_of"] != float64(42) {
				t.Fatalf("an unknown fingerprint field must survive, got %v", decoded)
			}
		})
	}
}

// Malformed input must be refused rather than stored, or a record nobody can
// evaluate ends up arbitrating ownership.
func TestMobilityRejectsIncompleteRecords(t *testing.T) {
	for name, store := range mobilityStores(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			for _, tc := range []struct {
				name   string
				mutate func(*MobilityRecord)
			}{
				{"no sandbox id", func(r *MobilityRecord) { r.SandboxID = " " }},
				{"no origin", func(r *MobilityRecord) { r.OriginNodeID = "" }},
				{"no generation", func(r *MobilityRecord) { r.Generation = "" }},
				{"unknown state", func(r *MobilityRecord) { r.State = "somewhere" }},
			} {
				record := mobilityRecord("sandbox-1", "node-a", 1)
				tc.mutate(&record)
				if _, err := store.Upsert(ctx, record); err == nil {
					t.Fatalf("%s: must be refused", tc.name)
				}
			}
		})
	}
}

// Redis Cluster is the case the key layout exists for and cannot be simulated:
// a single instance accepts cross-slot access happily.
func TestMobilityStoreOnRedisCluster(t *testing.T) {
	addrs := startRedisClusterForTest(t, 3)
	client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	t.Cleanup(func() { _ = client.Close() })
	if err := redisMobilityUpsertScript.Load(context.Background(), client).Err(); err != nil {
		// Loaded lazily by Upsert's NOSCRIPT path too; this just front-loads it.
		t.Logf("preload script: %v", err)
	}

	store := NewRedisMobilityStore(client)
	ctx := context.Background()

	// Enough sandboxes that they cannot all land in one slot.
	const count = 24
	for i := 0; i < count; i++ {
		if _, err := store.Upsert(ctx, mobilityRecord(fmt.Sprintf("sandbox-%02d", i), "node-a", 1)); err != nil {
			t.Fatalf("upsert %d across the cluster: %v", i, err)
		}
	}
	listed, err := store.ListByOrigin(ctx, "node-a")
	if err != nil {
		t.Fatalf("list across the cluster: %v", err)
	}
	if len(listed) != count {
		t.Fatalf("expected %d records, got %d", count, len(listed))
	}

	// The compare-and-set has to work per key, in whatever slot the key hashed
	// to, which is what a cross-slot script would have made impossible.
	claim := mobilityRecord("sandbox-07", "node-a", 2)
	claim.State = MobilityClaimed
	claim.HolderNodeID = "node-b"
	if applied, err := store.Upsert(ctx, claim); err != nil || !applied {
		t.Fatalf("claim on a cluster: applied=%v err=%v", applied, err)
	}
	stale := mobilityRecord("sandbox-07", "node-a", 1)
	if applied, err := store.Upsert(ctx, stale); err != nil || applied {
		t.Fatalf("a stale write must lose on a cluster too: applied=%v err=%v", applied, err)
	}

	if err := store.Remove(ctx, "sandbox-07"); err != nil {
		t.Fatalf("remove on a cluster: %v", err)
	}
	listed, err = store.ListByOrigin(ctx, "node-a")
	if err != nil {
		t.Fatalf("list after remove: %v", err)
	}
	if len(listed) != count-1 {
		t.Fatalf("expected %d records after remove, got %d", count-1, len(listed))
	}
}

// Scripts are cached per server, so a cluster loses them per shard. Inside a
// pipeline EVALSHA cannot fall back to EVAL; this path uses Run directly, so
// verify the NOSCRIPT recovery actually works.
func TestMobilityStoreSurvivesScriptFlush(t *testing.T) {
	addr := startRedisServerForTest(t)
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = client.Close() })
	store := NewRedisMobilityStore(client)
	ctx := context.Background()

	if _, err := store.Upsert(ctx, mobilityRecord("sandbox-1", "node-a", 1)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if _, err := store.Upsert(ctx, mobilityRecord("sandbox-2", "node-a", 1)); err != nil {
		t.Fatalf("a write after a script flush must recover: %v", err)
	}
	if _, found, _ := store.Get(ctx, "sandbox-2"); !found {
		t.Fatal("the record written after a flush should be readable")
	}
}

var _ = time.Second
