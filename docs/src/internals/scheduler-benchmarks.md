# Scheduler benchmarks

The scheduler's cost model has three terms that scale differently: placement is
O(candidates inspected), lookup is O(1) against a store that only grows, and
heartbeat reconcile is O(sandboxes on the reporting node) on every heartbeat
from every node. The benchmarks in `services/scheduler/internal/*_bench_test.go`
put a number on each against a synthetic fleet that drives the real `Service`
in-process — no gRPC, no containers, no hosts — so the shape of a 10k-node
fleet can be measured on one machine, and the in-memory and Redis binding
stores are measured on the same axis.

These are **laptop numbers, not fleet numbers.** They were taken on a developer
Mac that other work was running on at the time, and they are here as the
baseline a change is compared against (same machine, same command, before and
after), not as capacity figures. The design's regression bound — reconcile
ns/op must not move more than 10% as the fleet goes 1k → 10k nodes — is a
statement about slope, and slope is what a laptop can measure.

The older placement-only page, [Scheduler scaling
baseline](./scheduler-scaling-baseline.md), records the before/after of bounded
candidate sampling with a simpler fleet; the numbers here supersede it for
everything but that history.

## Running them

From `services/`:

```bash
make bench-services                                    # everything below, both stores
make bench-services BENCH_FLAGS='-fleet.nodes=1000'    # a smaller fleet
make bench-services BENCH='Reconcile/store=memory' BENCH_FLAGS='-fleet.churn=0.02 -fleet.staleness=5s'
```

or the `go test` line the target expands to:

```bash
go test ./scheduler/internal/ -run XXX \
  -bench 'BenchmarkLookupNode_1MBindings|BenchmarkHeartbeatReconcile_10kNodes_500Sandboxes|BenchmarkScheduleFleet' \
  -benchmem -benchtime=1s -timeout=60m
```

The Redis leaves start a private `redis-server` the way the Redis store tests
do: `REDIS_SERVER_BIN` names the binary, or one is found on `PATH`. Without one
they skip and say so; a skipped benchmark is acceptable where a skipped Redis
test is not, because nothing here asserts. The Redis leaves are named
`store=redis` and can be selected or excluded through `-bench`.

### Knobs

Every knob is a `go test` flag, so one binary sweeps the space and both stores
see the same shape.

| Flag | Default | Meaning |
| --- | --- | --- |
| `-fleet.nodes` | 10000 | Nodes in the synthetic fleet |
| `-fleet.sandboxes` | 500 | Sandboxes on every node |
| `-fleet.bindings` | 1000000 | Bindings the store holds in the lookup benchmark |
| `-fleet.churn` | 0 | Fraction of a node's roster replaced on every heartbeat: that many depart, as many arrive |
| `-fleet.staleness` | 0 | How long before the heartbeat the arrivals its roster omits were bound by the gateway |
| `-fleet.grace` | `scheduler.reconcile_grace` default (10s) | Reconcile grace the binding store is built with |

Churn is the work a heartbeat has to do beyond refreshing: at zero, every
heartbeat is a refresh of an unchanged roster and the digest-elided path
exists; at any positive value the roster changes every heartbeat and can never
be elided. Staleness is the roster race made adjustable — a node collects its
roster, the gateway places a sandbox on it, the heartbeat arrives: the sandbox
is bound but unlisted. Below the grace those bindings must survive the
heartbeat; at or past it the roster is believed and they are lost. The
reconcile benchmark reports the outcome directly as `lost_frac`, the share of
omitted arrivals `LookupNode` can no longer resolve.

Those two knobs are pinned by `TestReconcileFleetChurnAndStalenessKnobs`, which
runs the same fleet at a size that fits under `-race` and asserts, per
heartbeat, what reconcile deleted and retained and what fraction of the
omitted arrivals still routes. A harness that quietly listed the arrivals it
claimed to omit would report a flattering `lost_frac` of zero at every
staleness; the test fails it.

## What each benchmark measures

**`BenchmarkLookupNode_1MBindings`** — the scheduler's hottest read. Every
proxied request resolves its sandbox through `LookupNode`, so this is the path
that has to hold up as the store fills. One million UUID-shaped bindings are
seeded through `RecordAssignments` across 2,000 nodes, then read back serially
and from every core at once (`serial` and `parallel` leaves).
`TestLookupFleetSeedResolvesToTheSeededNode` pins the seed against the
placement it claims to make.

**`BenchmarkHeartbeatReconcile_10kNodes_500Sandboxes`** — the scheduler's
steady write load. Heartbeats rotate through the fleet rather than repeating
one node, so the per-node state is as cold as it is on a scheduler hearing
from every node in turn. `roster=full` sends the whole roster without a
digest, which is what a node whose roster changed, or a scheduler that
restarted, costs; `roster=elided` is the digest-only heartbeat a stable roster
settles into, and exists only at zero churn. Besides ns/op the leaves report
`deleted/op` and `retained/op` — what reconcile did to the bindings each
roster omitted — and `lost_frac` when staleness is set.

**`BenchmarkScheduleFleet`** — placement cost per shipped strategy, with the
reservation ledger fed: each placement is reported back as the create the
chosen node would emit, so a load-aware strategy sees its own decisions before
the node's next heartbeat confirms them. Runs at the design's 100-node point
and at 10,000 nodes, and reports `max/mean`, the busiest node's load over the
fleet mean, from an even start. The load-ratio bound itself is asserted by
`TestScheduleFleetLeastLoadedOfTwoBoundsTheLoadRatio`: from a skewed start
(node *i* holds *i* sandboxes) and 10,000 placements, `least_loaded_of_two`
must land under 1.2 and under round-robin's own ratio. A load-blind strategy
preserves the skew and lands near 1.4 every time; the sampled one fills from
the bottom and lands near 1.02.

The invariant all of this rests on — every sandbox the gateway has recorded
stays resolvable through `LookupNode` for at least one heartbeat period,
whatever the node's heartbeats say in the meantime — is
`TestEveryRecordedSandboxStaysResolvableForOneHeartbeatPeriod`, run against
both stores in two interleavings: one channel-ordered so the defect path is
taken on every round by construction, one racing so `-race` sees the shared
state contended.

## Recorded baseline
