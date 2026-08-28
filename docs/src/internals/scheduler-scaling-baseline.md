# Scheduler scaling baseline

Measured with `BenchmarkSchedulePlacement` and `BenchmarkHeartbeatReconcile`
(`services/scheduler/internal/fleet_simulator_test.go`) against a synthetic
fleet, so behaviour at 1k–10k nodes can be observed without that many hosts.

Run them with:

```bash
go test ./scheduler/internal/ -run XXX -bench 'BenchmarkSchedulePlacement|BenchmarkHeartbeatReconcile' -benchtime=200x
```

## Baseline before bounded candidate selection

| Nodes | ns/op | B/op | allocs/op |
|------:|------:|-----:|----------:|
| 100 | 37,998 | 33,156 | 109 |
| 1,000 | 457,388 | 324,484 | 1,009 |
| 10,000 | 5,151,048 | 3,222,634 | 10,009 |

Placement cost is linear in fleet size. `Schedule` copies and sorts the whole
discovered node list and clones a `NodeSnapshot` per node on every request, so
at 10,000 nodes one placement costs ~5.2 ms and allocates ~3.2 MB — roughly 190
placements per second per core, before any of that pressure reaches a node.

This is the measurement that decides whether bounded candidate selection is
worth building: the algorithm is already datastore-free and lock-free, so the
remaining cost is entirely the per-request O(nodes) copy.

## Heartbeat reconcile

| Sandboxes on the reporting node | ns/op | B/op |
|--------------------------------:|------:|-----:|
| 10 | 3,184 | 858 |
| 100 | 11,632 | 4,046 |
| 1,000 | 119,860 | 57,690 |

Reconcile scales with the reporting node's roster rather than the fleet, and
runs once per node per heartbeat interval. At 1,000 sandboxes per node it costs
~120 µs per heartbeat, which is what a constant-size roster summary would
remove.
