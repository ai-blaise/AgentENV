# Metadata store contention baseline

The orchestrator's sandbox metadata lives in one `RwLock<HashMap>` shared by
every path on the node. Creates take it exclusively; the heartbeat roster, the
metrics snapshot and the eviction sweep each walk the whole map under a read
lock. That is obviously fine at one sandbox. Whether it is still fine at the
counts a dense node is meant to reach was not something anyone had measured,
and "one lock for everything" is the shape that degrades quietly as a node
fills up.

So it was measured before anything was changed.

## Method

`src/orchestrator/store/contention.rs`, ignored by default because these are
measurements rather than assertions — their numbers depend on the host, and a
threshold tuned to one machine fails on another for reasons unrelated to the
code.

```bash
cargo test -p agentenv --lib store::contention -- --ignored --nocapture
```

Host: 224-core x86_64 Linux, `dev` profile.

## Results

Full scan — what every heartbeat does to build its roster:

| sandboxes | per scan | per sandbox |
| --- | --- | --- |
| 16 | 0.93 µs | 58.2 ns |
| 128 | 2.63 µs | 20.5 ns |
| 512 | 8.86 µs | 17.3 ns |
| 2048 | 33.8 µs | 16.5 ns |

Linear, at about 16.5 ns per sandbox once the map is large enough to amortise
the lock acquisition itself.

Create throughput against continuous whole-map scanners — a deliberately
pathological read load, since real scans happen per heartbeat rather than in a
loop:

| concurrent scanners | creates/s | per create |
| --- | --- | --- |
| 0 | 194,518 | 5.1 µs |
| 1 | 25,439 | 39.3 µs |
| 4 | 19,526 | 51.2 µs |
| 16 | 12,147 | 82.3 µs |

Worst observed create wait with 2048 sandboxes and eight continuous scanners:
**216 µs**, mean 97 µs.

## Conclusion: leave it alone

The store is not the contention point and does not need sharding, a concurrent
map, or a read-optimised copy.

The numbers say so with room to spare. The worst case measured here — eight
threads scanning a 2048-sandbox map without pause — still sustains twelve
thousand creates a second and bounds a single create's wait at a fifth of a
millisecond. A node creating sandboxes at even a hundred a second, against
scans that arrive every few seconds rather than continuously, is three orders
of magnitude away from this.

Tokio's `RwLock` being write-preferring is what makes the worst case a bound
rather than a hope: a create waiting behind an in-progress scan is not
overtaken by scans that arrive after it, so create latency degrades smoothly
with read load instead of starving under it.

What would change this conclusion: a read path that holds the lock across an
await (none does today — every scan collects and releases), or a sandbox count
an order of magnitude beyond 2048 per node combined with a scan on a hot path
rather than per heartbeat. Re-run the measurement if either becomes true.
