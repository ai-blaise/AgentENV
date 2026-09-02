# Measurement Harness

Three instruments that need no hypervisor: a load generator against the HTTP
API, two orchestrator-path snapshot benchmarks, and three fault-injection
scripts. Everything here runs on a host with no `/dev/kvm` and no `ublk_drv`,
against nodes started with `[machine].backend = "mock"`.

**Every number on this page is a single-host, mock-backend number.** One
machine, four processes on `127.0.0.1`, and no guest anywhere. They are not
fleet numbers, they are not sandbox-start numbers, and they are not comparable
to anything measured with real microVMs. What they measure is the control
plane's own cost — the orchestrator state machine, the metadata store, the
scheduler protocol, the gateway path — with the hypervisor removed from the
picture rather than made fast.

The numbers below were recorded on the AgentENV build host on 2026-09-02:
Rocky Linux 9.7, kernel `5.14.0-687.39.1+2.1.el9_8_ciq.x86_64`, 224 vCPU,
~2.9 TiB RAM, no `/dev/kvm`, no `ublk_drv`. Every command is given exactly as
it was run.

## The load generator

`crates/loadgen` builds `aenv-loadgen`, which drives the E2B-compatible HTTP
API. The same binary points at a node or at a gateway, so one run measures the
node's ceiling and the next measures what the gateway costs on top of it.

It reads the environment contract the e2e suites read (`AENV_URL`,
`AENV_API_KEY`, `AENV_TEMPLATE_ID`), writes one line of newline-delimited JSON
per request, and prints a summary on stderr. Two modes:

- **closed loop** — a fixed number of requests in flight. The offered rate is
  whatever the system will take, so a system that slows down is offered less
  load, and saturation reads as "fewer requests" rather than as a queue.
- **open loop** — Poisson arrivals at a fixed rate. Arrivals that find the
  in-flight ceiling full are counted as `shed` rather than queued: an open loop
  that waits for a permit is a closed loop wearing a rate flag.

Per request it records create latency (`201` plus an `x-agentenv-sandbox-id`
header), time to `running` (polling `GET /sandboxes/{id}`), and — when
`--proxy-port` is given — the first successful proxied request. Against a mock
node the proxy stage is left off: there is no guest to answer.

Two counters in the summary are the reason the generator exists rather than
`ab` or `wrk`:

- `self_inflicted_404` — a `404` on a sandbox **this run was handed by a 201**.
  That is the direct signature of a create that never acquired a scheduler
  binding, or of a binding reconciled away underneath a live sandbox. A `404`
  with no id in hand is a bad request and is not counted here.
- `bad_gateway` — `502`s at any stage. A gateway turns an unreadable or
  over-limit upstream body into one of these *after* the node has already done
  the work.

A run that reports either exits non-zero, so a CI job can gate on it.

```
make load-burst N=200 CONCURRENCY=16 MODE=closed IMAGE=ubuntu:24.04
```

`IMAGE=` switches to `POST /sandboxes-cold`. That is the only create a
mock-backend node can serve: its snapshot repository holds no templates, while
its image resolver answers every reference with an empty placeholder. Without
it a run against a mock node measures nothing but `400`s.

### Recorded numbers

Fleet under test: one scheduler, one gateway and two mock nodes as plain
processes on `127.0.0.1`, exactly the fleet `scripts/tests/fault/lib.sh` brings
up (2 s heartbeats, 12 s binding TTL). 200 sandboxes per run; each request also
polls the sandbox to `running` and then deletes it, so `creates_per_sec` counts
the create rate of a workload that is doing three requests per sandbox, not a
create-only rate.

| Target | Mode | Concurrency | creates/s | create p50 | p90 | p99 | max | ready p50 | ready p99 |
|---|---|---|---|---|---|---|---|---|---|
| gateway | closed | 16 | 2790 | 2.84 ms | 4.66 | 8.54 | 9.21 | 0.86 ms | 2.73 ms |
| gateway | closed | 64 | 2373 | 10.94 ms | 39.27 | 49.09 | 53.25 | 2.18 ms | 24.65 ms |
| gateway | open, 100/s | 64 ceiling | 89.4 | 2.02 ms | 2.21 | 3.58 | 4.48 | 0.84 ms | 1.14 ms |
| node, direct | closed | 16 | 5037 | 1.40 ms | 1.71 | 2.94 | 3.24 | 0.58 ms | 0.91 ms |

Every run: zero errors of any status, zero `502`s, zero self-inflicted `404`s,
zero shed arrivals.

Two things worth reading off this table and nothing else. The gateway roughly
halves the achievable create rate against a node that does no work
(5037 → 2790), which bounds how much of a real create's cost the gateway can
ever be. And raising concurrency from 16 to 64 lowers throughput while
quadrupling p99: with no guest to wait for, more concurrency is only more
contention. Neither says anything about a node that boots microVMs.

The open-loop run achieved 89.4 creates/s against an offered 100/s because
elapsed time includes draining the last arrivals: 200 arrivals at 100/s is 2.0 s
of arrivals measured over 2.24 s of wall clock. Nothing was shed.

```
# The fleet, then the four runs above.
export TMPDIR=/dev/shm
source scripts/tests/fault/lib.sh && fault_fleet_up
cargo build --release -p agentenv-loadgen --bin aenv-loadgen
target/release/aenv-loadgen --url "$FAULT_GATEWAY_URL" --api-key "$AENV_API_KEY" \
  --image ubuntu:24.04 -n 200 -c 16 --mode closed --out /tmp/lg-closed-16.ndjson
target/release/aenv-loadgen --url "$FAULT_GATEWAY_URL" --api-key "$AENV_API_KEY" \
  --image ubuntu:24.04 -n 200 -c 64 --mode closed --out /tmp/lg-closed-64.ndjson
target/release/aenv-loadgen --url "$FAULT_GATEWAY_URL" --api-key "$AENV_API_KEY" \
  --image ubuntu:24.04 -n 200 -c 64 --mode open --rate 100 --out /tmp/lg-open-100.ndjson
target/release/aenv-loadgen --url "$FAULT_NODE_A_URL" --api-key "$AENV_API_KEY" \
  --image ubuntu:24.04 -n 200 -c 16 --mode closed --out /tmp/lg-node-16.ndjson
```

## Orchestrator-path snapshot benchmarks

`crates/benchmarks/benches/snapshot_benchmark.rs` holds six benchmarks that
need `/dev/kvm` and `ublk_drv`, and two that do not. The two drive
`Orchestrator` directly against the mock sandbox backend.

**What they measure:** the orchestrator's own per-capture and per-fork-child
cost — state-machine transitions, the metadata store, the handle map, the proxy
route table, the lifecycle event fan-out.

**What they do not measure:** anything a guest does. A mock sandbox has no
guest, so there is no pause, no dirty-page scan, no memory layer written, and
no restore. A number from here is a control-plane number and is worthless as a
snapshot number. In particular `bench_repeated_capture_latency` cannot see the
memory-chain defect that `mem_snapshot_parent_config_path` fixes: the mock
backend writes no layers, so there is no chain to get wrong. It bounds the
orchestrator overhead that the KVM benchmarks' numbers sit on top of, and it
answers whether repeated capture of one sandbox costs more each time for
reasons unrelated to memory.

```
make bench-snapshot-mock
```

which is

```
mkdir -p target/bench-mock-state/home target/bench-mock-state/run
AENV_SANDBOX_BACKEND=mock \
AENV_HOME_PATH=target/bench-mock-state/home \
AENV_RUNTIME_PATH=target/bench-mock-state/run \
cargo bench -p agentenv-benchmarks --bench snapshot -- \
  repeated_capture_latency fork_fanout
```

The two path variables are not decoration. Even with a mock backend the
orchestrator seeds its managed access-token secret under `[home_path]`, which
ships as `/var/lib/aenv`. Without them the run on an unprivileged account fails
inside `Orchestrator::new` with `create managed secret directory
/var/lib/aenv/secrets: Permission denied (os error 13)` and measures nothing.
`BENCH_MOCK_STATE=` moves that state elsewhere.

A run in which either named benchmark produces no number exits non-zero. The
whole output of this target is two numbers, so a run that produced neither must
not be able to leave through the same exit code as one that produced both.

### Recorded numbers

`repeated_capture_latency` — `POST /snapshots`-equivalent capture of the same
sandbox, eight times in a row:

| capture | 1 | 2 | 4 | 8 |
|---|---|---|---|---|
| wall time | 0.04 ms | 0.04 ms | 0.02 ms | 0.01 ms |

mean 0.02 ms, min 0.01 ms, max 0.04 ms over 8 samples. Flat, which is the
answer that was wanted: the orchestrator's capture path costs the same on the
eighth capture as on the first, so any slope the KVM benchmark shows belongs to
the memory chain and not to bookkeeping.

`bench_fork_fanout` — one fork of N children, reported per child:

| children | 1 | 8 | 32 | 100 |
|---|---|---|---|---|
| per child | 0.05 ms | 0.01 ms | 0.01 ms | 0.00 ms |

mean 0.02 ms, min 0.00 ms, max 0.05 ms. Per-child cost falls with fanout and
flattens by 32, so the fixed cost of a fork is paid once and the API's ceiling
of 100 children is not a cliff on the orchestrator side. At 100 children the
per-child cost rounds to 0.00 ms at the two decimal places the runner prints —
that is the resolution running out, not a free fork.

The benchmarks refuse to run unless `[machine].backend` is `mock`, rather than
overriding it: a run that silently swapped the backend would report mock numbers
under the names of the hypervisor benchmarks.

## Fault injection

`scripts/tests/fault/` brings up the same single-host mock fleet and injects one
fault each. Every check asserts a **recovery property** — what the control plane
is still doing correctly during the fault, and what it has put back afterwards.
Injecting a fault and observing that something broke proves nothing.

`lib.sh` renders the fleet from `config/default.toml` with two edits (the mock
backend, and no network-slot maintenance) and runs the scheduler and gateway
from `services/`. Timings are scaled down — 2 s heartbeats, 12 s binding TTL,
8 s report TTL, 6 s reconcile grace — so a fault window and its recovery both
fit in a test run. They satisfy the same relations the scheduler validates at
startup, so the fleet is a small deployment rather than a differently-shaped
one.

### `partition_scheduler.sh`

Takes the scheduler's listener away for N seconds. On a two-host Docker fleet
that is `docker network disconnect`; here the scheduler process leaves its port
and comes back on it, which puts the same thing on the wire the gateway cares
about — a refused connection, surfaced as `codes.Unavailable`. The distinction
matters: a *hung* scheduler yields `DeadlineExceeded`, which the gateway maps to
`502` and a client reads as "the fleet is broken", while a refused one maps to
`503`, which a client reads as "retry".

Asserts: a sandbox bound before the outage stays routable through the gateway
during it (the binding cache is what makes the data plane independent of the
scheduler); a create during the outage is refused `503`, not `502` and not a
hang; and neither the scheduler that saw the outage begin nor the one that came
back reconciles a binding away.

```
TMPDIR=/dev/shm bash scripts/tests/fault/partition_scheduler.sh
# [partition_scheduler] All 10 tests passed.
```

### `sigstop_node.sh`

Freezes one node past the binding TTL. `SIGSTOP` is the interesting failure
because the node is neither alive nor gone: its listener still accepts, it holds
its port, and it answers nothing.

Asserts: the surviving node keeps serving; every placement moves off the frozen
node once its last heartbeat ages past `scheduler.report_ttl` (ten creates, all
landing on the live node); reconcile deletes nothing for a node that has simply
stopped reporting — the binding TTL is what reaps its bindings, and an absent
roster is not authoritative deletion; and on `SIGCONT` the node is observed again
and its sandbox is routable again within two heartbeat intervals.

One further check is **recorded, not enforced**: `GET /v2/sandboxes` while a
node is frozen. It is the control-plane scale-out acceptance gate, and it fails
today — `fetchClusterList` cancels the whole fan-out on the first node error and
returns it (`services/gateway/internal/cluster_list.go:151-181`). The script
prints `[GATE OPEN] … got HTTP 502`. Run with `AENV_FAULT_STRICT_GATES=1` to
make it fatal once the owning workstream believes it passes.

```
TMPDIR=/dev/shm bash scripts/tests/fault/sigstop_node.sh
# [GATE OPEN] GET /v2/sandboxes answers while one node is frozen — got HTTP 502
# [sigstop_node] All 9 tests passed.
```

### `fill_disk.sh`

Fills the filesystem a node keeps its state on. It needs a filesystem of its own
and refuses to run without one: filling a shared host's root filesystem takes
down everything else on it, and no assertion is worth that. The script checks
the mount is neither `/` nor the repository's before writing a byte.

The fault is aimed at the operation that has to reach the disk. On a
mock-backend node a create persists nothing, so it is not the interesting
request; **pause** is, because it writes the paused sandbox's state under
`[orchestrator].persisted_sandbox_store_path` before the node may call the
sandbox paused.

Asserts: nothing hangs; a pause that cannot persist fails with `500` and a
message naming `No space left on device`, rather than succeeding and losing the
state or returning a generic error an operator cannot act on; the node keeps
answering `/health`; and freeing the space restores both create and pause with
no restart.

```
truncate -s 3G /var/tmp/aenv-fault.img
mkfs.ext4 -q -F /var/tmp/aenv-fault.img
sudo mkdir -p /mnt/aenv-fault-scratch
sudo mount -o loop /var/tmp/aenv-fault.img /mnt/aenv-fault-scratch
sudo chown "$(id -u):$(id -g)" /mnt/aenv-fault-scratch

AENV_FAULT_FILL_DIR=/mnt/aenv-fault-scratch bash scripts/tests/fault/fill_disk.sh
# [fault] create on a full filesystem answered 201 in 0s
# [fault] pause on a full filesystem answered 500 in 0s
# [fill_disk] All 9 tests passed.
```

The create answering `201` on a completely full filesystem is not a defect; it
is the mock backend persisting nothing. On a Firecracker node the same request
writes a rootfs and a memory image, and that path is not exercised here.

## What is not measured here

- **Cold create.** `H3`'s per-node cold-create ceiling needs real microVMs; a
  mock create measures the orchestrator and nothing else.
- **Guest restore.** `snapshot_resume`, `snapshot_resume_cold` and
  `concurrent_resume` in the same benchmark file need `/dev/kvm` and
  `ublk_drv`.
- **Fleet behaviour.** Every number here comes from one machine. Two nodes on
  one host share a page cache, a scheduler and a loopback interface; nothing
  crosses a network.
