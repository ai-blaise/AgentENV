# Operating AgentENV at scale

What an operator needs to turn on, what it costs, and how to turn it back off.
Everything here ships off or at its previous behaviour unless stated, so an
upgrade changes nothing until someone decides it should.

## Off switches

Each of these is asserted in both directions by the conformance harness
(`services/*/internal/offswitch_test.go` and `src/offswitch.rs`): off must
remove the behaviour, on must produce it. A gate that does nothing is worse
than no gate, because it reads as a rollback that will not roll back.

| Behaviour | Setting | Default | Off means |
| --- | --- | --- | --- |
| Node stops accepting work when out of contact | `observability.scheduler_report.kill_switch.action` | `disabled` | A partitioned node keeps accepting creates |
| Health-gated placement | scheduler `WithHealthGate` | on | Any discovered node is a candidate, however stale |
| Bounded candidate sampling | scheduler `WithCandidateSampleSize` | 32 | Every placement inspects the whole fleet |
| Gateway binding cache | `gateway.binding_cache_ttl` | 2s | Negative disables; every request re-resolves |
| Snapshot artifact sealing | `snapshot.artifact_sealing_secret` | unset | Fixed artifacts are not advertised to peers at all |
| Snapshot P2P | `snapshot.p2p_enabled` | on | Resolution goes to the repository only |
| Warm slot prewarm | `pool.network.startup_prewarm` | on | The first callers pay full slot construction cost |
| Warm pool maintenance | `pool.network.maintenance_enabled` | on | No background refill; slots are built on demand |

Zero is never "off" for a duration. It is what an unset config field looks
like, and an operator who never touched a setting must get the default rather
than silently lose the behaviour. Disabling is always an explicit value.

## Rolling an upgrade

Nodes and schedulers upgrade independently, and every wire change so far adds
fields an older peer will not send. Absent always means the conservative
reading, which is not the same direction for every field:

- **No roster digest** — the roster on the wire stays authoritative, including
  an empty one.
- **No roster completeness** — the roster is *not* authoritative, so a node
  still recovering at startup cannot wipe its own bindings.
- **No event count** — the node does not implement the counter, rather than
  having lost every event it emitted.
- **No heartbeat interval** — the scheduler does not validate TTL ordering
  against a value it invented.
- **No disown header** — a 404 is the application's, so the cached binding
  stands.

A node only starts eliding its roster after a scheduler tells it digests are
understood, so a new node against an old scheduler keeps sending everything.
The matrix is walked explicitly in
`services/scheduler/internal/version_skew_test.go`.

Upgrade order does not matter. Upgrading schedulers first buys the wire
savings sooner, because nodes cannot elide until a scheduler says they may.

## Snapshot artifact sealing

Without a secret, snapshot fixed artifacts — the guest's CPU state and the
manifest naming every layer it is built from — are not advertised to peers.
Resolution falls back to the repository, which costs bandwidth and nothing
else.

Turning it on means provisioning **the same** 32-byte secret on every node
that may resolve a snapshot:

```bash
openssl rand -hex 32
```

Set it as `snapshot.artifact_sealing_secret` or
`AENV_SNAPSHOT_ARTIFACT_SEALING_SECRET`. A node with the wrong secret fails to
open rather than reading garbage — the tag check fails first — and falls back
to the repository, so a partial rollout degrades to slower rather than broken.

Do not generate one per node. Each node would seal with a different key, every
peer fetch would fail authentication, and the deployment would look protected
while delivering nothing. That is why nothing is generated automatically.

## P2P: what would justify turning it on by default

It is not on by default and this is the bar it has not yet cleared:

1. **Sealing is deployed fleet-wide.** Until then, enabling snapshot P2P
   publishes nothing anyway, and enabling overlaybd P2P advertises layers with
   no confidentiality story of their own.
2. **A measured hit rate on a real fleet.** The facade caches misses to avoid
   re-paying the lookup budget per layer, so a low hit rate is cheap but not
   free. The number to beat is the repository fetch it replaces.
3. **Bounded worst case under partition.** A mesh that cannot reach peers must
   degrade to the repository within the lookup timeout, not accumulate them.

Until all three hold, P2P is a per-deployment opt-in.

## Migration and draining

### Making a sandbox movable

Turn on `snapshot.mobility.enabled` and the node records every sandbox it
pauses. The record will say the sandbox cannot move, and that is accurate: a
pause writes its artifacts to node-local storage, so no destination can read
them. `agentenv_mobility_stranded_sandboxes` counts exactly these.

`POST /sandboxes/{id}/snapshots` on a **paused** sandbox publishes those
artifacts to the repository and marks the record movable. The publish copies
or hard-links, so the sandbox stays resumable on its own node either way, and
a failed publish never marks it movable. With `repository_backend = "oss"` the
snapshot is readable cluster-wide; with `posix_fs` it is still one machine's
disk and the plan will keep refusing it.

So the sequence for a drainable node is: mobility enabled, an object-store
repository, and each paused sandbox snapshotted.

### Planning and executing

Draining is planned before it is run, and the plan can be inspected without
touching anything. What it cannot place is reported with why — "no compatible
node" and "no room" are kept distinct because they call for opposite
responses.

Execution is bounded on three axes: concurrency (every move is a memory image
crossing a network that is also carrying live traffic), a failure budget (a
systematic failure should be discovered once, not once per sandbox), and a
per-move timeout.

The safety property — never two live copies of one sandbox — depends on clock
synchronisation. It holds exactly while skew between any two nodes stays
within `abandon_margin + takeover_grace`, which in the shipped configuration
is 25 seconds. `docs/specs/README.md` has the model-checked result. A fleet
without time synchronisation is outside what the protocol can promise.

## Staying current with upstream

This fork tracks `kvcache-ai/AgentENV`. Everything here is additive or
narrowing, and the places most likely to conflict are:

- `services/api/proto/scheduler.proto` — the field registry comment above
  `HeartbeatRequest` exists so two independent changes cannot claim the same
  number. Read it before allocating.
- `src/snapshot/manager.rs` and `src/snapshot/p2p.rs` — the publication set
  was narrowed, not extended, so an upstream change that adds artifacts needs
  a deliberate decision about whether they may leave the node.
- `src/sandbox/network/slot.rs` — the shared netlink connection replaced
  per-operation sockets.

Rebase rather than merge, so the reasoning in each commit message stays
attached to the change it explains. Re-run the conformance harness and the
version-skew matrix after every rebase: both exist to catch a behaviour that
was quietly restored.

## Known findings not yet fixed

A verification campaign — supply-chain scanning, fuzzing, mutation testing,
Miri, TLA+ model checking and adversarial review — ran over this branch. Most
of what it found is fixed and covered by tests. These are the ones that are
not, recorded with enough detail to act on rather than rediscover.

**The pre-authentication memory bound for P2P artifacts is not where the code
says it is.** `sealing::MAX_CHUNK_SIZE` bounds the parser's buffer, but the
transport has already materialised the whole peer-supplied blob before the
parser sees it: `src/p2p/iroh/transport.rs` `download_blob` is bounded only by
`fetch_timeout_ms`, and `read_local_blob_bytes` then reads it into memory. A
byte cap belongs there, checked against the blob's stored size before the
read. Only reachable with P2P enabled, which is off by default.

**The drain's per-move timeout cancels the saga from outside**, bypassing the
compensation paths it is careful to run itself.

**The snapshot rootfs delta is still published to the mesh unsealed.** The
rationale for keeping rootfs layers — that they are registry-shaped content —
does not hold for the delta a snapshot adds, which is every guest write to `/`.
That is the same data class the memory and attached-drive layers were dropped
for.

**Per-node maps are unbounded under churn.** `rosterCache`, `eventLossTracker`
and `ReservationLedger` are pruned only by `UnregisterNode`, the graceful path.

**The scheduler caches a roster under a digest it never verifies**, so a single
inconsistent heartbeat poisons the cache for every later elided one.

**The elision safety rule is weaker than documented.** The claim that a node
keeps sending its full roster until a scheduler says it understands digests is
not quite what the code implements; TLC found a trace where a node keeps
eliding to a scheduler that never acknowledged. Liveness itself holds.

**`resolveRoster`'s safety argument cites a TTL/heartbeat-interval validation
that does not exist.** The comment claims the registry validates the ordering
against the node's reported interval; nothing does.

## What is not covered

Deliberate gaps, so nobody plans around them:

- **Tenant isolation and fairness.** AgentENV has one API key and no tenant
  model. Sealing derives a key per snapshot and per artifact so a tenant key
  hierarchy can be slotted in later without a format change, but nothing here
  isolates tenants from each other, because there are no tenants to isolate.
- **Guest memory density.** Free-page hinting, balloon statistics, and
  observed-usage-based placement all depend on what this Firecracker build's
  balloon device actually does to guest memory. That has to be measured on a
  host with `/dev/kvm` before anything is built on it; building it blind would
  risk corrupting guests to save memory.
- **Connection continuity inside the guest.** A moved sandbox keeps its
  client-facing routing (the gateway follows it), but connections terminated
  inside the guest do not survive. That needs a guest-side sidecar and a
  tools-drive ABI change, which is a one-way door.
- **The cross-node call that executes a plan.** Everything either side of it
  exists: the origin records and fences, the planner decides, the saga orders
  the steps and their compensations, and the destination's restore is an
  ordinary snapshot resume. What is missing is the request that asks a
  destination to claim and restore a specific sandbox, which is new node-to-node
  API surface. Implement `MoveExecutor` to send it and `MigrationSteps` to
  service it; both are traits precisely so the ordering and compensation logic
  could be tested without a fleet, and both have recorded tests showing the
  contract they have to satisfy. It was not built here because verifying it
  needs two hosts with `/dev/kvm`, and a migration path whose failure modes
  have never once been executed is worse than an honest gap.
