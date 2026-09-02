# Operating AgentENV at scale

What an operator needs to turn on, what it costs, and how to turn it back off.
Everything here ships off or at its previous behaviour unless stated, so an
upgrade changes nothing until someone decides it should.

## Off switches

Each of these is asserted in both directions by the conformance harness
(`services/*/internal/offswitch_test.go` and `src/offswitch.rs`): off must
remove the behaviour, on must produce it. A gate that does nothing is worse
than no gate, because it reads as a rollback that will not roll back. Where a
switch's behavioural assertions live elsewhere — the warm pool's worker is
covered in `crates/warm-pool` — the harness still pins the wiring, and says so
in a comment naming the tests it defers to.

| Behaviour | Setting | Default | Off means |
| --- | --- | --- | --- |
| Node stops accepting work when out of contact | `observability.scheduler_report.kill_switch.action` (window: `.after_secs`, default 60) | `disabled` | A partitioned node keeps accepting creates |
| Health-gated placement | scheduler `WithHealthGate` | on | Any discovered node is a candidate, however stale |
| Bounded candidate sampling | scheduler `WithCandidateSampleSize` | 32 | Every placement inspects the whole fleet |
| Reservation ledger | `scheduler.reservations_enabled` | off | Placement reads each heartbeat snapshot verbatim; a burst inside one interval sees the same numbers |
| Scheduler gRPC authentication | `scheduler.auth_token` | unset | Every RPC is accepted; the health service is open either way |
| Gateway binding cache | `gateway.binding_cache_size` (or a negative `gateway.binding_cache_ttl`) | 65536 / 2s | Zero or negative size, or a negative TTL, disables; every request re-resolves |
| Gateway scheduler credential | `gateway.scheduler_auth_token` | unset | Scheduler RPCs carry no `authorization` metadata |
| Snapshot artifact sealing | `snapshot.artifact_sealing_secret` | unset | Fixed artifacts are not advertised to peers at all |
| Snapshot P2P | `snapshot.p2p_enabled` | on | Resolution goes to the repository only |
| Warm slot prewarm | `pool.network.startup_prewarm` | on | The first callers pay full slot construction cost |
| Warm pool maintenance | `pool.network.maintenance_enabled` (ANDed with `pool.network.enabled`) | on | No background refill; slots are built on demand |
| Node-local admission control | `orchestrator.admission.enabled` | off | Every create is admitted whatever the node is carrying |

Zero is never "off" for a duration a switch depends on. It is what an unset
config field looks like, and an operator who never touched a setting must get
the default rather than silently lose the behaviour, so
`kill_switch.after_secs = 0` alongside an action is refused at startup rather
than quietly arming nothing. Disabling is always an explicit value. The one
duration that does read zero as off is `p2p.reannounce_interval_secs`, whose
default is 60: it turns off a refresh, not a safety gate, and an untouched
config never reaches it.

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
The first four rows are walked explicitly in
`services/scheduler/internal/version_skew_test.go`; the disown header is a
gateway concern and is covered in `services/gateway/internal/cutover_test.go`.
Only the directions reachable in-tree are testable — an old node against a new
scheduler. The reverse needs a released binary, which is what the `a54bacd`
floor below is for.

That permission expires with each response rather than latching on the first
one that granted it. Schedulers are replaced by rollouts and rollbacks, and a
node can reach a different one without ever seeing an error, so a permission
that survived the process that gave it would let a node elide to a scheduler
that reads an elided roster as an empty one and deletes every binding it holds.

Because the heartbeat that discovers the change has already been sent, an
elided heartbeat also declines to call itself complete: `roster_complete`
describes the message, not just the node. A scheduler that resolves the digest
then reconciles nothing instead of deleting everything, taking the authority to
delete from the cached roster the digest resolves to, raised but never lowered
by the wire bit.

That protection needs a scheduler that acts on `roster_complete`, which begins
at `a54bacd`. To anything older — every scheduler up to and including `v0.1.3`
— the field is an unknown one, discarded, and an elided heartbeat reads as "this
node owns nothing" and deletes every binding for it, with no grace. Expiring the
elision permission per response bounds that to the single heartbeat that
discovers the change, roughly one interval (5s by default) of 404s for every
sandbox on the node, per grant.

So there is a floor, and it is the reason to upgrade schedulers first rather
than a preference: once any node that can elide is deployed, no scheduler below
`a54bacd` may serve heartbeats. No rollback past it, and no pre-floor replica
left in the load-balancer rotation — with a shared binding store, one stale
replica wipes what the rest of the fleet is serving.

## Snapshot artifact sealing

Without a secret, snapshot fixed artifacts — the guest's CPU state and the
manifest naming every layer it is built from — are not advertised to peers.
Resolution falls back to the repository, which costs bandwidth and nothing
else.

Sealed or not, the only snapshot layers that reach the mesh are the ones that
came from a registry, identified by the digest of the same bytes any peer
could already pull. The guest's own data never does: not the memory layers,
not the attached drives, and not the rootfs delta — every write the guest made
to `/`. All three are served from the repository, and none of them has a read
path that would consume a peer copy. The delta is the one that has to be
excluded rather than simply not offered, because it rides inside the rootfs
image config; it is excluded by where it came from, not by whether it has a
digest, since the restack gives it one.

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

1. **Sealing is deployed fleet-wide.** Until then, snapshot P2P still
   advertises the registry-origin rootfs layers — bytes any peer could already
   pull from the registry, keyed by the digest of that same content — while
   withholding the fixed artifacts that are what actually accelerates
   resolution. That is exposure-shaped surface without the payoff. Overlaybd
   P2P advertises the same class of layers with no confidentiality story of
   their own. Note that layers pulled from a credentialed private registry
   become servable to mesh peers by digest the moment `[p2p]` is enabled, so a
   private-registry deployment has to treat mesh membership as inside the
   registry's trust boundary.
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

## Control-plane-only nodes: the mock backend

`[machine].backend = "mock"` (or `AENV_SANDBOX_BACKEND=mock`) starts the whole
node — HTTP API, orchestrator, scheduler heartbeats, mobility records,
observability — with an in-process sandbox backend that runs **no guest and
isolates nothing**. It exists so the control plane can be deployed, scaled and
exercised on hosts that cannot virtualize: every scheduler, gateway, binding,
roster and mobility behaviour is real, only the sandbox is not.

It is never a fallback. A node configured for `firecracker` on a host without
`/dev/kvm` or `ublk_drv` refuses to start, exactly as before; only an explicit
`mock` makes those checks advisory. A mock node logs a warning at startup and
on every sandbox it builds, reports `sandbox_backend = "mock"` in every
heartbeat, and is shown as such by the gateway's `GET /nodes`
(`machineInfo.sandboxBackend`), so a fleet can be audited for one at a glance.
Older nodes that predate the field are read as `firecracker`, which is the
only thing they could have been.

Mock mode skips the ublk daemon, the warm VM pool, the warm slot pool and the
runtime dependency downloads, all of which exist to start guests. It still
generates the overlaybd runtime configuration the orchestrator reads at
construction, and still honours every off switch above.

## Fleet: sixteen mock units across two VMs

`scripts/fleet/` brings the mock backend up as a fleet rather than a single
process: four node units, a gateway, the primary scheduler, Redis and MinIO on
one GCE VM; twelve node units, a gateway and query-only scheduler replicas on a
second; one static `scheduler.nodes` list spanning both hosts' internal IPs.
Every unit is a Docker container under a per-host `aenvfleet.slice` whose
cpuset and memory cap stay clear of the GPU workload and the build jobs the two
hosts already carry, and every unit runs with host networking — safe only
because a mock node never primes a network slot, so nothing writes the host
veths and iptables rules that `slot.rs` would otherwise name identically in
every unit. No unit runs a guest.

The bootstrap gates each unit on `scripts/tests/smoke/mock_node.sh` run in that
unit's exact shape, then on the primary scheduler reporting it `ready` with
`sandboxBackend = "mock"` through the gateway's `GET /nodes`, which is also how
formation is verified: the ready count converging on sixteen.
`scripts/fleet/README.md` has the carve tables, the port map, the firewall
prerequisites and the teardown.

## Known findings not yet fixed

A verification campaign — supply-chain scanning, fuzzing, mutation testing,
Miri, TLA+ model checking and adversarial review — ran over this branch. This
section is the ledger of what it found and what is still open. Nothing is open
right now: all four entries recorded here have since been closed and covered
by tests. They are kept as a record rather than deleted, so the reasoning stays
discoverable.

Closing a finding includes deleting or annotating its entry here. That step was
missed twice — `2b4bf2a` landed before this document's next edit and its entry
survived, and `dd3397a` closed six review findings without touching the ledger —
which is how a list of fixed defects came to read as a list of live ones.

**Closed by `2b4bf2a`: the pre-authentication byte bound for P2P artifacts.**
`max_bytes` is now a parameter of the whole fetch API (`src/p2p/transport.rs`),
enforced against the bytes as they arrive in `download_blob_bounded` and
against the blob's stored size before a local read, with every caller updated.

**Closed by `dd3397a`: the drain no longer cancels the saga from outside.**
`run_move` puts the move on its own task and a timeout requests a cooperative
`MoveCancel`, waiting out `unwind_grace` rather than aborting the task, so the
compensation paths still run.

**Closed by `dd3397a`: the scheduler verifies the roster digest it caches.**
`rosterCache.remember` recomputes the digest over the ids and refuses a
mismatch, so one inconsistent heartbeat cannot poison later elided ones.

**Closed by `dd3397a`: `resolveRoster`'s safety argument now cites something
real.** `validateSchedulerTTLOrdering` exists and is wired into config
validation, `mayElideRoster` re-checks the TTL/interval relation on every
heartbeat, and the comment names the opt-in check plus that runtime
enforcement.

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
