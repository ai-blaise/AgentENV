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
