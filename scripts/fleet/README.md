# Two-VM control-plane fleet

Sixteen AgentENV node units, two gateways, a primary scheduler plus query-only
replicas, Redis and MinIO, spread across two GCE `a3-ultragpu-8g` VMs that
cannot virtualize. **Every unit runs `[machine].backend = "mock"` and runs no
guest.** What the fleet exercises is the control plane — heartbeats, rosters,
bindings, placement, mobility records, the gateway path — across a real network,
with enough plurality that a scheduler's behaviour is distinguishable from a
two-node toy.

| | Host A | Host B |
| --- | --- | --- |
| Instance | `instance-20260415-161450` | `instance-20260415-20260415-235136` |
| Fleet IP | `10.240.0.2` | `10.240.0.3` |
| Already there | GPU0 busy (102 GB job, GPUs 0-3 on NUMA0, affinity `0-55,112-167`), k3s server, Postgres, registry, NFS, slurm | k3s agent, registry, NATS; the rgate build host (`CARGO_BUILD_JOBS=48`) |
| Carve | 32 threads `96-111,208-223` (NUMA1), 128 G | 64 threads `80-111,192-223` (NUMA1), 512 G |
| Units | gateway, primary scheduler, Redis, MinIO, 4 nodes | gateway, query-only scheduler replica(s), 12 nodes |

## What it builds

Each host gets one systemd slice, `aenvfleet.slice`, carrying the host's
`AllowedCPUs`, `MemoryMax`, `MemoryHigh` and `CPUQuota` (B also pins
`AllowedMemoryNodes=1`; A does not, because a hard memory cap pinned to NUMA1
would force reclaim of the owner's NUMA1 page cache). Every fleet container is
placed under that slice with `cgroup_parent`, so `systemctl stop
aenvfleet.slice` ends the whole fleet and the slice's `MemoryMax` is a backstop
the owner's workloads can rely on regardless of any single container's limit.

Units are Docker containers from one generated compose file per host,
`/opt/aenv-fleet/compose/<A|B>.yml`, project `aenv-fleet`. All of them run with
**host networking**. That is deliberate: `src/sandbox/network/slot.rs` names the
host-side veth `veth-{idx}` and writes host iptables when it primes slots, so two
units sharing a network namespace would collide on `veth-0`. With no KVM there
are no slots, `[pool.network] maintenance_enabled = false` in every unit, and
nothing ever writes a veth or a rule. Host networking then buys the thing the
fleet exists for: every unit is reachable at the VM's real `10.240.0.x` address,
so a scheduler on A places onto a node on B with no NAT in the way. Port
separation replaces namespace separation; every listener lives in
**19000-19199**, swept free on both hosts.

Every process runs with `--group-add <gid of aenvio>` because io_uring on these
hosts is group-gated (`kernel.io_uring_disabled=1`, `kernel.io_uring_group`).
Node units additionally drop every capability: a mock node touches no
namespaces, veths or block devices, and demanding capabilities for it would
force a privileged container for nothing.

### Carve tables

Host A — `aenvfleet.slice`: `AllowedCPUs=96-111,208-223`, `MemoryMax=128G`,
`MemoryHigh=96G`, `CPUQuota=3200%`.

| unit | image | cpuset | memory | port(s) |
| --- | --- | --- | --- | --- |
| `aenv-gw-a` | agentenv-gateway | `96-97,208-209` | 4 G | 19001 HTTP, 19003 metrics |
| `aenv-sched-a` | agentenv-scheduler | `98-99,210-211` | 4 G | 19091 gRPC, 19101 metrics |
| `aenv-redis-0` | redis:7.4 | `100-101,212-213` | 8 G | 19079 |
| `aenv-minio` | minio | `102-103,214-215` | 16 G | 19000 API, 19009 console |
| `aenv-node-a0..a3` | agentenv-runtime | `104-105,216-217` … `110-111,222-223` | 24 G each | 19010-19013 API, 19110-19113 p2p |

With `AENV_FLEET_REDIS_MODE=cluster` the single Redis becomes three masters:
`aenv-redis-0..2` on `100-101`, `212`, `213`, 2 G each, ports 19079-19081 with
cluster bus 19179-19181 (`--cluster-port` keeps the bus inside the range).
Total committed is 128 G single / 126 G cluster against the 128 G cap.

Host B — `aenvfleet.slice`: `AllowedCPUs=80-111,192-223`,
`AllowedMemoryNodes=1`, `MemoryMax=512G`, `MemoryHigh=384G`, `CPUQuota=6400%`.

| unit | image | cpuset | memory | port(s) |
| --- | --- | --- | --- | --- |
| `aenv-gw-b` | agentenv-gateway | `80-81,192-193` | 4 G | 19002 HTTP, 19004 metrics |
| `aenv-sched-b0` (…`b7`) | agentenv-scheduler `-query-only` | `82-83,194-195` | 2 G each | 19092+i gRPC, 19102+i metrics |
| `aenv-node-b0..b11` | agentenv-runtime | `84-85,196-197` … `106-107,218-219` | 24 G each | 19020-19031 API, 19120-19131 p2p |

`108-111,220-223` stay spare. Total committed: 296 G of 512 G.

Ports 19140-19155 are used transiently by the per-unit smoke run (below).

### Cluster formation

The scheduler's discovery mode is `static`, and static takes any list of
`{id, endpoint}` (`services/shared/config/config.go`). Both hosts render the
same 16-entry list — `node-a0..a3` at `http://10.240.0.2:1901N`, `node-b0..b11`
at `http://10.240.0.3:190NN` — and every node's `[cluster].scheduler_endpoint`
is the primary scheduler on A, `http://10.240.0.2:19091`. Heartbeats are
single-homed, so every heartbeat from B crosses the VPC and killing a node on B
is a remote failure as far as the scheduler on A is concerned.

The scheduler replicas on B run `-query-only` against the shared Redis store:
they answer `LookupNode` and nothing else. A full scheduler on B would hold the
same static list but never receive a heartbeat, so it would see 16 nodes that
never reported. B's gateway therefore uses A's scheduler for placement and
`ListObservedNodes` (`scheduler_addr`) and its local replica for binding lookups
(`query_only_scheduler_addr`). The Redis binding store is what makes the two
schedulers one control plane.

MinIO runs on A as the S3-compatible backend for snapshot-repository work. The
node template leaves `[snapshot] repository_backend = "posix_fs"`; point
`[backend.oss]` at `http://10.240.0.2:19000` with the credentials in the fleet
`.env` when a test needs it.

### Secrets

`/opt/aenv-fleet/compose/.env` (mode 0600) holds one `AENV_API_KEY`, one
`AENV_FLEET_CLUSTER_ID` and MinIO's root credentials. Host A generates it; host
B **must be given a copy** before its bootstrap runs, otherwise B would be a
second cluster that happens to share IPs. Compose reads `.env` from the
compose file's directory, so `docker compose -f /opt/aenv-fleet/compose/A.yml
ps` works by hand with the same interpolation the bootstrap used.

Redis and the scheduler's gRPC port carry no authentication, as in the
repository's own deployments; the VPC firewall (below) is the boundary. Redis
binds to the fleet IP and loopback only.

## How to run

Preconditions the bootstrap **asserts and never installs** on each host:
docker with the compose plugin, cgroup v2 with the systemd cgroup driver and
`cpuset` delegated at the root, the `aenvio` group, a `/etc/sysctl.d` drop-in
setting `kernel.io_uring_group` to that group's gid with
`kernel.io_uring_disabled = 1` live, the expected hostname and fleet IP for the
role, CPUs up to 223 online, `jq`, `curl`, `ss`, `python3`. The repository
checkout must be on the host: the bootstrap builds images from
`deploy/docker/Dockerfile.*` and runs `scripts/tests/smoke/mock_node.sh`.

```sh
# Host A first: it owns the primary scheduler and the binding store.
sudo scripts/fleet/bootstrap.sh A

# Copy the secret set to B (IAP-tunnelled scp, same path, mode 0600), then:
sudo scripts/fleet/bootstrap.sh B
```

The bootstrap is idempotent. A re-run re-renders every file (writing only what
changed), `docker compose up -d` leaves healthy units alone, and every gate runs
again. Knobs, all environment variables:

| variable | effect |
| --- | --- |
| `AENV_FLEET_REGISTRY=10.240.0.3:5000/aenv` | pull `agentenv-{runtime,gateway,scheduler}:<tag>` from a registry before building; with `AENV_FLEET_PUSH=1` push after a local build. The registry must already be in docker's `insecure-registries`; the bootstrap does not edit `daemon.json`. |
| `AENV_FLEET_IMAGE_TAG` | image tag, default `fleet` |
| `AENV_FLEET_REBUILD=1` | rebuild images even if present |
| `AENV_FLEET_SERVER_BIN=/path/to/server` | skip the multi-GB runtime image: node units run `ubuntu:24.04` with the release binary bind-mounted at `/server` |
| `AENV_FLEET_REDIS_MODE=cluster` | three Redis masters on A instead of one |
| `AENV_FLEET_SCHEDULER_REPLICAS_B=n` | query-only replicas on B (1-8, default 1) |
| `AENV_FLEET_IP_A`, `AENV_FLEET_IP_B` | fleet IPs, default `10.240.0.2/.3`; the secondary subnet `10.242.0.2/.3` is the alternative if the primary network's firewall is not opened |
| `AENV_FLEET_FORCE_HOST=1` | skip the hostname guard (renamed instance only) |
| `AENV_FLEET_SKIP_FIREWALL_CHECK=1` | downgrade firewall findings to warnings |

Rendering without a host, for review or CI:

```sh
scripts/fleet/bootstrap.sh A --render-only /tmp/fleet-a
scripts/fleet/bootstrap.sh B --render-only /tmp/fleet-b
```

### What the bootstrap gates on

In order, per host, and it stops at the first failure:

1. Host preconditions above; firewall rules (next section); on B, TCP reach to
   A's scheduler and Redis; fleet ports free on first bring-up.
2. `aenvfleet.slice` active with a non-empty effective cpuset.
3. Images present (pulled or built); with `AENV_FLEET_SERVER_BIN`, the binary
   runs inside the base image.
4. `docker compose config -q`, then `up -d`. In cluster mode Redis comes up
   first and `redis-cli --cluster create` forms the masters.
5. Redis, MinIO and the scheduler(s) healthy by their container healthchecks;
   gateway `GET /health` → 204.
6. For every node unit: container healthy (`GET /health` → 204 over `/dev/tcp`,
   `scripts/fleet/node_healthcheck.sh`); the mock startup rail
   `sandbox backend is "mock"` present in its logs; then
   `scripts/tests/smoke/mock_node.sh` run with
   `scripts/fleet/smoke_in_unit_shape.sh` as the server launcher, which starts
   the server in that unit's exact image, cpuset, memory cap, slice, host
   network namespace, group and dropped capabilities on a port in 19140-19155.
   The smoke proves a cold create returns 201, the sandbox is listed, and all
   three mock rails are logged.
7. One unit's cgroup path is verified to sit under `aenvfleet.slice`.
8. Through the local gateway, `GET /nodes` shows every one of this host's node
   ids `ready` with `machineInfo.sandboxBackend == "mock"`. Any other backend
   aborts.

## Verifying cross-host formation

Both gateways talk to the primary scheduler, so either shows the whole cluster:

```sh
sudo scripts/fleet/status.sh A      # or B
```

prints the slice, `docker compose ps`, and a `/nodes` summary: observed and
ready counts out of 16, a breakdown by status, by host prefix and by backend,
then one line per node. The health gate has converged when it reads
`ready: 16/16`, `host: a=4 b=12`, `backend: mock=16`. By hand:

```sh
set -a; source /opt/aenv-fleet/compose/.env; set +a
curl -s -H "x-api-key: $AENV_API_KEY" http://10.240.0.2:19001/nodes \
  | jq '[.[] | select(.status == "ready")] | length'          # 16
curl -s -H "x-api-key: $AENV_API_KEY" http://10.240.0.3:19002/nodes \
  | jq 'map(.machineInfo.sandboxBackend) | unique'            # ["mock"]
```

A create through either gateway is placed round-robin across both hosts; the
node it landed on is `X-Aenv-Node-Id` when the gateway runs with `debug_mode`,
or visible as `sandboxCount` moving in `/nodes`:

```sh
curl -s -H "x-api-key: $AENV_API_KEY" -H 'Content-Type: application/json' \
  -d '{"image":"ubuntu:24.04"}' http://10.240.0.3:19002/sandboxes-cold
```

Scheduler metrics at `http://10.240.0.2:19101/metrics` carry
`agentenv_scheduler_observed_nodes{status="ready"}`, which is the same gate as a
time series. A node's own heartbeat interval is 5 s and the scheduler's
`report_ttl` is 30 s, so a killed unit on B leaves the ready set within 30 s and
rejoins within one interval of `docker compose up -d` on B.

## Firewall

Measured on the project, not assumed. Both instances carry the network tag
`legacy-compromised-a3u`; the effective rules (all INGRESS, priority 1000) are:

| rule | network | source | allows | target tag | state |
| --- | --- | --- | --- | --- | --- |
| `a3u-gvnic-asia-south1-0-allow-internal-ssh` | `a3u-gvnic-asia-south1-0` (10.240.0.0/20) | `35.235.240.0/20` (IAP) | tcp:22 | `command-center-clean` | enabled |
| `a3u-gvnic-asia-south1-1-allow-internal` | `a3u-gvnic-asia-south1-1` (10.242.0.0/20) | `10.242.0.0/20` | tcp, udp, icmp | `command-center-clean` | enabled |
| `a3u-gvnic-asia-south1-1-allow-ssh-external` | `a3u-gvnic-asia-south1-1` | `0.0.0.0/0` | tcp:22 | `legacy-compromised-a3u` | **disabled** |

There is no internal-allow rule on the primary network at all, and the IAP rule
targets a tag the instances do not currently carry. The fleet needs, on the
primary network `a3u-gvnic-asia-south1-0`:

1. IAP ssh: tcp:22 from `35.235.240.0/20` to the instances' actual tag.
2. An internal allow: `tcp:19000-19199,tcp:6379,tcp:16379` from
   `10.240.0.0/20` to the instances' tag. (6379/16379 are the Redis defaults an
   operator would reach for; the fleet's own Redis listens inside 19000-19199.)
3. `a3u-gvnic-asia-south1-1-allow-ssh-external` stays disabled; the fleet never
   needs public ssh.

`bootstrap.sh` asserts all three when `gcloud` is present and authorised on
the VM (it reads the instance's tags and network from the metadata server and
evaluates the rule list; it never creates a rule) and stops with the commands
below when one is missing. Without `gcloud` it prints the requirements and
continues; on B the TCP reach check to A's scheduler is the practical test.
Two ways to satisfy them; the operator decides which:

**Option 1 — re-add the tag the existing rules target, and add the missing
internal allow on the primary network:**

```sh
gcloud compute instances add-tags instance-20260415-161450 --zone asia-south1-b --tags command-center-clean
gcloud compute instances add-tags instance-20260415-20260415-235136 --zone asia-south1-b --tags command-center-clean
gcloud compute firewall-rules create a3u-gvnic-asia-south1-0-allow-internal-fleet \
  --network a3u-gvnic-asia-south1-0 --direction INGRESS --priority 1000 --action ALLOW \
  --rules tcp:19000-19199,tcp:6379,tcp:16379 --source-ranges 10.240.0.0/20 \
  --target-tags command-center-clean
```

**Option 2 — tag-scoped rules for the tag the instances carry today:**

```sh
gcloud compute firewall-rules create a3u-gvnic-asia-south1-0-allow-iap-ssh-legacy \
  --network a3u-gvnic-asia-south1-0 --direction INGRESS --priority 1000 --action ALLOW \
  --rules tcp:22 --source-ranges 35.235.240.0/20 --target-tags legacy-compromised-a3u
gcloud compute firewall-rules create a3u-gvnic-asia-south1-0-allow-internal-fleet-legacy \
  --network a3u-gvnic-asia-south1-0 --direction INGRESS --priority 1000 --action ALLOW \
  --rules tcp:19000-19199,tcp:6379,tcp:16379 --source-ranges 10.240.0.0/20 \
  --target-tags legacy-compromised-a3u
```

## Teardown

```sh
sudo scripts/fleet/teardown.sh A     # and B
```

`docker compose down -v` removes the containers and the fleet's named volumes
(per-unit `AENV_HOME`, Redis, MinIO); anything still labelled with the project
is removed by label; smoke containers are removed; `aenvfleet.slice` is
stopped, its unit file deleted and systemd reloaded; `/opt/aenv-fleet` is
removed. Nothing else is touched: not the io_uring drop-in, not the `aenvio`
group, not `daemon.json`, not images (`AENV_FLEET_TEARDOWN_IMAGES=1` removes the
three fleet-tagged ones; there is never a `docker system prune`). Host A's
leftover `10.11.0.0/16 -i veth-+` iptables rules predate the fleet and are
reported, not deleted. Teardown is idempotent.

## Files

| file | role |
| --- | --- |
| `bootstrap.sh` | preconditions, slice, images, render, `up -d`, gates |
| `teardown.sh` | the reverse |
| `status.sh` | read-only slice / compose / `/nodes` view |
| `lib.sh` | carve tables, port map, role guards, firewall rule evaluator |
| `node.toml.tmpl` | node config, derived from `config/default.toml`: mock backend, slot maintenance off, fixed p2p port, ublk metrics off, heartbeats on, templated identity and scheduler endpoint |
| `node_healthcheck.sh` | `/dev/tcp` `GET /health` → 204, mounted into every node unit |
| `smoke_in_unit_shape.sh` | server launcher for `mock_node.sh` that runs the server in one unit's exact shape |

## What this fleet is not

It runs no guests. `05_template_lifecycle`, `06_proxy`, `12_template_shared_repository`
and the artifact half of `13_snapshot` in `scripts/tests/e2e` still need a host
with `/dev/kvm` and `ublk_drv`. The P2P listen ports are rendered but
`[p2p].enabled` stays `false`; enabling it across hosts is an unresolved
addressing question in the design notes. The pre-existing k3s cluster spanning
both VMs is a possible second topology for `kubernetes` discovery, capped at
one node pod per VM by the DaemonSet, and is not touched by these scripts.
