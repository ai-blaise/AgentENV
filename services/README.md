# services

Go implementation of a distributed Gateway and pluggable Scheduler for AgentENV.

## Features

- Gateway routes control-plane requests by real-time scheduling.
- Gateway aggregates `GET /sandboxes` and `GET /v2/sandboxes` across all scheduler nodes.
- Gateway aggregates `GET /nodes` across all observed nodes in the scheduler.
- Gateway resolves `GET /nodes/{id}` via scheduler and proxies to the target node.
- Gateway routes sandbox requests by existing sandbox-to-node binding.
- Scheduler exposes gRPC API and supports pluggable strategy providers.
- Built-in strategies in v1: round_robin and random.
- Scheduler supports both static node configuration and Kubernetes EndpointSlice discovery.
- Scheduler sandbox binding store can be in-memory or Redis-backed.
- Scheduler can run as a primary read/write service or as query-only replicas that serve only `LookupNode` from Redis.
- Scheduler observes node health and sandbox roster from heartbeats, and drops expired sandbox-to-node bindings on heartbeat, node unregistration, or lookup.
- HTTP and WebSocket forwarding.

## Header compatibility

Gateway treats these headers as sandbox-routing markers:

- x-agentenv-sandbox-id
- e2b-sandbox-id

If one of them exists, gateway resolves node from scheduler binding and forwards request there.

When `gateway.sandbox_proxy_domains` is configured, gateway also accepts host-based sandbox
data-plane URLs in the form `{port}-{sandboxID}.{proxy_domain}`. The host-derived
sandbox ID and port take precedence over conflicting routing headers; the gateway logs
that conflict at debug level and forwards the request to the backend node's `/proxy`
endpoint. Host-based routing requires the sandbox ID to be RFC 952/1123 DNS-label compatible
(`[a-z0-9]([a-z0-9-]*[a-z0-9])?`), and the full `{port}-{sandboxID}` label must fit
within the 63-character DNS label limit.

Sandbox data-plane routing is host- or header-based. Path-derived sandbox IDs are
only used for sandbox control-plane APIs such as `/sandboxes/{id}/pause`; clients
that proxy sandbox traffic through the gateway must use a sandbox proxy host or a
sandbox routing header.

## Build

Prerequisites:

- Go 1.21+

Commands (from `services/`):

```bash
make tidy
make proto
make build      # builds both gateway and scheduler
make test       # tests both services
```

Per-service (from `services/gateway/` or `services/scheduler/`):

```bash
make build
make test
```

## Run locally

Start scheduler:

```bash
make run-scheduler
```

Start gateway with the same API key configured on every AgentENV runtime node:

```bash
export AENV_API_KEY="e2b_$(openssl rand -hex 32)"
make run-gateway
```

The default local config uses `127.0.0.1:9090` for the scheduler.

The gateway and runtime nodes require the same API key for control-plane APIs.
The gateway reads `AENV_API_KEY` or `/run/secrets/api-key`; it does not generate
a key. The gateway routes data-plane requests without authenticating them
because only the owning runtime has the sandbox policy needed to distinguish
public ingress, private ingress, and secure envd. Private application proxy
requests use the sandbox response's `trafficAccessToken` in the
`e2b-traffic-access-token` header; secure envd requests use `X-Access-Token`.

## Scheduler configuration

Scheduler discovery modes:

- `static` (default): use `scheduler.nodes` from config.
- `kubernetes`: watch EndpointSlices for a headless Service and build the node list from serving Pod endpoints. Terminating endpoints, or Pods matching `no_schedule_pod_selector`, are kept as lingering/no-schedule nodes; Pods matching `ignore_pod_selector` are excluded.

General config notes:

- `scheduler.report_ttl` must be a duration string such as `"30s"` in JSON config files.
- `scheduler.binding_ttl` must be a duration string such as `"30s"` in JSON config files.
- `scheduler.report_ttl` controls how long an observed node heartbeat stays healthy. Unset, it is the smaller of `30s` and `scheduler.binding_ttl`. It must not exceed `scheduler.binding_ttl`: routing has to outlive placement eligibility, or a node's bindings expire while the scheduler is still filling it and its live sandboxes 404. The relation is checked at config load.
- `scheduler.binding_ttl` controls how long sandbox-to-node bindings survive without a fresh `RecordAssignment` or heartbeat roster refresh.
- `scheduler.reconcile_grace` is how recently a binding must have been written for a heartbeat reconcile to leave it alone when the node's roster omits it; it covers the gap between a node collecting its roster and the scheduler acting on it, during which a newly placed sandbox is bound but in no roster yet. Unset, it is the smaller of `10s` and half of `scheduler.binding_ttl`. An explicit value must be shorter than `scheduler.binding_ttl` and, when `scheduler.heartbeat_interval` is set, at least two intervals long; both relations are checked at config load and again when the binding store is built.
- `scheduler.heartbeat_interval` is the interval nodes are expected to report at. It is used only to validate the TTLs and grace above against it at startup: `scheduler.report_ttl` and `scheduler.binding_ttl` must be at least three intervals, `scheduler.reconcile_grace` at least two. Unset, those checks are skipped; the scheduler still re-checks both TTL relations on every heartbeat against the interval each node reports, and withholds roster elision from a node whose interval either TTL cannot cover three of — that node's roster is reconciled in full on every heartbeat, and the misordering is logged once per node. The heartbeat itself is never refused for it: refusing would take a live node's data plane down over a configuration relation.
- `scheduler.schedule_health_gate` (default `true`) excludes nodes whose last heartbeat is older than `scheduler.report_ttl`, or that report themselves unhealthy or draining, from placement. Setting it to `false` restores placement on any discovered node regardless of heartbeat age.
- `scheduler.redis_addr` selects Redis-backed sandbox binding storage when set; when empty, the scheduler uses the in-memory binding store. It accepts `host:port`, a comma-separated list of cluster seeds (`host1:6379,host2:6379,host3:6379`), or a Redis URL such as `redis://[:password@]host:6379/db`.

  Whether to speak the cluster protocol is asked of the server rather than inferred from the address, so a single-seed cluster works and a misconfiguration fails at startup rather than on the first `MOVED`. Bindings are keyed by sandbox id and node indexes by node id, each with its own hash tag, so they shard across the cluster instead of piling into one slot — the read path that every proxied request takes is a single key in a single slot.
- `--query-only` starts a read-only scheduler that supports only `LookupNode`; it requires `scheduler.redis_addr` and does not need node discovery config.
- `scheduler.artifact_store_capacity` controls how many distinct P2P artifact keys the in-memory artifact index keeps before LRU eviction; defaults to `1000000`. Evictions are counted by `agentenv_scheduler_p2p_artifact_evictions_total`.
- `scheduler.artifact_lookup_node_limit` controls how many node IDs a P2P artifact lookup returns; defaults to `8`. A node dials at most four candidates concurrently and stops at the first hit, so a longer answer is response bytes spent on nodes that are never contacted. The returned subset is a prefix of Go's randomised map order, which spreads fetches across providers; `agentenv_scheduler_p2p_lookup_peers` is the distribution of answer sizes. Values `<= 0` return all matching nodes.
- `scheduler.auth_token` is the shared bearer token every gRPC caller — the gateway, each node's heartbeat reporter — must present as `authorization: Bearer <token>` request metadata. When unset the listener accepts every call, which is how every deployment before this key behaved; the scheduler logs `scheduler gRPC authentication is disabled` once at startup. When set, every RPC on the `Scheduler` service — placement, bindings, heartbeats, event reports, the P2P index and the mobility records — is refused with `UNAUTHENTICATED` unless the token matches (constant-time compare); only `grpc.health.v1.Health` stays open so readiness and liveness probes keep working. Refusals are counted by `agentenv_scheduler_auth_rejected_total{reason="missing"|"malformed"|"invalid"}` and appear under `status="unauthenticated"` in `agentenv_scheduler_rpc_duration_seconds`. Roll out by distributing the token to gateways and nodes first, then setting it here; unsetting it is the rollback.
- `scheduler.auth_token_file` names a file holding the token instead, for deployments that mount secrets rather than render them into config. The file must exist and be non-empty when the key is set; setting both `auth_token` and `auth_token_file` is refused.
- `scheduler.reservations_enabled` (default `false`) lets the reservation ledger adjust each node's last heartbeat snapshot between heartbeats: node-reported create/fork/delete/pause/resume events move the running, paused, allocated-CPU and allocated-memory counts the way the node's own accounting does, and each placement the scheduler makes reserves one sandbox, one start in flight and the requested CPU and memory against the chosen node until that node reports the create or its next heartbeat overtakes it. This is what lets two placements inside one heartbeat interval see each other. The ledger is advisory — events are delivered best-effort and the heartbeat always wins — and ships off because a defect in it would refuse placements for capacity the fleet has. `agentenv_scheduler_reservation_drift` is the histogram of how far, in sandboxes, the ledger had moved a node when its heartbeat replaced that view; a distribution stuck at zero means the interval is short enough that the ledger is not doing anything.
- `scheduler.max_reservation_delta` (default `512`) clamps how far the ledger may move one node's sandbox count from what its last heartbeat reported, in either direction. Events that would cross it are dropped whole. Events are lossy by construction, and the clamp is what keeps a node that has stopped heartbeating from carrying unbounded phantom load until its entries expire at twice `scheduler.report_ttl`.
- `SCHEDULER_BINDING_TTL=<duration>` overrides `scheduler.binding_ttl` from the environment.
- `SCHEDULER_RECONCILE_GRACE=<duration>` overrides `scheduler.reconcile_grace` from the environment.
- `SCHEDULER_HEARTBEAT_INTERVAL=<duration>` overrides `scheduler.heartbeat_interval` from the environment.
- `SCHEDULER_REDIS_ADDR=<addr>` overrides `scheduler.redis_addr` from the environment.
- `SCHEDULER_ARTIFACT_STORE_CAPACITY=<count>` overrides `scheduler.artifact_store_capacity` from the environment.
- `SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT=<count>` overrides `scheduler.artifact_lookup_node_limit` from the environment.
- `SCHEDULER_AUTH_TOKEN=<token>` and `SCHEDULER_AUTH_TOKEN_FILE=<path>` override `scheduler.auth_token` and `scheduler.auth_token_file` from the environment.
- `SCHEDULER_RESERVATIONS_ENABLED=<bool>` and `SCHEDULER_MAX_RESERVATION_DELTA=<count>` override `scheduler.reservations_enabled` and `scheduler.max_reservation_delta` from the environment.

### Scheduling strategy

`scheduler.strategy` selects the algorithm used to pick a node from the eligible candidate list. Built-in strategies:

| Strategy | Behaviour |
|---|---|
| `round_robin` (default) | Cycles through eligible nodes in stable order |
| `random` | Picks a uniformly random eligible node |
| `least_loaded_of_two` (alias `p2c`) | Samples two eligible nodes and picks the less loaded by heartbeat snapshot; bounds the maximum load far more tightly than round-robin against a view up to one heartbeat interval stale, without the herding that "pick the least loaded" produces |
| `bin_pack` | Fills the most loaded eligible node — the one closest to its `scheduler.node_resource_limit` ceiling without being over it. Right for draining or consolidating a fleet; **wrong for tail latency**, because a burst of creates all land on one node where each start contends with the others for that node's network-slot and iptables locks, so the slowest create in the burst gets slower. Only bounded by `scheduler.node_resource_limit`: with no limit configured it fills one node indefinitely |

The strategy interface receives `RichNode` values that carry the node identity (ID + endpoint) together with the latest heartbeat `NodeSnapshot` (sandbox counts, CPU, memory, disk metrics). `round_robin` and `random` ignore the snapshot; `least_loaded_of_two` and `bin_pack` score it. Only `round_robin` asks for its candidates in a stable order (`Strategy.NeedsStableOrder`); the others skip the per-placement sort that order costs.

### Node resource limit

`scheduler.node_resource_limit` defines per-node resource thresholds that are evaluated **before** the strategy runs. Any node whose heartbeat snapshot exceeds a configured limit is removed from the candidate list, regardless of which strategy is in use. This is a generic guard-rail that sits above the strategy layer — strategies only see nodes that already passed the resource filter.

Nodes that have not yet sent a heartbeat (no snapshot available) are always kept in the candidate list, since there are no metrics to evaluate.

All fields are optional. Omitting a field (or setting the whole block to `null`) disables that particular check.

| Field | Type | Description |
|---|---|---|
| `max_sandbox_count` | uint32 | Maximum total sandbox count |
| `max_sandbox_starting_count` | uint32 | Maximum concurrently starting sandboxes |
| `max_cpu_used_percent` | uint32 | Maximum observed CPU usage (0–100) |
| `max_cpu_allocated_percent` | uint32 | Maximum allocated-CPU-to-physical-CPU ratio; can exceed 100 when overcommit is allowed |
| `max_memory_used_percent` | uint32 | Maximum observed memory usage (0–100) |
| `max_memory_allocated_percent` | uint32 | Maximum allocated-memory-to-physical-memory ratio; can exceed 100 when overcommit is allowed |
| `max_sandbox_count_including_paused` | uint32 | Maximum sandbox count over the active set plus paused sandboxes. Paused sandboxes have released their VM-side CPU and memory but still hold persisted state on the node |
| `max_allocated_cpu_including_paused` | uint32 | Maximum allocated CPU, in cores, over the active set plus paused sandboxes |
| `max_allocated_memory_bytes_including_paused` | uint64 | Maximum allocated memory, in bytes, over the active set plus paused sandboxes |

Example:

```json
"node_resource_limit": {
  "max_sandbox_count": 50,
  "max_sandbox_starting_count": 10,
  "max_cpu_used_percent": 90,
  "max_cpu_allocated_percent": 150,
  "max_memory_used_percent": 85,
  "max_memory_allocated_percent": 150
}
```

When every node is filtered out, the scheduler returns `RESOURCE_EXHAUSTED` — the fleet exists but has no capacity right now, and a retry after a moment may find some; the message says whether the resource limits or the caller's own `exclude_node_ids` emptied the list. When discovery has no nodes at all it returns `UNAVAILABLE`, which no retry will fix. The gateway maps the two to `503` with and without `Retry-After` respectively.

## Gateway configuration

- `gateway.scheduler_addr` points to the primary scheduler. The gateway uses it for scheduling, assignment writes, node listing, node detail resolution, and P2P scheduler APIs.
- `gateway.query_only_scheduler_addr` optionally points to a query-only scheduler. When set, sandbox `LookupNode` routing uses this client; when unset, gateway falls back to `gateway.scheduler_addr`.
- `gateway.request_timeout` must be a duration string such as `"30s"` in JSON config files.
- `gateway.debug_mode` (default `false`) enables debug-only behaviour such as exposing the backend node id on proxied responses. `GATEWAY_DEBUG_MODE=<bool>` overrides it from the environment.
- `gateway.max_idle_conns_per_host` bounds the pooled idle upstream connections the gateway keeps per node; zero uses the gateway default.
- `gateway.request_timeout` applies to regular proxied HTTP requests. Streaming requests and WebSocket connections reuse the client context and are not cut off by this timeout.
- `gateway.forward_response_size` only limits how much of a successful `POST /sandboxes` response the gateway buffers while extracting a sandbox ID for `RecordAssignment`; it is not a global response-size cap for all proxied traffic.
- Cluster list requests (`GET /sandboxes`, `GET /v2/sandboxes`) fan out to every scheduler node and merge results in the gateway. Direct requests to a backend node remain node-scoped.
- Cluster list requests are strict all-or-nothing: if any node times out, returns a non-2xx response, or cannot be reached, the gateway fails the whole list request rather than returning partial data.
- `GET /nodes` returns scheduler-observed node snapshots (including runtime/resource counters), with optional `clusterID` filtering.
- `GET /nodes/{id}` resolves node endpoint via scheduler and then proxies to the runtime node's admin endpoint.
- `GATEWAY_REQUEST_TIMEOUT=<duration>` overrides `gateway.request_timeout` from the environment (for example, `1m30s`).
- `GATEWAY_QUERY_ONLY_SCHEDULER_ADDR=<addr>` overrides `gateway.query_only_scheduler_addr` from the environment.
- `gateway.sandbox_proxy_domains` enables host-based sandbox data-plane routing for `{port}-{sandboxID}.{domain}` URLs. Domains are normalized to lowercase, deduplicated, and must be valid DNS names. Sandbox IDs used in host routes must be lowercase RFC 952/1123 DNS labels, and the full `{port}-{sandboxID}` label must be at most 63 characters.
- `GATEWAY_SANDBOX_PROXY_DOMAINS=<domain>[,<domain>...]` overrides `gateway.sandbox_proxy_domains` from the environment.
- `gateway.max_in_flight_creates` bounds how many `POST /sandboxes` / `POST /sandboxes-cold` placements one gateway carries at once; creates beyond it are refused immediately with `503` and `x-agentenv-refusal-reason: gateway_shed`. Zero uses the gateway default (512). Only creates are shed; management-plane requests such as `/templates` and `/snapshots` are never counted against it.
- `gateway.max_schedule_retries` bounds how many further nodes a create is offered to after one refuses it for capacity. Zero uses the gateway default (2, so three attempts); a negative value gives every create a single attempt.
- `GATEWAY_MAX_IN_FLIGHT_CREATES=<count>` and `GATEWAY_MAX_SCHEDULE_RETRIES=<count>` override those two keys from the environment.
- `gateway.binding_cache_size` (default `65536`) bounds how many sandbox-to-node bindings the gateway holds locally so data-plane requests skip the scheduler round trip. `0` or a negative value disables the cache; leaving the key unset keeps the default. Concurrent misses for one sandbox share a single `LookupNode` call.
- `gateway.binding_cache_ttl` (default `2s`) is how long a resolved binding is reused. It must stay well below `scheduler.binding_ttl`; a negative value also disables the cache.
- `gateway.binding_cache_negative_ttl` (default `200ms`) is how long a `LookupNode` that found no binding is reused, so a client polling a sandbox that does not exist cannot hammer the scheduler. It may not exceed `gateway.binding_cache_ttl` and must not be negative. Bindings the gateway records from its own create responses are installed into the cache directly and take precedence over any lookup that was already in flight.
- `GATEWAY_BINDING_CACHE_SIZE=<count>`, `GATEWAY_BINDING_CACHE_TTL=<duration>` and `GATEWAY_BINDING_CACHE_NEGATIVE_TTL=<duration>` override those three keys from the environment.
- `gateway.scheduler_auth_token` is presented to the scheduler on every RPC, on both `gateway.scheduler_addr` and `gateway.query_only_scheduler_addr`, as gRPC metadata `authorization: Bearer <token>`. Unset (the default) dials exactly as before, for a scheduler that does not enforce a token yet; the gateway logs a warning at startup when it is unset. `GATEWAY_SCHEDULER_AUTH_TOKEN=<token>` overrides it from the environment.

### Create refusals

A refused create is a `503` carrying `x-agentenv-refusal-reason`, so a client can tell the refusals apart and respond to each correctly. `Retry-After` is the other half of the signal: it is sent when waiting can change the answer and withheld when only an operator can, so a client that retries on `Retry-After` alone and gives up otherwise behaves correctly without reading the reason at all.

| Reason | Origin | `Retry-After` | Meaning |
|---|---|---|---|
| `node_at_capacity` | node | yes | The node's admission gate refused this create and the gateway was configured not to retry (`gateway.max_schedule_retries` negative). With retries on, a client never sees this reason from the gateway; it sees one of the two below. |
| `retries_exhausted` | gateway | yes (the node's) | Every node the create was offered to refused it for capacity, up to `gateway.max_schedule_retries`. The body and `Retry-After` are the last node's. |
| `body_not_replayable` | gateway | yes (the node's) | A node refused the create for capacity and the gateway could not offer it elsewhere because the request body exceeded the 64 KiB it holds for a second attempt. Send a smaller create. |
| `fleet_exhausted` | gateway | yes | The scheduler saw nodes and none would take the sandbox (gRPC `ResourceExhausted`). Capacity frees up as sandboxes end. |
| `no_nodes` | gateway | no | The scheduler could offer no node at all (gRPC `Unavailable`): none discovered, or the scheduler unreachable. Waiting does not fix it. |
| `gateway_shed` | gateway | yes | The gateway declined before placing anything because it is already at `gateway.max_in_flight_creates`. The fleet may be fine; slow down. |

The gateway retries a create only on `503` **with** `node_at_capacity`. A `503` without that reason — a node mid-shutdown, a proxy in front of it, an older node — is the node's own answer and is returned to the client unchanged. Non-create requests are never retried on another node: a `503` to a `DELETE` is an answer, and re-running the `DELETE` elsewhere is not. Fork is never rescheduled either: it is routed to the parent sandbox's node and the children cannot exist anywhere else, so a fork refused for capacity reaches the client as the node's `node_at_capacity`.

Scheduler failures on non-create requests use the same `Retry-After` rule (`ResourceExhausted` sends it, `Unavailable` does not) but carry no refusal reason, because they were not creates.

`agentenv_gateway_create_refusals_total{reason}` counts every gateway-issued refusal by the reasons above; `agentenv_gateway_binding_cache_total{result}` counts the binding cache's `hit`, `miss`, `negative_hit` and `evict` outcomes.

### Off switches

Each gateway gate is asserted in both directions by `services/gateway/internal/offswitch_test.go`: off must remove the behaviour, on must produce it.

| Behaviour | Setting | Default | Off means |
|---|---|---|---|
| Binding cache | `gateway.binding_cache_size` (also a negative `gateway.binding_cache_ttl`) | `65536` / `2s` | Every data-plane request resolves through the scheduler |
| Disown invalidation | node sends `x-agentenv-sandbox-disowned` | on | A cached binding survives a node's 404 until the TTL |
| Create rescheduling | `gateway.max_schedule_retries` | `2` | Negative: one attempt, the node's refusal passes through unchanged |
| Scheduler credential | `gateway.scheduler_auth_token` | unset | RPCs carry no `authorization` metadata |

### Schedule hints

`POST /sandboxes` and `POST /sandboxes-cold` bodies are parsed into a `ScheduleRequestHint` (cpu, memory, images, metadata) that travels with the `Schedule` RPC. No shipped strategy reads it yet: placement is not aware of the requested resources or images, and `scheduler.node_resource_limit` filters on node heartbeats, not on the request. The hint is kept so a resource- or image-aware strategy can land without a wire change.

Logging format defaults to `auto`:

- `auto`: console when stdout looks like an interactive terminal, otherwise JSON
- `console`: force human-readable terminal logs
- `json`: force structured logs for containers and log pipelines

Examples:

```bash
LOG_FORMAT=console make run-scheduler
LOG_FORMAT=json make run-gateway
```

## Deploy with Docker Compose

From **repository root**, start gateway + scheduler + two backend nodes:

```bash
# Run scripts/docker-setup.sh first for host prerequisites.
make deploy-up
```

Optional host-based sandbox data-plane routing:

```bash
SANDBOX_PROXY_DOMAINS=sandbox.example.com \
make deploy-up
```

Repository deployment helpers also accept `SANDBOX_PROXY_DOMAINS=<domain>[,<domain>...]`
and pass it to both gateway and runtime node processes.

Check status / logs / teardown:

```bash
make deploy-ps
make deploy-logs
make deploy-down
```

Container deployments use `deploy/docker/config/default.json`, where scheduler service-discovery and backend node endpoints are set for the Docker network.

The compose stack also wires each runtime node for scheduler heartbeat reporting:

- `AENV_NODE_ID` is set per runtime container (`node-a`, `node-b`).
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true` enables heartbeat reporting.
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` points runtime nodes at `http://scheduler:9090`.
- `SANDBOX_PROXY_DOMAINS`, when set, is passed through as both `GATEWAY_SANDBOX_PROXY_DOMAINS` and `AENV_SANDBOX_PROXY_DOMAINS`.

## Deploy on Kubernetes

From the repository root:

```bash
make k8s-render
make k8s-apply
```

Optional host-based sandbox data-plane routing:

```bash
SANDBOX_PROXY_DOMAINS=sandbox.example.com make k8s-apply
```

The default overlay is `deploy/k8s/overlays/default`.
The make targets materialize a temporary Kustomize build context so Kubernetes runtime nodes always consume the repository's single AgentENV runtime config source: `config/default.toml`.

The DaemonSet injects scheduler-report identity and endpoint wiring for runtime nodes:

- `AENV_NODE_ID` comes from Pod metadata name.
- `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true` enables heartbeat reporting.
- `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` is set to `http://agentenv-scheduler:9090`.
- `AENV_SANDBOX_PROXY_DOMAINS` comes from the shared sandbox proxy ConfigMap.

Shared Kubernetes helpers:

```bash
make k8s-build
make k8s-redeploy
```

For single-machine development, `make k8s-apply-dev` uses the `local-dev`
overlay and mounts the repository `env/` directory directly into the AgentENV
DaemonSet at `/workspace/env`. This avoids copying runtime assets into `/var/lib/agentenv/env`.

For local k3s-style development, use:

```bash
make k8s-load-dev
make k8s-refresh-dev
```

`k8s-load-dev` imports the locally built images into k3s/containerd, while
`k8s-refresh-dev` runs build, load, and rollout restart together.

Deployment model:

- `gateway`: Deployment + ClusterIP Service
- `scheduler`: single-replica Deployment + ClusterIP Service
- `agentenv-node`: privileged DaemonSet with `/dev/kvm` and hostPath `/var/lib/agentenv`
- `agentenv-nodes`: headless Service used by scheduler EndpointSlice discovery

Kubernetes config keys:

- `scheduler.discovery.mode`
- `scheduler.discovery.kubernetes.namespace`
- `scheduler.discovery.kubernetes.service_name`
- `scheduler.discovery.kubernetes.port`
- `scheduler.discovery.kubernetes.scheme` (defaults to `http`)
- `scheduler.discovery.kubernetes.ignore_pod_selector` (optional Kubernetes label selector; matching Pods are excluded from discovery)
- `scheduler.discovery.kubernetes.no_schedule_pod_selector` (optional Kubernetes label selector; matching Pods are kept as lingering/no-schedule nodes)

Kubernetes endpoint address handling:

- Scheduler only accepts EndpointSlice addresses that parse as valid IPs.
- Both IPv4 and IPv6 endpoint addresses are supported.
- IPv6 endpoints are emitted using bracketed host:port form (for example, `http://[2001:db8::10]:8000`).

Operational notes:

- The scheduler uses in-cluster Kubernetes config and watches EndpointSlices plus Pods for service discovery.
- Only serving, non-terminating DaemonSet Pods are schedulable. Use `no_schedule_pod_selector` for drain/no-new-work labels and `ignore_pod_selector` for Pods that should be completely hidden from discovery.
- For the default `memory` binding store, `scheduler` should stay single-replica because sandbox bindings are process-local.
- For high availability, run one primary scheduler with `scheduler.redis_addr` set and multiple query-only scheduler replicas started with `--query-only` against the same Redis. Point gateways at the primary with `gateway.scheduler_addr` and at the query-only service with `gateway.query_only_scheduler_addr`. The primary writes sandbox bindings to Redis while query-only replicas continue to serve data-plane `LookupNode` during primary restarts or upgrades. This HA mode is intentionally data-plane only: requests that proxy to existing sandboxes can keep routing, but control-plane operations that need the primary scheduler, such as creating new sandboxes, scheduling, assignment writes, node listing, node detail resolution, and P2P scheduler APIs, still fail while the primary scheduler is unavailable. Artifact store state is still in-memory and is not covered by this HA mode.
- The gateway is intentionally left as ClusterIP by default; attach an Ingress or LoadBalancer based on your environment.

## gRPC API

Proto contract: api/proto/scheduler.proto

Methods:

- Schedule
- ListNodes
- LookupNode
- RecordAssignment
- Heartbeat
- ListObservedNodes
- ReportSandboxEvent
- GetNode
- UnregisterNode
