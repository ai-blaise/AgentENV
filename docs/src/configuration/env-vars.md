# Environment Variables

## Deployment Helpers

These variables are consumed by the repository's Docker Compose and Kubernetes helpers, then passed to the server or gateway-specific variables listed below.

| Variable | Default | Description |
|----------|---------|-------------|
| `SANDBOX_PROXY_DOMAINS` | empty | Comma-separated DNS domains for host-based sandbox data-plane URLs. In multi-node deployments this single value is applied to both gateway routing and runtime sandbox response metadata. |

## Server

| Variable | Default | Description |
|----------|---------|-------------|
| `AENV_API_KEY` | generated under `$AENV_HOME/secrets/api-key` | Optional API-key override. Runtime nodes also check `/run/secrets/api-key` before creating a managed key. Use one shared value or secret in multi-node deployments. |
| `API_ADDR` | `0.0.0.0:8000` | Address and port the API server listens on |
| `AENV_CONFIG_PATH` | `config/default.toml` | Path to the TOML configuration file |
| `AENV_LOG_FORMAT` | `compact` | Server log output format: `compact`, `pretty`, or `json` |
| `AENV_LOG_SPAN_EVENTS` | `off` | Tracing span lifecycle events to emit: `off`, `new`, `enter`, `exit`, `close`, `active`, or `full` |
| `AENV_NODE_ID` | hostname-derived | Override the runtime node identifier used in observability/admin snapshots |
| `AENV_CLUSTER_ID` | nil UUID | Override the cluster UUID used for P2P peer discovery and scheduler grouping |
| `AENV_SERVICE_INSTANCE_ID` | random UUIDv7 | Override the per-process service instance UUID included in heartbeats |
| `AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED` | from config | Enable scheduler heartbeat reporting |
| `AENV_OBSERVABILITY_SCHEDULER_ENDPOINT` | unset | Override scheduler heartbeat reporting endpoint |
| `AENV_OBSERVABILITY_REPORT_INTERVAL_SECS` | `5` | Override heartbeat reporting interval in seconds |
| `AENV_CUSTOM_EXTENSION_URL` | unset | Override `[custom_extension].url`, the HTTP base URL of the custom extension service |
| `AENV_SANDBOX_ACCESS_TOKEN_HASH_SEED` | auto-generated under `$AENV_HOME/secrets` | Optional runtime override for the secret used to derive sandbox envd and traffic access tokens. Configure the same value on every runtime node in clustered deployments. |
| `AENV_SANDBOX_PROXY_DOMAINS` | from config | Comma-separated DNS domains that enable server-side host-based sandbox proxy URLs like `{port}-{sandboxID}.{domain}` and populate the sandbox response `domain` field. Empty or unset keeps `[sandbox_proxy].domains`. |
| `AENV_HOME_PATH` | `/var/lib/aenv` | Override the base directory from which AgentENV derives local state, caches, logs, generated configs, and downloaded dependencies. Component-specific path settings remain available as advanced overrides. |
| `AENV_RUNTIME_PATH` | `/run/aenv` | Override the transient runtime directory used for network namespace mount points and the default ublk daemon socket. |
| `AENV_DEPS_PATH` | `$AENV_HOME/deps` | Override root directory for auto-downloaded runtime assets (Firecracker, kernel, tools drive). |
| `AENV_VIRTUALIZATION_MODE` | `kvm` | Select the node virtualization mode. Leave unset for normal installations; set to `pvm` only when following the [PVM Deployment](../deployment/pvm.md) guide. |
| `AENV_SNAPSHOT_LOCAL_CACHE_PATH` | `$AENV_HOME/snapshot-local-cache` | Override the snapshot manager's node-local artifact/cache root |
| `AENV_SNAPSHOT_STORE` | `$AENV_HOME/snapshot-store` | Override the posix_fs snapshot repository root directory |
| `AENV_SNAPSHOT_STORE_LOCK_STRATEGY` | `flock` | How the posix_fs catalog takes its alias and record locks. `flock` is kernel-enforced and released on process death; `create_new` is for filesystems that do not honour `flock` between every writer sharing one repository, and is strictly weaker. |
| `AENV_UBLK_DAEMON_BINARY_PATH` | `$AENV_HOME/ublk/uvm-ublk-daemon` | Override path to the `uvm-ublk-daemon` binary |
| `AENV_UBLK_DAEMON_METRICS_LISTEN_ADDR` | `0.0.0.0:9103` | Override ublk daemon Prometheus metrics listen address; empty string disables it |
| `AENV_FORCE_SYSCTL_TUNING` | unset | Set to `1` to force sysctl tuning in a privileged container with writable host sysctls. Normally skipped automatically inside containers. |
| `AENV_FIRECRACKER_WORK_DIR` | `$AENV_HOME/firecracker-work` | Override the parent directory for per-sandbox Firecracker work directories. |
| `AENV_FIRECRACKER_SERIAL_DIR` | `$AENV_HOME/logs/serial` | Override the directory for persistent Firecracker serial output. Files are grouped under `{serial_dir}/{sandbox_id}/`. |
| `AENV_PERSISTED_SANDBOX_STORE_PATH` | `$AENV_HOME/persisted-sandboxes` | Override the directory where paused sandbox state is persisted across server restarts. |

## E2B SDK / CLI

These variables configure the E2B SDK and CLI to point at an AgentENV server. Values depend on your deployment mode.

| Variable | Description |
|----------|-------------|
| `E2B_API_URL` | AgentENV server API base URL |
| `E2B_SANDBOX_URL` | Sandbox proxy URL (for WebSocket and process interaction) |
| `E2B_API_KEY` | Set to the deployment's `AENV_API_KEY` |

### Values by Deployment Mode

**Manual compile (single node)**:

```bash
export E2B_API_URL=http://127.0.0.1:8000
export E2B_SANDBOX_URL=${E2B_API_URL}
export E2B_API_KEY=${AENV_API_KEY}
```

**Docker Compose / Kubernetes (multi-node)**:

```bash
export E2B_API_URL=http://127.0.0.1:8080
export E2B_SANDBOX_URL=${E2B_API_URL}
export E2B_API_KEY=${AENV_API_KEY}
```

> In both modes, sandbox data-plane requests can use routing headers with
> `E2B_SANDBOX_URL=${E2B_API_URL}`. The explicit `/proxy` prefix
> (`${E2B_API_URL}/proxy`) is still accepted for back-compat.

See [Authentication](../concepts/authentication.md) for key generation and storage.

## Gateway and Scheduler

These variables apply to both the gateway and scheduler processes.

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, or `error` |
| `LOG_FORMAT` | `auto` | Log output format: `auto`, `console`, or `json` |

## Gateway

| Variable | Default | Description |
|----------|---------|-------------|
| `AENV_API_KEY` | unset | Shared single-tenant API key. The gateway uses the environment value when set, otherwise it reads `/run/secrets/api-key`. |
| `GATEWAY_HTTP_LISTEN_ADDR` | `:8080` | HTTP listen address |
| `GATEWAY_METRICS_LISTEN_ADDR` | `:9102` | Prometheus metrics listen address |
| `GATEWAY_SCHEDULER_ADDR` | `127.0.0.1:9090` | Scheduler gRPC address for routing and node lookup. One address or a comma-separated list; RPCs are balanced round-robin over every address the target resolves to, so a headless service name spreads them over the scheduler replicas rather than pinning one. |
| `GATEWAY_QUERY_ONLY_SCHEDULER_ADDR` | unset | Optional secondary scheduler gRPC address used only for sandbox data-plane `LookupNode` queries. When set, creation and control-plane calls still go to `GATEWAY_SCHEDULER_ADDR`. |
| `GATEWAY_REQUEST_TIMEOUT` | `30s` | Override the gateway's HTTP request timeout (for example, `1m30s`) |
| `GATEWAY_SANDBOX_PROXY_DOMAINS` | from config | Comma-separated DNS domains that enable gateway host-based sandbox proxy URLs like `{port}-{sandboxID}.{domain}`. Empty or unset keeps `gateway.sandbox_proxy_domains`. |
| `GATEWAY_DEBUG_MODE` | `false` | Enable gateway debug mode |
| `GATEWAY_BINDING_CACHE_SIZE` | `65536` | Bound on locally cached sandbox-to-node bindings. `0` or negative disables the cache. |
| `GATEWAY_BINDING_CACHE_TTL` | `2s` | How long a resolved binding is reused. Must stay well below `scheduler.binding_ttl`; negative disables the cache. |
| `GATEWAY_BINDING_CACHE_NEGATIVE_TTL` | `200ms` | How long a lookup that found no binding is reused. Must not be negative or exceed `GATEWAY_BINDING_CACHE_TTL`. |
| `GATEWAY_SCHEDULER_AUTH_TOKEN` | unset | Bearer token presented to the scheduler on every RPC as `authorization: Bearer <token>`, on both scheduler addresses. Unset dials without a credential. |

## Scheduler

| Variable | Default | Description |
|----------|---------|-------------|
| `SCHEDULER_GRPC_LISTEN_ADDR` | `:9090` | gRPC listen address |
| `SCHEDULER_METRICS_LISTEN_ADDR` | `:9101` | Prometheus metrics listen address |
| `SCHEDULER_STRATEGY` | `round_robin` | Node selection strategy for new sandboxes: `round_robin`, `random`, `least_loaded_of_two` (alias `p2c`) or `bin_pack`. `bin_pack` fills the fullest eligible node and is only bounded by `scheduler.node_resource_limit`; it concentrates concurrent starts on one node and is the wrong choice for tail latency. |
| `SCHEDULER_AUTH_TOKEN` | unset | Shared bearer token every scheduler gRPC caller must send as `authorization: Bearer <token>` metadata. Unset = every RPC accepted (logged once at startup). Set = every `Scheduler` RPC without a matching token is refused `UNAUTHENTICATED`; `grpc.health.v1.Health` stays open for probes. |
| `SCHEDULER_AUTH_TOKEN_FILE` | unset | Path to a file holding the token instead of `SCHEDULER_AUTH_TOKEN`. Must exist and be non-empty when set; setting both is refused. |
| `SCHEDULER_RESERVATIONS_ENABLED` | `false` | Let node-reported sandbox events and the scheduler's own placements adjust each node's last heartbeat between heartbeats, so placements inside one interval see each other. Advisory; the heartbeat always wins. |
| `SCHEDULER_MAX_RESERVATION_DELTA` | `512` | Clamp on how far the reservation ledger may move one node's sandbox count from its last heartbeat, in either direction. |
| `SCHEDULER_MODE` | `primary` | How this scheduler takes part in a cluster: `primary` (single scheduler, the only mode that may run without `scheduler.redis_addr`), `replica` (one of N behind one address; refuses to start without a shared binding store), or `query-only` (serves `LookupNode` alone). The `--mode` flag wins over this; `--query-only` is a deprecated alias. |
| `SCHEDULER_NODE_STREAM_ENABLED` | on for `replica`, off otherwise | Replicate node liveness and capacity between schedulers over Redis Streams, so each replica knows the nodes whose sticky connections landed on another. `false` is the rollback: each scheduler then knows only the nodes that heartbeat to it. |
| `SCHEDULER_NODE_STREAM_WARMUP_TIMEOUT` | `15s` | How long a starting replica reports `NOT_SERVING` while it replays the retained node state before serving anyway. |
| `SCHEDULER_STORE_PROBE_INTERVAL` | `2s` | How often a scheduler asks its binding store whether it is reachable. Three consecutive failures report `NOT_SERVING`, which is what takes a Redis-partitioned replica out of a gateway's round-robin rotation. |
| `SCHEDULER_REDIS_ADDR` | unset | Redis address for persistent sandbox-to-node bindings (for example, `redis:6379`). Unset = in-memory bindings, lost on scheduler restart. |
| `SCHEDULER_BINDING_TTL` | `30s` | How long a sandbox-to-node binding is kept without a confirming heartbeat. Accepts Go duration strings (for example, `1m`). `scheduler.report_ttl` (config only; unset = the smaller of `30s` and this) must not exceed it. |
| `SCHEDULER_RECONCILE_GRACE` | smaller of `10s` and half `SCHEDULER_BINDING_TTL` | How recently a binding must have been written for a heartbeat reconcile to leave it alone when the node's roster omits it. Must be shorter than the binding TTL. |
| `SCHEDULER_HEARTBEAT_INTERVAL` | unset | The interval nodes are expected to heartbeat at. Set only to have the TTLs and grace validated against it at startup; unset skips those checks. |
| `SCHEDULER_ARTIFACT_STORE_CAPACITY` | `1000000` | Maximum number of P2P artifact entries held in the scheduler's in-memory index |
| `SCHEDULER_ARTIFACT_LOOKUP_NODE_LIMIT` | `8` | Maximum number of nodes named per P2P artifact lookup. `0` means no limit. |
