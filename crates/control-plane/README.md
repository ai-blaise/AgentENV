# AgentENV Rust control plane

`agentenv-control-plane` is the production replacement for the legacy Go
scheduler. It implements the existing `scheduler.v1.Scheduler` gRPC contract
while adding the invariants needed for large microVM fleets:

- scheduling requires a fresh, ready heartbeat; missing metrics fail closed;
- draining, version-incompatible, commit-incompatible, architecture-incompatible,
  and post-admission-over-capacity nodes are excluded;
- each request probes a bounded number of nodes and selects the least saturated
  member of a power-of-N sample;
- create requests carry a stable UUIDv7 from the gateway;
- Redis Lua scripts atomically claim that UUID and reserve node capacity across
  scheduler replicas;
- assignment confirmation releases pending capacity and extends routing state;
- route generations are explicit in the wire contract;
- Redis TLS and control-plane mTLS are supported; plaintext and in-memory modes
  require explicit unsafe flags.

Run `cargo run -p agentenv-control-plane -- --help` for the complete option set.
A minimal single-process development invocation is:

```bash
cargo run -p agentenv-control-plane -- \
  --cluster-id local \
  --node local-node=http://127.0.0.1:8000 \
  --allow-ephemeral-state \
  --allow-insecure-transport
```

Production deployments should provide `AGENTENV_REDIS_URL` using `rediss://`,
configure the server certificate/private key/client CA, set explicit capacity
ceilings, and run more than one replica. The Redis key hash tag is the cluster
ID, so one AgentENV cell is atomic within one Redis Cluster slot. Partition very
large fleets into independent cells rather than placing an unbounded global
fleet behind a single Redis slot.

Safe rolling upgrade order:

1. deploy envd nodes that accept `x-agentenv-sandbox-id` on create;
2. deploy the gateway/protobuf update that generates and forwards stable IDs;
3. deploy the Rust control plane and point gateway scheduler clients at it;
4. retire the legacy Go scheduler only after route lookups are healthy.

The stable create header is deliberately forwarded to envd, where a canonical
request fingerprint prevents the same UUID from being reused with a different
body. Redis chooses one node; envd ensures one launch on that node.
