#!/usr/bin/env bash
# Pins what makes more than one scheduler replica correct.
#
# Every piece is already implemented -- the stream-fed registry, the Redis
# binding store, the gateway's round_robin config -- so the failure this guards
# against is not a missing feature. It is a manifest that scales the tier while
# one of those pieces is switched off, which does not fail: a second replica
# comes up healthy, schedules against its own partial view of the fleet, and
# the damage is bad placement rather than an error.
#
# No cluster, no cargo. Runs in the lane `make test-unit` already uses.
set -euo pipefail

repo_root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
base="${repo_root}/deploy/k8s/base"
deployment="${base}/scheduler-deployment.yaml"
headless="${base}/scheduler-headless-service.yaml"
scheduler_config="${base}/config/scheduler.json"
gateway_config="${base}/config/gateway.json"
kustomization="${base}/kustomization.yaml"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

for file in "$deployment" "$scheduler_config" "$gateway_config" "$kustomization"; do
  [ -f "$file" ] || fail "missing ${file#"$repo_root"/}"
done

replicas="$(sed -n 's/^[[:space:]]*replicas:[[:space:]]*\([0-9]\{1,\}\).*/\1/p' "$deployment" | head -1)"
[ -n "$replicas" ] || fail "scheduler-deployment.yaml declares no replica count"

if [ "$replicas" -le 1 ]; then
  echo "verify-scheduler-replicas: OK (single replica; shared state not required)"
  exit 0
fi

# Node capacity and liveness reach a replica over the Redis node stream. A
# replica without it hears only from the nodes whose heartbeat RPC it happened
# to take, and schedules as though the rest of the fleet did not exist.
python3 - "$scheduler_config" "$replicas" <<'PY' || exit 1
import json, sys
config_path, replicas = sys.argv[1], sys.argv[2]
scheduler = json.load(open(config_path)).get("scheduler", {})
problems = []
if not scheduler.get("redis_addr"):
    problems.append("scheduler.redis_addr is unset, so bindings and paused-sandbox "
                    "ownership stay per-replica and in memory")
if scheduler.get("node_stream_enabled") is not True:
    problems.append("scheduler.node_stream_enabled is not true, so node capacity and "
                    "liveness are not replicated between replicas")
if problems:
    print(f"FAIL: {replicas} scheduler replicas are configured, but:", file=sys.stderr)
    for problem in problems:
        print(f"  - {problem}", file=sys.stderr)
    sys.exit(1)
PY

# A redis_addr naming a Service that base does not ship is a scheduler that
# comes up pointing at nothing.
redis_host="$(python3 -c 'import json,sys; a=json.load(open(sys.argv[1]))["scheduler"]["redis_addr"]; print(a.rsplit(":",1)[0].split(".")[0])' "$scheduler_config")"
grep -rqE "^[[:space:]]*name:[[:space:]]*${redis_host}[[:space:]]*$" "$base" --include='*.yaml' \
  || fail "scheduler.redis_addr names ${redis_host}, which no manifest in deploy/k8s/base declares"
grep -qE '^[[:space:]]*-[[:space:]]*redis\.yaml[[:space:]]*$' "$kustomization" \
  || fail "redis.yaml is not in kustomization.yaml, so the store the replicas share never gets applied"

grep -qE '^[[:space:]]*-[[:space:]]*replica[[:space:]]*$' "$deployment" \
  || fail "the deployment must pass -mode replica; without it node_stream_enabled's default is off"

# round_robin balances over whatever the resolver names, and a ClusterIP service
# names exactly one address. Dialing it pins every gateway to one pod, so the
# tier scales and no traffic moves.
[ -f "$headless" ] || fail "scheduler-headless-service.yaml is missing, so there is nothing to balance over"
grep -qE '^[[:space:]]*clusterIP:[[:space:]]*None[[:space:]]*$' "$headless" \
  || fail "scheduler-headless-service.yaml is not headless (clusterIP: None)"
grep -qE '^[[:space:]]*-[[:space:]]*scheduler-headless-service\.yaml[[:space:]]*$' "$kustomization" \
  || fail "scheduler-headless-service.yaml is not in kustomization.yaml, so it never gets applied"

headless_name="$(sed -n 's/^[[:space:]]*name:[[:space:]]*\([a-z0-9-]\{1,\}\).*/\1/p' "$headless" | head -1)"
[ -n "$headless_name" ] || fail "scheduler-headless-service.yaml declares no name"

python3 - "$gateway_config" "$headless_name" <<'PY' || exit 1
import json, sys
config_path, headless_name = sys.argv[1], sys.argv[2]
addr = json.load(open(config_path)).get("gateway", {}).get("scheduler_addr", "")
host = addr.rsplit(":", 1)[0] if ":" in addr else addr
if host.split(".")[0] != headless_name:
    print(f"FAIL: gateway.scheduler_addr is {addr!r}, which does not name the headless "
          f"service {headless_name!r}; round_robin over a ClusterIP VIP balances over "
          f"one address and pins every gateway to one replica", file=sys.stderr)
    sys.exit(1)
PY

echo "verify-scheduler-replicas: OK (${replicas} replicas, shared state and headless LB in place)"
