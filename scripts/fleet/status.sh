#!/usr/bin/env bash
# Read-only view of one fleet host and of the cluster as the primary scheduler
# sees it through this host's gateway. Needs access to the docker socket and
# read access to the fleet .env (root, in practice).
#
#   sudo scripts/fleet/status.sh <A|B>
set -euo pipefail

FLEET_SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${FLEET_SELF_DIR}/lib.sh"

fleet_role_init "${1:-}"
require_cmd docker
require_cmd jq
require_cmd curl

printf '== %s\n' "${FLEET_SLICE}"
systemctl show "${FLEET_SLICE}" -p ActiveState -p AllowedCPUs -p AllowedMemoryNodes -p MemoryMax -p MemoryHigh -p MemoryCurrent 2>/dev/null || echo "not present"

printf '\n== compose project %s (%s)\n' "${FLEET_PROJECT}" "${FLEET_COMPOSE_FILE}"
if [[ -f "${FLEET_COMPOSE_FILE}" && -f "${FLEET_ENV_FILE}" ]]; then
  docker compose --env-file "${FLEET_ENV_FILE}" -f "${FLEET_COMPOSE_FILE}" ps
else
  echo "not bootstrapped on this host"
  exit 0
fi

set -a
# shellcheck source=/dev/null
source "${FLEET_ENV_FILE}"
set +a

printf '\n== cluster view via gateway :%d (primary scheduler %s)\n' "${FLEET_GW_HTTP_PORT}" "${FLEET_SCHED_A_ADDR}"
body="$(fleet_api "http://127.0.0.1:${FLEET_GW_HTTP_PORT}/nodes" 2>/dev/null || true)"
if ! jq -e 'type == "array"' <<<"${body}" >/dev/null 2>&1; then
  echo "gateway did not answer /nodes: ${body:-no response}"
  exit 1
fi
jq -r --argjson total "${FLEET_NODE_TOTAL}" '
  def by(f): group_by(f) | map({key: (.[0] | f), count: length}) | map("\(.key)=\(.count)") | join(" ");
  "nodes observed: \(length)/\($total)   ready: \([.[] | select(.status == "ready")] | length)/\($total)",
  "status:   \(by(.status))",
  "host:     \(by(.id | sub("^node-(?<h>[ab]).*$"; "\(.h)")))",
  "backend:  \(by(.machineInfo.sandboxBackend))",
  "",
  (.[] | "  \(.id)\t\(.status)\t\(.machineInfo.sandboxBackend)\tsandboxes=\(.sandboxCount)\tinstance=\(.serviceInstanceID)")
' <<<"${body}"
