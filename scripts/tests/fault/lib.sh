#!/usr/bin/env bash
# A single-host mock fleet, and the fault-injection helpers built on it.
#
# The fleet is one scheduler, one gateway and two nodes, all plain processes on
# 127.0.0.1, every node running [machine].backend = "mock". No Docker, no root,
# no /dev/kvm, no second host. What it exercises is exactly what a fault script
# needs: heartbeats, rosters, bindings, placement and the gateway path.
#
# The properties these scripts assert are recovery properties — what the
# control plane is still doing correctly during a fault, and what it has put
# back afterwards. Injecting a fault and observing that something broke proves
# nothing; every check here names the invariant it is protecting.
#
# Timings are deliberately short (2 s heartbeats, a 12 s binding TTL) so a fault
# window and its recovery both fit inside a test run. They satisfy the same
# relations the scheduler validates at startup — binding_ttl >= 3 intervals,
# report_ttl <= binding_ttl, reconcile_grace >= 2 intervals and < binding_ttl —
# so the fleet is a scaled-down deployment rather than a differently-shaped one.

if [[ -n "${AENV_FAULT_LIB_LOADED:-}" ]]; then
  return 0
fi
AENV_FAULT_LIB_LOADED=1

FAULT_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FAULT_REPO_ROOT="$(cd "${FAULT_LIB_DIR}/../../.." && pwd)"

# shellcheck source=../e2e/lib/assertions.sh
source "${FAULT_REPO_ROOT}/scripts/tests/e2e/lib/assertions.sh"

# Ports sit in 19200-19299, clear of the two-VM fleet bootstrap's 19000-19199.
: "${FAULT_SCHEDULER_GRPC_PORT:=19210}"
: "${FAULT_SCHEDULER_METRICS_PORT:=19211}"
: "${FAULT_GATEWAY_PORT:=19212}"
: "${FAULT_GATEWAY_METRICS_PORT:=19213}"
: "${FAULT_NODE_A_PORT:=19214}"
: "${FAULT_NODE_B_PORT:=19215}"

: "${FAULT_HEARTBEAT_SECS:=2}"
: "${FAULT_BINDING_TTL:=12s}"
: "${FAULT_REPORT_TTL:=8s}"
: "${FAULT_RECONCILE_GRACE:=6s}"
# Longer than any fault window a script opens, so a data-plane request during
# the fault is answered from the cache rather than from a scheduler that is not
# there. This is the property partition_scheduler.sh exists to check.
: "${FAULT_BINDING_CACHE_TTL:=30s}"

: "${AENV_API_KEY:=e2b_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef}"
export AENV_API_KEY

# Read by the fault scripts that source this file, not here.
# shellcheck disable=SC2034
FAULT_GATEWAY_URL="http://127.0.0.1:${FAULT_GATEWAY_PORT}"
FAULT_NODE_A_URL="http://127.0.0.1:${FAULT_NODE_A_PORT}"
FAULT_NODE_B_URL="http://127.0.0.1:${FAULT_NODE_B_PORT}"
FAULT_SCHEDULER_METRICS_URL="http://127.0.0.1:${FAULT_SCHEDULER_METRICS_PORT}/metrics"

FAULT_ROOT=""
FAULT_SCHEDULER_PID=""
FAULT_GATEWAY_PID=""
FAULT_NODE_A_PID=""
FAULT_NODE_B_PID=""

fault_log() {
  printf '[fault] %s\n' "$*"
}

fault_die() {
  printf '[fault] %s\n' "$*" >&2
  fault_dump_logs
  exit 1
}

# Tails every process log. A fleet that failed to come up is almost always
# explained in the first lines of one of these, and a script that dies without
# showing them costs a whole extra run to diagnose.
fault_dump_logs() {
  [[ -n "${FAULT_ROOT}" && -d "${FAULT_ROOT}" ]] || return 0
  local log
  for log in "${FAULT_ROOT}"/*.log; do
    [[ -f "${log}" ]] || continue
    printf '\n----- %s (last 25 lines) -----\n' "$(basename "${log}")" >&2
    tail -25 "${log}" >&2
  done
}

fault_require() {
  local missing=()
  local tool
  for tool in "$@"; do
    command -v "$tool" >/dev/null 2>&1 || missing+=("$tool")
  done
  if ((${#missing[@]} > 0)); then
    fault_die "missing required tools: ${missing[*]}"
  fi
}

# ── build ────────────────────────────────────────────────────────────────────

# Builds the three binaries the fleet needs, reusing whatever cargo and go
# already have. AENV_FAULT_SERVER_BIN skips the Rust build entirely, which is
# what a CI job with a prebuilt artifact wants.
fault_build() {
  fault_require cargo go curl python3

  FAULT_BIN_DIR="${FAULT_ROOT}/bin"
  mkdir -p "${FAULT_BIN_DIR}"

  if [[ -n "${AENV_FAULT_SERVER_BIN:-}" ]]; then
    FAULT_SERVER_BIN="${AENV_FAULT_SERVER_BIN}"
    [[ -x "${FAULT_SERVER_BIN}" ]] || fault_die "AENV_FAULT_SERVER_BIN is not executable: ${FAULT_SERVER_BIN}"
  else
    fault_log "building the node binary"
    (cd "${FAULT_REPO_ROOT}" && cargo build --bin server) >&2
    FAULT_SERVER_BIN="${CARGO_TARGET_DIR:-${FAULT_REPO_ROOT}/target}/debug/server"
    [[ -x "${FAULT_SERVER_BIN}" ]] || fault_die "cargo did not produce ${FAULT_SERVER_BIN}"
  fi

  fault_log "building the scheduler and the gateway"
  (cd "${FAULT_REPO_ROOT}/services" &&
    go build -o "${FAULT_BIN_DIR}/scheduler" ./scheduler/cmd &&
    go build -o "${FAULT_BIN_DIR}/gateway" ./gateway/cmd) >&2
}

# ── fleet ────────────────────────────────────────────────────────────────────

fault_render_configs() {
  # The node config is default.toml with the two edits a mock node on a shared
  # host needs: the mock backend, and no network-slot maintenance. Slot refill
  # writes host veths and iptables, which two nodes in one network namespace
  # would collide on and which carry nothing without a guest.
  cp "${FAULT_REPO_ROOT}/config/default.toml" "${FAULT_ROOT}/node.toml"
  python3 - "${FAULT_ROOT}/node.toml" <<'PY'
import sys

path = sys.argv[1]
text = open(path).read()

text = text.replace('backend = "firecracker"', 'backend = "mock"', 1)

head = text.index("[pool.network]")
key = text.index("maintenance_enabled = true", head)
text = text[:key] + "maintenance_enabled = false" + text[key + len("maintenance_enabled = true"):]

open(path, "w").write(text)
PY

  python3 - "${FAULT_ROOT}/services.json" <<PY
import json, sys

config = {
    "log_level": "info",
    "log_format": "json",
    "scheduler": {
        "grpc_listen_addr": "127.0.0.1:${FAULT_SCHEDULER_GRPC_PORT}",
        "metrics_listen_addr": "127.0.0.1:${FAULT_SCHEDULER_METRICS_PORT}",
        "strategy": "round_robin",
        "report_ttl": "${FAULT_REPORT_TTL}",
        "binding_ttl": "${FAULT_BINDING_TTL}",
        "reconcile_grace": "${FAULT_RECONCILE_GRACE}",
        "heartbeat_interval": "${FAULT_HEARTBEAT_SECS}s",
        "redis_addr": "",
        "nodes": [
            {"id": "fault-node-a", "endpoint": "${FAULT_NODE_A_URL}"},
            {"id": "fault-node-b", "endpoint": "${FAULT_NODE_B_URL}"},
        ],
    },
    "gateway": {
        "http_listen_addr": "127.0.0.1:${FAULT_GATEWAY_PORT}",
        "metrics_listen_addr": "127.0.0.1:${FAULT_GATEWAY_METRICS_PORT}",
        "scheduler_addr": "127.0.0.1:${FAULT_SCHEDULER_GRPC_PORT}",
        "request_timeout": "20s",
        "forward_response_size": 4194304,
        "binding_cache_size": 4096,
        "binding_cache_ttl": "${FAULT_BINDING_CACHE_TTL}",
        "sandbox_proxy_domains": [],
    },
}
json.dump(config, open(sys.argv[1], "w"), indent=2)
PY
}

# A time-ordered identifier for one run of a node process. The scheduler orders
# writes by comparing these lexicographically, so a random v4 would be fine
# within one run and wrong across a restart.
_fault_incarnation() {
  python3 -c '
import os, time
ms = int(time.time() * 1000) & ((1 << 48) - 1)
rand = int.from_bytes(os.urandom(10), "big")
value = (ms << 80) | (0x7 << 76) | ((rand >> 4) & ((1 << 74) - 1)) | (0b10 << 62)
hexed = f"{value:032x}"
print(f"{hexed[:8]}-{hexed[8:12]}-{hexed[12:16]}-{hexed[16:20]}-{hexed[20:]}")
'
}

# Starts one node and leaves its pid in FAULT_NODE_PID.
#
# The pid is returned through a global rather than on stdout: a command
# substitution would put the background job in a subshell, and the fleet
# teardown could then neither wait on it nor be sure it had reaped it.
_fault_start_node() {
  local name="$1" port="$2"
  local home="${FAULT_ROOT}/${name}/home"
  local runtime="${FAULT_ROOT}/${name}/run"
  local incarnation
  mkdir -p "${home}" "${runtime}"
  incarnation="$(_fault_incarnation)"

  env \
    AENV_CONFIG_PATH="${FAULT_ROOT}/node.toml" \
    AENV_HOME_PATH="${home}" \
    AENV_RUNTIME_PATH="${runtime}" \
    AENV_SANDBOX_BACKEND=mock \
    AENV_NODE_ID="fault-${name}" \
    AENV_SERVICE_INSTANCE_ID="${incarnation}" \
    AENV_CLUSTER_ID="00000000-0000-0000-0000-0000000000fa" \
    AENV_OBSERVABILITY_SCHEDULER_REPORT_ENABLED=true \
    AENV_OBSERVABILITY_SCHEDULER_ENDPOINT="http://127.0.0.1:${FAULT_SCHEDULER_GRPC_PORT}" \
    AENV_OBSERVABILITY_REPORT_INTERVAL_SECS="${FAULT_HEARTBEAT_SECS}" \
    API_ADDR="127.0.0.1:${port}" \
    RUST_LOG="agentenv=info" \
    "${FAULT_SERVER_BIN}" >"${FAULT_ROOT}/${name}.log" 2>&1 &
  FAULT_NODE_PID=$!
}

# Brings the whole fleet up and blocks until every part answers.
fault_fleet_up() {
  FAULT_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/aenv-fault.XXXXXX")"
  fault_build
  fault_render_configs

  fault_log "starting the scheduler"
  "${FAULT_BIN_DIR}/scheduler" -config "${FAULT_ROOT}/services.json" \
    >"${FAULT_ROOT}/scheduler.log" 2>&1 &
  FAULT_SCHEDULER_PID=$!
  fault_wait_for "${FAULT_SCHEDULER_METRICS_URL}" 30 "scheduler metrics"

  fault_log "starting node-a and node-b"
  _fault_start_node node-a "${FAULT_NODE_A_PORT}"
  FAULT_NODE_A_PID="${FAULT_NODE_PID}"
  _fault_start_node node-b "${FAULT_NODE_B_PORT}"
  FAULT_NODE_B_PID="${FAULT_NODE_PID}"
  fault_wait_for "${FAULT_NODE_A_URL}/health" 90 "node-a"
  fault_wait_for "${FAULT_NODE_B_URL}/health" 90 "node-b"

  fault_log "starting the gateway"
  "${FAULT_BIN_DIR}/gateway" -config "${FAULT_ROOT}/services.json" \
    >"${FAULT_ROOT}/gateway.log" 2>&1 &
  FAULT_GATEWAY_PID=$!
  fault_wait_for "http://127.0.0.1:${FAULT_GATEWAY_METRICS_PORT}/metrics" 30 "gateway metrics"

  # Both nodes must be observed before any placement, or a round-robin fleet is
  # a one-node fleet and every assertion about the other node is vacuous.
  fault_wait_for_observed_nodes 2 30
  fault_log "fleet up (state under ${FAULT_ROOT})"
}

fault_fleet_down() {
  local pid
  for pid in "${FAULT_GATEWAY_PID}" "${FAULT_NODE_A_PID}" "${FAULT_NODE_B_PID}" "${FAULT_SCHEDULER_PID}"; do
    [[ -n "${pid}" ]] || continue
    # A stopped process ignores SIGTERM until it is continued; a script that
    # failed mid-fault would otherwise leave a node running forever.
    kill -CONT "${pid}" 2>/dev/null || true
    kill "${pid}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  if [[ -n "${FAULT_ROOT}" && "${AENV_FAULT_KEEP_STATE:-0}" != "1" ]]; then
    rm -rf "${FAULT_ROOT}"
  fi
}

fault_wait_for() {
  local url="$1" timeout="$2" what="$3"
  local i
  for ((i = 0; i < timeout * 4; i++)); do
    if curl -sf -m 2 -o /dev/null "${url}"; then
      return 0
    fi
    sleep 0.25
  done
  fault_die "${what} never became reachable at ${url}"
}

# ── HTTP ─────────────────────────────────────────────────────────────────────

FAULT_BODY_FILE=""
FAULT_STATUS=""
FAULT_BODY=""
FAULT_SANDBOX_ID=""

# Results land in globals rather than on stdout, because a caller writing
# `id=$(fault_create_sandbox)` would run the request in a subshell and throw
# the status away with it — the whole point of these scripts is which status
# came back.
fault_http() {
  if [[ -z "${FAULT_BODY_FILE}" ]]; then
    FAULT_BODY_FILE="$(mktemp "${TMPDIR:-/tmp}/aenv-fault-body.XXXXXX")"
  fi
  local method="$1" url="$2"
  shift 2
  FAULT_STATUS="$(curl -sS -m 25 -X "${method}" \
    -H "X-API-Key: ${AENV_API_KEY}" \
    -H 'Content-Type: application/json' \
    -o "${FAULT_BODY_FILE}" -w '%{http_code}' \
    "$@" "${url}" 2>/dev/null || printf '000')"
  FAULT_BODY="$(cat "${FAULT_BODY_FILE}")"
}

# Creates one sandbox, leaving the status in FAULT_STATUS and the id in
# FAULT_SANDBOX_ID (empty when the create did not return 201).
#
# Cold create rather than POST /sandboxes: a template id has to resolve to a
# committed snapshot, and a mock node's repository is empty. The cold path
# takes an image reference, which the mock image resolver answers with an empty
# placeholder. It is the same orchestrator create and the same gateway binding
# path (services/gateway/internal/server.go:873).
fault_create_sandbox() {
  local base="$1" timeout_secs="${2:-120}"
  FAULT_SANDBOX_ID=""
  fault_http POST "${base}/sandboxes-cold" \
    -d "{\"image\":\"${AENV_FAULT_IMAGE:-ubuntu:24.04}\",\"timeout\":${timeout_secs},\"autoPause\":false}"
  if [[ "${FAULT_STATUS}" == "201" ]]; then
    # shellcheck disable=SC2034  # read by the sourcing script
    FAULT_SANDBOX_ID="$(printf '%s' "${FAULT_BODY}" |
      python3 -c 'import json,sys; print(json.load(sys.stdin)["sandboxID"])')"
  fi
}

# ── metrics ──────────────────────────────────────────────────────────────────

# Sums a Prometheus counter or gauge across every series whose label set
# contains each `key="value"` given. Prints 0 when the family is absent, which
# is what a counter that has never been incremented looks like.
fault_metric_sum() {
  local url="$1" metric="$2"
  shift 2
  curl -sf -m 5 "${url}" 2>/dev/null | python3 -c '
import sys

metric = sys.argv[1]
selectors = sys.argv[2:]
total = 0.0
for line in sys.stdin:
    line = line.strip()
    if not line or line.startswith("#") or not line.startswith(metric):
        continue
    rest = line[len(metric):]
    if rest[:1] not in ("{", " "):
        continue
    if any(selector not in line for selector in selectors):
        continue
    try:
        total += float(line.rsplit(" ", 1)[1])
    except (IndexError, ValueError):
        continue
print(int(total) if total.is_integer() else total)
' "${metric}" "$@"
}

# How many nodes the gateway's aggregated view reports as ready, left in
# FAULT_READY_NODES.
#
# Read through the gateway rather than off the scheduler's gauge: the gauge is
# refreshed on its own interval, so a fleet that is up can still read zero for
# a moment, and the aggregated view is what a client actually sees.
fault_ready_nodes() {
  FAULT_READY_NODES=0
  fault_http GET "${FAULT_GATEWAY_URL}/nodes"
  [[ "${FAULT_STATUS}" == "200" ]] || return 0
  FAULT_READY_NODES="$(printf '%s' "${FAULT_BODY}" | python3 -c '
import json, sys

try:
    nodes = json.load(sys.stdin)
except ValueError:
    print(0)
    sys.exit()
print(sum(1 for node in nodes if node.get("status") == "ready"))
')"
}

fault_wait_for_observed_nodes() {
  local want="$1" timeout="$2"
  local i
  for ((i = 0; i < timeout * 4; i++)); do
    fault_ready_nodes
    if [[ "${FAULT_READY_NODES:-0}" -ge "${want}" ]]; then
      return 0
    fi
    sleep 0.25
  done
  fault_die "the gateway never reported ${want} ready nodes (saw ${FAULT_READY_NODES:-0}; last body: ${FAULT_BODY})"
}

# ── acceptance gates ─────────────────────────────────────────────────────────

# Reports a check that belongs to another workstream's acceptance criteria.
#
# These are recorded, not enforced: a fault script whose own recovery
# assertions pass must not fail because a capability it depends on has not
# landed yet. AENV_FAULT_STRICT_GATES=1 makes them fatal, which is how the
# owning workstream turns one into a gate once it believes it passes.
fault_gate() {
  local ok="$1" name="$2" detail="${3:-}"
  if [[ "${ok}" == "1" ]]; then
    printf '  [GATE PASS] %s\n' "${name}"
    return 0
  fi
  printf '  [GATE OPEN] %s%s\n' "${name}" "${detail:+ — ${detail}}" >&2
  if [[ "${AENV_FAULT_STRICT_GATES:-0}" == "1" ]]; then
    _fail "${name}" "gate satisfied" "gate open"
  fi
}
