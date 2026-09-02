#!/usr/bin/env bash
# Takes the scheduler away from the gateway, and checks what survives.
#
#   scripts/tests/fault/partition_scheduler.sh [outage_seconds]
#
# The fault is the scheduler's listener disappearing. On a two-host Docker
# fleet that is `docker network disconnect`; here it is the scheduler process
# leaving its port and coming back on it, which puts the same thing on the wire
# the gateway cares about — a refused connection, surfaced as
# codes.Unavailable. That distinction is load-bearing: a *hung* scheduler
# yields DeadlineExceeded, which the gateway maps to 502 and a client reads as
# "the fleet is broken", while a refused one maps to 503, which a client reads
# as "retry" (services/gateway/internal/server.go:554-566).
#
# Three properties are asserted, all about what the control plane keeps doing:
#
#   1. A sandbox bound before the outage stays routable through the gateway
#      during it. The binding cache is what makes the data plane independent of
#      the scheduler's availability.
#   2. A create during the outage is refused with 503, not 502 and not a hang.
#      The gateway must not place blind, and must tell the client it is worth
#      retrying.
#   3. Nothing is deleted. Neither the scheduler that saw the outage begin nor
#      the one that came back may reconcile a binding away: an outage is not
#      evidence that a sandbox is gone.
set -euo pipefail

FAULT_SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${FAULT_SELF_DIR}/lib.sh"

OUTAGE_SECS="${1:-6}"

trap fault_fleet_down EXIT
fault_fleet_up

# ── before the fault ─────────────────────────────────────────────────────────

fault_create_sandbox "${FAULT_GATEWAY_URL}"
assert_status "${FAULT_STATUS}" "201" "sandbox created through the gateway"
assert_not_empty "${FAULT_SANDBOX_ID}" "gateway returned a sandbox id"
SANDBOX_ID="${FAULT_SANDBOX_ID}"

fault_http GET "${FAULT_GATEWAY_URL}/sandboxes/${SANDBOX_ID}"
assert_status "${FAULT_STATUS}" "200" "sandbox is routable before the outage"

DELETES_BEFORE="$(fault_metric_sum "${FAULT_SCHEDULER_METRICS_URL}" \
  agentenv_scheduler_reconcile_bindings_total 'outcome="deleted"')"
assert_eq "${DELETES_BEFORE}" "0" "no binding had been reconciled away before the fault"

# ── the fault ────────────────────────────────────────────────────────────────

fault_log "taking the scheduler off ${FAULT_SCHEDULER_GRPC_PORT} for ${OUTAGE_SECS}s"
kill "${FAULT_SCHEDULER_PID}"
wait "${FAULT_SCHEDULER_PID}" 2>/dev/null || true
FAULT_SCHEDULER_PID=""

# Give the gateway's pooled connection a moment to notice; until it does, a
# request would be answered over a socket that is already gone.
sleep 1

fault_http GET "${FAULT_GATEWAY_URL}/sandboxes/${SANDBOX_ID}"
assert_status "${FAULT_STATUS}" "200" \
  "an already-bound sandbox stays routable while the scheduler is unreachable"

fault_create_sandbox "${FAULT_GATEWAY_URL}"
assert_status "${FAULT_STATUS}" "503" \
  "a create is refused as retryable while the scheduler is unreachable"

sleep "${OUTAGE_SECS}"

# ── recovery ─────────────────────────────────────────────────────────────────

fault_log "restoring the scheduler"
"${FAULT_BIN_DIR}/scheduler" -config "${FAULT_ROOT}/services.json" \
  >>"${FAULT_ROOT}/scheduler.log" 2>&1 &
FAULT_SCHEDULER_PID=$!
fault_wait_for "${FAULT_SCHEDULER_METRICS_URL}" 30 "scheduler metrics"

# A restarted scheduler starts with no bindings; the nodes' next heartbeat
# rosters are what put them back. Two intervals is one for a roster to be
# collected and one for it to arrive.
sleep $((FAULT_HEARTBEAT_SECS * 2 + 1))

fault_http GET "${FAULT_GATEWAY_URL}/sandboxes/${SANDBOX_ID}"
assert_status "${FAULT_STATUS}" "200" \
  "the sandbox is routable again once heartbeats rebuild the binding"

fault_create_sandbox "${FAULT_GATEWAY_URL}"
assert_status "${FAULT_STATUS}" "201" "creates are accepted again after recovery"
assert_not_empty "${FAULT_SANDBOX_ID}" "the post-recovery create returned a sandbox id"

# Read from the restarted process, whose counters start at zero: a roster that
# rebuilds a binding must add, never delete.
DELETES_AFTER="$(fault_metric_sum "${FAULT_SCHEDULER_METRICS_URL}" \
  agentenv_scheduler_reconcile_bindings_total 'outcome="deleted"')"
assert_eq "${DELETES_AFTER}" "0" \
  "the recovered scheduler rebuilt bindings without deleting any"

suite_summary "partition_scheduler"
