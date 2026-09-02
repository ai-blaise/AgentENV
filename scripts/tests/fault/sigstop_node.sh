#!/usr/bin/env bash
# Freezes one node past the binding TTL, and checks the fleet's answer.
#
#   scripts/tests/fault/sigstop_node.sh [freeze_seconds]
#
# SIGSTOP is the interesting failure because the node is neither alive nor
# gone: its TCP listener still accepts, its process still holds its port, and
# it answers nothing. A fleet that only handles clean exits mistakes it for a
# healthy node and keeps placing on it.
#
# Four properties are asserted:
#
#   1. The surviving node keeps serving directly.
#   2. Placement moves off the frozen node once its heartbeat has aged past
#      scheduler.report_ttl — that is what scheduler.schedule_health_gate is
#      for, and without it a node dead for hours still takes its share.
#   3. Reconcile deletes nothing for the frozen node. A node that stops
#      reporting says nothing about what it owns; the binding TTL is what reaps
#      its bindings, and an empty or absent roster must never be read as
#      authoritative deletion.
#   4. On SIGCONT the node is observed again and its sandbox is routable again
#      within two heartbeat intervals.
#
# One further check is recorded rather than enforced: `GET /v2/sandboxes` while
# a node is frozen. It is the control-plane scale-out acceptance gate, and it
# fails today — fetchClusterList cancels the whole fan-out on the first node
# error and returns it (services/gateway/internal/cluster_list.go:151-181). Run
# with AENV_FAULT_STRICT_GATES=1 to make it fatal once that is fixed.
set -euo pipefail

FAULT_SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${FAULT_SELF_DIR}/lib.sh"

# Long enough for the frozen node's last heartbeat to age past report_ttl and
# for its bindings to age past binding_ttl, which is what makes properties 2
# and 3 distinguishable from each other.
FREEZE_SECS="${1:-16}"

trap fault_fleet_down EXIT
fault_fleet_up

# ── before the fault ─────────────────────────────────────────────────────────

# Placed directly on node-b so the sandbox under test is certainly the frozen
# node's, whatever the strategy does.
fault_create_sandbox "${FAULT_NODE_B_URL}"
assert_status "${FAULT_STATUS}" "201" "sandbox created on node-b"
assert_not_empty "${FAULT_SANDBOX_ID}" "node-b returned a sandbox id"
SANDBOX_ID="${FAULT_SANDBOX_ID}"

# The binding reaches the scheduler through node-b's heartbeat roster; a
# create placed straight on a node does not go through the gateway's
# RecordAssignment path.
sleep $((FAULT_HEARTBEAT_SECS * 2 + 1))
fault_http GET "${FAULT_GATEWAY_URL}/sandboxes/${SANDBOX_ID}"
assert_status "${FAULT_STATUS}" "200" "node-b's sandbox is routable before the freeze"

DELETES_BEFORE="$(fault_metric_sum "${FAULT_SCHEDULER_METRICS_URL}" \
  agentenv_scheduler_reconcile_bindings_total 'outcome="deleted"' 'node_id="fault-node-b"')"

# ── the fault ────────────────────────────────────────────────────────────────

fault_log "freezing node-b (pid ${FAULT_NODE_B_PID}) for ${FREEZE_SECS}s"
kill -STOP "${FAULT_NODE_B_PID}"
sleep "${FREEZE_SECS}"

# /health is a 204 by contract (src/api/openapi.yml:1298-1304).
fault_http GET "${FAULT_NODE_A_URL}/health"
assert_status "${FAULT_STATUS}" "204" "the surviving node keeps serving while node-b is frozen"

# Every placement must land on node-a now. Ten is enough that a round-robin
# strategy still handing out node-b would be caught rather than skipped over.
LANDED_ON_A=0
CREATE_FAILURES=0
for _ in $(seq 1 10); do
  fault_create_sandbox "${FAULT_GATEWAY_URL}" 60
  if [[ "${FAULT_STATUS}" == "201" ]]; then
    LANDED_ON_A=$((LANDED_ON_A + 1))
  else
    CREATE_FAILURES=$((CREATE_FAILURES + 1))
  fi
done
assert_eq "${CREATE_FAILURES}" "0" \
  "every create during the freeze was placed on a node that could serve it"
assert_eq "${LANDED_ON_A}" "10" "all ten creates succeeded"

DELETES_DURING="$(fault_metric_sum "${FAULT_SCHEDULER_METRICS_URL}" \
  agentenv_scheduler_reconcile_bindings_total 'outcome="deleted"' 'node_id="fault-node-b"')"
assert_eq "${DELETES_DURING}" "${DELETES_BEFORE}" \
  "a node that stopped reporting had no binding reconciled away"

fault_http GET "${FAULT_GATEWAY_URL}/v2/sandboxes"
CLUSTER_LIST_OK=0
[[ "${FAULT_STATUS}" == "200" ]] && CLUSTER_LIST_OK=1
fault_gate "${CLUSTER_LIST_OK}" \
  "GET /v2/sandboxes answers while one node is frozen" \
  "got HTTP ${FAULT_STATUS}; the fan-out cancels on the first node error (cluster_list.go:151-181)"

# ── recovery ─────────────────────────────────────────────────────────────────

fault_log "thawing node-b"
kill -CONT "${FAULT_NODE_B_PID}"
fault_wait_for "${FAULT_NODE_B_URL}/health" 30 "node-b after SIGCONT"
sleep $((FAULT_HEARTBEAT_SECS * 2 + 1))

fault_wait_for_observed_nodes 2 30
fault_http GET "${FAULT_GATEWAY_URL}/sandboxes/${SANDBOX_ID}"
assert_status "${FAULT_STATUS}" "200" \
  "the thawed node's sandbox is routable again within two heartbeat intervals"

DELETES_AFTER="$(fault_metric_sum "${FAULT_SCHEDULER_METRICS_URL}" \
  agentenv_scheduler_reconcile_bindings_total 'outcome="deleted"' 'node_id="fault-node-b"')"
assert_eq "${DELETES_AFTER}" "${DELETES_BEFORE}" \
  "the thawed node's first roster rebuilt its bindings without deleting any"

suite_summary "sigstop_node"
