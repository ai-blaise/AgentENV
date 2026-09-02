#!/usr/bin/env bash
# Fills the filesystem a node keeps its state on, and checks how it fails.
#
#   AENV_FAULT_FILL_DIR=/mnt/aenv-fault-scratch \
#     scripts/tests/fault/fill_disk.sh [free_bytes_target]
#
# This one needs a filesystem of its own and refuses to run without one.
# Filling the root filesystem of a shared host takes down everything else on it
# — the build cache, other agents' work, the host's own logging — and no
# assertion is worth that. The caller provides a dedicated mount; the script
# checks it is one before writing a byte:
#
#   truncate -s 3G /var/tmp/aenv-fault.img
#   mkfs.ext4 -q -F /var/tmp/aenv-fault.img
#   sudo mkdir -p /mnt/aenv-fault-scratch
#   sudo mount -o loop /var/tmp/aenv-fault.img /mnt/aenv-fault-scratch
#   sudo chown "$(id -u):$(id -g)" /mnt/aenv-fault-scratch
#
# The fault is aimed at the operation that has to reach the disk. On a
# mock-backend node a create persists nothing — no rootfs, no memory image, no
# guest — so it is not the interesting request; pause is, because it writes the
# paused sandbox's state under [orchestrator].persisted_sandbox_store_path
# before the node can call the sandbox paused. Four properties:
#
#   1. Nothing hangs. A request that never answers holds the client's
#      connection, the gateway's, and a scheduler binding, and tells the client
#      nothing; it is a far worse failure than a refusal.
#   2. Pause fails, and says why. ENOSPC has to reach the caller as an error
#      naming it, not as a success that lost the state, and not as a generic
#      500 an operator cannot act on.
#   3. The node keeps answering /health, so the fleet's view of it stays true.
#   4. Freeing the space restores both operations with no restart: disk
#      pressure must leave no damage behind.
set -euo pipefail

FAULT_SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${FAULT_SELF_DIR}/lib.sh"

# Bytes to leave free. Zero by default: a node's durable writes are small, and
# leaving even a few megabytes lets every one of them through, which makes the
# fault invisible rather than mild.
FREE_TARGET_BYTES="${1:-0}"

FILL_DIR="${AENV_FAULT_FILL_DIR:-}"
if [[ -z "${FILL_DIR}" ]]; then
  fault_die "set AENV_FAULT_FILL_DIR to a directory on a filesystem this script may fill; the header says how to make one"
fi
[[ -d "${FILL_DIR}" ]] || fault_die "AENV_FAULT_FILL_DIR does not exist: ${FILL_DIR}"
[[ -w "${FILL_DIR}" ]] || fault_die "AENV_FAULT_FILL_DIR is not writable: ${FILL_DIR}"

fault_require df stat

fill_device="$(df -P "${FILL_DIR}" | awk 'NR==2 {print $1}')"
root_device="$(df -P / | awk 'NR==2 {print $1}')"
repo_device="$(df -P "${FAULT_REPO_ROOT}" | awk 'NR==2 {print $1}')"
if [[ "${fill_device}" == "${root_device}" ]]; then
  fault_die "AENV_FAULT_FILL_DIR is on the root filesystem (${fill_device}); refusing to fill it"
fi
if [[ "${fill_device}" == "${repo_device}" ]]; then
  fault_die "AENV_FAULT_FILL_DIR shares a filesystem with the repository (${fill_device}); refusing to fill it"
fi

FILL_FILE="${FILL_DIR}/aenv-fault-fill"
cleanup() {
  rm -f "${FILL_FILE}"
  fault_fleet_down
}
trap cleanup EXIT

free_bytes() {
  local block_size available
  block_size="$(stat -f -c %S "${FILL_DIR}")"
  available="$(stat -f -c %a "${FILL_DIR}")"
  printf '%s' "$((block_size * available))"
}

# The node's state has to live on the filesystem being filled, or the fault is
# injected somewhere the node never writes. fault_fleet_up makes its scratch
# directory under TMPDIR.
mkdir -p "${FILL_DIR}/fleet"
TMPDIR="${FILL_DIR}/fleet" fault_fleet_up

# ── before the fault ─────────────────────────────────────────────────────────

fault_create_sandbox "${FAULT_NODE_A_URL}"
assert_status "${FAULT_STATUS}" "201" "node-a creates a sandbox with space to spare"
SANDBOX_ID="${FAULT_SANDBOX_ID}"
assert_not_empty "${SANDBOX_ID}" "node-a returned a sandbox id"

# ── the fault ────────────────────────────────────────────────────────────────

before="$(free_bytes)"
fault_log "filling ${fill_device}: ${before} bytes free, leaving ${FREE_TARGET_BYTES}"
if ((before <= FREE_TARGET_BYTES)); then
  fault_die "${FILL_DIR} already has only ${before} free bytes; nothing to fill"
fi

fill_size=$((before - FREE_TARGET_BYTES))
if command -v fallocate >/dev/null 2>&1 && fallocate -l "${fill_size}" "${FILL_FILE}" 2>/dev/null; then
  :
else
  # dd stops at ENOSPC, which is the target anyway; its own failure is not one.
  dd if=/dev/zero of="${FILL_FILE}" bs=1M status=none 2>/dev/null || true
fi

after="$(free_bytes)"
fault_log "${after} bytes free"
if ((after > FREE_TARGET_BYTES + 1048576)); then
  fault_die "fill did not take: ${after} bytes still free"
fi

# A create on this backend persists nothing, so it is not expected to fail —
# what it must not do is hang while the filesystem is full.
started="$(date +%s)"
fault_create_sandbox "${FAULT_NODE_A_URL}"
create_status="${FAULT_STATUS}"
assert_not_eq "${create_status}" "000" \
  "a create answered rather than hanging while the filesystem was full"
fault_log "create on a full filesystem answered ${create_status} in $(($(date +%s) - started))s"

# Pause is the operation that has to reach the disk: it writes the paused
# sandbox's state before reporting the sandbox paused.
started="$(date +%s)"
fault_http POST "${FAULT_NODE_A_URL}/sandboxes/${SANDBOX_ID}/pause"
pause_status="${FAULT_STATUS}"
pause_body="${FAULT_BODY}"
assert_not_eq "${pause_status}" "000" \
  "a pause answered rather than hanging while the filesystem was full"
assert_eq "${pause_status}" "500" \
  "a pause that cannot persist its state reports failure instead of success"
assert_contains "${pause_body}" "No space left on device" \
  "the failure names the condition an operator has to fix"
fault_log "pause on a full filesystem answered ${pause_status} in $(($(date +%s) - started))s"

fault_http GET "${FAULT_NODE_A_URL}/health"
assert_status "${FAULT_STATUS}" "204" "the node still answers /health on a full filesystem"

# ── recovery ─────────────────────────────────────────────────────────────────

rm -f "${FILL_FILE}"
fault_log "$(free_bytes) bytes free again"

fault_create_sandbox "${FAULT_NODE_A_URL}"
assert_status "${FAULT_STATUS}" "201" "creates succeed again once there is space, with no restart"
RECOVERED_ID="${FAULT_SANDBOX_ID}"

fault_http POST "${FAULT_NODE_A_URL}/sandboxes/${RECOVERED_ID}/pause"
assert_status "${FAULT_STATUS}" "204" "pause persists again once there is space, with no restart"

suite_summary "fill_disk"
