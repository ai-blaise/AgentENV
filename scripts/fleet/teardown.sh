#!/usr/bin/env bash
# Reverses scripts/fleet/bootstrap.sh on one VM: the compose project and its
# volumes, the aenvfleet.slice unit, and /opt/aenv-fleet. Nothing else on the
# host is touched — not the io_uring sysctl drop-in, not the aenvio group, not
# docker's daemon.json, not any image the bootstrap built or pulled, and never
# a `docker system prune`. Idempotent: running it on a clean host is a no-op.
#
#   sudo scripts/fleet/teardown.sh <A|B>
#   AENV_FLEET_TEARDOWN_IMAGES=1 sudo scripts/fleet/teardown.sh <A|B>   # also drop the fleet-tagged images
set -euo pipefail

FLEET_SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${FLEET_SELF_DIR}/lib.sh"

fleet_role_init "${1:-}"
[[ "${EUID}" -eq 0 ]] || die "run with sudo"
require_cmd docker
require_cmd systemctl

short_host="$(hostname -s)"
if [[ "${short_host}" != "${FLEET_HOSTNAME_EXPECTED}" && -z "${AENV_FLEET_FORCE_HOST:-}" ]]; then
  die "this is ${short_host}; role ${FLEET_ROLE} is ${FLEET_HOSTNAME_EXPECTED} (AENV_FLEET_FORCE_HOST=1 to override)"
fi

# The compose file interpolates \${AENV_API_KEY:?} and friends, so `down`
# needs the .env it was brought up with. Without it, fall back to the project
# label, which is what compose stamps on everything it created.
down_compose() {
  if [[ -f "${FLEET_COMPOSE_FILE}" && -f "${FLEET_ENV_FILE}" ]]; then
    log "docker compose down -v (${FLEET_COMPOSE_FILE})"
    docker compose --env-file "${FLEET_ENV_FILE}" -f "${FLEET_COMPOSE_FILE}" down -v --remove-orphans -t 30 || true
  fi
  local leftovers
  leftovers="$(docker ps -aq --filter "label=com.docker.compose.project=${FLEET_PROJECT}")"
  if [[ -n "${leftovers}" ]]; then
    warn "removing containers still labelled ${FLEET_PROJECT}"
    # shellcheck disable=SC2086
    docker rm -f ${leftovers} >/dev/null
  fi
  leftovers="$(docker volume ls -q --filter "label=com.docker.compose.project=${FLEET_PROJECT}")"
  if [[ -n "${leftovers}" ]]; then
    warn "removing volumes still labelled ${FLEET_PROJECT}"
    # shellcheck disable=SC2086
    docker volume rm ${leftovers} >/dev/null
  fi
  leftovers="$(docker ps -aq --filter 'name=^aenv-smoke-')"
  if [[ -n "${leftovers}" ]]; then
    # shellcheck disable=SC2086
    docker rm -f ${leftovers} >/dev/null
  fi
}

# Stopping the slice ends anything that escaped compose, because every fleet
# process was parented to it. Removing the unit and reloading leaves systemd
# exactly as the bootstrap found it.
remove_slice() {
  if systemctl list-units --all --plain --no-legend "${FLEET_SLICE}" 2>/dev/null | grep -q "${FLEET_SLICE}"; then
    systemctl stop "${FLEET_SLICE}" || true
  fi
  if [[ -f "${FLEET_SLICE_UNIT}" ]]; then
    rm -f "${FLEET_SLICE_UNIT}"
    systemctl daemon-reload
    log "removed ${FLEET_SLICE_UNIT}"
  fi
  systemctl reset-failed "${FLEET_SLICE}" 2>/dev/null || true
  if [[ -d "/sys/fs/cgroup/${FLEET_SLICE}" ]]; then
    warn "/sys/fs/cgroup/${FLEET_SLICE} still exists; processes left in it:"
    cat "/sys/fs/cgroup/${FLEET_SLICE}/cgroup.procs" >&2 || true
  fi
}

remove_images() {
  [[ -n "${AENV_FLEET_TEARDOWN_IMAGES:-}" ]] || return 0
  local name
  for name in agentenv-runtime agentenv-gateway agentenv-scheduler; do
    docker image rm "$(fleet_image_ref "${name}")" >/dev/null 2>&1 && log "removed image $(fleet_image_ref "${name}")" || true
  done
}

# Slot rules are what a unit in the VM profile would have left behind. This
# fleet never creates slots, so any hit here predates it and is reported, not
# deleted: it belongs to whoever ran AgentENV on this host before.
report_foreign_slot_rules() {
  command -v iptables-save >/dev/null 2>&1 || return 0
  local count
  count="$(iptables-save 2>/dev/null | grep -c '10.11.0.0/16' || true)"
  if [[ "${count:-0}" -gt 0 ]]; then
    warn "${count} iptables rule(s) for 10.11.0.0/16 remain on this host; they are not the fleet's and were left in place"
  fi
}

down_compose
remove_slice
remove_images
if [[ -d "${FLEET_ROOT}" ]]; then
  rm -rf "${FLEET_ROOT}"
  log "removed ${FLEET_ROOT}"
fi
report_foreign_slot_rules
log "fleet host ${FLEET_ROLE} torn down; images, the io_uring gate and the ${FLEET_IOURING_GROUP} group are untouched"
