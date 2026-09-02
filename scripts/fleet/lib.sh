#!/usr/bin/env bash
# shellcheck disable=SC2034  # every value here is consumed by the scripts that source this file
# Shared by scripts/fleet/*.sh: the carve tables, the port map, the host-role
# guards and the render helpers. Sourced, never executed.
#
# The numbers here are measured facts from the fleet design, not tuning knobs.
# Host A carries a live GPU job whose GPUs sit on NUMA0 with CPU affinity
# 0-55,112-167, plus the owner's k3s control plane; its carve is the NUMA1 range
# no running workload is pinned to. Host B is the rgate build host and must keep
# 160 threads free for CARGO_BUILD_JOBS=48, so its carve is also NUMA1 only.
# Every fleet listener lives in 19000-19199, swept free on both hosts.

if [[ -n "${FLEET_LIB_LOADED:-}" ]]; then return 0; fi
FLEET_LIB_LOADED=1

FLEET_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLEET_REPO_ROOT="$(cd "${FLEET_SCRIPT_DIR}/../.." && pwd)"

# shellcheck source=../lib/common.sh
source "${FLEET_REPO_ROOT}/scripts/lib/common.sh"

FLEET_ROOT="${AENV_FLEET_ROOT:-/opt/aenv-fleet}"
FLEET_COMPOSE_DIR="${FLEET_ROOT}/compose"
FLEET_CONFIG_DIR="${FLEET_ROOT}/config"
# Compose loads `.env` from the directory of the first compose file, so an
# operator running `docker compose -f A.yml ps` by hand gets the same
# interpolation the bootstrap did, and `up` can never silently reset a secret.
FLEET_ENV_FILE="${FLEET_COMPOSE_DIR}/.env"
FLEET_PROJECT=aenv-fleet
FLEET_SLICE=aenvfleet.slice
FLEET_SLICE_UNIT="/etc/systemd/system/${FLEET_SLICE}"
FLEET_IOURING_GROUP=aenvio
FLEET_IMAGE_TAG="${AENV_FLEET_IMAGE_TAG:-fleet}"
FLEET_REGISTRY="${AENV_FLEET_REGISTRY:-}"
FLEET_SERVER_BIN="${AENV_FLEET_SERVER_BIN:-}"
FLEET_REDIS_MODE="${AENV_FLEET_REDIS_MODE:-single}"
FLEET_REDIS_CLUSTER_MASTERS=3
FLEET_SCHED_REPLICAS_B="${AENV_FLEET_SCHEDULER_REPLICAS_B:-1}"
FLEET_REDIS_IMAGE="${AENV_FLEET_REDIS_IMAGE:-redis:7.4}"
FLEET_MINIO_IMAGE="${AENV_FLEET_MINIO_IMAGE:-quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z}"
FLEET_BIN_BASE_IMAGE="${AENV_FLEET_BIN_BASE_IMAGE:-ubuntu:24.04}"

FLEET_IP_A="${AENV_FLEET_IP_A:-10.240.0.2}"
FLEET_IP_B="${AENV_FLEET_IP_B:-10.240.0.3}"
FLEET_HOSTNAME_A=instance-20260415-161450
FLEET_HOSTNAME_B=instance-20260415-20260415-235136
FLEET_PRIMARY_NETWORK=a3u-gvnic-asia-south1-0
FLEET_PRIMARY_SUBNET=10.240.0.0/20
FLEET_IAP_RANGE=35.235.240.0/20

FLEET_PORT_MIN=19000
FLEET_PORT_MAX=19199
FLEET_MINIO_API_PORT=19000
FLEET_MINIO_CONSOLE_PORT=19009
FLEET_GW_HTTP_PORT_A=19001
FLEET_GW_HTTP_PORT_B=19002
FLEET_GW_METRICS_PORT_A=19003
FLEET_GW_METRICS_PORT_B=19004
FLEET_NODE_PORT_BASE_A=19010
FLEET_NODE_PORT_BASE_B=19020
FLEET_REDIS_PORT_BASE=19079
FLEET_REDIS_BUS_PORT_BASE=19179
FLEET_SCHED_GRPC_PORT_A=19091
FLEET_SCHED_METRICS_PORT_A=19101
FLEET_SCHED_GRPC_PORT_BASE_B=19092
FLEET_SCHED_METRICS_PORT_BASE_B=19102
FLEET_P2P_PORT_BASE_A=19110
FLEET_P2P_PORT_BASE_B=19120
FLEET_SMOKE_PORT_BASE=19140

FLEET_NODE_COUNT_A=4
FLEET_NODE_COUNT_B=12
FLEET_NODE_MEMORY=24g
FLEET_NODE_TOTAL=$((FLEET_NODE_COUNT_A + FLEET_NODE_COUNT_B))

fleet_role_init() {
  case "${1:-}" in
    A | a)
      FLEET_ROLE=A
      FLEET_HOST_IP="${FLEET_IP_A}"
      FLEET_PEER_IP="${FLEET_IP_B}"
      FLEET_HOSTNAME_EXPECTED="${FLEET_HOSTNAME_A}"
      # 16 physical cores / 32 threads on NUMA1, away from GPU0's affinity mask.
      FLEET_CPUSET="96-111,208-223"
      FLEET_MEMORY_MAX=128G
      FLEET_MEMORY_HIGH=96G
      FLEET_CPU_QUOTA=3200%
      # No AllowedMemoryNodes on A: pinning to NUMA1 under a hard MemoryMax
      # would force reclaim of the owner's NUMA1 page cache.
      FLEET_MEMORY_NODES=""
      FLEET_NODE_COUNT="${FLEET_NODE_COUNT_A}"
      FLEET_NODE_PORT_BASE="${FLEET_NODE_PORT_BASE_A}"
      FLEET_P2P_PORT_BASE="${FLEET_P2P_PORT_BASE_A}"
      FLEET_NODE_CPU_BASE=104
      FLEET_NODE_HT_BASE=216
      FLEET_GW_HTTP_PORT="${FLEET_GW_HTTP_PORT_A}"
      FLEET_GW_METRICS_PORT="${FLEET_GW_METRICS_PORT_A}"
      FLEET_GW_CPUSET="96-97,208-209"
      FLEET_SCHED_CPUSET="98-99,210-211"
      FLEET_SMOKE_INDEX_BASE=0
      ;;
    B | b)
      FLEET_ROLE=B
      FLEET_HOST_IP="${FLEET_IP_B}"
      FLEET_PEER_IP="${FLEET_IP_A}"
      FLEET_HOSTNAME_EXPECTED="${FLEET_HOSTNAME_B}"
      # 32 physical cores / 64 threads on NUMA1; 0-79,112-191 stay free for
      # rgate's 48-job cargo builds.
      FLEET_CPUSET="80-111,192-223"
      FLEET_MEMORY_MAX=512G
      FLEET_MEMORY_HIGH=384G
      FLEET_CPU_QUOTA=6400%
      # B has 2 TB genuinely free, so NUMA1-local memory is safe and keeps the
      # fleet's pages beside its cores.
      FLEET_MEMORY_NODES=1
      FLEET_NODE_COUNT="${FLEET_NODE_COUNT_B}"
      FLEET_NODE_PORT_BASE="${FLEET_NODE_PORT_BASE_B}"
      FLEET_P2P_PORT_BASE="${FLEET_P2P_PORT_BASE_B}"
      FLEET_NODE_CPU_BASE=84
      FLEET_NODE_HT_BASE=196
      FLEET_GW_HTTP_PORT="${FLEET_GW_HTTP_PORT_B}"
      FLEET_GW_METRICS_PORT="${FLEET_GW_METRICS_PORT_B}"
      FLEET_GW_CPUSET="80-81,192-193"
      FLEET_SCHED_CPUSET="82-83,194-195"
      FLEET_SMOKE_INDEX_BASE="${FLEET_NODE_COUNT_A}"
      ;;
    *)
      die "usage: $(basename "$0") <A|B> [options]  (A=${FLEET_HOSTNAME_A}, B=${FLEET_HOSTNAME_B})"
      ;;
  esac
  FLEET_ROLE_LC="$(printf '%s' "${FLEET_ROLE}" | tr '[:upper:]' '[:lower:]')"
  FLEET_COMPOSE_FILE="${FLEET_COMPOSE_DIR}/${FLEET_ROLE}.yml"
  FLEET_GW_NAME="aenv-gw-${FLEET_ROLE_LC}"
  FLEET_SCHED_A_NAME="aenv-sched-a"
  FLEET_SCHED_A_ADDR="${FLEET_IP_A}:${FLEET_SCHED_GRPC_PORT_A}"
  FLEET_SCHED_ENDPOINT="http://${FLEET_SCHED_A_ADDR}"

  case "${FLEET_REDIS_MODE}" in
    single | cluster) ;;
    *) die "AENV_FLEET_REDIS_MODE must be 'single' or 'cluster', got '${FLEET_REDIS_MODE}'" ;;
  esac
  if ! [[ "${FLEET_SCHED_REPLICAS_B}" =~ ^[1-8]$ ]]; then
    die "AENV_FLEET_SCHEDULER_REPLICAS_B must be 1..8, got '${FLEET_SCHED_REPLICAS_B}'"
  fi
}

# Re-root every generated path. `--render-only` uses it to write a full render
# into a scratch directory on a machine that is not one of the two hosts.
fleet_set_root() {
  FLEET_ROOT="$1"
  FLEET_COMPOSE_DIR="${FLEET_ROOT}/compose"
  FLEET_CONFIG_DIR="${FLEET_ROOT}/config"
  FLEET_ENV_FILE="${FLEET_COMPOSE_DIR}/.env"
  FLEET_COMPOSE_FILE="${FLEET_COMPOSE_DIR}/${FLEET_ROLE}.yml"
}

fleet_node_id() {
  local role_lc="$1" idx="$2"
  printf 'node-%s%d' "${role_lc}" "${idx}"
}

fleet_node_name() {
  local role_lc="$1" idx="$2"
  printf 'aenv-node-%s%d' "${role_lc}" "${idx}"
}

fleet_node_port() {
  local role="$1" idx="$2"
  case "${role}" in
    A) printf '%d' $((FLEET_NODE_PORT_BASE_A + idx)) ;;
    B) printf '%d' $((FLEET_NODE_PORT_BASE_B + idx)) ;;
  esac
}

fleet_node_p2p_port() {
  local role="$1" idx="$2"
  case "${role}" in
    A) printf '%d' $((FLEET_P2P_PORT_BASE_A + idx)) ;;
    B) printf '%d' $((FLEET_P2P_PORT_BASE_B + idx)) ;;
  esac
}

# Two threads plus their two SMT siblings per node unit, so a unit's 4 vCPU
# are 2 whole physical cores and never straddle another unit's core.
fleet_node_cpuset() {
  local idx="$1"
  local c="$((FLEET_NODE_CPU_BASE + 2 * idx))" h="$((FLEET_NODE_HT_BASE + 2 * idx))"
  printf '%d-%d,%d-%d' "${c}" "$((c + 1))" "${h}" "$((h + 1))"
}

fleet_smoke_port() {
  local idx="$1"
  printf '%d' $((FLEET_SMOKE_PORT_BASE + FLEET_SMOKE_INDEX_BASE + idx))
}

fleet_redis_port() { printf '%d' $((FLEET_REDIS_PORT_BASE + $1)); }
fleet_redis_bus_port() { printf '%d' $((FLEET_REDIS_BUS_PORT_BASE + $1)); }

fleet_redis_count() {
  if [[ "${FLEET_REDIS_MODE}" == cluster ]]; then
    printf '%d' "${FLEET_REDIS_CLUSTER_MASTERS}"
  else
    printf '1'
  fi
}

# Comma-separated seeds: services/shared/config parses them into a list, and
# the scheduler's store probes `INFO cluster` on the first seed to decide
# between a single client and a cluster client.
fleet_redis_addr() {
  local i out=""
  for ((i = 0; i < $(fleet_redis_count); i++)); do
    out+="${out:+,}${FLEET_IP_A}:$(fleet_redis_port "${i}")"
  done
  printf '%s' "${out}"
}

fleet_sched_b_grpc_port() { printf '%d' $((FLEET_SCHED_GRPC_PORT_BASE_B + $1)); }
fleet_sched_b_metrics_port() { printf '%d' $((FLEET_SCHED_METRICS_PORT_BASE_B + $1)); }

fleet_image_ref() {
  local name="$1"
  if [[ -n "${FLEET_REGISTRY}" ]]; then
    printf '%s/%s:%s' "${FLEET_REGISTRY%/}" "${name}" "${FLEET_IMAGE_TAG}"
  else
    printf '%s:%s' "${name}" "${FLEET_IMAGE_TAG}"
  fi
}

# The whole cluster, both hosts, as the scheduler's static node list.
fleet_all_nodes_json() {
  local i
  {
    for ((i = 0; i < FLEET_NODE_COUNT_A; i++)); do
      printf '%s\thttp://%s:%d\n' "$(fleet_node_id a "${i}")" "${FLEET_IP_A}" "$(fleet_node_port A "${i}")"
    done
    for ((i = 0; i < FLEET_NODE_COUNT_B; i++)); do
      printf '%s\thttp://%s:%d\n' "$(fleet_node_id b "${i}")" "${FLEET_IP_B}" "$(fleet_node_port B "${i}")"
    done
  } | jq -R -s 'split("\n") | map(select(length > 0) | split("\t") | {id: .[0], endpoint: .[1]})'
}

# Every TCP port this host's role listens on. Used by the free-port check
# before first bring-up and by the README's firewall table.
fleet_role_ports() {
  local i
  printf '%d\n%d\n' "${FLEET_GW_HTTP_PORT}" "${FLEET_GW_METRICS_PORT}"
  for ((i = 0; i < FLEET_NODE_COUNT; i++)); do
    fleet_node_port "${FLEET_ROLE}" "${i}"
    printf '\n'
  done
  if [[ "${FLEET_ROLE}" == A ]]; then
    printf '%d\n%d\n' "${FLEET_SCHED_GRPC_PORT_A}" "${FLEET_SCHED_METRICS_PORT_A}"
    printf '%d\n%d\n' "${FLEET_MINIO_API_PORT}" "${FLEET_MINIO_CONSOLE_PORT}"
    for ((i = 0; i < $(fleet_redis_count); i++)); do
      fleet_redis_port "${i}"
      printf '\n'
      if [[ "${FLEET_REDIS_MODE}" == cluster ]]; then
        fleet_redis_bus_port "${i}"
        printf '\n'
      fi
    done
  else
    for ((i = 0; i < FLEET_SCHED_REPLICAS_B; i++)); do
      fleet_sched_b_grpc_port "${i}"
      printf '\n'
      fleet_sched_b_metrics_port "${i}"
      printf '\n'
    done
  fi
}

# Write only when the content differs, so a re-run never bumps mtimes or
# triggers a systemd daemon-reload for nothing. Returns 0 when it wrote.
fleet_write_if_changed() {
  local path="$1" mode="$2" tmp
  tmp="$(mktemp "${path}.XXXXXX")"
  cat >"${tmp}"
  chmod "${mode}" "${tmp}"
  if [[ -f "${path}" ]] && cmp -s "${tmp}" "${path}"; then
    rm -f "${tmp}"
    return 1
  fi
  mv -f "${tmp}" "${path}"
  return 0
}

fleet_api() {
  curl -sS -m 15 -H "x-api-key: ${AENV_API_KEY}" "$@"
}

# One rule-evaluation helper for the GCE firewall preconditions, kept in the
# library so it can be exercised offline against a captured rule list.
#   fleet_fw_rule_present RULES_JSON NETWORK TAGS_JSON SOURCE_CIDR PORT_LO PORT_HI
# True when an enabled INGRESS rule on NETWORK admits tcp PORT_LO-PORT_HI from
# SOURCE_CIDR (or from anywhere) to instances carrying one of TAGS_JSON (or to
# every instance when the rule has no target tags).
fleet_fw_rule_present() {
  local rules="$1" network="$2" tags="$3" src="$4" lo="$5" hi="$6"
  jq -e --arg net "${network}" --argjson tags "${tags}" --arg src "${src}" \
    --argjson lo "${lo}" --argjson hi "${hi}" '
    def covers($lo; $hi):
      (split("-") | map(tonumber) | if length == 1 then [.[0], .[0]] else . end) as $r
      | $r[0] <= $lo and $hi <= $r[1];
    [ .[]
      | select(.direction == "INGRESS")
      | select((.disabled // false) | not)
      | select((.network | split("/") | last) == $net)
      | select(((.targetTags // []) | length) == 0 or any((.targetTags // [])[]; IN($tags[])))
      | select((.sourceRanges // []) | (index($src) != null or index("0.0.0.0/0") != null))
      | select(any((.allowed // [])[];
          (.IPProtocol == "tcp" or .IPProtocol == "all")
          and any((.ports // ["0-65535"])[]; covers($lo; $hi))))
    ] | length > 0' <<<"${rules}" >/dev/null
}

# True when some enabled INGRESS rule on NETWORK admits tcp:22 from 0.0.0.0/0
# to instances carrying one of TAGS_JSON. The fleet never needs public ssh, so
# this must stay false.
fleet_fw_public_ssh_open() {
  local rules="$1" network="$2" tags="$3"
  jq -e --arg net "${network}" --argjson tags "${tags}" '
    def covers22: (split("-") | map(tonumber) | if length == 1 then [.[0], .[0]] else . end) as $r
      | $r[0] <= 22 and 22 <= $r[1];
    [ .[]
      | select(.direction == "INGRESS")
      | select((.disabled // false) | not)
      | select((.network | split("/") | last) == $net)
      | select(((.targetTags // []) | length) == 0 or any((.targetTags // [])[]; IN($tags[])))
      | select((.sourceRanges // []) | index("0.0.0.0/0") != null)
      | select(any((.allowed // [])[];
          (.IPProtocol == "tcp" or .IPProtocol == "all")
          and any((.ports // ["0-65535"])[]; covers22)))
    ] | length > 0' <<<"${rules}" >/dev/null
}

# The port specification the internal-allow rule has to carry. 6379/16379 are
# listed because they are the documented Redis defaults an operator would
# reach for; the fleet's own Redis listens inside 19000-19199.
fleet_fw_internal_ports() {
  printf 'tcp:%d-%d,tcp:6379,tcp:16379' "${FLEET_PORT_MIN}" "${FLEET_PORT_MAX}"
}
