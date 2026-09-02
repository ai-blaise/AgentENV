#!/usr/bin/env bash
# Brings one VM of the two-VM AgentENV control-plane fleet up. Idempotent: a
# re-run re-renders, re-applies and re-verifies without restarting healthy units.
#
#   sudo scripts/fleet/bootstrap.sh A          # on instance-20260415-161450, first
#   sudo scripts/fleet/bootstrap.sh B          # on instance-20260415-20260415-235136
#   scripts/fleet/bootstrap.sh <A|B> --render-only <dir>   # no root, no docker
#
# Host A carries the primary scheduler, Redis, MinIO, a gateway and 4 node
# units; host B carries a gateway, query-only scheduler replicas and 12 node
# units. Every unit runs [machine].backend = "mock": these hosts have no
# /dev/kvm and no ublk_drv, so no unit ever runs a guest. What the fleet
# exercises is the control plane itself — heartbeats, bindings, rosters,
# placement and the gateway path — across a real network.
#
# Knobs (environment): AENV_FLEET_REGISTRY, AENV_FLEET_IMAGE_TAG,
# AENV_FLEET_PUSH=1, AENV_FLEET_REBUILD=1, AENV_FLEET_SERVER_BIN,
# AENV_FLEET_REDIS_MODE=single|cluster, AENV_FLEET_SCHEDULER_REPLICAS_B,
# AENV_FLEET_FORCE_HOST=1, AENV_FLEET_SKIP_FIREWALL_CHECK=1. See README.md.
set -euo pipefail

FLEET_SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
source "${FLEET_SELF_DIR}/lib.sh"

usage() {
  sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

ROLE_ARG="${1:-}"
[[ $# -gt 0 ]] && shift
RENDER_ONLY=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --render-only)
      [[ $# -ge 2 ]] || die "--render-only needs a directory"
      RENDER_ONLY="$2"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *) die "unknown option: $1" ;;
  esac
done
if [[ "${ROLE_ARG}" == "-h" || "${ROLE_ARG}" == "--help" ]]; then
  usage
  exit 0
fi

fleet_role_init "${ROLE_ARG}"
if [[ -n "${RENDER_ONLY}" ]]; then
  mkdir -p "${RENDER_ONLY}"
  fleet_set_root "$(cd "${RENDER_ONLY}" && pwd)"
fi

TEMPLATE="${FLEET_SCRIPT_DIR}/node.toml.tmpl"
SMOKE_SCRIPT="${FLEET_REPO_ROOT}/scripts/tests/smoke/mock_node.sh"
NODE_HEALTHCHECK_SRC="${FLEET_SCRIPT_DIR}/node_healthcheck.sh"
COMPOSE=(docker compose --env-file "${FLEET_ENV_FILE}" -f "${FLEET_COMPOSE_FILE}")
AENVIO_GID=""
NODE_IMAGE=""
GATEWAY_IMAGE="$(fleet_image_ref agentenv-gateway)"
SCHEDULER_IMAGE="$(fleet_image_ref agentenv-scheduler)"

# ---------------------------------------------------------------------------
# Preconditions. Asserted, never installed: every one of them is either the
# owner's (docker, cgroup layout, the io_uring gate) or a measured fact the
# carve depends on (which host this is, which CPUs exist).
# ---------------------------------------------------------------------------

preflight_host() {
  [[ "${EUID}" -eq 0 ]] || die "run with sudo: the slice, the compose project and the fleet directory are root-owned"
  local cmd
  for cmd in docker systemctl jq curl ss python3 getent ip hostname; do require_cmd "${cmd}"; done
  docker compose version >/dev/null 2>&1 || die "docker compose plugin is required"
  [[ -f "${SMOKE_SCRIPT}" ]] || die "missing ${SMOKE_SCRIPT}"
  [[ -f "${TEMPLATE}" ]] || die "missing ${TEMPLATE}"

  local short_host
  short_host="$(hostname -s)"
  if [[ "${short_host}" != "${FLEET_HOSTNAME_EXPECTED}" && -z "${AENV_FLEET_FORCE_HOST:-}" ]]; then
    die "this is ${short_host}; role ${FLEET_ROLE} is ${FLEET_HOSTNAME_EXPECTED}. The carve is per host — set AENV_FLEET_FORCE_HOST=1 only if the instance was renamed"
  fi
  if ! ip -4 -o addr show | awk '{print $4}' | cut -d/ -f1 | grep -qx "${FLEET_HOST_IP}"; then
    die "no interface carries ${FLEET_HOST_IP}; role ${FLEET_ROLE} expects it (override with AENV_FLEET_IP_${FLEET_ROLE})"
  fi

  [[ "$(stat -fc %T /sys/fs/cgroup)" == cgroup2fs ]] || die "cgroup v2 unified hierarchy is required"
  local driver
  driver="$(docker info --format '{{.CgroupDriver}}' 2>/dev/null || true)"
  [[ "${driver}" == systemd ]] || die "docker cgroup driver is '${driver}', the slice parent needs 'systemd'"
  grep -qw cpuset /sys/fs/cgroup/cgroup.subtree_control || die "cpuset controller is not delegated at the cgroup root"

  local online max_cpu
  online="$(cat /sys/devices/system/cpu/online)"
  max_cpu="${online##*[-,]}"
  [[ "${max_cpu}" -ge 223 ]] || die "carve ${FLEET_CPUSET} needs CPUs up to 223 online; this host reports ${online}"
  if [[ -n "${FLEET_MEMORY_NODES}" && ! -d "/sys/devices/system/node/node${FLEET_MEMORY_NODES}" ]]; then
    die "NUMA node ${FLEET_MEMORY_NODES} does not exist on this host"
  fi

  AENVIO_GID="$(getent group "${FLEET_IOURING_GROUP}" | cut -d: -f3 || true)"
  [[ -n "${AENVIO_GID}" ]] || die "group ${FLEET_IOURING_GROUP} does not exist; io_uring is gated on it and every fleet process must carry it"
  local dropin
  dropin="$(grep -ls 'kernel.io_uring_group' /etc/sysctl.d/*.conf 2>/dev/null | head -n1 || true)"
  [[ -n "${dropin}" ]] || die "no /etc/sysctl.d drop-in sets kernel.io_uring_group; the io_uring gate is not persisted"
  local disabled group
  disabled="$(sysctl -n kernel.io_uring_disabled)"
  group="$(sysctl -n kernel.io_uring_group)"
  if [[ "${disabled}" != 1 || "${group}" != "${AENVIO_GID}" ]]; then
    die "io_uring gate is not live (kernel.io_uring_disabled=${disabled}, kernel.io_uring_group=${group}, expected 1 and ${AENVIO_GID}); apply ${dropin} with 'sysctl --system'"
  fi

  if [[ -n "${FLEET_SERVER_BIN}" ]]; then
    [[ -x "${FLEET_SERVER_BIN}" ]] || die "AENV_FLEET_SERVER_BIN=${FLEET_SERVER_BIN} is not an executable file"
  fi
  log "host preconditions hold: ${short_host} (${FLEET_HOST_IP}), cgroup2/systemd, cpuset delegated, ${FLEET_IOURING_GROUP}=${AENVIO_GID}, io_uring gated"
}

fw_print_requirements() {
  cat >&2 <<EOF
Firewall prerequisites for the fleet (network ${FLEET_PRIMARY_NETWORK}, subnet ${FLEET_PRIMARY_SUBNET}):
  1. IAP ssh: INGRESS tcp:22 from ${FLEET_IAP_RANGE} to the instances' actual network tag.
  2. Internal allow: INGRESS $(fleet_fw_internal_ports) from ${FLEET_PRIMARY_SUBNET} to the instances' tag.
  3. The disabled rule a3u-gvnic-asia-south1-1-allow-ssh-external (tcp:22 from 0.0.0.0/0) stays disabled.
See scripts/fleet/README.md, section "Firewall", for the two operator commands.
EOF
}

fw_print_remediation() {
  local tags_csv="$1" zone="$2"
  cat >&2 <<EOF
Instance tags: ${tags_csv}. Two ways to satisfy the rules above; the operator decides.

Option 1 — re-add the tag the existing rules target, and add the missing internal allow:
  gcloud compute instances add-tags ${FLEET_HOSTNAME_A} --zone ${zone} --tags command-center-clean
  gcloud compute instances add-tags ${FLEET_HOSTNAME_B} --zone ${zone} --tags command-center-clean
  gcloud compute firewall-rules create ${FLEET_PRIMARY_NETWORK}-allow-internal-fleet \\
    --network ${FLEET_PRIMARY_NETWORK} --direction INGRESS --priority 1000 --action ALLOW \\
    --rules $(fleet_fw_internal_ports) --source-ranges ${FLEET_PRIMARY_SUBNET} --target-tags command-center-clean

Option 2 — tag-scoped rules for the tag the instances carry today (legacy-compromised-a3u):
  gcloud compute firewall-rules create ${FLEET_PRIMARY_NETWORK}-allow-iap-ssh-legacy \\
    --network ${FLEET_PRIMARY_NETWORK} --direction INGRESS --priority 1000 --action ALLOW \\
    --rules tcp:22 --source-ranges ${FLEET_IAP_RANGE} --target-tags legacy-compromised-a3u
  gcloud compute firewall-rules create ${FLEET_PRIMARY_NETWORK}-allow-internal-fleet-legacy \\
    --network ${FLEET_PRIMARY_NETWORK} --direction INGRESS --priority 1000 --action ALLOW \\
    --rules $(fleet_fw_internal_ports) --source-ranges ${FLEET_PRIMARY_SUBNET} --target-tags legacy-compromised-a3u

Never re-enable a3u-gvnic-asia-south1-1-allow-ssh-external: the fleet needs no public ssh.
EOF
}

# The rules are the project's, so this only reads them. Without gcloud or its
# credentials on the VM the requirements are printed and the run continues;
# with them, a missing rule stops the bootstrap because a fleet split across
# two VMs with no internal allow would come up as two half-fleets.
preflight_firewall() {
  local meta=http://169.254.169.254/computeMetadata/v1/instance
  local tags network zone
  if ! tags="$(curl -sf -m 3 -H 'Metadata-Flavor: Google' "${meta}/tags")"; then
    warn "GCE metadata server unreachable; firewall preconditions not checked"
    fw_print_requirements
    return 0
  fi
  network="$(curl -sf -m 3 -H 'Metadata-Flavor: Google' "${meta}/network-interfaces/0/network" || true)"
  network="${network##*/}"
  zone="$(curl -sf -m 3 -H 'Metadata-Flavor: Google' "${meta}/zone" || true)"
  zone="${zone##*/}"
  if [[ "${network}" != "${FLEET_PRIMARY_NETWORK}" ]]; then
    warn "primary NIC is on network '${network}'; the design measured ${FLEET_PRIMARY_NETWORK}"
  fi

  local rules
  if ! command -v gcloud >/dev/null 2>&1 || ! rules="$(gcloud compute firewall-rules list --format=json 2>/dev/null)"; then
    warn "gcloud is unavailable or unauthorised on this VM; firewall rules not verified"
    fw_print_requirements
    return 0
  fi

  local failed=0
  if ! fleet_fw_rule_present "${rules}" "${network}" "${tags}" "${FLEET_IAP_RANGE}" 22 22; then
    error "no enabled rule on ${network} admits IAP ssh (tcp:22 from ${FLEET_IAP_RANGE}) to this instance's tags"
    failed=1
  fi
  if ! fleet_fw_rule_present "${rules}" "${network}" "${tags}" "${FLEET_PRIMARY_SUBNET}" "${FLEET_PORT_MIN}" "${FLEET_PORT_MAX}"; then
    error "no enabled rule on ${network} admits tcp:${FLEET_PORT_MIN}-${FLEET_PORT_MAX} from ${FLEET_PRIMARY_SUBNET} to this instance's tags"
    failed=1
  fi
  if fleet_fw_public_ssh_open "${rules}" "${network}" "${tags}"; then
    error "an enabled rule on ${network} admits tcp:22 from 0.0.0.0/0 to this instance; the fleet never needs public ssh"
    failed=1
  fi
  if [[ "${failed}" -eq 0 ]]; then
    log "firewall preconditions hold on ${network} for tags $(jq -r 'join(",")' <<<"${tags}")"
    return 0
  fi
  fw_print_requirements
  fw_print_remediation "$(jq -r 'join(",")' <<<"${tags}")" "${zone:-asia-south1-b}"
  if [[ -n "${AENV_FLEET_SKIP_FIREWALL_CHECK:-}" ]]; then
    warn "continuing despite firewall findings (AENV_FLEET_SKIP_FIREWALL_CHECK set)"
    return 0
  fi
  die "firewall preconditions not met"
}

tcp_open() {
  local host="$1" port="$2"
  timeout 3 bash -c "exec 3<>/dev/tcp/${host}/${port}" 2>/dev/null
}

# B's schedulers and gateway are useless without A: the primary scheduler and
# the binding store live there, and this is the one cross-host dependency that
# cannot retry its way out of a firewall hole.
preflight_peer() {
  [[ "${FLEET_ROLE}" == B ]] || return 0
  tcp_open "${FLEET_IP_A}" "${FLEET_SCHED_GRPC_PORT_A}" ||
    die "cannot reach the primary scheduler at ${FLEET_SCHED_A_ADDR}; bootstrap host A first, then check the internal firewall allow"
  tcp_open "${FLEET_IP_A}" "$(fleet_redis_port 0)" ||
    die "cannot reach Redis at ${FLEET_IP_A}:$(fleet_redis_port 0); host A's fleet is not up or the internal allow is missing"
  log "host A reachable: scheduler ${FLEET_SCHED_A_ADDR}, redis $(fleet_redis_addr)"
}

fleet_running() {
  [[ -n "$(docker ps -q --filter "label=com.docker.compose.project=${FLEET_PROJECT}" 2>/dev/null)" ]]
}

# Only on first bring-up: once the fleet is running its own listeners occupy
# these ports, and that is what idempotent re-runs look like.
preflight_ports() {
  if fleet_running; then return 0; fi
  local port busy=()
  while IFS= read -r port; do
    [[ -n "${port}" ]] || continue
    if [[ -n "$(ss -ltnH "( sport = :${port} )" 2>/dev/null)" ]]; then busy+=("${port}"); fi
  done < <(fleet_role_ports)
  if [[ "${#busy[@]}" -gt 0 ]]; then
    ss -ltnp "( $(printf 'sport = :%s or ' "${busy[@]}" | sed 's/ or $//') )" >&2 || true
    die "fleet ports already in use on this host: ${busy[*]}"
  fi
  log "fleet ports free on this host"
}

# ---------------------------------------------------------------------------
# Rendering.
# ---------------------------------------------------------------------------

ensure_dirs() {
  install -d -m 0750 "${FLEET_ROOT}" "${FLEET_COMPOSE_DIR}" "${FLEET_CONFIG_DIR}"
}

random_hex() {
  od -An -N"$1" -tx1 /dev/urandom | tr -d ' \n'
}

# One secret set for the whole cluster: the API key every node and gateway
# authenticates with, the cluster UUID every heartbeat carries, and MinIO's
# root credentials. A generates it once; B must receive a copy, because a B
# with its own key would be a second cluster that happens to share IPs.
ensure_env() {
  if [[ ! -f "${FLEET_ENV_FILE}" ]]; then
    if [[ "${FLEET_ROLE}" == B && -z "${RENDER_ONLY}" ]]; then
      die "${FLEET_ENV_FILE} is missing. Copy it from host A (same path, mode 0600) so B joins A's cluster with A's API key"
    fi
    local api_key="${AENV_API_KEY:-$(random_hex 32)}"
    {
      printf '# Generated by scripts/fleet/bootstrap.sh on host %s. Copy verbatim to the other host.\n' "${FLEET_ROLE}"
      printf 'AENV_FLEET_CLUSTER_ID=%s\n' "$(cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen | tr '[:upper:]' '[:lower:]')"
      printf 'AENV_API_KEY=%s\n' "${api_key}"
      printf 'AENV_FLEET_MINIO_ROOT_USER=aenvfleet\n'
      printf 'AENV_FLEET_MINIO_ROOT_PASSWORD=%s\n' "$(random_hex 24)"
    } | fleet_write_if_changed "${FLEET_ENV_FILE}" 0600 || true
    log "wrote ${FLEET_ENV_FILE}"
  fi
  chmod 0600 "${FLEET_ENV_FILE}"
  set -a
  # shellcheck source=/dev/null
  source "${FLEET_ENV_FILE}"
  set +a
  local key
  for key in AENV_FLEET_CLUSTER_ID AENV_API_KEY AENV_FLEET_MINIO_ROOT_USER AENV_FLEET_MINIO_ROOT_PASSWORD; do
    [[ -n "${!key:-}" ]] || die "${FLEET_ENV_FILE} lacks ${key}"
  done
  if ! [[ "${#AENV_API_KEY}" -ge 32 && "${#AENV_API_KEY}" -le 256 && "${AENV_API_KEY}" =~ ^[A-Za-z0-9._~-]+$ ]]; then
    die "AENV_API_KEY in ${FLEET_ENV_FILE} must be 32..256 URL-safe characters (src/api_key.rs)"
  fi
}

render_slice_unit() {
  cat <<EOF
# Managed by scripts/fleet/bootstrap.sh (host ${FLEET_ROLE}). Every fleet
# container is parented here, so one 'systemctl stop ${FLEET_SLICE}' ends the
# whole fleet and the memory cap is a hard backstop the owner's workloads can
# rely on regardless of what any single container asks for.
[Unit]
Description=AgentENV control-plane fleet, host ${FLEET_ROLE} carve
Before=slices.target

[Slice]
AllowedCPUs=${FLEET_CPUSET}
EOF
  if [[ -n "${FLEET_MEMORY_NODES}" ]]; then
    printf 'AllowedMemoryNodes=%s\n' "${FLEET_MEMORY_NODES}"
  fi
  cat <<EOF
MemoryMax=${FLEET_MEMORY_MAX}
MemoryHigh=${FLEET_MEMORY_HIGH}
CPUQuota=${FLEET_CPU_QUOTA}
EOF
}

ensure_slice() {
  if [[ -n "${RENDER_ONLY}" ]]; then
    render_slice_unit | fleet_write_if_changed "${FLEET_ROOT}/${FLEET_SLICE}" 0644 || true
    return 0
  fi
  if render_slice_unit | fleet_write_if_changed "${FLEET_SLICE_UNIT}" 0644; then
    systemctl daemon-reload
    log "wrote ${FLEET_SLICE_UNIT}"
  fi
  systemctl start "${FLEET_SLICE}"
  local effective
  effective="$(cat "/sys/fs/cgroup/${FLEET_SLICE}/cpuset.cpus.effective" 2>/dev/null || true)"
  [[ -n "${effective}" ]] || die "${FLEET_SLICE} is active but has no effective cpuset"
  log "${FLEET_SLICE}: cpus ${effective}, MemoryMax ${FLEET_MEMORY_MAX}, MemoryHigh ${FLEET_MEMORY_HIGH}"
}

ensure_image() {
  local name="$1" dockerfile="$2" ref
  ref="$(fleet_image_ref "${name}")"
  if [[ -z "${AENV_FLEET_REBUILD:-}" ]] && docker image inspect "${ref}" >/dev/null 2>&1; then
    log "image present: ${ref}"
    return 0
  fi
  if [[ -n "${FLEET_REGISTRY}" && -z "${AENV_FLEET_REBUILD:-}" ]] && docker pull "${ref}"; then
    log "pulled ${ref}"
    return 0
  fi
  log "building ${ref} from ${dockerfile} (context ${FLEET_REPO_ROOT})"
  docker build -f "${FLEET_REPO_ROOT}/${dockerfile}" -t "${ref}" "${FLEET_REPO_ROOT}"
  if [[ -n "${FLEET_REGISTRY}" && -n "${AENV_FLEET_PUSH:-}" ]]; then
    docker push "${ref}"
    log "pushed ${ref}"
  fi
}

ensure_pulled() {
  local ref="$1"
  docker image inspect "${ref}" >/dev/null 2>&1 || docker pull "${ref}"
}

# The runtime image bakes `server --setup-only` (firecracker, kernel, tools
# drive) that a mock node never opens; the bind-mounted binary path exists so
# a host with a fresh release build can skip that multi-GB image entirely.
ensure_images() {
  if [[ -n "${FLEET_SERVER_BIN}" ]]; then
    NODE_IMAGE="${FLEET_BIN_BASE_IMAGE}"
    ensure_pulled "${NODE_IMAGE}"
    docker run --rm --entrypoint /server -v "${FLEET_SERVER_BIN}:/server:ro" "${NODE_IMAGE}" --help >/dev/null ||
      die "${FLEET_SERVER_BIN} does not run inside ${NODE_IMAGE}; check 'ldd' against that image's libraries"
    log "node units will bind-mount ${FLEET_SERVER_BIN} into ${NODE_IMAGE}"
  else
    NODE_IMAGE="$(fleet_image_ref agentenv-runtime)"
    ensure_image agentenv-runtime deploy/docker/Dockerfile.agentenv
  fi
  ensure_image agentenv-gateway deploy/docker/Dockerfile.gateway
  ensure_image agentenv-scheduler deploy/docker/Dockerfile.scheduler
  if [[ "${FLEET_ROLE}" == A ]]; then
    ensure_pulled "${FLEET_REDIS_IMAGE}"
    ensure_pulled "${FLEET_MINIO_IMAGE}"
  fi
}

render_node_config() {
  local idx="$1" node_id path
  node_id="$(fleet_node_id "${FLEET_ROLE_LC}" "${idx}")"
  path="${FLEET_CONFIG_DIR}/$(fleet_node_name "${FLEET_ROLE_LC}" "${idx}").toml"
  sed \
    -e "s|@NODE_ID@|${node_id}|g" \
    -e "s|@SERVICE_INSTANCE_ID@|${node_id}.${FLEET_ROLE_LC}.${FLEET_PROJECT}|g" \
    -e "s|@CLUSTER_ID@|${AENV_FLEET_CLUSTER_ID}|g" \
    -e "s|@SCHEDULER_ENDPOINT@|${FLEET_SCHED_ENDPOINT}|g" \
    -e "s|@P2P_PORT@|$(fleet_node_p2p_port "${FLEET_ROLE}" "${idx}")|g" \
    "${TEMPLATE}" | fleet_write_if_changed "${path}" 0644 || true
  if grep -q '@[A-Z_]\{1,\}@' "${path}"; then
    die "unrendered placeholder in ${path}: $(grep -o '@[A-Z_]\{1,\}@' "${path}" | sort -u | tr '\n' ' ')"
  fi
}

render_scheduler_config() {
  local path="$1" grpc_port="$2" metrics_port="$3"
  jq -n \
    --arg listen ":${grpc_port}" \
    --arg metrics ":${metrics_port}" \
    --arg redis "$(fleet_redis_addr)" \
    --argjson nodes "$(fleet_all_nodes_json)" \
    '{
      log_level: "info",
      log_format: "json",
      scheduler: {
        grpc_listen_addr: $listen,
        metrics_listen_addr: $metrics,
        strategy: "round_robin",
        report_ttl: "30s",
        binding_ttl: "30s",
        heartbeat_interval: "5s",
        redis_addr: $redis,
        discovery: { mode: "static" },
        nodes: $nodes
      }
    }' | fleet_write_if_changed "${path}" 0644 || true
}

render_gateway_config() {
  local query_only=""
  if [[ "${FLEET_ROLE}" == B ]]; then
    query_only="${FLEET_IP_B}:$(fleet_sched_b_grpc_port 0)"
  fi
  jq -n \
    --arg http ":${FLEET_GW_HTTP_PORT}" \
    --arg metrics ":${FLEET_GW_METRICS_PORT}" \
    --arg sched "${FLEET_SCHED_A_ADDR}" \
    --arg query_only "${query_only}" \
    '{
      log_level: "info",
      log_format: "json",
      gateway: {
        http_listen_addr: $http,
        metrics_listen_addr: $metrics,
        scheduler_addr: $sched,
        query_only_scheduler_addr: $query_only,
        request_timeout: "90s",
        forward_response_size: 4194304,
        sandbox_proxy_domains: []
      }
    }' | fleet_write_if_changed "${FLEET_CONFIG_DIR}/gateway.json" 0644 || true
}

render_configs() {
  local i
  for ((i = 0; i < FLEET_NODE_COUNT; i++)); do render_node_config "${i}"; done
  if [[ "${FLEET_ROLE}" == A ]]; then
    render_scheduler_config "${FLEET_CONFIG_DIR}/scheduler-a.json" "${FLEET_SCHED_GRPC_PORT_A}" "${FLEET_SCHED_METRICS_PORT_A}"
  else
    for ((i = 0; i < FLEET_SCHED_REPLICAS_B; i++)); do
      render_scheduler_config "${FLEET_CONFIG_DIR}/scheduler-b${i}.json" "$(fleet_sched_b_grpc_port "${i}")" "$(fleet_sched_b_metrics_port "${i}")"
    done
  fi
  render_gateway_config
  fleet_write_if_changed "${FLEET_CONFIG_DIR}/node_healthcheck.sh" 0755 <"${NODE_HEALTHCHECK_SRC}" || true
  log "rendered ${FLEET_NODE_COUNT} node configs, scheduler and gateway configs into ${FLEET_CONFIG_DIR}"
}

# Fields shared by every fleet service. Host networking is deliberate: with no
# guests there are no network slots, so nothing writes host veths or iptables,
# and every unit is reachable at the VM's real 10.240.0.x address — which is
# what lets a scheduler on A place onto a node on B without NAT. Port
# separation replaces namespace separation.
compose_common() {
  local name="$1" image="$2" cpuset="$3" memory="$4"
  cat <<EOF
    container_name: ${name}
    image: ${image}
    network_mode: host
    cgroup_parent: ${FLEET_SLICE}
    cpuset: "${cpuset}"
    mem_limit: ${memory}
    group_add:
      - "${AENVIO_GID}"
    restart: unless-stopped
    stop_grace_period: 30s
    logging:
      driver: json-file
      options:
        max-size: "50m"
        max-file: "5"
EOF
}

compose_node_service() {
  local idx="$1" name port
  name="$(fleet_node_name "${FLEET_ROLE_LC}" "${idx}")"
  port="$(fleet_node_port "${FLEET_ROLE}" "${idx}")"
  printf '  %s:\n' "${name}"
  compose_common "${name}" "${NODE_IMAGE}" "$(fleet_node_cpuset "${idx}")" "${FLEET_NODE_MEMORY}"
  # A mock node touches no namespaces, veths or block devices, so it runs with
  # every capability dropped — the same footing the smoke script has on a
  # developer machine.
  cat <<EOF
    init: true
    cap_drop:
      - ALL
    security_opt:
      - no-new-privileges:true
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
EOF
  if [[ -n "${FLEET_SERVER_BIN}" ]]; then
    printf '    entrypoint: ["/server"]\n'
  fi
  cat <<EOF
    environment:
      AENV_API_KEY: \${AENV_API_KEY:?fleet .env is missing AENV_API_KEY}
      AENV_CONFIG_PATH: /etc/aenv/node.toml
      AENV_HOME_PATH: /var/lib/aenv
      AENV_RUNTIME_PATH: /run/aenv
      API_ADDR: 0.0.0.0:${port}
      RUST_LOG: agentenv=info
    volumes:
      - ${FLEET_CONFIG_DIR}/${name}.toml:/etc/aenv/node.toml:ro
      - ${FLEET_CONFIG_DIR}/node_healthcheck.sh:/etc/aenv/node_healthcheck.sh:ro
      - ${name}-home:/var/lib/aenv
EOF
  if [[ -n "${FLEET_SERVER_BIN}" ]]; then
    printf '      - %s:/server:ro\n' "${FLEET_SERVER_BIN}"
  fi
  cat <<EOF
    healthcheck:
      test: ["CMD", "bash", "/etc/aenv/node_healthcheck.sh", "${port}"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 20s
EOF
  if [[ "${FLEET_ROLE}" == A ]]; then
    cat <<EOF
    depends_on:
      ${FLEET_SCHED_A_NAME}:
        condition: service_healthy
EOF
  fi
}

compose_gateway_service() {
  local depends="$1"
  printf '  %s:\n' "${FLEET_GW_NAME}"
  compose_common "${FLEET_GW_NAME}" "${GATEWAY_IMAGE}" "${FLEET_GW_CPUSET}" 4g
  cat <<EOF
    command: ["-config", "/config/gateway.json"]
    environment:
      AENV_API_KEY: \${AENV_API_KEY:?fleet .env is missing AENV_API_KEY}
    volumes:
      - ${FLEET_CONFIG_DIR}/gateway.json:/config/gateway.json:ro
    depends_on:
      ${depends}:
        condition: service_healthy
EOF
}

compose_scheduler_a_service() {
  printf '  %s:\n' "${FLEET_SCHED_A_NAME}"
  compose_common "${FLEET_SCHED_A_NAME}" "${SCHEDULER_IMAGE}" "${FLEET_SCHED_CPUSET}" 4g
  cat <<EOF
    command: ["-config", "/config/scheduler.json"]
    volumes:
      - ${FLEET_CONFIG_DIR}/scheduler-a.json:/config/scheduler.json:ro
    healthcheck:
      test: ["CMD", "/grpc_health_probe", "-addr=127.0.0.1:${FLEET_SCHED_GRPC_PORT_A}"]
      interval: 5s
      timeout: 3s
      retries: 12
      start_period: 3s
    depends_on:
      aenv-redis-0:
        condition: service_healthy
EOF
}

# B's replicas answer LookupNode from the shared Redis store and nothing else.
# Heartbeats are single-homed on A's scheduler, so a full scheduler on B would
# see every node as never having reported; a query-only one gives B's gateway a
# local binding lookup and keeps the placement view in one place.
compose_scheduler_b_service() {
  local idx="$1" name="aenv-sched-b${1}"
  printf '  %s:\n' "${name}"
  compose_common "${name}" "${SCHEDULER_IMAGE}" "${FLEET_SCHED_CPUSET}" 2g
  cat <<EOF
    command: ["-config", "/config/scheduler.json", "-query-only"]
    volumes:
      - ${FLEET_CONFIG_DIR}/scheduler-b${idx}.json:/config/scheduler.json:ro
    healthcheck:
      test: ["CMD", "/grpc_health_probe", "-addr=127.0.0.1:$(fleet_sched_b_grpc_port "${idx}")"]
      interval: 5s
      timeout: 3s
      retries: 12
      start_period: 3s
EOF
}

# Bound to the fleet IP and loopback only: the VPC firewall is the boundary
# for a store that holds sandbox-to-node bindings and nothing else, and the
# RDMA and secondary NICs never need to see it.
compose_redis_service() {
  local idx="$1" name="aenv-redis-${1}" port bus cpuset memory maxmemory
  port="$(fleet_redis_port "${idx}")"
  bus="$(fleet_redis_bus_port "${idx}")"
  if [[ "${FLEET_REDIS_MODE}" == cluster ]]; then
    case "${idx}" in
      0) cpuset="100-101" ;;
      1) cpuset="212" ;;
      *) cpuset="213" ;;
    esac
    memory=2g
    maxmemory=1536mb
  else
    cpuset="100-101,212-213"
    memory=8g
    maxmemory=6gb
  fi
  printf '  %s:\n' "${name}"
  compose_common "${name}" "${FLEET_REDIS_IMAGE}" "${cpuset}" "${memory}"
  cat <<EOF
    command:
      - redis-server
      - --port
      - "${port}"
      - --bind
      - ${FLEET_HOST_IP}
      - 127.0.0.1
      - --appendonly
      - "yes"
      - --save
      - ""
      - --maxmemory
      - ${maxmemory}
      - --maxmemory-policy
      - noeviction
EOF
  if [[ "${FLEET_REDIS_MODE}" == cluster ]]; then
    cat <<EOF
      - --cluster-enabled
      - "yes"
      - --cluster-port
      - "${bus}"
      - --cluster-config-file
      - nodes.conf
      - --cluster-announce-ip
      - ${FLEET_HOST_IP}
      - --cluster-announce-port
      - "${port}"
      - --cluster-announce-bus-port
      - "${bus}"
EOF
  fi
  cat <<EOF
    volumes:
      - ${name}-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "-p", "${port}", "ping"]
      interval: 5s
      timeout: 3s
      retries: 12
      start_period: 2s
EOF
}

compose_minio_service() {
  printf '  aenv-minio:\n'
  compose_common aenv-minio "${FLEET_MINIO_IMAGE}" "102-103,214-215" 16g
  cat <<EOF
    command: ["server", "/data", "--address", ":${FLEET_MINIO_API_PORT}", "--console-address", ":${FLEET_MINIO_CONSOLE_PORT}"]
    environment:
      MINIO_ROOT_USER: \${AENV_FLEET_MINIO_ROOT_USER:?fleet .env is missing AENV_FLEET_MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: \${AENV_FLEET_MINIO_ROOT_PASSWORD:?fleet .env is missing AENV_FLEET_MINIO_ROOT_PASSWORD}
    volumes:
      - aenv-minio-data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://127.0.0.1:${FLEET_MINIO_API_PORT}/minio/health/live"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 10s
EOF
}

render_compose() {
  local i
  {
    cat <<EOF
# Generated by scripts/fleet/bootstrap.sh for host ${FLEET_ROLE} (${FLEET_HOSTNAME_EXPECTED}).
# Edit scripts/fleet/*, not this file; re-running the bootstrap rewrites it.
name: ${FLEET_PROJECT}

services:
EOF
    if [[ "${FLEET_ROLE}" == A ]]; then
      for ((i = 0; i < $(fleet_redis_count); i++)); do compose_redis_service "${i}"; done
      compose_minio_service
      compose_scheduler_a_service
      compose_gateway_service "${FLEET_SCHED_A_NAME}"
    else
      for ((i = 0; i < FLEET_SCHED_REPLICAS_B; i++)); do compose_scheduler_b_service "${i}"; done
      compose_gateway_service aenv-sched-b0
    fi
    for ((i = 0; i < FLEET_NODE_COUNT; i++)); do compose_node_service "${i}"; done
    printf '\nvolumes:\n'
    for ((i = 0; i < FLEET_NODE_COUNT; i++)); do
      printf '  %s-home: {}\n' "$(fleet_node_name "${FLEET_ROLE_LC}" "${i}")"
    done
    if [[ "${FLEET_ROLE}" == A ]]; then
      for ((i = 0; i < $(fleet_redis_count); i++)); do printf '  aenv-redis-%d-data: {}\n' "${i}"; done
      printf '  aenv-minio-data: {}\n'
    fi
  } | fleet_write_if_changed "${FLEET_COMPOSE_FILE}" 0640 || true
  log "rendered ${FLEET_COMPOSE_FILE}"
}

# ---------------------------------------------------------------------------
# Bring-up and gates.
# ---------------------------------------------------------------------------

wait_container_healthy() {
  local name="$1" timeout="${2:-180}" i state
  for ((i = 0; i < timeout; i++)); do
    state="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${name}" 2>/dev/null || echo missing)"
    case "${state}" in
      healthy | running) return 0 ;;
      exited | dead)
        docker logs --tail 50 "${name}" >&2 || true
        die "${name} ${state}"
        ;;
    esac
    sleep 1
  done
  docker logs --tail 50 "${name}" >&2 || true
  die "${name} not healthy after ${timeout}s (state ${state})"
}

wait_http() {
  local label="$1" url="$2" want="$3" timeout="${4:-120}" i code
  for ((i = 0; i < timeout; i++)); do
    code="$(curl -s -o /dev/null -m 5 -w '%{http_code}' -H "x-api-key: ${AENV_API_KEY}" "${url}" || true)"
    [[ "${code}" == "${want}" ]] && return 0
    sleep 1
  done
  die "${label}: ${url} answered ${code:-nothing}, wanted ${want}, after ${timeout}s"
}

compose_up() {
  local i
  "${COMPOSE[@]}" config -q
  if [[ "${FLEET_ROLE}" == A && "${FLEET_REDIS_MODE}" == cluster ]]; then
    local services=()
    for ((i = 0; i < FLEET_REDIS_CLUSTER_MASTERS; i++)); do services+=("aenv-redis-${i}"); done
    "${COMPOSE[@]}" up -d "${services[@]}"
    for ((i = 0; i < FLEET_REDIS_CLUSTER_MASTERS; i++)); do wait_container_healthy "aenv-redis-${i}" 60; done
    ensure_redis_cluster
  fi
  "${COMPOSE[@]}" up -d --remove-orphans
}

# Three masters, no replicas: the point is a real cluster client path in the
# scheduler store (per-master script loading, slot routing), not durability.
ensure_redis_cluster() {
  local port0 seeds="" i
  port0="$(fleet_redis_port 0)"
  if docker exec aenv-redis-0 redis-cli -p "${port0}" cluster info 2>/dev/null | grep -q 'cluster_state:ok'; then
    log "redis cluster already formed"
    return 0
  fi
  for ((i = 0; i < FLEET_REDIS_CLUSTER_MASTERS; i++)); do
    seeds+=" ${FLEET_HOST_IP}:$(fleet_redis_port "${i}")"
  done
  # shellcheck disable=SC2086
  docker exec aenv-redis-0 redis-cli -p "${port0}" --cluster create ${seeds} --cluster-replicas 0 --cluster-yes
  for ((i = 0; i < 30; i++)); do
    if docker exec aenv-redis-0 redis-cli -p "${port0}" cluster info | grep -q 'cluster_state:ok'; then
      log "redis cluster formed:${seeds}"
      return 0
    fi
    sleep 1
  done
  die "redis cluster did not reach cluster_state:ok"
}

gate_control_plane() {
  local i
  if [[ "${FLEET_ROLE}" == A ]]; then
    for ((i = 0; i < $(fleet_redis_count); i++)); do wait_container_healthy "aenv-redis-${i}" 60; done
    wait_container_healthy aenv-minio 120
    wait_container_healthy "${FLEET_SCHED_A_NAME}" 120
  else
    for ((i = 0; i < FLEET_SCHED_REPLICAS_B; i++)); do wait_container_healthy "aenv-sched-b${i}" 120; done
  fi
  wait_container_healthy "${FLEET_GW_NAME}" 60
  wait_http gateway "http://127.0.0.1:${FLEET_GW_HTTP_PORT}/health" 204 60
  log "control plane up: gateway :${FLEET_GW_HTTP_PORT}, scheduler(s) healthy"
}

# One container is enough to prove the parenting: docker places every service
# of this project the same way, and the slice's effective cpuset is what bounds
# them all.
gate_slice_parenting() {
  local name pid
  name="$(fleet_node_name "${FLEET_ROLE_LC}" 0)"
  [[ "$(docker inspect -f '{{.HostConfig.CgroupParent}}' "${name}")" == "${FLEET_SLICE}" ]] ||
    die "${name} is not parented to ${FLEET_SLICE}"
  pid="$(docker inspect -f '{{.State.Pid}}' "${name}")"
  grep -q "${FLEET_SLICE}" "/proc/${pid}/cgroup" || die "${name} (pid ${pid}) is not inside ${FLEET_SLICE}"
}

# mock_node.sh is the repo's own proof that a mock node works; running it with
# the unit-shaped launcher proves it in this unit's image, cpuset, memory cap,
# slice and group before the unit is declared joined.
smoke_unit_shape() {
  local idx="$1" name
  name="$(fleet_node_name "${FLEET_ROLE_LC}" "${idx}")"
  docker rm -f "aenv-smoke-${name}" >/dev/null 2>&1 || true
  (
    cd "${FLEET_REPO_ROOT}"
    AENV_FLEET_SMOKE_IMAGE="${NODE_IMAGE}" \
      AENV_FLEET_SMOKE_CPUSET="$(fleet_node_cpuset "${idx}")" \
      AENV_FLEET_SMOKE_MEMORY="${FLEET_NODE_MEMORY}" \
      AENV_FLEET_SMOKE_GID="${AENVIO_GID}" \
      AENV_FLEET_SMOKE_NAME="aenv-smoke-${name}" \
      AENV_FLEET_SMOKE_SLICE="${FLEET_SLICE}" \
      AENV_FLEET_SERVER_BIN="${FLEET_SERVER_BIN}" \
      bash "${SMOKE_SCRIPT}" "${FLEET_SCRIPT_DIR}/smoke_in_unit_shape.sh" "$(fleet_smoke_port "${idx}")"
  ) || die "mock_node.sh failed in the shape of ${name}"
  docker rm -f "aenv-smoke-${name}" >/dev/null 2>&1 || true
}

gate_units() {
  local i name port
  for ((i = 0; i < FLEET_NODE_COUNT; i++)); do
    name="$(fleet_node_name "${FLEET_ROLE_LC}" "${i}")"
    port="$(fleet_node_port "${FLEET_ROLE}" "${i}")"
    wait_container_healthy "${name}" 150
    wait_http "${name}" "http://127.0.0.1:${port}/health" 204 30
    # The startup rail is the one line that distinguishes a mock node from a
    # real one in the logs; a unit without it is not the unit this fleet runs.
    docker logs "${name}" 2>&1 | grep -q 'sandbox backend is "mock"' ||
      die "${name} did not log the mock startup rail; refusing to treat it as a fleet unit"
    smoke_unit_shape "${i}"
    log "unit ${name} (:${port}) healthy, rails logged, smoke passed"
  done
  gate_slice_parenting
}

# Joined means the primary scheduler reports the node ready and, through the
# gateway, shows sandboxBackend = "mock" for it. Both gateways talk to the same
# scheduler, so the count printed is the whole cluster's.
gate_joined() {
  local timeout=150 i body ready_ours want_ours backend_bad ready_total joined=0 missing="no /nodes response"
  want_ours="$(for ((i = 0; i < FLEET_NODE_COUNT; i++)); do fleet_node_id "${FLEET_ROLE_LC}" "${i}"; echo; done | jq -R -s 'split("\n") | map(select(length > 0))')"
  for ((i = 0; i < timeout; i++)); do
    body="$(fleet_api "http://127.0.0.1:${FLEET_GW_HTTP_PORT}/nodes" 2>/dev/null || true)"
    if jq -e 'type == "array"' <<<"${body}" >/dev/null 2>&1; then
      missing="$(jq -r --argjson want "${want_ours}" '[.[] | select(.status == "ready") | .id] as $ready | $want - $ready | join(" ")' <<<"${body}")"
      if [[ -z "${missing}" ]]; then
        joined=1
        break
      fi
    fi
    sleep 1
  done
  [[ "${joined}" -eq 1 ]] || die "nodes not ready in the scheduler after ${timeout}s: ${missing}"

  backend_bad="$(jq -r --argjson want "${want_ours}" '[.[] | select(.id as $id | $want | index($id)) | select(.machineInfo.sandboxBackend != "mock") | "\(.id)=\(.machineInfo.sandboxBackend)"] | join(" ")' <<<"${body}")"
  [[ -z "${backend_bad}" ]] || die "units joined with a non-mock backend: ${backend_bad}"

  ready_ours="$(jq -r --argjson want "${want_ours}" '[.[] | select(.status == "ready") | select(.id as $id | $want | index($id))] | length' <<<"${body}")"
  ready_total="$(jq -r '[.[] | select(.status == "ready")] | length' <<<"${body}")"
  log "joined: ${ready_ours}/${FLEET_NODE_COUNT} host-${FLEET_ROLE} units ready; cluster ${ready_total}/${FLEET_NODE_TOTAL} ready as seen by the primary scheduler"
  if [[ "${ready_total}" -lt "${FLEET_NODE_TOTAL}" ]]; then
    log "the other host is not fully joined yet; run the bootstrap there, then 'scripts/fleet/status.sh ${FLEET_ROLE}' converges on ${FLEET_NODE_TOTAL}"
  fi
}

print_summary() {
  local i
  printf '\nFleet host %s (%s) — mock units, no guests\n' "${FLEET_ROLE}" "${FLEET_HOST_IP}"
  printf '  slice        %s  cpus=%s  MemoryMax=%s\n' "${FLEET_SLICE}" "${FLEET_CPUSET}" "${FLEET_MEMORY_MAX}"
  printf '  gateway      http://%s:%d  (metrics :%d)\n' "${FLEET_HOST_IP}" "${FLEET_GW_HTTP_PORT}" "${FLEET_GW_METRICS_PORT}"
  if [[ "${FLEET_ROLE}" == A ]]; then
    printf '  scheduler    %s  (metrics :%d)\n' "${FLEET_SCHED_A_ADDR}" "${FLEET_SCHED_METRICS_PORT_A}"
    printf '  redis        %s  (%s)\n' "$(fleet_redis_addr)" "${FLEET_REDIS_MODE}"
    printf '  minio        http://%s:%d  console :%d\n' "${FLEET_HOST_IP}" "${FLEET_MINIO_API_PORT}" "${FLEET_MINIO_CONSOLE_PORT}"
  else
    for ((i = 0; i < FLEET_SCHED_REPLICAS_B; i++)); do
      printf '  scheduler-b%d %s:%d query-only (metrics :%d)\n' "${i}" "${FLEET_HOST_IP}" "$(fleet_sched_b_grpc_port "${i}")" "$(fleet_sched_b_metrics_port "${i}")"
    done
  fi
  for ((i = 0; i < FLEET_NODE_COUNT; i++)); do
    printf '  %-12s http://%s:%d  cpuset=%s  mem=%s\n' "$(fleet_node_id "${FLEET_ROLE_LC}" "${i}")" "${FLEET_HOST_IP}" "$(fleet_node_port "${FLEET_ROLE}" "${i}")" "$(fleet_node_cpuset "${i}")" "${FLEET_NODE_MEMORY}"
  done
  printf '  compose      %s\n  configs      %s\n\n' "${FLEET_COMPOSE_FILE}" "${FLEET_CONFIG_DIR}"
}

main() {
  if [[ -n "${RENDER_ONLY}" ]]; then
    AENVIO_GID="${AENV_FLEET_AENVIO_GID:-1000}"
    NODE_IMAGE="${FLEET_SERVER_BIN:+${FLEET_BIN_BASE_IMAGE}}"
    NODE_IMAGE="${NODE_IMAGE:-$(fleet_image_ref agentenv-runtime)}"
    ensure_dirs
    ensure_env
    ensure_slice
    render_configs
    render_compose
    log "render-only: wrote host ${FLEET_ROLE} fleet under ${FLEET_ROOT} (nothing applied)"
    return 0
  fi

  preflight_host
  preflight_firewall
  preflight_ports
  ensure_dirs
  ensure_env
  preflight_peer
  ensure_slice
  ensure_images
  render_configs
  render_compose
  compose_up
  gate_control_plane
  gate_units
  gate_joined
  print_summary
}

main
