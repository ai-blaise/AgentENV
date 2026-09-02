#!/usr/bin/env bash
# Stand-in for the `server` binary that scripts/tests/smoke/mock_node.sh
# launches. It runs the server in exactly one fleet unit's shape — same image,
# cpuset, memory cap, slice, host network namespace, io_uring group and dropped
# capabilities — so the smoke proves the shape a unit will run in, not a bare
# binary on the host.
#
# mock_node.sh owns the environment (AENV_CONFIG_PATH, AENV_HOME_PATH,
# AENV_RUNTIME_PATH, API_ADDR, AENV_API_KEY) and a scratch directory that holds
# the config it rendered; that directory is bind-mounted at the same path so
# the config it wrote is the config the container reads. SIGTERM from
# mock_node.sh reaches the server through the docker client's signal proxy.
set -euo pipefail

: "${AENV_FLEET_SMOKE_IMAGE:?set by scripts/fleet/bootstrap.sh}"
: "${AENV_FLEET_SMOKE_CPUSET:?set by scripts/fleet/bootstrap.sh}"
: "${AENV_FLEET_SMOKE_MEMORY:?set by scripts/fleet/bootstrap.sh}"
: "${AENV_FLEET_SMOKE_GID:?set by scripts/fleet/bootstrap.sh}"
: "${AENV_FLEET_SMOKE_NAME:?set by scripts/fleet/bootstrap.sh}"
: "${AENV_CONFIG_PATH:?set by mock_node.sh}"
: "${AENV_HOME_PATH:?set by mock_node.sh}"
: "${AENV_RUNTIME_PATH:?set by mock_node.sh}"
: "${API_ADDR:?set by mock_node.sh}"
: "${AENV_API_KEY:?set by mock_node.sh}"

scratch="$(dirname "${AENV_CONFIG_PATH}")"

args=(
  run --rm --init
  --name "${AENV_FLEET_SMOKE_NAME}"
  --network host
  --cgroup-parent "${AENV_FLEET_SMOKE_SLICE:-aenvfleet.slice}"
  --cpuset-cpus "${AENV_FLEET_SMOKE_CPUSET}"
  --memory "${AENV_FLEET_SMOKE_MEMORY}"
  --group-add "${AENV_FLEET_SMOKE_GID}"
  --cap-drop ALL
  --security-opt no-new-privileges:true
  --ulimit nofile=65536:65536
  -v "${scratch}:${scratch}"
  -e AENV_API_KEY -e AENV_CONFIG_PATH -e AENV_HOME_PATH -e AENV_RUNTIME_PATH -e API_ADDR
  -e "RUST_LOG=${RUST_LOG:-agentenv=info}"
  --entrypoint /server
)
if [[ -n "${AENV_FLEET_SERVER_BIN:-}" ]]; then
  args+=(-v "${AENV_FLEET_SERVER_BIN}:/server:ro")
fi
args+=("${AENV_FLEET_SMOKE_IMAGE}")

exec docker "${args[@]}"
