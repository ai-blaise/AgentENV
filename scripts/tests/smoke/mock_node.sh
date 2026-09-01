#!/usr/bin/env bash
# Starts one agentenv node with the mock sandbox backend on the current host
# and proves the control plane works without a hypervisor: the API answers,
# a cold create returns 201, the sandbox is listed running, and every mock
# safety rail is visible in the log.
#
# Needs only a built `server` binary and a writable scratch directory. It is
# what the fleet bootstrap runs on each unit before joining it to a cluster.
#
#   scripts/tests/smoke/mock_node.sh [path/to/server] [port]
set -euo pipefail

SERVER_BIN="${1:-target/debug/server}"
PORT="${2:-19000}"
SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/aenv-mock-smoke.XXXXXX")"
trap 'kill "${PID:-}" 2>/dev/null || true; rm -rf "$SCRATCH"' EXIT

cp config/default.toml "$SCRATCH/config.toml"
sed -i.bak -e 's/^backend = "firecracker"/backend = "mock"/' "$SCRATCH/config.toml"
# No slots exist without guests, and priming them writes host veths.
python3 - "$SCRATCH/config.toml" <<'PY'
import sys
path = sys.argv[1]
text = open(path).read()
head = text.index("[pool.network]")
key = text.index("maintenance_enabled = true", head)
text = text[:key] + "maintenance_enabled = false" + text[key + len("maintenance_enabled = true"):]
open(path, "w").write(text)
PY

export AENV_API_KEY="mocksmoke0123456789abcdefghijklmnopqrstuv"
export AENV_CONFIG_PATH="$SCRATCH/config.toml"
export AENV_HOME_PATH="$SCRATCH/home"
export AENV_RUNTIME_PATH="$SCRATCH/run"
export API_ADDR="127.0.0.1:$PORT"
mkdir -p "$AENV_HOME_PATH" "$AENV_RUNTIME_PATH"

"$SERVER_BIN" >"$SCRATCH/server.log" 2>&1 &
PID=$!

api() { curl -sS -m 30 -H "x-api-key: $AENV_API_KEY" "$@"; }
for _ in $(seq 1 60); do
  if api -o /dev/null "http://$API_ADDR/sandboxes" 2>/dev/null; then break; fi
  kill -0 "$PID" 2>/dev/null || { echo "server exited during startup:" >&2; cat "$SCRATCH/server.log" >&2; exit 1; }
  sleep 1
done

created="$(api -X POST "http://$API_ADDR/sandboxes-cold" -H 'Content-Type: application/json' \
  -d '{"image":"ubuntu:24.04"}' -w '\n%{http_code}')"
code="${created##*$'\n'}"
[ "$code" = "201" ] || { echo "cold create returned $code: $created" >&2; exit 1; }
sandbox_id="$(printf '%s' "${created%$'\n'*}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["sandboxID"])')"

api "http://$API_ADDR/sandboxes" | grep -q "\"sandboxID\":\"$sandbox_id\"" \
  || { echo "created sandbox $sandbox_id is not listed" >&2; exit 1; }

# The rails are the point: a mock node that could be mistaken for a real one
# is worse than no mock node.
for rail in 'sandbox backend is "mock"' 'building a MOCK sandbox' 'image resolved to an empty placeholder'; do
  grep -q "$rail" "$SCRATCH/server.log" || { echo "missing safety rail in log: $rail" >&2; exit 1; }
done

echo "mock node OK: sandbox $sandbox_id created and listed; all safety rails logged"
