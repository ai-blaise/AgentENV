#!/usr/bin/env bash
# Demonstration: a preStop drain-loop pin that fits the lane `make test-unit`
# already runs (alongside verify-install-service.sh, which extracts a heredoc
# out of scripts/install.sh the same way). No cluster, no /dev/kvm, no cargo.
set -euo pipefail

# Default to the repository this script ships in, so `make test-unit` can call
# it with no arguments the way it calls its siblings.
repo_root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
manifest="${repo_root}/deploy/k8s/base/agentenv-daemonset.yaml"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
hook="${tmp_dir}/prestop.sh"

# Extract the preStop literal-block scalar, dedented.
awk '
  /^[[:space:]]*preStop:/ { inhook = 1 }
  inhook && /^[[:space:]]*postStart:/ { exit }
  inhook && capture {
    if (indent == 0 && $0 ~ /[^[:space:]]/) {
      match($0, /^[[:space:]]*/); indent = RLENGTH
    }
    print substr($0, indent + 1)
    next
  }
  inhook && /^[[:space:]]*- \|[[:space:]]*$/ { capture = 1 }
' "$manifest" > "$hook"

[ -s "$hook" ] || { echo "no preStop script found in $manifest" >&2; exit 1; }
sh -n "$hook"

# Stubs: curl answers with $RESPONSE, jq reads one field, date advances 400s a
# call so the 3000s give_up is reached in eight passes, sleep is recorded.
mkdir -p "${tmp_dir}/bin"
cat > "${tmp_dir}/bin/curl" <<'EOF'
#!/bin/sh
echo curl >> "$STUBDIR/curls"
printf '%s' "$RESPONSE"
EOF
cat > "${tmp_dir}/bin/jq" <<'EOF'
#!/bin/sh
field=$(printf '%s' "$1" | sed 's/^\.//')
cat > "$STUBDIR/body"
sed -n "s/.*\"$field\":\([0-9]*\).*/\1/p" "$STUBDIR/body"
EOF
cat > "${tmp_dir}/bin/date" <<'EOF'
#!/bin/sh
if [ "$1" = "+%s" ]; then
  n=$(cat "$STUBDIR/clock" 2>/dev/null || echo 0)
  echo $(( 1000000000 + n * 400 ))
  echo $(( n + 1 )) > "$STUBDIR/clock"
else
  /bin/date "$@"
fi
EOF
cat > "${tmp_dir}/bin/sleep" <<'EOF'
#!/bin/sh
echo "$1" >> "$STUBDIR/sleeps"
EOF
chmod +x "${tmp_dir}/bin/"*

# $1 canned drain answer -> "<passes> <sleeps> <last line>"
run_hook() {
  STUBDIR="$(mktemp -d)"
  export STUBDIR
  RESPONSE="$1" AENV_API_KEY=stub PATH="${tmp_dir}/bin:$PATH" sh "$hook" > "$STUBDIR/out" 2>&1
  # The stubs only create their files once called, and a redirect from a
  # missing file is a shell error before wc ever runs -- noise that reads like
  # a failure in a gate script.
  count_lines() { [ -f "$1" ] && wc -l < "$1" || echo 0; }
  passes=$(count_lines "$STUBDIR/curls")
  sleeps=$(count_lines "$STUBDIR/sleeps")
  last=$(tail -1 "$STUBDIR/out")
  echo "$(echo $passes) $(echo $sleeps) $last"
  rm -rf "$STUBDIR"
}

fail() { echo "FAIL: $*" >&2; exit 1; }

# A publish that failed leaves its sandbox Paused, so it drops out of
# `remaining`. Exiting there would let the pod be killed with that sandbox's
# state still only on this node.
read -r passes sleeps last <<< "$(run_hook '{"remaining":0,"published":0,"failed":1}')"
[ "$passes" -gt 1 ] || fail "a failed publish must not end the drain after one pass (passes=$passes, last=$last)"

# One sandbox that will not pause keeps `remaining` non-zero for the whole
# window; a loop with no delay re-POSTs continuously for it.
read -r passes sleeps last <<< "$(run_hook '{"remaining":1,"published":0,"failed":0}')"
[ "$sleeps" -ge $(( passes - 1 )) ] || fail "the loop must sleep between passes (passes=$passes sleeps=$sleeps)"

# And a node that really is empty still exits on the first pass.
read -r passes sleeps last <<< "$(run_hook '{"remaining":0,"published":2,"failed":0}')"
[ "$passes" -eq 1 ] || fail "a drained node must exit after one pass (passes=$passes)"

echo "verify-prestop-drain: OK"
