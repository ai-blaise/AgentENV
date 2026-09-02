#!/usr/bin/env bash
# Checks that the shipped Kubernetes manifests reference only files that exist.
#
# `deploy/k8s/base/kustomization.yaml` named `config/agentenv.toml` for the whole
# life of the repository and that file was never committed, so `kubectl
# kustomize` -- and therefore `deploy/k8s/run.sh render|apply` -- failed on the
# shipped manifests. Nothing caught it because nothing in the gate built them.
#
# Deliberately does not require kubectl or kustomize: the check that would have
# caught this needs neither, and a check that only runs where a tool happens to
# be installed is a check that does not run. When kubectl is available the real
# build runs too.
set -euo pipefail

repo_root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
failed=0

for kustomization in $(find "${repo_root}/deploy/k8s" -name kustomization.yaml | sort); do
  dir="$(dirname "$kustomization")"
  # Every `- <path>` under resources:/files: is a path relative to the
  # kustomization. Directories are kustomizations in their own right and are
  # covered by their own iteration of this loop.
  while read -r entry; do
    [ -n "$entry" ] || continue
    case "$entry" in
      /*|*://*) continue ;;
    esac
    if [ ! -e "${dir}/${entry}" ]; then
      echo "FAIL: ${kustomization#"$repo_root"/} references ${entry}, which does not exist" >&2
      failed=1
    fi
  done < <(sed -n 's/^[[:space:]]*-[[:space:]]*\([A-Za-z0-9._][A-Za-z0-9._/-]*\)[[:space:]]*$/\1/p' "$kustomization")
done

[ "$failed" -eq 0 ] || exit 1

if command -v kubectl >/dev/null 2>&1; then
  for overlay in "${repo_root}"/deploy/k8s/overlays/*/; do
    [ -f "${overlay}kustomization.yaml" ] || continue
    if ! kubectl kustomize "$overlay" >/dev/null 2>"${TMPDIR:-/tmp}/kustomize-err.$$"; then
      echo "FAIL: kubectl kustomize ${overlay#"$repo_root"/} failed:" >&2
      head -5 "${TMPDIR:-/tmp}/kustomize-err.$$" >&2
      rm -f "${TMPDIR:-/tmp}/kustomize-err.$$"
      exit 1
    fi
    rm -f "${TMPDIR:-/tmp}/kustomize-err.$$"
  done
  echo "verify-k8s-manifests: OK (references resolve; every overlay builds)"
else
  echo "verify-k8s-manifests: OK (references resolve; kubectl absent, build not attempted)"
fi
