#!/usr/bin/env bash
# Container healthcheck for a fleet node unit: GET /health must answer 204.
# Bash's /dev/tcp instead of curl, so the same check works whether the unit
# runs the agentenv-runtime image or a bare base image with a bind-mounted
# release binary. Units share the host network namespace, so 127.0.0.1:<port>
# is the unit's own listener.
set -euo pipefail

port="${1:?usage: node_healthcheck.sh <port>}"
exec 3<>"/dev/tcp/127.0.0.1/${port}"
printf 'GET /health HTTP/1.0\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n' >&3
IFS= read -r -t 5 status <&3
exec 3>&-
[[ "${status}" == *" 204 "* ]]
