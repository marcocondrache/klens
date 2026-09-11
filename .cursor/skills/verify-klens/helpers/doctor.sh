#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"
load_env

fail() {
  echo "doctor: $1" >&2
  exit 1
}

pid="${KLENS_VERIFY_PID:-}"
[[ -n "$pid" ]] || fail "no KLENS_VERIFY_PID in $env_file"
pid_alive "$pid" || fail "pid $pid is not running"

comm="$(tr -d '\0' <"/proc/$pid/comm" || true)"
[[ "$comm" == "klens" ]] || fail "pid $pid comm is '$comm', expected klens"

if command -v ss >/dev/null; then
  ss -lntp 2>/dev/null | grep -E ":${KLENS_VERIFY_PORT}\\b" | grep -q "pid=$pid" \
    || fail "port ${KLENS_VERIFY_PORT} is not owned by pid $pid"
fi

health="$(curl -sS -o /dev/null -w '%{http_code}' "$KLENS_VERIFY_URL/health")"
[[ "$health" == "204" ]] || fail "GET /health returned $health, expected 204"

me="$(curl -sS "$KLENS_VERIFY_URL/auth/me")"
echo "$me" | grep -q '"enabled":false' || fail "/auth/me enabled is not false: $me"
echo "$me" | grep -q '"user":null' || fail "/auth/me user is not null: $me"

html="$(curl -sS "$KLENS_VERIFY_URL/")"
echo "$html" | grep -q '<title>klens</title>' || fail "GET / is missing <title>klens</title>"

clusters="$(curl -sS -X POST "$KLENS_VERIFY_URL/graphql" \
  -H 'content-type: application/json' \
  -d '{"query":"query { clusters { name status topicCount } }"}')"
echo "$clusters" | grep -q "\"name\":\"${KLENS_VERIFY_CLUSTER}\"" \
  || fail "graphql clusters missing ${KLENS_VERIFY_CLUSTER}: $clusters"
echo "$clusters" | grep -q '"status":"HEALTHY"' \
  || fail "cluster is not HEALTHY: $clusters"

echo "doctor: ok"
echo "  pid $pid"
echo "  url $KLENS_VERIFY_URL"
echo "  auth $me"
echo "  clusters $clusters"
