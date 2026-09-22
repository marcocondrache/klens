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
prefixed="$(curl -sS -o /dev/null -w '%{http_code}' "$KLENS_VERIFY_URL/api/health")"
[[ "$prefixed" == "204" ]] || fail "GET /api/health returned $prefixed, expected 204"

me="$(curl -sS "$KLENS_VERIFY_URL/api/auth/me")"
echo "$me" | grep -q '"enabled":false' || fail "/api/auth/me enabled is not false: $me"
echo "$me" | grep -q '"user":null' || fail "/api/auth/me user is not null: $me"

html="$(curl -sS "$KLENS_VERIFY_URL/")"
echo "$html" | grep -q '<title>klens</title>' || fail "GET / is missing <title>klens</title>"

clusters=""
for _ in $(seq 1 40); do
  clusters="$(curl -sS "$KLENS_VERIFY_URL/api/clusters")"
  echo "$clusters" | grep -q "\"cluster\":\"${KLENS_VERIFY_CLUSTER}\"" \
    || fail "api clusters missing ${KLENS_VERIFY_CLUSTER}: $clusters"
  if echo "$clusters" | grep -q '"updatedAt":"' && ! echo "$clusters" | grep -q '"lastError":"'; then
    echo "doctor: ok"
    echo "  pid $pid"
    echo "  url $KLENS_VERIFY_URL"
    echo "  auth $me"
    echo "  clusters $clusters"
    exit 0
  fi
  sleep 0.25
done
fail "clusters topology did not become ready: $clusters"
