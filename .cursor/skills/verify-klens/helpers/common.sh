set -euo pipefail

HELPER_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SKILL_DIR=$(cd "$HELPER_DIR/.." && pwd)
ROOT=$(cd "$SKILL_DIR/../../.." && pwd)

RUN_DIR=${KLENS_VERIFY_RUN_DIR:-$SKILL_DIR/run}
PORT=${KLENS_VERIFY_PORT:-18080}
BROKERS=${KLENS_VERIFY_BROKERS:-127.0.0.1:9092}
URL="http://127.0.0.1:${PORT}"
CLUSTER=local
TOPIC=klens-verify-topics
RECORD_KEY=verify-1
RECORD_VALUE=hello-from-verify-klens

die() {
  printf 'verify-klens: %s\n' "$*" >&2
  exit 1
}

port_open() {
  local host="$1"
  local port="$2"
  timeout 1 bash -c "echo >/dev/tcp/${host}/${port}" >/dev/null 2>&1
}

pid_alive() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

read_pid() {
  [[ -f "$RUN_DIR/pid" ]] || die "no pid file in $RUN_DIR. Run helpers/launch.sh first."
  tr -d '[:space:]' <"$RUN_DIR/pid"
}

recorded_url() {
  [[ -f "$RUN_DIR/url" ]] || die "no url file in $RUN_DIR. Run helpers/launch.sh first."
  tr -d '[:space:]' <"$RUN_DIR/url"
}

http_code() {
  curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "$1" || true
}
