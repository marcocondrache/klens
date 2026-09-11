# Shared paths and defaults for verify-klens helpers.
# shellcheck shell=bash

set -euo pipefail

helpers_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
skill_dir="$(cd "$helpers_dir/.." && pwd)"
repo_root="$(cd "$skill_dir/../.." && pwd)"

: "${KLENS_VERIFY_RUN_DIR:="$skill_dir/run"}"
: "${KLENS_VERIFY_PORT:=18080}"
: "${KLENS_VERIFY_BIND:=127.0.0.1}"
: "${KLENS_VERIFY_BROKERS:=127.0.0.1:9092}"
: "${KLENS_VERIFY_CLUSTER:=local}"
: "${KLENS_VERIFY_TOPIC:=klens-verify-topics}"
: "${KLENS_VERIFY_KEY:=verify-1}"
: "${KLENS_VERIFY_VALUE:=hello-from-verify-klens}"
: "${RPK:=rpk}"
: "${CHROME:=/usr/bin/google-chrome}"

run_dir="$KLENS_VERIFY_RUN_DIR"
pid_file="$run_dir/klens.pid"
env_file="$run_dir/env"
config_file="$run_dir/config.yaml"
log_file="$run_dir/klens.log"
kafka_flag="$run_dir/started_kafka"
url="http://${KLENS_VERIFY_BIND}:${KLENS_VERIFY_PORT}"

mkdir -p "$run_dir"

write_env() {
  cat >"$env_file" <<EOF
KLENS_VERIFY_URL=$url
KLENS_VERIFY_PID=$(cat "$pid_file" 2>/dev/null || true)
KLENS_VERIFY_PORT=$KLENS_VERIFY_PORT
KLENS_VERIFY_BIND=$KLENS_VERIFY_BIND
KLENS_VERIFY_BROKERS=$KLENS_VERIFY_BROKERS
KLENS_VERIFY_CLUSTER=$KLENS_VERIFY_CLUSTER
KLENS_VERIFY_TOPIC=$KLENS_VERIFY_TOPIC
KLENS_CONFIG_PATH=$config_file
ARTIFACT_DIR=${ARTIFACT_DIR:-}
EOF
}

load_env() {
  if [[ ! -f "$env_file" ]]; then
    echo "doctor: missing $env_file (run helpers/launch.sh first)" >&2
    return 1
  fi
  # shellcheck disable=SC1090
  source "$env_file"
}

pid_alive() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

port_open() {
  local host="$1"
  local port="$2"
  if command -v python3 >/dev/null; then
    python3 - "$host" "$port" <<'PY'
import socket, sys
host, port = sys.argv[1], int(sys.argv[2])
s = socket.socket()
s.settimeout(1)
try:
    s.connect((host, port))
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
    return
  fi
  (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1
}
