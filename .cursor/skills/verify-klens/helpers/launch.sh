#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if [[ -f "$pid_file" ]] && pid_alive "$(cat "$pid_file")"; then
  echo "launch: refuse to start. $pid_file is alive ($(cat "$pid_file")). Use a new KLENS_VERIFY_RUN_DIR or run cleanup.sh." >&2
  exit 2
fi

run_id="${KLENS_VERIFY_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
ARTIFACT_DIR="${ARTIFACT_DIR:-"$skill_dir/artifacts/$run_id"}"
mkdir -p "$ARTIFACT_DIR"

export PATH="$HOME/.bun/bin:${PATH}"

if [[ ! -f "$repo_root/web/node_modules/.bin/vp" ]]; then
  (cd "$repo_root/web" && bun install --frozen-lockfile)
fi

if [[ ! -f "$repo_root/static/index.html" ]]; then
  (cd "$repo_root/web" && bun run build)
  test -f "$repo_root/static/index.html"
fi

if [[ ! -x "$repo_root/target/debug/klens" ]]; then
  (cd "$repo_root" && cargo build --locked --features ui)
fi

cat >"$config_file" <<EOF
bind: ${KLENS_VERIFY_BIND}:${KLENS_VERIFY_PORT}
log_level: info
clusters:
  - name: ${KLENS_VERIFY_CLUSTER}
    bootstrap_servers:
      - ${KLENS_VERIFY_BROKERS}
    schema_registry:
      url: http://127.0.0.1:8081
EOF

if port_open 127.0.0.1 9092; then
  echo "launch: kafka already listening on 127.0.0.1:9092"
  rm -f "$kafka_flag"
else
  if ! command -v "$RPK" >/dev/null; then
    echo "launch: port 9092 is closed and rpk is not on PATH. Start a broker or install rpk (mise.toml kafka:up)." >&2
    exit 1
  fi
  echo "launch: starting redpanda via rpk container start"
  "$RPK" container start --kafka-ports 9092 --schema-registry-ports 8081 --console-port 8083
  echo 1 >"$kafka_flag"
fi

"$RPK" topic create "$KLENS_VERIFY_TOPIC" -p 1 -r 1 -X "brokers=$KLENS_VERIFY_BROKERS" >/dev/null 2>&1 || true
printf '%s\n' "$KLENS_VERIFY_VALUE" | "$RPK" topic produce "$KLENS_VERIFY_TOPIC" \
  -k "$KLENS_VERIFY_KEY" -X "brokers=$KLENS_VERIFY_BROKERS"

export KLENS_CONFIG_PATH="$config_file"
: >"$log_file"
"$repo_root/target/debug/klens" >>"$log_file" 2>&1 &
echo $! >"$pid_file"
write_env
echo "KLENS_VERIFY_RUN_ID=$run_id" >>"$env_file"
echo "ARTIFACT_DIR=$ARTIFACT_DIR" >>"$env_file"

echo "launch: pid $(cat "$pid_file") url $url"

for _ in $(seq 1 40); do
  if curl -sS -o /dev/null -w '%{http_code}' "$url/health" 2>/dev/null | grep -qx 204; then
    echo "launch: ready (GET /health 204)"
    echo "$run_id"
    exit 0
  fi
  if ! pid_alive "$(cat "$pid_file")"; then
    echo "launch: process exited. last log lines:" >&2
    tail -n 40 "$log_file" >&2
    exit 1
  fi
  sleep 0.25
done

echo "launch: timed out waiting for /health. log:" >&2
tail -n 40 "$log_file" >&2
exit 1
