#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

mkdir -p "$RUN_DIR"
exec 9>"$RUN_DIR/launch.lock"
if ! flock -n 9; then
  die "refusing to start a second klens in $RUN_DIR"
fi

if [[ -f "$RUN_DIR/pid" ]]; then
  existing=$(tr -d '[:space:]' <"$RUN_DIR/pid")
  if pid_alive "$existing"; then
    die "refusing to start a second klens in $RUN_DIR (pid $existing)"
  fi
fi

if port_open 127.0.0.1 "$PORT"; then
  die "port $PORT is already in use. Set KLENS_VERIFY_PORT and KLENS_VERIFY_RUN_DIR together for another run."
fi

build_ui() {
  if [[ "${KLENS_VERIFY_SKIP_BUILD:-}" == "1" ]]; then
    [[ -x "$ROOT/target/debug/klens" ]] || die "KLENS_VERIFY_SKIP_BUILD=1 but $ROOT/target/debug/klens is missing"
    [[ -f "$ROOT/static/index.html" ]] || die "KLENS_VERIFY_SKIP_BUILD=1 but static/index.html is missing. The ui feature embeds that directory."
    return
  fi

  if command -v mise >/dev/null 2>&1; then
    (cd "$ROOT" && mise run web:build)
  elif command -v bun >/dev/null 2>&1; then
    (cd "$ROOT/web" && bun install && bun run build)
  elif command -v npm >/dev/null 2>&1; then
    (cd "$ROOT/web" && npm install && npm run build)
  else
    die "need mise, bun, or npm on PATH to build web/"
  fi

  [[ -f "$ROOT/static/index.html" ]] || die "web build did not write static/index.html"

  local embed="$ROOT/src/server/web.rs"
  local backup status
  backup=$(mktemp)
  cp "$embed" "$backup"
  touch "$embed"
  set +e
  (cd "$ROOT" && cargo build --locked --features ui)
  status=$?
  set -e
  cp "$backup" "$embed"
  rm -f "$backup"
  [[ "$status" -eq 0 ]] || die "cargo build --locked --features ui failed"
}

container_runtime() {
  if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    return 0
  fi
  if command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

broker_host=${BROKERS%%:*}
broker_port=${BROKERS##*:}

start_kafka() {
  if port_open "$broker_host" "$broker_port"; then
    printf '0\n' >"$RUN_DIR/started-kafka"
    return
  fi

  if command -v mise >/dev/null 2>&1 && container_runtime; then
    (cd "$ROOT" && mise run kafka:up)
    printf 'container\n' >"$RUN_DIR/started-kafka"
  elif command -v rpk >/dev/null 2>&1 && container_runtime; then
    rpk container start --kafka-ports 9092 --schema-registry-ports 8081 --console-port 8002
    printf 'container\n' >"$RUN_DIR/started-kafka"
  elif command -v rpk >/dev/null 2>&1 && rpk redpanda start --help >/dev/null 2>&1; then
    start_redpanda_binary
    printf 'binary\n' >"$RUN_DIR/started-kafka"
  else
    die "port ${broker_port} is closed. Start Kafka with mise run kafka:up, or put rpk on PATH with Docker, Podman, or a local redpanda binary."
  fi

  local i
  for i in $(seq 1 60); do
    if port_open "$broker_host" "$broker_port"; then
      return
    fi
    sleep 1
  done
  die "Kafka did not accept connections on ${BROKERS}"
}

start_redpanda_binary() {
  local rpc=33145
  local admin=9644
  if port_open 127.0.0.1 "$rpc" || port_open 127.0.0.1 "$admin"; then
    die "Redpanda binary start needs free ports ${rpc} and ${admin}"
  fi

  mkdir -p "$RUN_DIR/redpanda-data" "$RUN_DIR/redpanda-coredump"
  cat >"$RUN_DIR/redpanda.yaml" <<EOF
redpanda:
  data_directory: ${RUN_DIR}/redpanda-data
  seed_servers: []
  rpc_server:
    address: 127.0.0.1
    port: ${rpc}
  kafka_api:
    - address: ${broker_host}
      port: ${broker_port}
  admin:
    - address: 127.0.0.1
      port: ${admin}
  developer_mode: true
rpk:
  overprovisioned: true
  coredump_dir: ${RUN_DIR}/redpanda-coredump
EOF

  setsid rpk redpanda start \
    --config "$RUN_DIR/redpanda.yaml" \
    --mode dev-container \
    --check=false \
    >"$RUN_DIR/kafka.log" 2>&1 < /dev/null &
  printf '%s\n' "$!" >"$RUN_DIR/kafka.pid"
}

seed_topic() {
  command -v rpk >/dev/null 2>&1 || die "rpk is required to seed ${TOPIC}"
  rpk topic create "$TOPIC" -p 1 -r 1 -X "brokers=${BROKERS}" >/dev/null 2>&1 || true
  printf '%s\n' "$RECORD_VALUE" | rpk topic produce "$TOPIC" -k "$RECORD_KEY" -X "brokers=${BROKERS}" \
    >/dev/null
}

write_config() {
  cat >"$RUN_DIR/config.yaml" <<EOF
bind: 127.0.0.1:${PORT}
log_level: info
clusters:
  - name: ${CLUSTER}
    bootstrap_servers:
      - ${BROKERS}
EOF

  if port_open 127.0.0.1 8081; then
    cat >>"$RUN_DIR/config.yaml" <<EOF
    schema_registry:
      url: http://127.0.0.1:8081
EOF
    printf '1\n' >"$RUN_DIR/schema-registry"
  else
    printf '0\n' >"$RUN_DIR/schema-registry"
  fi
}

wait_ready() {
  local i code
  for i in $(seq 1 90); do
    if [[ -f "$RUN_DIR/pid" ]]; then
      local pid
      pid=$(tr -d '[:space:]' <"$RUN_DIR/pid")
      if ! pid_alive "$pid"; then
        tail -n 40 "$RUN_DIR/klens.log" >&2 || true
        die "klens exited before it was ready"
      fi
    fi
    code=$(http_code "$URL/health")
    if [[ "$code" == "204" ]]; then
      code=$(http_code "$URL/ready")
      if [[ "$code" == "204" ]]; then
        return
      fi
    fi
    sleep 1
  done
  tail -n 40 "$RUN_DIR/klens.log" >&2 || true
  die "klens did not become ready on $URL"
}

build_ui
start_kafka
seed_topic
write_config

run_id=$(date -u +%Y%m%dT%H%M%SZ)
printf '%s\n' "$run_id" >"$RUN_DIR/run-id"
printf '%s\n' "$URL" >"$RUN_DIR/url"
printf '%s\n' "$PORT" >"$RUN_DIR/port"
printf '%s\n' "$CLUSTER" >"$RUN_DIR/cluster"
printf '%s\n' "$TOPIC" >"$RUN_DIR/topic"
printf '%s\n' "$RECORD_KEY" >"$RUN_DIR/record-key"
printf '%s\n' "$RECORD_VALUE" >"$RUN_DIR/record-value"

KLENS_CONFIG_PATH="$RUN_DIR/config.yaml" "$ROOT/target/debug/klens" >"$RUN_DIR/klens.log" 2>&1 &
printf '%s\n' "$!" >"$RUN_DIR/pid"
wait_ready

printf 'klens ready at %s (run %s)\n' "$URL" "$run_id"
