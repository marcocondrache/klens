#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if [[ ! -d "$RUN_DIR" ]]; then
  printf 'cleanup ok (no run directory)\n'
  exit 0
fi

kill_tree() {
  local pid="$1"
  local child
  if ! pid_alive "$pid"; then
    return
  fi
  for child in $(pgrep -P "$pid" 2>/dev/null || true); do
    kill_tree "$child"
  done
  kill -TERM "$pid" 2>/dev/null || true
}

if [[ -f "$RUN_DIR/pid" ]]; then
  pid=$(tr -d '[:space:]' <"$RUN_DIR/pid")
  if pid_alive "$pid"; then
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
      pid_alive "$pid" || break
      sleep 0.2
    done
    if pid_alive "$pid"; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
  fi
fi

mode=$(tr -d '[:space:]' <"$RUN_DIR/started-kafka" 2>/dev/null || true)
case "$mode" in
  container)
    if command -v mise >/dev/null 2>&1; then
      (cd "$ROOT" && mise run kafka:down) || true
    elif command -v rpk >/dev/null 2>&1; then
      rpk container stop || true
    fi
    ;;
  binary)
    if [[ -f "$RUN_DIR/kafka.pid" ]]; then
      kafka_pid=$(tr -d '[:space:]' <"$RUN_DIR/kafka.pid")
      kill_tree "$kafka_pid"
      sleep 0.5
      if pid_alive "$kafka_pid"; then
        kill -KILL "$kafka_pid" 2>/dev/null || true
      fi
    fi
    ;;
esac

rm -rf "$RUN_DIR"
printf 'cleanup ok\n'
