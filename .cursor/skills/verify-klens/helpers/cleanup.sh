#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if [[ ! -f "$pid_file" ]]; then
  echo "cleanup: no pid file at $pid_file"
else
  pid="$(cat "$pid_file")"
  if pid_alive "$pid"; then
    comm="$(tr -d '\0' <"/proc/$pid/comm" || true)"
    if [[ "$comm" != "klens" ]]; then
      echo "cleanup: refuse to signal pid $pid (comm=$comm, expected klens)" >&2
      exit 2
    fi
    echo "cleanup: SIGTERM pid $pid"
    kill -TERM "$pid" || true
    for _ in $(seq 1 20); do
      pid_alive "$pid" || break
      sleep 0.2
    done
    if pid_alive "$pid"; then
      echo "cleanup: SIGKILL pid $pid"
      kill -KILL "$pid" || true
    fi
  else
    echo "cleanup: pid $pid already gone"
  fi
fi

if [[ -f "$kafka_flag" ]]; then
  if command -v "$RPK" >/dev/null; then
    echo "cleanup: stopping rpk container cluster this run started"
    "$RPK" container stop || true
  fi
fi

if [[ -n "${KLENS_VERIFY_RUN_DIR:-}" && -d "$run_dir" ]]; then
  echo "cleanup: removing $run_dir"
  rm -rf "$run_dir"
fi

echo "cleanup: artifacts left in $skill_dir/artifacts"
