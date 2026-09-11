#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

usage() {
  echo "usage: $0 <install|start>" >&2
  exit 1
}

install() {
  mise install
  mise run web:codegen
  cargo fetch --locked
}

start() {
  if ! docker info >/dev/null 2>&1; then
    # No systemd/sysvinit docker service in Cloud Agent VMs — start dockerd directly.
    sudo dockerd >/tmp/dockerd.log 2>&1 &
    for _ in $(seq 1 150); do
      if docker info >/dev/null 2>&1; then
        break
      fi
      sleep 0.2
    done
    if ! docker info >/dev/null 2>&1; then
      echo "dockerd failed to become ready; last log lines:" >&2
      tail -n 50 /tmp/dockerd.log >&2 || true
      exit 1
    fi
  fi

  # Redpanda Console health can flake (not required for klens); wait on the Kafka API.
  if ! rpk cluster info -X brokers=127.0.0.1:9092 >/dev/null 2>&1; then
    mise kafka:up || true
    for _ in $(seq 1 90); do
      if rpk cluster info -X brokers=127.0.0.1:9092 >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
    if ! rpk cluster info -X brokers=127.0.0.1:9092 >/dev/null 2>&1; then
      echo "Kafka broker failed to become ready on 127.0.0.1:9092" >&2
      exit 1
    fi
  fi
}

case "${1:-}" in
  install) install ;;
  start) start ;;
  *) usage ;;
esac
