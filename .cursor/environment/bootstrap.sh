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
  sudo service docker start
  until docker info >/dev/null 2>&1; do
    sleep 0.2
  done

  mise kafka:up
}

case "${1:-}" in
  install) install ;;
  start) start ;;
  *) usage ;;
esac
