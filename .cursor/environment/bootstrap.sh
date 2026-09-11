#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"

if ! docker info >/dev/null 2>&1; then
  sudo service docker start
fi

mise kafka:up
mise run web:codegen

cargo fetch --locked
