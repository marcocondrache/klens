#!/usr/bin/env bash
# Idempotent Cloud Agent setup for klens. All dependencies are installed with
# mise (see mise.toml: rust, node, bun, vendir, viteplus, java, and the
# confluent CLI). The confluent CLI runs Kafka + Schema Registry locally via
# `confluent local`, so no Docker is required. Safe to run repeatedly.
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$PWD"

echo "==> Installing mise"
if ! command -v mise >/dev/null 2>&1 && [ ! -x "$HOME/.local/bin/mise" ]; then
  curl -fsSL https://mise.run | sh
fi
export PATH="$HOME/.local/bin:$PATH"
export MISE_YES=1
mise --version

echo "==> Installing the project toolchain via mise"
mise trust "$REPO_ROOT"
mise install
mise ls

# Put the mise-managed tools and env (CONFLUENT_HOME, RUSTUP_TOOLCHAIN, PATH)
# in scope for the rest of this script.
eval "$(mise env -s bash)"

echo "==> Downloading the Confluent Platform archive for confluent local"
mise run confluent:install

echo "==> Building the web UI (bun) and the klens backend (cargo, ui feature)"
# The `ui` feature embeds static/, so build the web UI first. bun runs the
# pinned, lockfile-resolved vite-plus toolchain from web/node_modules.
pushd web >/dev/null
bun install --frozen-lockfile
bun run codegen
bun run build
popd >/dev/null
test -f "$REPO_ROOT/static/index.html"
cargo build --features ui

echo "==> Install complete"
