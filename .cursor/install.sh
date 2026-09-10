#!/usr/bin/env bash
# Idempotent Cloud Agent setup for klens. Dependencies are installed with mise
# (see mise.toml: rust, node, bun, vendir, viteplus). Docker is the only
# dependency mise cannot manage, since it is a system daemon rather than a tool.
# Safe to run repeatedly.
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

echo "==> Installing the project toolchain via mise (rust, node, bun, vendir, viteplus)"
mise trust "$REPO_ROOT"
mise install
mise ls

# Put the mise-managed tools (and RUSTUP_TOOLCHAIN) on PATH for this script.
eval "$(mise env -s bash)"

echo "==> Ensuring Docker Engine (system daemon; not managed by mise)"
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com -o /tmp/get-docker.sh
  sudo sh /tmp/get-docker.sh
  sudo usermod -aG docker "$(id -un)" || true
fi
docker --version

echo "==> Starting Docker daemon and pre-pulling infra images (cached in snapshot)"
sudo service docker start || true
for _ in $(seq 1 15); do sudo docker info >/dev/null 2>&1 && break; sleep 2; done
sudo docker pull confluentinc/cp-kafka:8.3.1
sudo docker pull confluentinc/cp-schema-registry:8.3.1

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
