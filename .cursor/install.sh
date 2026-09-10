#!/usr/bin/env bash
# Idempotent Cloud Agent setup for klens: toolchains, Docker, and builds.
# Runs after the repository is checked out. Safe to run repeatedly.
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$PWD"

echo "==> Ensuring Rust stable toolchain (edition 2024 needs >= 1.85)"
rustup toolchain install stable --profile minimal -c rustfmt -c clippy
rustup default stable
rustc --version

echo "==> Ensuring bun is installed"
if ! command -v bun >/dev/null 2>&1 && [ ! -x "$HOME/.bun/bin/bun" ]; then
  curl -fsSL https://bun.sh/install | bash
fi
export PATH="$HOME/.bun/bin:$PATH"
bun --version

echo "==> Ensuring Docker Engine is installed"
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

echo "==> Building the klens backend (with embedded UI feature)"
# Build the web UI first so the `ui` feature can embed static/.
pushd web >/dev/null
bun install --frozen-lockfile
bun run codegen
bun run build
popd >/dev/null
test -f "$REPO_ROOT/static/index.html"
cargo build --features ui

echo "==> Install complete"
