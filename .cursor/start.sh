#!/usr/bin/env bash
# Per-boot startup for klens: bring up the Docker daemon, the Kafka + Schema
# Registry stack, and ensure a local config.yaml exists. Idempotent.
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$PWD"

echo "==> Starting Docker daemon"
sudo service docker start || true
for _ in $(seq 1 15); do sudo docker info >/dev/null 2>&1 && break; sleep 2; done

echo "==> Bringing up Kafka + Schema Registry"
bash "$REPO_ROOT/.cursor/kafka-stack.sh"

if [ ! -f "$REPO_ROOT/config.yaml" ]; then
  echo "==> Writing default config.yaml (points klens at the local stack)"
  cat > "$REPO_ROOT/config.yaml" <<'YAML'
bind: 0.0.0.0:8080
log_level: info

clusters:
  - name: local
    bootstrap_servers:
      - localhost:9092
    schema_registry:
      url: http://localhost:8081
YAML
fi

echo "==> Startup complete (Kafka: localhost:9092, Schema Registry: localhost:8081)"
