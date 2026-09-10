#!/usr/bin/env bash
# Per-boot startup for klens: start a local Kafka + Schema Registry with
# `confluent local` and ensure a local config.yaml exists. Idempotent.
set -euo pipefail

cd "$(dirname "$0")/.."
REPO_ROOT="$PWD"

export PATH="$HOME/.local/bin:$PATH"
eval "$(mise env -s bash)"

# Make sure the Confluent Platform archive is present (no-op if already there).
mise run confluent:install

echo "==> Starting Kafka + Schema Registry (confluent local)"
confluent local services schema-registry start

echo "==> Waiting for Schema Registry to respond"
for _ in $(seq 1 30); do
  if curl -fsS http://localhost:8081/subjects >/dev/null 2>&1; then
    echo "Schema Registry is ready."
    break
  fi
  sleep 2
done

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
