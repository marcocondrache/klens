#!/usr/bin/env bash
# Bring up the local Kafka + Schema Registry stack for Cloud Agent development.
#
# Cloud Agent VMs run Docker in a nested environment where container-to-container
# traffic on user-defined bridge networks does not flow. The committed
# compose.yaml relies on such a network, so instead of `docker compose up` we run
# the same images with host networking, which the VM supports. klens (run on the
# host) then reaches Kafka on localhost:9092 and Schema Registry on
# localhost:8081, exactly like the compose setup exposes them.
set -euo pipefail

KAFKA_IMAGE="confluentinc/cp-kafka:8.3.1"
SR_IMAGE="confluentinc/cp-schema-registry:8.3.1"
CLUSTER_ID="EmptNWtoR4GGWx-BH6nGLQ"

docker="docker"
if ! docker info >/dev/null 2>&1; then
  docker="sudo docker"
fi

ensure_running() {
  local name="$1"
  if $docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null | grep -q true; then
    return 0
  fi
  $docker rm -f "$name" >/dev/null 2>&1 || true
  return 1
}

if ! ensure_running klens-kafka; then
  echo "Starting Kafka ($KAFKA_IMAGE)..."
  $docker run -d --name klens-kafka --network host --restart unless-stopped \
    -e CLUSTER_ID="$CLUSTER_ID" \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
    -e KAFKA_LISTENERS=INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://localhost:29092,EXTERNAL://localhost:9092 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    "$KAFKA_IMAGE" >/dev/null
fi

echo "Waiting for Kafka to accept API requests..."
for _ in $(seq 1 30); do
  if $docker exec klens-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    echo "Kafka is ready."
    break
  fi
  sleep 2
done

if ! ensure_running klens-schema-registry; then
  echo "Starting Schema Registry ($SR_IMAGE)..."
  $docker run -d --name klens-schema-registry --network host --restart unless-stopped \
    -e SCHEMA_REGISTRY_HOST_NAME=localhost \
    -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=localhost:29092 \
    -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 \
    "$SR_IMAGE" >/dev/null
fi

echo "Waiting for Schema Registry to respond..."
for _ in $(seq 1 30); do
  if curl -fsS http://localhost:8081/subjects >/dev/null 2>&1; then
    echo "Schema Registry is ready."
    break
  fi
  sleep 2
done

echo "Kafka + Schema Registry stack is up (Kafka: localhost:9092, Schema Registry: localhost:8081)."
