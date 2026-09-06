# klens

A fast, modern Kafka UI for inspecting topics, messages, consumer groups, and
more.

It is a small Rust service with an embedded web UI, not a platform. Point it at
one or more clusters, then browse topics, brokers, consumer groups, schemas, and
ACLs from a single process that is easy to run and easy to replace.

## ✦ What It Includes

- A React UI for topics, messages, brokers, consumer groups, schemas, and ACLs
- A GraphQL API over Axum, with GraphiQL available in debug builds
- YAML cluster configuration, including optional SASL, TLS, and extra librdkafka
  properties
- Multi-cluster support from a single process
- Production-oriented HTTP defaults around request visibility, failure handling,
  and safe logging
- Graceful shutdown for local development and orchestrated environments
- Structured, non-blocking logs written to stdout
- An optional embedded UI compiled into a small container image

## ✦ Getting Started

Copy the example cluster config, then start the server:

    cp config/clusters.example.yaml config/clusters.yaml
    cargo run

The process listens on `0.0.0.0:8080` by default. Override bind address, log
filter, and config path with `--bind`, `--log`, and `--config`, or the `BIND`,
`RUST_LOG`, and `CONFIG` environment variables.

For UI work, run the Vite dev server alongside it. It proxies `/health` and
`/api` to the backend:

    mise run web:dev

To serve the UI from the same binary, build the frontend and enable the `ui`
feature:

    mise run web:build
    cargo run --features ui

The source is split by concern:

- `src/main.rs` — the binary: configuration and startup
- `src/app/` — GraphQL API and health routes
- `src/kafka/` — cluster registry and Kafka clients
- `src/server/` — HTTP serving and the embedded UI
- `src/telemetry.rs` — structured, non-blocking logging
- `web/` — the React UI

## ✦ Local Kafka

`compose.yaml` ships a single-node Kafka broker on `localhost:9092`:

    docker compose up -d            # broker and the klens UI on :8080
    docker compose up -d kafka      # broker only, for `cargo run` against localhost:9092

Create topics with:

    docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic orders --partitions 3

## ✦ Philosophy

klens tries to stay a lens, not a control plane. Kafka already has enough moving
parts; the UI should be a clear window onto a cluster, not another system to
operate.

It should give you a usable view of topics, messages, and consumers on day one,
then get out of the way. How you run Kafka, who may change it, and what you
deploy around it stay yours.
