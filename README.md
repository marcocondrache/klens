# klens

A Kafka UI for inspecting topics, messages, consumer groups, and more.

It is a small Rust service with a web UI, not a Kafka platform. Point it at one
or more clusters, then browse topics, brokers, consumer groups, schemas, and
ACLs from a single process.

## ✦ What It Includes

- A React UI for topics, messages, brokers, consumer groups, schemas, and ACLs
- A GraphQL API over Axum, with GraphiQL in debug builds
- YAML configuration for multiple clusters, including optional SASL, TLS, and
  extra librdkafka properties
- An embedded UI compiled into the same binary
- A Compose file with a single-node Kafka broker
- Production-oriented HTTP defaults around request visibility, failure handling,
  and safe logging
- Structured logs on stdout and graceful shutdown

## ✦ Layout

The source is split by concern:

- `src/main.rs` — the binary: configuration and startup
- `src/app/` — GraphQL API and health routes
- `src/kafka/` — cluster registry and Kafka clients
- `src/server/` — HTTP serving and the embedded UI
- `src/telemetry.rs` — structured, non-blocking logging
- `web/` — the React UI

## ✦ Philosophy

klens stays a lens on a cluster, not a control plane. Kafka already has enough
moving parts. The UI should show you the cluster, not become another system to
operate.

It should give you a usable view of topics, messages, and consumers on day one,
then get out of the way. How you run Kafka, who may change it, and what you
deploy around it stay yours.
