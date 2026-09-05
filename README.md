# klens

A production-minded Rust service template with a small set of strong defaults.

It is not a framework. The goal is to start new microservices from a baseline
that is simple to understand, easy to replace, and shaped by defaults that have
worked well for me in production.

## ✦ What It Includes

- A clear separation between startup, serving, and application routing
- Environment-driven configuration with a simple command-line interface
- Production-oriented HTTP defaults around request visibility, failure handling,
  and safe logging
- Graceful shutdown for local development and orchestrated environments
- Structured, non-blocking logs written to stdout
- A small container image for deployment
- A default CI workflow suitable for pre-merge checks

## ✦ Using The Template

This repository is intended to be used as a GitHub template repository or with
`cargo generate`. Create a new repository from it, then rename the crate to
match the actual service and binary name.

The source is split by concern:

- `src/main.rs` — the binary: configuration and startup
- `src/app/` — application routing, the part you replace first
- `src/server/` — HTTP serving with production defaults
- `src/telemetry.rs` — structured, non-blocking logging

## ✦ Local Kafka

`compose.yaml` ships a single-node Kafka broker on `localhost:9092`:

    docker compose up -d            # broker and the klens UI on :8080
    docker compose up -d kafka      # broker only, for `cargo run` against localhost:9092

Create topics with:

    docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic orders --partitions 3

## ✦ Philosophy

This template tries to follow the [12-factor app](https://12factor.net/) style
without turning that into ceremony.

It should give a service the operational baseline it needs on day one, then get
out of the way. Application-specific decisions, business logic, and deployment
policy stay with each service.
