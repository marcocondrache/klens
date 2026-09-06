# klens

Run klens against a Kafka cluster.

## Start a local Kafka broker

Start the broker on `localhost:9092`:

	docker compose up -d kafka

To create a topic:

	docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic orders --partitions 3

If you want the klens container, copy `config/clusters.example.yaml` to `config.yaml` first. Then start both services:

	cp config/clusters.example.yaml config.yaml
	docker compose up -d

The container listens on port 8080.

## Run the server from source

1. Copy `config/clusters.example.yaml` to `config/clusters.yaml`.
2. Start the server:

	cargo run

The server listens on `0.0.0.0:8080`. It reads `config/clusters.yaml`.

If the cluster uses SASL or TLS, fill in the commented `security` fields in `config/clusters.yaml`.

## Run the web UI

Start the Vite dev server. It proxies `/health` and `/api` to port 8080:

	mise run web:dev

## Serve the UI from the binary

Build the frontend into `static/`. Then run with the `ui` feature:

	mise run web:build
	cargo run --features ui
