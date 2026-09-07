# klens

A Kafka UI for inspecting topics, messages, consumer groups, and more.

It is a small Rust service with a web UI, not a Kafka platform. Point it at one
or more clusters, then browse topics, brokers, consumer groups, schemas, and
ACLs from a single process.

## Install

Images are published to GHCR on each release from `main`:

```sh
docker pull ghcr.io/marcocondrache/klens:latest
```

Copy [config/clusters.example.yaml](config/clusters.example.yaml) to `config.yaml`, then run with that file mounted:

```sh
docker run --rm -p 8080:8080 \
  -v "$PWD/config.yaml:/config.yaml:ro" \
  ghcr.io/marcocondrache/klens:latest
```

[compose.yaml](compose.yaml) starts a local Kafka broker, Schema Registry, and builds klens from this repository.

## Authentication

By default the UI and GraphQL API are open to anyone who can reach the process.

To require a login, add an OIDC provider to `config.yaml`. klens uses the
authorization code flow with PKCE. `/health` stays public.

```yaml
auth:
  oidc:
    issuer: https://keycloak.example.com/realms/klens
    client_id: klens
    client_secret: "..."
    redirect_uri: http://localhost:8080/auth/callback
```

Register `redirect_uri` with the identity provider. Any authenticated user has
the same access as an open deployment.

## Schema Registry

Each cluster can optionally point at a Confluent-compatible Schema Registry.
When omitted, the Schemas page is empty for that cluster.

```yaml
schema_registry:
  url: http://localhost:8081
  # username: user
  # password: secret
```
