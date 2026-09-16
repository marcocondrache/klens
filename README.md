# klens

A Kafka UI for inspecting topics, messages, consumer groups, and more.

It is a small Rust service with a web UI, not a Kafka platform. Point it at one
or more clusters, then browse topics, brokers, consumer groups, and schemas from
a single process.

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

A Helm chart lives in [`charts/klens`](charts/klens). `config` is the same YAML
the process loads here.

```sh
helm install klens ./charts/klens -n klens --create-namespace -f my-values.yaml
```

The topics and consumer groups pages read a background catalog snapshot.
Override the poll interval with `KLENS_CATALOG_POLL_INTERVAL` (seconds,
default 5, minimum 1).

## Authentication

By default the UI and GraphQL API are open to anyone who can reach the process.

To require a login, add an OIDC provider to `config.yaml`. klens uses the
authorization code flow with PKCE. Sessions use
[axum-login](https://github.com/maxcountryman/axum-login). `/health` stays
public. A process restart drops in-memory sessions and requires a new login.

```yaml
auth:
  oidc:
    issuer: https://keycloak.example.com/realms/klens
    client_id: klens
    client_secret: "..."
    redirect_uri: http://localhost:8080/auth/callback
```

Register `redirect_uri` with the identity provider. Without `roles`, any
authenticated user has the same access as an open deployment.

To map IdP groups to `admin` or `viewer`, add `roles`. Unmatched users cannot
sign in. Admins can read records, live broker/topic configs, schema text, and
ACL bindings on their clusters. Viewers see the catalog only. Omit `clusters` on a binding
to allow every configured cluster.

```yaml
auth:
  oidc:
    issuer: https://keycloak.example.com/realms/klens
    client_id: klens
    client_secret: "..."
    redirect_uri: http://localhost:8080/auth/callback
  roles:
    # claim: groups
    bindings:
      - groups: [klens-admins]
        role: admin
      - groups: [klens-viewers]
        role: viewer
      - groups: [payments-viewers]
        role: viewer
        clusters: [payments]
```

## Schema Registry

Each cluster can optionally point at a Confluent-compatible Schema Registry.
When omitted, the Schemas page is empty for that cluster. Framed Avro, JSON, and
Protobuf payloads decode to JSON when a registry is configured.

```yaml
schema_registry:
  url: http://localhost:8081
  # username: user
  # password: secret
```
