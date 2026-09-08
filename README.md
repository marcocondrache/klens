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
    client_secret: ${OIDC_CLIENT_SECRET}
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
  username: ${SCHEMA_REGISTRY_USERNAME}
  password: ${SCHEMA_REGISTRY_PASSWORD}
```

## Secrets

`config.yaml` expands environment variables before it is parsed:

- `${VAR}` or `$VAR` — required; startup fails if the variable is unset
- `${VAR:-default}` — use `default` when the variable is unset
- `$$` — a literal `$`

On Kubernetes, keep non-secret config in a ConfigMap and inject credentials
from a Secret. Do not put passwords in the ConfigMap.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: klens
data:
  config.yaml: |
    bind: 0.0.0.0:8080
    auth:
      oidc:
        issuer: https://keycloak.example.com/realms/klens
        client_id: klens
        client_secret: ${OIDC_CLIENT_SECRET}
        redirect_uri: https://klens.example/auth/callback
    clusters:
      - name: prod
        bootstrap_servers:
          - kafka:9092
        security:
          protocol: SASL_SSL
          sasl:
            mechanism: SCRAM-SHA-512
            username: ${KAFKA_USERNAME}
            password: ${KAFKA_PASSWORD}
          tls:
            ca_cert: /var/run/secrets/klens/ca.pem
```

```yaml
env:
  - name: OIDC_CLIENT_SECRET
    valueFrom:
      secretKeyRef:
        name: klens
        key: oidc-client-secret
  - name: KAFKA_USERNAME
    valueFrom:
      secretKeyRef:
        name: klens
        key: kafka-username
  - name: KAFKA_PASSWORD
    valueFrom:
      secretKeyRef:
        name: klens
        key: kafka-password
```

TLS material is still file paths. Mount those Secret keys as files and leave
`ca_cert` / `client_cert` / `client_key` pointing at the mount.
