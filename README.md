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

The Helm chart is published to the same registry:

```sh
helm install klens oci://ghcr.io/marcocondrache/klens/charts/klens \
  -n klens --create-namespace -f my-values.yaml
```

`config` is the same YAML the process loads here. The chart also lives in
[`charts/klens`](charts/klens) if you want to install from a checkout.

Every page reads a background projection of each cluster, refreshed by
independent lanes. Override a lane's cadence with `KLENS_TOPOLOGY_LANE_INTERVAL`
(default 10), `KLENS_WATERMARK_LANE_INTERVAL` (3), `KLENS_CONFIG_LANE_INTERVAL`
(60), or `KLENS_SUBJECT_LANE_INTERVAL` (30), in seconds, minimum 1. Consumer
group offsets refresh at `KLENS_FAST_OFFSET_INTERVAL` (2) for groups someone is
looking at and `KLENS_SLOW_OFFSET_INTERVAL` (20) for the rest.

## Authentication

By default the UI and GraphQL API are open to anyone who can reach the process.

To require a login, add an OIDC provider to `config.yaml`. klens uses the
authorization code flow with PKCE. Sessions use
[axum-login](https://github.com/maxcountryman/axum-login). `/health` stays
public. A process restart drops in-memory sessions and requires a new login.

Set `auth.session_key` (or `KLENS_SESSION_KEY`, which takes precedence) to a
base64 or plain secret of at least 32 bytes so the session cookie survives a
restart. Without one, klens generates a key per boot and every deploy logs
everyone out.

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
ACL bindings on their clusters. Viewers see the catalog only. Omit `clusters` on
a binding to allow every configured cluster.

Bindings are evaluated per cluster and never merged: a user's role on a cluster
is the highest role among the bindings that name it, so a cluster-wide `admin`
binding plus a `payments`-only `viewer` binding still leaves that user an admin
on `payments` (the wide binding covers it) while a `viewer` binding alone never
gains privileges from an admin binding scoped elsewhere. A cluster no binding
covers is invisible: it is reported as unknown rather than forbidden, so nobody
can probe for clusters they are not allowed to know about.

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

Credentials may only travel over plaintext `http://` when the host is loopback.
A remote registry that needs a username and password must be reached over
`https://`.
