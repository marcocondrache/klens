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

## Obfuscation

A cluster can hide parts of its records from everyone browsing it, so a topic
stays debuggable without showing card numbers or emails in cleartext. Rules are
per topic, compiled at boot, and applied inside the scan, before filters and
before anything is rendered.

```yaml
obfuscation:
  # Required as soon as one rule hashes. Base64 or plain text, 32 bytes or more.
  secret: ${KLENS_OBFUSCATION_SECRET}
  rules:
    # Field rules walk the JSON a registry decode produced.
    - topics: ["payments.*"] # exact name, or a trailing-* prefix
      fields:
        - path: card.number
          strategy: hash # deterministic token: kx:3f9a2c481b7d6e05
        - path: card.cvv
          strategy: drop # the field disappears
        - path: customer.email
          strategy: mask # ***
      # A value that never decoded cannot be walked. Default: mask it whole.
      unparsed: mask # mask | allow
    # Whole-field rules, for topics browsed as raw text.
    - topics: ["audit.raw"]
      key: mask
      value: hash
      headers: ["x-user-id"] # header values to mask, by name
```

A path met by an array fans out over its elements, so `items.sku` covers every
element's `sku`. `hash` is `HMAC-SHA256` truncated to 64 bits: equal values
render as equal tokens, so records stay correlatable, but the token cannot be
enumerated back without the secret. Rotating the secret changes every token.

Filters see the obfuscated record, not the wire record: a `contains` or CEL
filter over a protected field matches the token, never the value behind it.
That is deliberate — a filter that searched the cleartext would recover a
hidden value one character at a time. What the page can show is what a query
can search.

Rules apply to every session, including admins. A topic must be covered by at
most one rule, hashing without a secret is rejected, and both are boot-time
errors rather than a silently weaker policy. Offsets, timestamps, `sizeBytes`,
and schema ids keep describing the wire record.
