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
helm install klens oci://ghcr.io/marcocondrache/charts/klens \
  -n klens --create-namespace -f my-values.yaml
```

`config` is the same YAML the process loads here. The chart also lives in
[`charts/klens`](charts/klens) if you want to install from a checkout.

Every page reads a background projection of each cluster, refreshed by
independent lanes. Override a cluster's cadence with `ingest` on that cluster
(`topology_secs` 10, `watermark_secs` 3, `config_secs` 60, `subjects_secs` 30,
`offset_tick_secs` 1, `fast_offset_secs` 2, `slow_offset_secs` 20). Values are
seconds and must be at least 1. Offsets use the fast interval for groups
someone is looking at and the slow interval for the rest.

## Live tail

`GET /api/clusters/{cluster}/topics/{topic}/records/tail` follows a topic from
its current end as a server-sent event stream. It takes the same `partition`,
`contains`, and `schemaId` parameters as a record page. It needs the `records`
privilege, and it applies obfuscation the same way a page does. The first frame
is `ready` and names each partition's start offset. After that, `records` frames
arrive oldest first.

A tail samples a busy topic rather than streaming all of it. Each frame carries
at most `KLENS_TAIL_BATCH_LIMIT` (100) of the newest records, and frames are at
least `KLENS_TAIL_INTERVAL_MS` (250) apart. A partition that falls too far
behind skips ahead. `skipped` counts what was passed over. Each tail holds its
own consumer, and `KLENS_MAX_LIVE_TAILS` (32) caps how many run at once. Past
that cap, a new tail gets `503 TOO_MANY_TAILS`.

## Authentication

By default the UI and JSON API are open to anyone who can reach the process.

To require a login, add an OIDC provider to `config.yaml`. klens uses the
authorization code flow with PKCE. Sessions use
[axum-login](https://github.com/maxcountryman/axum-login). Auth, catalog, and
live routes are under `/api`. `/health` and `/ready` stay public. A process
restart drops in-memory sessions and requires a new login.

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
    redirect_uri: http://localhost:8080/api/auth/callback
```

Register `redirect_uri` with the identity provider. Without `roles`, any
authenticated user has the same access as an open deployment.

To restrict what signed-in users may do, add `roles`. A role is nothing but a
name for a set of privileges, defined by you: there are no built-in roles. The
privileges are `records`, `configs`, `schema_text`, and `acls`; a role that
lists none still sees the catalog (clusters, topics, groups, lag) but no
payloads, live configs, schema bodies, or ACL bindings. Bindings map IdP groups
from the `claim` to those roles, and unmatched users cannot sign in. Omit
`clusters` on a binding to allow every configured cluster.

Bindings are evaluated per cluster: a user's privileges on a cluster are the
union of the roles bound to their groups **whose scope covers that cluster**.
Two roles need not be comparable — an `operator` and an `auditor` binding
combine into both privilege sets — but a role scoped to `prod` contributes
nothing to `payments`, so a wide `admin` binding never lifts a narrow one
elsewhere. A cluster no binding covers is invisible: it is reported as unknown
rather than forbidden, so nobody can probe for clusters they are not allowed to
know about.

```yaml
auth:
  oidc:
    issuer: https://keycloak.example.com/realms/klens
    client_id: klens
    client_secret: "..."
    redirect_uri: http://localhost:8080/api/auth/callback
  roles:
    # claim: groups
    definitions:
      admin: [records, configs, schema_text, acls]
      viewer: [] # catalog only
      operator: [records, configs]
      auditor: [acls, schema_text]
    bindings:
      - groups: [klens-admins]
        role: admin
      - groups: [klens-viewers]
        role: viewer
      - groups: [kafka-operators]
        role: operator
        clusters: [staging, dev]
      - groups: [security-team]
        role: auditor
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
    # Pattern rules, for schemaless text topics no registry decodes.
    - topics: ["app.logs"]
      patterns:
        - regex: '\b\d{13,19}\b'
          strategy: hash
        - regex: '[\w.+-]+@[\w-]+\.[\w.]+'
          strategy: mask
```

A path met by an array fans out over its elements, so `items.sku` covers every
element's `sku`. `hash` is `HMAC-SHA256` truncated to 64 bits: equal values
render as equal tokens, so records stay correlatable, but the token cannot be
enumerated back without the secret. Rotating the secret changes every token.

Field rules only reach JSON a registry decode produced. Pattern rules reach the
rendered text of key and value instead, which is all a schemaless topic ever
has: every match is replaced by its strategy's output, and `drop` deletes the
match. They only run on topics a rule names, never by detection — and they are
the weaker of the two, because a value written in an unexpected format slips
past the regex. Use field rules wherever a schema exists.

Filters see the obfuscated record, not the wire record: a `contains`
filter over a protected field matches the token, never the value behind it.
That is deliberate — a filter that searched the cleartext would recover a
hidden value one character at a time. What the page can show is what a query
can search.

Rules apply to every session, including admins. A topic must be covered by at
most one rule, hashing without a secret is rejected, and both are boot-time
errors rather than a silently weaker policy. Offsets, timestamps, `sizeBytes`,
and schema ids keep describing the wire record. A record page reports
`obfuscated: true` for a covered topic, which is what the UI badges.
