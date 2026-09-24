# Configuration

klens reads one YAML file: `config.yaml` in the working directory, or the path
in `KLENS_CONFIG_PATH`. The container image runs from `/`, so mount the file at
`/config.yaml`. The [example config](../config/clusters.example.yaml) walks
through every section with comments.

Values may reference environment variables, so secrets can stay out of the
file: `${VAR}`, `$VAR`, `${VAR:-default}`. Use `$$` for a literal dollar sign.

```yaml
bind: 0.0.0.0:8080
log_level: info
clusters:
  - name: local
    bootstrap_servers:
      - localhost:9092
```

## Clusters

One process serves every cluster listed under `clusters`. Each needs a unique
`name` and its `bootstrap_servers`. The rest is optional:

- `security`: `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`, with SASL
  `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512` and TLS certificates
- `properties`: `client_id`, `request_timeout_ms`, and `connect_timeout_ms`
- `ingest`: how often klens refreshes the cluster, see
  [Ingest cadence](#ingest-cadence)
- `schema_registry`: see [Schema Registry](#schema-registry)
- `obfuscation`: see [Obfuscation](obfuscation.md)

## Ingest cadence

Every page reads a background projection of each cluster, refreshed by
independent lanes. Override a cluster's cadence with `ingest` on that cluster
(`topology_secs` 10, `watermark_secs` 3, `config_secs` 60, `subjects_secs` 30,
`offset_tick_secs` 1, `fast_offset_secs` 2, `slow_offset_secs` 20). Values are
seconds and must be at least 1. Offsets use the fast interval for groups
someone is looking at and the slow interval for the rest.

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
