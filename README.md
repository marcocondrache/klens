# klens

**klens** is a small Kafka UI for developers who just want to see what's in
their clusters. Fast, live, and read-only by design.

Built with **Rust** and **React**.

### ✦ What it is

- A live view of topics, consumer groups, schemas, brokers, and ACLs, across as
  many clusters as you like
- A record browser that decodes Avro, Protobuf, and JSON Schema, and follows a
  topic as it's written
- A single process and a single YAML file, with OIDC login, per-cluster roles,
  and obfuscation of sensitive fields built in

### ✦ What it is not

- An admin console: it never creates topics, produces records, or resets offsets
- A monitoring stack: no dashboards, no alerts, no metrics history
- A platform to operate: no database, no agents, nothing to migrate

### ✦ Philosophy

Most Kafka UIs grow into control panels. They create topics, produce test
messages, reset offsets, and edit configs, and every one of those buttons is
something to secure, audit, and keep away from the wrong person on the wrong
cluster.

klens only looks. It never writes to a cluster, so the only thing left to
protect is the data itself, and the worst a wrong click can do is open the
wrong page. There is no database to run either: the configuration is one YAML
file, and everything on screen comes from the cluster. A lens, not a control
panel.

### ✦ Getting started

Point a `config.yaml` at your brokers:

```yaml
bind: 0.0.0.0:8080
clusters:
  - name: production
    bootstrap_servers: [kafka-1:9092, kafka-2:9092]
```

Run the image with it mounted, then open
[localhost:8080](http://localhost:8080):

```sh
docker run --rm -p 8080:8080 \
  -v "$PWD/config.yaml:/config.yaml:ro" \
  ghcr.io/marcocondrache/klens:latest
```

On Kubernetes, the [Helm chart](charts/klens) takes the same YAML under
`config`:

```sh
helm install klens oci://ghcr.io/marcocondrache/charts/klens \
  -n klens --create-namespace -f my-values.yaml
```

SASL and TLS, a Schema Registry, login, and obfuscation are all optional and
live in the same file. The [example config](config/clusters.example.yaml) walks
through each of them.

### ✦ Documentation

- [Configuration](docs/configuration.md): clusters, SASL and TLS, Schema
  Registry, and refresh intervals
- [Authentication](docs/authentication.md): OIDC login and per-cluster roles
- [Obfuscation](docs/obfuscation.md): hashing, masking, or dropping sensitive
  fields before they reach a browser
- [Live tail](docs/live-tail.md): following a topic over server-sent events
- [Helm chart](charts/klens/README.md): chart values and deployment notes
