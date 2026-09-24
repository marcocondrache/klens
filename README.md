# klens

**klens** is a small Kafka UI for developers who just want to see what's in
their clusters. Fast, live, and configured with a single YAML file.

Built with **Rust** and **React**.

### ✦ What it is

- A live view of topics, consumer groups, schemas, brokers, and ACLs, across as
  many clusters as you like
- A record browser that decodes Avro, Protobuf, and JSON Schema, and follows a
  topic as it's written
- A single process with OIDC login, per-cluster roles, and obfuscation of
  sensitive fields built in

### ✦ What it is not

- A monitoring stack: no dashboards, no alerts, no metrics history
- A platform to operate: no database, no agents, nothing to migrate

### ✦ Philosophy

Many Kafka UIs grow into platforms. They want a database, accounts of their
own, and an afternoon of setup before they show you a single record. That pays
off for a team that lives in them all day, but it's a lot to ask when you just
need to know why a consumer is lagging or what a message actually looks like.

klens asks for a list of brokers. There is no database to run and nothing to
migrate: the configuration is one YAML file, and everything on screen comes
from the cluster, kept fresh in the background so pages load instantly. A lens,
not a platform.

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
