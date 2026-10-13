# klens

A web UI for Kafka that is safe to point at production.

klens shows the topics, records, consumer groups, brokers, schemas, and ACLs of
one or more clusters. It is one Rust binary that serves the UI, a JSON API,
and, when you turn it on, tools for AI agents over MCP.

Read the [documentation](https://klens.marcocondrache.com/docs) to get started.

## Why klens

**Light on your brokers.** klens reads each cluster in the background into
memory, and every page reads that copy. Ten people on the topics page cost
Kafka the same as one. Only record reads, live tails, and changes reach the
brokers, and only when someone asks.

**Read-only until you say otherwise.** A cluster takes no changes until you
mark it `writable`, and then only from the OIDC groups you grant each change.

**Private fields stay private.** Obfuscation rules hash, mask, or drop card
numbers and emails inside every record before anyone sees them. The rules
cover admins and AI agents too.

**Nothing else to run.** No database and no sidecar. One YAML file describes
every cluster.

## License

[MIT](LICENSE)
