# Brokers

Brokers lists the cluster's Kafka nodes, marks the controller, and opens a node page.

## Sub-features

- `brokers-land` opens `/cluster/local/nodes` with heading `Brokers`.
- `brokers-controller` shows a `controller` badge on the controller row.
- `brokers-open` opens `/cluster/local/nodes/<id>`.

## How to get to it (user POV)

- Choose the `Brokers` sidebar link.
- Choose `Brokers` in the command palette Go to group.
- Open `/cluster/local/nodes`.

## Driving it with Playwright

Preconditions:

- Doctor reports `local` `HEALTHY` with a non-zero broker count.
- Start from `/`.

- **Open catalog.** Click sidebar `Brokers`. URL is `/cluster/local/nodes`. Heading is `Brokers`. The description includes `brokers` and a Kafka version or `—`.
- **See controller.** One row shows badge `controller`. The Host column includes `127.0.0.1:9092` on the default verify broker.
- **Open node.** Click that row. URL is `/cluster/local/nodes/<id>` and the heading contains the broker id.
- **Proof.** Screenshot the catalog with the controller badge. Save `POST /graphql` `brokers(cluster: "local") { id host port controller }`. The body has `controller: true` on one broker.

## Gotchas

- The route segment is `nodes`. The UI label is `Brokers`. Do not look for `/cluster/local/brokers`.
- Copy address is a clipboard control. It is not required for a catalog pass.
- An `OFFLINE` cluster yields an empty table plus `Cluster unreachable`. That is not a Brokers pass.
