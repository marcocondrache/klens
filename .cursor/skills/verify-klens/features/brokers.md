# Brokers

Brokers lists the brokers in the cluster and opens one broker to read its address and live configuration.

## Sub-features

- `brokers-list` shows broker id, host, and whether it is the controller.
- `brokers-open` opens one broker.
- `brokers-config` shows the live configuration table.

## How to get to it (user POV)

- Choose **Brokers** in the sidebar.
- Choose **Brokers** under **Go to** in the command palette.
- Open `/cluster/local/nodes`.

## Driving it with Playwright

Preconditions:

- `helpers/doctor.sh` passed.
- `GET /api/clusters/local/brokers` returns at least one broker.

- **Open the page.** Click the sidebar link named `Brokers`. The URL is `/cluster/local/nodes`. The heading is `Brokers`.
- **Controller.** One row includes the text `controller`.
- **Open.** Click the row for broker `0`, or the first id in the JSON. The URL is `/cluster/local/nodes/<id>`. The heading is `Broker <id>`.
- **Config.** The configuration table is visible. Auth-off can read configs, so the sentence `Live broker configuration is not available for your role.` is absent.
- **Proof.** Save a screenshot, an ARIA snapshot, the URL, and `GET /api/clusters/local/brokers`.

## Gotchas

- The page path is `/nodes`, not `/brokers`. The heading and the sidebar both say `Brokers`.
- Broker detail reads the id from the brokers list. There is no separate broker-detail JSON route.
- Live configuration is `GET /api/clusters/local/brokers/<id>/configs`. A missing privilege replaces the table with the role sentence above.
