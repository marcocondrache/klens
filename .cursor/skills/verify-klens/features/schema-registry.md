# Schema registry

Schema registry is the subject catalog. It lists subjects the configured registry reports for the active cluster and opens a payload sheet for one subject.

## Sub-features

- `schemas-land` opens `/cluster/local/schemas` with heading `Schema registry`.
- `schemas-search` filters by subject from `Search subjects…` and writes `?q=` on the URL.
- `schemas-open` opens a subject sheet from a table row.

## How to get to it (user POV)

- Choose the `Schema Registry` sidebar link.
- Choose `Schema Registry` in the command palette Go to group.
- Open `/cluster/local/schemas`.

## Driving it with Playwright

Preconditions:

- Doctor reports `clusters` includes `local` and `catalogHealth` has `updatedAt` with no `lastError`.
- Start from `/`.

- **Open catalog.** Click sidebar `Schema Registry`. URL is `/cluster/local/schemas`. Heading is `Schema registry`. The description includes `subjects registered`.
- **Search.** If a subject is visible, type a unique prefix into `Search subjects…`. The URL contains `q=`. Non-matching subjects leave the table.
- **Open subject.** Click a subject row. A sheet titled with that subject appears. The Schema block shows JSON. There is no `/schemas/<subject>` route.
- **Proof.** Screenshot the catalog with the heading and at least one column header (`Subject`, `Type`, `Compatibility`). Save `POST /graphql` `schemaSubjects(cluster: "local") { subject id type }`.

## Gotchas

- A verify launch with no registered subjects shows `No results.` That empty catalog is a pass only when GraphQL also returns `[]`.
- The heading is `Schema registry`. The sidebar and palette label are `Schema Registry`.
- Launch always writes `schema_registry.url: http://127.0.0.1:8081`. If that port is down, the page can error instead of showing an empty table. Quote the error and stop.
- A command palette SUBJECT hit lands on `/cluster/local/schemas?q=<subject>`. The sheet stays closed until the row is clicked.
