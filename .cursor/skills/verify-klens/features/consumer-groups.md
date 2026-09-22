# Consumer groups

Consumer groups lists group ids, state, assigned topics, and lag for the active cluster.

## Sub-features

- `groups-land` opens `/cluster/local/groups` with heading `Consumer groups`.
- `groups-search` filters by group id from `Search consumer groups…`.
- `groups-state` filters by the state select (`All states`, `Stable`, `Empty`, and the other stored states).
- `groups-open` opens a group row at `/cluster/local/groups/<id>`.

## How to get to it (user POV)

- Choose the `Consumer Groups` sidebar link.
- Choose `Consumer Groups` in the command palette Go to group.
- Open `/cluster/local/groups`.

## Driving it with Playwright

Preconditions:

- Doctor reports cluster `local` is ready and topology has `updatedAt` with no `lastError`.
- Start from `/`.

- **Open catalog.** Click sidebar `Consumer Groups`. URL is `/cluster/local/groups`. Heading is `Consumer groups`. The description includes `groups`. The table lists every matching group. There is no `Rows per page` footer.
- **Search.** If a group id is visible, type a unique prefix into `Search consumer groups…`. The URL contains `q=`. Non-matching ids leave the table.
- **State filter.** Open the state select and choose `Empty` or `Stable` to match a visible group. The URL contains `state=`.
- **Open group.** Click a group row. URL becomes `/cluster/local/groups/<id>` and the heading contains that id.
- **Proof.** Screenshot the catalog with the heading and at least one column header (`Group`, `State`, `Lag`). Save `GET /clusters/local/groups`.

## Gotchas

- A fresh Redpanda cluster can have zero user groups. The table then shows `No results.` That empty catalog is a pass only when `GET /clusters/local/groups` also returns `rows: []`.
- klens hides its own `klens.internal.` groups. Do not expect browse or list-offsets groups in the UI.
- Search matches group id, not assigned topic names.
- The heading is `Consumer groups`. The sidebar label is `Consumer Groups`.
- The state URL uses the API enum (`state=EMPTY`), not the select label (`Empty`).
- The Topics column shows one topic pill and `+N` for the rest. A group with a single topic has no overflow pill.
