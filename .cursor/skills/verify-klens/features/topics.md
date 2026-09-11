# Topics

Topics is the default catalog. After `/` loads a healthy cluster, the UI redirects to `/cluster/<name>/topics` and lists topic names the brokers report.

## Sub-features

- `topics-land` redirects `/` to `/cluster/local/topics` and shows heading `Topics`.
- `topics-row` shows seeded topic `klens-verify-topics` in the table.
- `topics-search` filters the table from the `Search topics…` field and writes `?q=` on the URL.
- `topics-internal` reveals internal topics when `Show internal` is on.
- `topics-open` opens a topic row and lands on `/cluster/local/topics/<name>`.

## How to get to it (user POV)

- Open `/`. The home page redirects to the first cluster's Topics page.
- Choose the `Topics` sidebar link.
- Choose `Topics` in the command palette Go to group.

## Driving it with Playwright

Preconditions:

- Doctor reports `local` `HEALTHY` at the verify URL.
- Topic `klens-verify-topics` exists.
- `helpers/drive-topics.mjs` is the scripted form of this recipe.

- **Land.** Open `/`. Wait for heading `Topics` and URL `/cluster/local/topics`. The sidebar wordmark reads `klens`.
- **See seed.** The table includes a cell `klens-verify-topics`. The page description matches `N of M topics`.
- **Search.** Fill `Search topics…` with `klens-verify-topics`. The URL contains `q=klens-verify-topics`. The table still shows that topic and does not show unrelated names that were visible before.
- **Open topic.** Click the `klens-verify-topics` row. The URL becomes `/cluster/local/topics/klens-verify-topics` and the heading contains `klens-verify-topics`.
- **Internal toggle.** Return to Topics. Turn on `Show internal`. The URL contains `internal=1`. At least one internal name (often `__consumer_offsets` or `_schemas`) appears. Turn the switch off and those rows leave.
- **Proof.** Write `landing.png`, `landing.aria.yml`, `topics.json`, and `open.png` under `artifacts/<run-id>/topics/`. The screenshot shows the `klens` wordmark and `klens-verify-topics`. The GraphQL body lists that topic name.

## Gotchas

- First metadata fetch can take several seconds. Wait for heading `Topics`, not a fixed sleep. The loading copy is `Loading clusters` then a skeleton table.
- Search uses a unicode ellipsis in the placeholder (`Search topics…`), not three dots.
- Internal topics stay hidden until `Show internal` is on. A missing seed topic is a Kafka problem, not this toggle.
- An `OFFLINE` cluster still redirects to Topics and shows alert `Cluster unreachable`. That is not a catalog pass.
- Clicking a row is the open path. Topic names in the table are not links.
