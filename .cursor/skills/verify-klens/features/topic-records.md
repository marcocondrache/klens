# Topic records

Topic records is the Data tab on a topic page. It lists recent records and opens a payload sheet for one row.

## Sub-features

- `records-open` shows the topic heading and the Data tab.
- `records-row` lists the seeded key `verify-1`.
- `records-payload` opens the record sheet for that row.
- `records-filter` filters records from the in-tab search field.

## How to get to it (user POV)

- From Topics, click row `klens-verify-topics`.
- Open `/cluster/local/topics/klens-verify-topics`.
- From the command palette, choose the topic under Topics.

## Driving it with Playwright

Preconditions:

- Doctor reports cluster `local` is ready and topology has `updatedAt` with no `lastError`.
- Topic `klens-verify-topics` has key `verify-1` and value `hello-from-verify-klens`.
- Start from `/cluster/local/topics`.

- **Open topic.** Click `klens-verify-topics`. Heading contains `klens-verify-topics`. Tab `Data` is selected.
- **See record.** The records table includes key `verify-1`. Wait for that cell, not the stats skeleton.
- **Open payload.** Click the `verify-1` row. A sheet titled `klens-verify-topics[0]@<offset>` appears. The Value block contains `hello-from-verify-klens`.
- **Filter.** Close the sheet. Type `verify-1` into `Search key or value…`. The row remains. Replace the query with `no-such-payload`. Empty title `No records` appears with `Nothing matched your search in the scanned offsets.`
- **Proof.** Screenshot the populated Data tab and the open sheet. Save `POST /graphql` `records` for cluster `local` topic `klens-verify-topics` and confirm key `verify-1`.

## Gotchas

- Stats (`Partitions`, `Messages`) can render before records finish. Assert the key cell, not the Messages stat.
- Record search compiles to a CEL filter on `keyText` and `valueText`. A topic-catalog `?q=` does not filter records.
- Empty topics show title `No records`, not the Topics `No results.` string.
- Schema Registry decode is a production boundary. Plain string payloads must appear without a registry.
- An `Obfuscated` badge appears only when `records.obfuscated` is true. The verify launch writes no obfuscation rules, so the seed payload stays plain.
- If Data shows `kafka request timed out` or `operation timed out: request (CLIENT)` while `rpk topic consume` still returns `verify-1`, that is a product gap after krafka 0.24, not a map miss. Do not rewrite this recipe to expect the timeout. Opening Data can also poison later live RPCs such as `acls`.
