# Proof artifacts

Each launch creates `artifacts/<run-id>/`. Cleanup deletes the run directory and leaves this tree.

## 20260911T181720Z

First live pass of this skill. Feature: Topics (`features/topics.md`).

- Launch: `target/debug/klens` on `127.0.0.1:18080`, cluster `local` `HEALTHY`
- Doctor: `/health` 204, `/auth/me` `enabled: false`, GraphQL clusters `HEALTHY`
- Drive: `/` → `/cluster/local/topics` → filter `klens-verify-topics` → topic page
- Cleanup: PID 23172 gone, port 18080 closed, these files still here

See `topics/NOTES.txt`, `topics/landing.png`, `topics/filtered.png`, `topics/open.png`, and `topics/topics.json`.

## 20260914T075155Z

Maintain pass. One launch, doctor, then every mapped feature.

- Launch: `target/debug/klens` on `127.0.0.1:18080`, cluster `local` `HEALTHY`. Kafka was already on `9092`.
- Doctor: `/health` 204, `/auth/me` `enabled: false`, GraphQL clusters `HEALTHY`.
- Topics: `/` → `/cluster/local/topics` → filter `klens-verify-topics` → topic page. `Show internal` shows `__consumer_offsets` and `_schemas` at `?internal=1`.
- Topic records: Data tab key `verify-1`, sheet `klens-verify-topics[0]@0`, value `hello-from-verify-klens`. Filter `no-such-payload` shows `No records` / `Nothing matched your search in the scanned offsets.`
- Consumer groups: `/cluster/local/groups`, heading `Consumer groups`, table `No results.`, GraphQL `consumerGroups` `[]`.
- Schema registry: `/cluster/local/schemas`, heading `Schema registry`, `Search subjects…`, table `No results.`, GraphQL `schemaSubjects` `[]`.
- Brokers: `/cluster/local/nodes`, host `127.0.0.1:9092`, open `/cluster/local/nodes/0`. GraphQL `controller` is `false` on the only broker. No `controller` badge. Product gap, left in `features/brokers.md`.
- Command palette: header `Search` opens dialog `Search klens`. `Control+K` after clicking the Topics heading. Topic hit `klens-verify-topics`. Go to `Brokers` → `/cluster/local/nodes`. `/` on Topics focuses `Search topics…`.

