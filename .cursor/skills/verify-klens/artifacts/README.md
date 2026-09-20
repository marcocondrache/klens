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

## 20260916T073041Z / 20260916T073212Z

Maintain pass after #224 / #220 / #209. One long-lived instance is not enough when the default 5s catalog poll's ListGroups RPC times out and poisons the shared krafka client. Later drives used `KLENS_CATALOG_POLL_INTERVAL=600` so catalog stayed green. That override is a session workaround, not a launch.sh change.

- Doctor: `/health` 204, `/auth/me` `enabled: false`, `query { clusters }` → `["local"]`, `catalogHealth.updatedAt` set, `lastError` null.
- Topics: `/` → `/cluster/local/topics`, `Rows per page` `100`, `Show internal` → `__consumer_offsets` and `_schemas` at `?internal=1`. A leftover `?q=` hides those names.
- Topic records: Data tab chrome matches the map (`Search key or value…`, `50 rows`). Fetch shows `kafka request timed out (TIMEOUT)`. `rpk topic consume` still returns `verify-1`. Product gap, left in `features/topic-records.md`.
- Consumer groups: `/cluster/local/groups`, search `klens-verify-group`, `state=EMPTY`, open `/cluster/local/groups/klens-verify-group`.
- Schema registry: `/cluster/local/schemas`, `No results.`, GraphQL `schemaSubjects` `[]`.
- Brokers: `/cluster/local/nodes`, host `127.0.0.1:9092`, open `/cluster/local/nodes/0`. GraphQL `controller` is `false`. No `controller` badge. Product gap, left in `features/brokers.md`.
- ACLs: `/cluster/local/acls`, `No ACL bindings.`, GraphQL `authorizer: ENABLED`, `bindings: []`. Search writes `?q=alice`. Resource `Topic` writes `?resource=TOPIC`.
- Command palette: header `Search` opens `Search klens` with Go to including `ACLs` and Switch cluster `local`. Topic hit `klens-verify-topics`. Go to `ACLs` / `Brokers`. `/` on Brokers opens the dialog. `/` on Topics focuses `Search topics…`. Playwright `Control+K` does not reach the page under Google Chrome; `window` `keydown` with `ctrlKey` does.

## 20260917T072143Z / 20260917T072432Z / 20260917T072757Z

Maintain pass after #225. Helm (#226) and CI dep bumps are not UI features.

- First launch used the default 5s catalog poll. Doctor started green. After `drive-topics.mjs`, doctor failed with `catalogHealth.lastError` `invalid state: list_consumer_groups failed: all brokers returned errors` and a stale `updatedAt`. Artifacts stayed after cleanup.
- Later launches used session-only `KLENS_CATALOG_POLL_INTERVAL=600`. Doctor stayed green. That override is not in `launch.sh`.
- Topics: `/` → `/cluster/local/topics`, `Rows per page` `100`, `Show internal` → `__consumer_offsets` and `_schemas` at `?internal=1`, filter and open `klens-verify-topics`.
- Topic records: Data tab chrome matches the map (`Search key or value…`, `50 rows`). Fetch shows `operation timed out: request (CLIENT)` or `kafka request timed out` (`TIMEOUT`). `rpk topic consume` still returns `verify-1` / `hello-from-verify-klens`. Product gap, left in `features/topic-records.md`.
- Consumer groups: `/cluster/local/groups?q=klens-verify-group&state=EMPTY`, open `/cluster/local/groups/klens-verify-group`.
- Schema registry: `/cluster/local/schemas`, `No results.`, GraphQL `schemaSubjects` `[]`. Search writes `?q=no-such-subject`.
- Brokers: `/cluster/local/nodes`, host `127.0.0.1:9092`, open `/cluster/local/nodes/0`. GraphQL `controller` is `false`. No `controller` badge. Product gap, left in `features/brokers.md`.
- ACLs: must run before topic Data. After a records timeout, `acls` returned `CLIENT` timeout. On a fresh client, `/cluster/local/acls?q=alice&resource=TOPIC` shows `No ACL bindings.`, GraphQL `authorizer: ENABLED`, `bindings: []`.
- Command palette: header button accessible name is `Search Ctrl+K /`. Dialog `Search klens` lists Go to `Topics`, `Consumer Groups`, `Schema Registry`, `Brokers`, `ACLs` and Switch cluster `local`. Topic hit `klens-verify-topics 1 partitions`. Go to `ACLs` / `Brokers`. `/` on Brokers opens the dialog. `/` on Topics focuses `Search topics…`. Playwright `Control+K` does not reach the page; `window` `keydown` with `ctrlKey` does.

## 20260918T071321Z

Maintain pass after #266. One long-lived instance. Doctor uses `clusters { cluster ready topology { updatedAt lastError } }`.

- First launch left `KLENS_CATALOG_POLL_INTERVAL=600` on the process. That name is ignored. Default 10s topology lane later set `topology.lastError` to `list_consumer_groups failed`. Artifacts stayed after cleanup.
- Later launch used session-only `KLENS_TOPOLOGY_LANE_INTERVAL=600`. Doctor stayed green through Topics, palette, groups, schemas, brokers, and ACLs. That override is not in `launch.sh`.
- Auth: `/auth/me` `enabled: false`. `whoami.subject` is null. No user menu. Custom roles stay unreachable.
- Topics: `/` → `/cluster/local/topics`, `Rows per page` `100`, `Show internal` → `__consumer_offsets` and `_schemas` at `?internal=1`, filter and open `klens-verify-topics`. GraphQL `topicRows`.
- Topic records: Data tab chrome matches the map (`Search key or value…`, `50 rows`). Fetch shows `operation timed out: request 57 timed out after 10s (CLIENT)`. `rpk topic consume` still returns `verify-1`. After that timeout, `acls` also returned `CLIENT` timeout. Product gap, left in `features/topic-records.md`.
- Consumer groups: `/cluster/local/groups` heading `Consumer groups`, row `klens-verify-group` / `Empty`, `state=EMPTY`, open `/cluster/local/groups/klens-verify-group`. GraphQL `groupRows`. Topics cell empty because `topicNames` is `[]`.
- Schema registry: `/cluster/local/schemas`, `No results.`, GraphQL `subjectRows.rows` `[]`. Search writes `?q=no-such-subject`.
- Brokers: `/cluster/local/nodes`, host `127.0.0.1:9092`, open `/cluster/local/nodes/0`. GraphQL `controller` is `false`. No `controller` badge. Product gap, left in `features/brokers.md` with cite `Topology::assemble`.
- ACLs: ran before Data. `/cluster/local/acls?q=alice&resource=TOPIC` shows `No ACL bindings.`, GraphQL `authorizer: ENABLED`, `bindings: []`.
- Command palette: header button accessible name is `Search Ctrl+K` (no slash kbd). Topic hit selected. ArrowDown stays on `klens-verify-topics`. Empty query shows `No matches in local.` and hides Go to. Go to `ACLs` / `Brokers`. `/` on Brokers opens the dialog. `/` on Topics focuses `Search topics…`. Search stays in the header on topic and group detail.
- Header Search and sidebar links stay on inspect routes. No new verify feature for #261 chrome.
- Obfuscation badge did not appear. Verify launch writes no rules. `records.obfuscated` was `false` on a pre-timeout GraphQL read.

