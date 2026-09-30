# Feature ideas

Ideas and research notes for what klens could do next. Nothing here is a
commitment. Each idea names what it would build on, so the cheap ones are easy
to spot. Written against `main` at `c5bb0ce`; code references name files and
symbols, which may have moved since.

## What klens already has to build on

Most ideas below reuse parts klens already runs. It does not need a new data
source for them.

- **Background lanes.** Topology (10s), watermarks (3s), configs (60s),
  subjects (30s) and committed group offsets (2s for groups someone is looking
  at, 20s otherwise) are polled continuously. See `tuning.ingest` in the README.
- **A change bus.** Each lane compares its new snapshot with the previous one and
  publishes a `Change` (`src/kafka/store/bus.rs`): topics and groups added,
  removed or changed, broker changes, config changes, subject changes, rates and
  group lag.
- **Interest leases.** klens knows which groups someone has open
  (`src/kafka/store/interest.rs`). Expensive per-group work can be limited to
  those.
- **Per-partition produce rates.** `rates_between` in
  `src/kafka/ingest/watermarks.rs` computes each partition's high watermark
  delta, then sums them per topic. `RateStore` keeps only the latest topic
  total.
- **Server-side obfuscation and per-cluster privileges.** Filters match the
  obfuscated view, and hashed tokens are deterministic, so equal values
  correlate without being revealed.
- **krafka admin calls klens does not use yet:** `describe_producers`,
  `list_transactions`, `describe_transactions`, `describe_log_dirs`,
  `describe_client_quotas`, `list_partition_reassignments`,
  `offsets_for_times`. Checked in krafka at the pinned rev `e6b154c`.

## Suggested order

1. **Overview page.** Fills the empty cluster landing page with data klens
   already has.
2. **Consumer group status and the stuck record.** Answers the most common
   reason people open a Kafka UI. Adds two building blocks later ideas reuse.
3. **Activity page.** Cheap, because the change detection already exists.
4. **Transactions page.** Hard to diagnose elsewhere, and no other Kafka UI does
   it well.
5. **Jump to offset, record links and key lookup.** Small, and key history and
   trace build on them.
6. **Storage page.**
7. **Flow page, first version.**
8. **MCP server and API tokens.**

The rest can follow in any order.

## Sidebar pages

The sidebar lists Topics, Consumer Groups, Schema Registry, Brokers and ACLs
(`web/src/lib/sections.ts`). `/cluster/$cluster` redirects to Topics
(`web/src/routes/cluster/$cluster/index.tsx`), so a cluster has no landing page.

Proposed layout:

```
Cluster
  Overview          new, replaces the redirect
  Topics
  Consumer Groups
  Schema Registry
  Brokers
  ACLs
Insights
  Activity          new
  Transactions      new
  Storage           new
  Flow              new
  Trace             new, needs the records privilege
```

Two sidebar changes support this:

- `visibleSections` only knows about the `acls` privilege. It should take the
  full privilege set so Trace can be hidden without `records`, the same way ACLs
  is hidden without `acls`.
- Menu badges are always muted (`SidebarMenuBadge` in
  `web/src/components/nav-main.tsx`). Activity and Transactions need a warning
  tone to say "5 changes" or "3 hanging".

### Overview

A one-screen answer to "is this cluster OK?":

- broker count and which broker is the controller
- partitions that are offline or under-replicated (`under_replicated` already
  exists on topics and partitions)
- total messages per second, and the busiest topics
- the groups furthest behind, and groups that are Empty or rebalancing
- how fresh each lane's data is (the sidebar already calls `useClusterHealth`)
- the latest entries from Activity
- a banner when a partition reassignment is running

Everything except the Activity feed and the reassignment banner is in the store
today.

### Activity

The cluster's recent changes, newest first:

```
14:02  payments-7   in-sync replicas shrank [1,2,3] → [1,2]
14:03  billing      rebalanced, 4 → 3 members (billing-7f9c left)
14:05  orders       retention.ms 7d → 1d
14:06  orders-value schema v12 registered
```

- **Source:** the change bus. `TopologyDelta`, `ConfigsDelta` and
  `SubjectsDelta` already say *what* changed. They only carry names, so the log
  also needs the before and after values.
- **Storage:** a bounded in-memory ring buffer per cluster.
- **Views:** a page for the whole cluster, plus the same log filtered on each
  topic and group page.
- **Badge:** changes in the last hour.
- **Limits:**
  - it is lost on restart
  - with several klens replicas, each keeps its own log
  - it is only as fine-grained as the lane interval (10s for topology)
  - Kafka does not say *who* changed a config

### Transactions

An open transaction that never finishes pins the last stable offset. Every
consumer reading with `read_committed` then stops at that offset: its lag grows
and nothing in its logs explains why. This page is the UI version of
`kafka-transactions.sh find-hanging`.

- `list_transactions` lists transactional ids with their producer id and state.
- `describe_transactions` adds the state, `timeout_ms`, `start_time_ms` and the
  partitions involved.
- `describe_producers` gives `current_txn_start_offset` per partition: the
  offset the open transaction started at, which is where the stable offset is
  pinned.
- Flag transactions open longer than their timeout. Also flag partitions where
  `describe_producers` shows an open transaction the coordinator does not know
  about. That second case is the true hanging transaction.
- Link to this page from a stalled partition on a group page.
- **Permissions:** klens's Kafka user needs `DESCRIBE` on transactional ids, and
  `READ` on topics for `describe_producers`. Without them the page is empty and
  should say why.

### Storage

klens shows message counts but no sizes on disk. `describe_log_dirs` returns the
size of every partition replica on every broker:

- the largest topics and partitions
- disk used per broker, and uneven use across brokers. Brokers on Kafka 3.3+
  also report total and usable bytes per log dir.
- the topics growing fastest
- topics close to their `retention.bytes`

Poll it in a slow lane, like configs. Needs `DESCRIBE` on the cluster.

### Flow

A graph of topics, consumer groups and, where known, producers, coloured by lag.
It answers "who reads this topic?" before someone changes its retention or
deletes it. The consumer side is exact. The producer side is partial: see
[Producer graph research](#producer-graph-research).

```
orders ──▶ [billing] ──▶ invoices        solid: exact (group assignments, transactions)
orders ┈┈▶ [fraud-svc]                   dashed: inferred from a header or a name
  ▲
  └── 3 unnamed producers (last write 2s ago)
```

Show a coverage line such as "producers identified for 62% of recently written
partitions", so nobody reads the map as complete.

### Trace

Search across topics for records with the same key or the same correlation
header (`traceparent`, `correlation-id`), within a time window around a
timestamp. Results are ordered by time, which shows the path a message took,
e.g. `orders → payments → payments.dlq`.

- Seeking by timestamp keeps each partition's scan short.
- Key lookup (below) skips most partitions.
- Filters match the obfuscated view. A hashed field can still be traced by its
  `kx:` token, because tokens are deterministic. Clicking a token in the record
  drawer could start a trace.
- Needs the `records` privilege on every cluster searched.

### Compare (optional)

Shown only when more than one cluster is configured. Topic configs and
partition counts side by side, to catch drift between staging and prod:
"`orders`: retention 7d on prod, 1d on staging; 12 vs 6 partitions; missing on
dr". The config lane already fetches every topic's configs. A user needs
`configs` on both clusters, and the comparison must never reveal a cluster
their bindings do not cover.

### Quotas (optional)

Producer and consumer byte-rate quotas per user and client id, from
`describe_client_quotas`. Useful but rarely visited.

## Features on existing pages

These make existing pages better and do not need their own sidebar entry.

### Consumer Groups: status, time behind and the stuck record

Give every partition of a group a status from its recent history:

| Status          | Meaning                                                 |
| --------------- | ------------------------------------------------------- |
| OK              | lag is small and not growing                            |
| Catching up     | lag is shrinking                                        |
| Falling behind  | lag is growing                                          |
| Stalled         | committed offset not moving, new messages still arrive  |
| Idle            | nothing new to consume                                  |

Roll the worst partition status up to the group, and show it as a column on the
groups list.

- **The stuck record.** For a stalled partition, link to the record at the
  committed offset. It is almost always the message the consumer keeps failing
  on.
- **Time behind.** Read the timestamp of the record at the committed offset and
  show "4m 12s behind" next to the message count.
- **Time to catch up.** Compare the consume rate with the produce rate: "clears
  in ~6 min" or "never, falling behind at 120 msg/s".
- **Cost.** Limit the time-behind reads to groups with an interest lease. The
  list page can keep message lag for every group.
- **Needs:** a short history of committed offsets and high watermarks per
  partition, and a way to read one record at an offset.

### Topics: hot partitions, last write and topic profile

- **Hot partitions.** Keep the per-partition rates `rates_between` already
  computes and warn when one partition takes most of the traffic. That usually
  means a bad key.
- **Last write.** Record when each partition's high watermark last moved. That
  shows a producer that went quiet, and it works for every client.
  `describe_producers`' `last_timestamp` gives the same per producer.
- **Topic profile.** From a sample of recent records:
  - the most frequent keys ("customer 42 is 38% of messages")
  - for JSON topics without a schema, the inferred fields with how often each
    appears and their types

  The profile runs on the obfuscated view, so it follows the same privacy rules
  as the rest of the UI.

### Record drawer: key history and links

- **Key history.** Every version of a record's key, with a JSON diff between
  consecutive values and deletions marked. For state, change-data-capture and
  event-sourced topics, that is the entity's audit trail. Hashed fields still
  show *that* they changed, because equal values give equal tokens.
- **Record links.** A URL for one record (cluster, topic, partition, offset) to
  paste into a ticket or chat.
- **Trace this record.** Opens Trace with the record's key, headers and
  timestamp filled in.

### Schema Registry: version diff

The subject API already returns every version. Show what changed between two
versions (fields added, removed or retyped). That helps when a compatibility
check fails.

## Record browsing

- **Jump to an offset.** `RecordParams` (`src/app/records/types.rs`) takes
  partitions, a time range, `contains`, a schema id and a cursor, but not a
  starting offset. Error logs and dead-letter headers usually give a partition
  and an offset.
- **Key lookup.** Find a key by scanning only the partitions it can be in. See
  [Key to partition](#key-to-partition) for why this is less simple than it
  looks.
- **Filters that target a field.**
  - Choose key, value or a named header, instead of one substring search over
    key and value together.
  - Conditions on decoded JSON, e.g. `value.status = "FAILED"`.
  - The staged evaluation in `src/kafka/scan/filter.rs` (`Verdict::NeedsPayload`)
    already fits this: cheap checks on raw bytes first, then decoded payloads.
  - Field conditions must run on the obfuscated view, like `contains`.
    Otherwise a filter could recover a hidden value one guess at a time.
- **Export.** Download a filtered range as NDJSON. Only single records can be
  downloaded today. Export reuses the scan, so the `records` privilege,
  obfuscation and `tuning.records` limits apply.

## Operations

- **Prometheus `/metrics`.** klens has no metrics. Useful ones:
  - how old each lane's data is
  - how often scans hit their deadline (`complete: false`)
  - live tails against `tuning.tail.max_live`
  - scan consumer pool size
  - optionally, per-group lag, since klens already computes it
- **Lag and throughput trends.** Keep the last hour or so of lag per group and
  rate per topic in memory. Show a small trend line that says whether a group is
  catching up or falling further behind. The same history feeds the group
  status above.
- **Audit log.** Log a structured event when someone reads records, starts a
  tail, exports, or impersonates a user: who, which cluster, which topic.
  PR #361 already logged writes under the `klens::audit` target, and reads can
  follow the same shape.

## MCP server and API tokens

Expose klens as an MCP server, so an engineer's agent can debug Kafka through
klens instead of with direct cluster credentials.

- **Why klens fits.** It already has what makes this safe: privileges checked
  per cluster, obfuscation applied before anything leaves the server, and the
  rule that a filter can only search what the page shows.
- **Tools, read only:** search, describe a topic, describe a group with its
  status, lag, read records, sample the latest records, trace.
- **Limits:** the same `tuning.records` caps as the UI, plus the audit log.
- **Auth:** needs non-browser authentication: personal access tokens, or OIDC
  bearer tokens. That also makes a CLI and scripts possible.

## Writes

PR #361 (reset and delete consumer group offsets) was closed without merging.
Its design is the reference for any write feature:

- writes are off unless a cluster lists them under `writes`
- signed-in users also need a role granting each write privilege
- `dryRun` on resets, and deletion requires repeating the group id
- every write attempt is logged under `klens::audit`
- cross-site requests and unknown body fields are rejected

Producing messages, e.g. to replay a dead-letter topic, fits the same shape
behind a `produce` privilege. It is the biggest step away from "not a Kafka
platform", so it comes last.

## Producer graph research

**Question:** can `describe_producers` give a full producer → topic → consumer
graph?

**Answer:** not on its own. It says *that* producer ids 4012 and 4013 wrote to
`orders` partition 3 recently, not *who* they are.

### What it returns

For each partition, a list of active producers (`ProducerStateInfo` in krafka's
`src/admin/mod.rs`):

- `producer_id`
- `producer_epoch`
- `last_sequence`
- `last_timestamp`
- `coordinator_epoch`
- `current_txn_start_offset`

There is no client id, principal or host.

### Why it cannot give a full graph

1. **No names.** A producer id is only a number.
2. **Only idempotent and transactional producers get an id.**
   - Java clients 3.0+ are idempotent by default.
   - librdkafka, which sits under the Confluent Python, Go, .NET and Node
     clients, defaults to not idempotent. So does Sarama.
   - Producers that are not idempotent do not appear at all. In a mixed setup
     that can be a large share of them.
3. **An id is one producer instance for one run.** Ten replicas show as ten ids,
   and a redeploy gives them all new ids. Brokers keep an idle producer's state
   until `producer.id.expiration.ms` (1 day by default on Kafka 3.4+), so
   producers that stopped hours ago still appear.
4. **It is queried per partition.** Each request goes to the partition leader,
   so it needs one batched request per broker, in a slow lane. It also needs
   `READ` on each topic, the same as consuming.

### How to name producers

From most to least reliable:

1. **Transactional producers.** `list_transactions` maps each `transactional.id`
   to its producer id, and those ids are usually meaningful names. Kafka Streams
   apps with exactly-once enabled use transactional ids that start with their
   `application.id`, which is also their consumer group id. That gives a real
   *topic A → app → topic B* chain.
2. **Kafka Streams internal topics.** `<application.id>-…-changelog` and
   `<application.id>-…-repartition` topics give write edges from the naming
   convention alone.
3. **A header set in config.** For example `producer_header: x-service` per
   cluster. klens samples the latest record on each partition and reads the
   header. This works with every client, idempotent or not.
4. **Naming guesses.** Group `billing` probably matches transactional id
   `billing-0`. Show these as dashed "inferred" edges, never solid ones.

To match unnamed producer ids to header names (3), klens needs each record's
producer id. krafka parses it from the record batch (`producer_id` in
`src/protocol/record.rs`), but `ConsumerRecord` does not expose it. That is a
small upstream change to krafka.

### Plan

1. Consumer edges, which klens already knows exactly, plus named transactional
   producers and unnamed producer ids grouped by the topics they write. No new
   config.
2. The producer header setting.
3. The krafka change to expose the batch producer id on `ConsumerRecord`.

The same `describe_producers` data also feeds the Transactions page
(`current_txn_start_offset`) and the last-write signal (`last_timestamp`).

## Shared building blocks

| Building block | Used by |
| --- | --- |
| Read one record at an offset | stuck record, time behind, jump to offset, record links |
| Short per-partition history of offsets and watermarks | group status, time to catch up, trends, hot partitions, last write |
| Change log of bus deltas with before and after values | Activity, Overview |
| Key to candidate partitions | key lookup, key history, Trace |
| Slow admin lane (log dirs, transactions, producers, quotas) | Storage, Transactions, Flow, Quotas |
| Non-browser auth (API tokens) | MCP server, CLI, scripts |
| Privilege-aware `visibleSections` and badge tones | every new sidebar page |

### Key to partition

Scanning only the partition a key hashes to is much faster than scanning all of
them, but clients disagree on the hash:

| Client                              | Default partitioner for keyed records |
| ----------------------------------- | ------------------------------------- |
| Java, franz-go                      | murmur2                               |
| librdkafka (Confluent clients)      | CRC32 (`consistent_random`)           |
| Sarama                              | FNV-1a                                |

Custom partitioners exist too, and adding partitions moves keys: older records
of a key can sit in a different partition than new ones.

So: compute the candidate partition under each common hash, which gives at most
three partitions. Scan those first, and fall back to every partition when
nothing matches or the partition count changed. A per-topic `partitioner`
setting can skip the guessing.

## Open questions

- Should time behind be visible to catalog-only users? It reveals a timestamp
  but no payload.
- Should Activity survive restarts? That needs storage, which klens has avoided
  so far.
- Which graph layout library for Flow? It has to stay readable with hundreds of
  topics.
- Should key lookup guess the partitioner, require it in config, or both?
- Should the MCP server be a separate listener, or routes under `/api`?
