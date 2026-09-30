# Feature ideas

Each feature below is a self-contained brief. To have an agent build one, copy
the fenced block under its heading and give it to the agent as its task. Every
block carries the context, repo conventions, rules and "done when" criteria the
agent needs, so it does not depend on this page or on the conversation that
produced it.

Some briefs build on others. The index lists what each one needs first, and the
brief itself tells the agent to check for it and stop if it is missing.

Nothing here is a commitment. Written against `main` at `c5bb0ce`; code
references name files and symbols, which may have moved since.

## Index

| # | Brief | Area | Size | Needs first |
| --- | --- | --- | --- | --- |
| 1 | [Sidebar groups, privileges and badge tones](#1-sidebar-groups-privileges-and-badge-tones) | web | S | |
| 2 | [Overview page](#2-overview-page) | both | M | 1 |
| 3 | [Offset and watermark history](#3-offset-and-watermark-history) | backend | M | |
| 4 | [Records at an offset and record links](#4-records-at-an-offset-and-record-links) | both | M | |
| 5 | [Consumer group status and the stuck record](#5-consumer-group-status-and-the-stuck-record) | both | L | 3, 4 |
| 6 | [Activity page](#6-activity-page) | both | L | 1 |
| 7 | [Transactions page](#7-transactions-page) | both | L | 1 |
| 8 | [Key lookup](#8-key-lookup) | both | M | |
| 9 | [Sizes on Topics, partitions and Brokers](#9-sizes-on-topics-partitions-and-brokers) | both | L | |
| 10 | [Flow page](#10-flow-page) | both | L | 1 |
| 11 | [Audit log](#11-audit-log) | backend | S | |
| 12 | [API tokens](#12-api-tokens) | backend | M | |
| 13 | [MCP server](#13-mcp-server) | backend | L | 11, 12 |
| 14 | [Trace page](#14-trace-page) | both | L | 1, 4, 8 |
| 15 | [Key history](#15-key-history) | both | M | 4, 8 |
| 16 | [Hot partitions and last write](#16-hot-partitions-and-last-write) | both | S | |
| 17 | [Lag and throughput trends](#17-lag-and-throughput-trends) | both | M | 3 |
| 18 | [Topic profile](#18-topic-profile) | both | M | |
| 19 | [Field filters](#19-field-filters) | both | L | |
| 20 | [Export records](#20-export-records) | both | M | 11 |
| 21 | [Prometheus metrics](#21-prometheus-metrics) | backend | M | |
| 22 | [Schema version diff](#22-schema-version-diff) | web | S | |
| 23 | [Compare clusters](#23-compare-clusters) | both | M | 1 |
| 24 | [Quotas page](#24-quotas-page) | both | S | 1 |
| 25 | [Produce and replay messages](#25-produce-and-replay-messages) | both | L | 11 |

## Suggested order

1. Briefs 1 and 2: the sidebar groundwork, then Overview, which fills the empty
   cluster landing page with data klens already has.
2. Briefs 3, 4 and 5: consumer group status answers the most common reason
   people open a Kafka UI, and 3 and 4 are reused by later briefs.
3. Brief 6, Activity: cheap, because the change detection already exists.
4. Brief 7, Transactions: hard to diagnose anywhere else.
5. Briefs 8 and 9: key lookup, then sizes on existing pages.
6. Brief 10, Flow, then 11, 12 and 13 for the MCP server.

The rest can follow in any order once what they need has landed.

The proposed sidebar once the page briefs land:

```
Cluster
  Overview          brief 2, replaces the redirect to Topics
  Topics
  Consumer Groups
  Schema Registry
  Brokers
  ACLs
Insights
  Activity          brief 6
  Transactions      brief 7
  Flow              brief 10
  Trace             brief 14, needs the records privilege
  Compare           brief 23, only with more than one cluster
  Quotas            brief 24, needs the configs privilege
```

Sizes on disk (brief 9) deliberately have no page: they go on the pages that
already list topics, partitions and brokers.

---

## 1. Sidebar groups, privileges and badge tones

Groundwork for every new page: sidebar groups, sections hidden by any privilege,
and badges that can warn.

````markdown
# klens: Sidebar groups, privileges and badge tones

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Every page reads a background projection of each cluster that ingest lanes
refresh on their own schedules (`src/kafka/ingest/`, `src/kafka/store/`). Users
have per-cluster privileges (`records`, `configs`, `schema_text`, `acls`). The
web app reads them with `useAccess().can(cluster, "RECORDS")`
(`web/src/hooks/use-access.ts`).

The sidebar is built from `SECTIONS` in `web/src/lib/sections.ts`: Topics,
Consumer Groups, Schema Registry, Brokers, ACLs. `visibleSections(canAcls)` only
knows one privilege. `web/src/components/nav-main.tsx` renders every section
under one "Cluster" group label, with an optional count badge that is always
muted (`SidebarMenuBadge` with `text-muted-foreground`).
`web/src/components/app-sidebar.tsx` passes the counts, and
`web/src/components/command-palette.tsx` also lists sections through
`visibleSections`.

Several planned pages (Overview, Activity, Transactions, Flow, Trace, Compare,
Quotas) need the sidebar to support more than it does today.

## Goal

Make the sidebar ready for new pages without adding any page yet.

## What to build

- **Groups.** A section belongs to a sidebar group: `cluster` (the existing
  "Cluster" label) or `insights` (a new "Insights" label below it). A group with
  no visible sections renders nothing, so "Insights" stays hidden until a page
  joins it.
- **Visibility rules.** A section can declare the privilege it needs (any of the
  four) and an extra condition, such as "the user can see more than one
  cluster". Replace `visibleSections(canAcls)` with a function that takes the
  user's access for the current cluster. ACLs keeps working exactly as today.
- **Badge tones.** A badge has a value and a tone: `muted` (today's look),
  `warn` or `error`. Use the existing tone tokens (`web/src/lib/tone.ts`,
  `web/src/components/status.tsx`) so light and dark themes both work. Counts
  keep the muted tone.
- **Ordering.** A section can be placed before Topics, for a future Overview
  entry.
- The command palette and breadcrumbs (`useActiveSection`) follow the same rules
  as the sidebar.

## Rules

- No visible change for existing users: same five sections, same order, same
  ACLs rule.
- Keep the collapsed icon-only sidebar and the mobile sheet working.

## Done when

- A new section can be added with a one-line entry that names its group,
  privilege and optional condition.
- Unit tests cover the visibility rules (privilege missing, condition false,
  empty group hidden).
- `bunx vp check` and `bun run build` pass in `web/`.

## Out of scope

- Any new page or route.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 2. Overview page

A landing page for each cluster that answers "is this cluster OK?".

````markdown
# klens: Overview page

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Every page reads a background projection of each cluster that ingest lanes
refresh on their own schedules (`src/kafka/ingest/`, `src/kafka/store/`). Users
have per-cluster privileges (`records`, `configs`, `schema_text`, `acls`),
checked through `session.cluster(name)?` and `.records()?`, `.configs()?`, etc.
in `src/app/context.rs`. Without a privilege a user still sees the catalog:
topics, groups, brokers and lag.

`/cluster/$cluster` has no page: `web/src/routes/cluster/$cluster/index.tsx`
redirects to Topics. The store already holds everything an overview needs:
brokers and the controller, topics with `under_replicated` partitions, per-topic
message rates (`RateStore`), groups with their state and total lag, and each
lane's health (`useClusterHealth` in `web/src/lib/api/catalog.ts`, shown by
`web/src/components/lane-caption.tsx`). Live changes arrive over the
`/api/clusters/{cluster}/updates` server-sent event stream
(`src/app/updates.rs`, `web/src/lib/api/updates.ts`).

## Goal

A one-screen answer to "is this cluster OK?", as the cluster's landing page.

## What to build

### Backend

- `GET /api/clusters/{cluster}/overview`, built from the store only (no Kafka
  calls), returning:
  - broker count, the controller, and broker ids that partitions list as
    replicas but that metadata no longer reports (likely down)
  - counts of offline and under-replicated partitions, and the topics they
    belong to (top 10)
  - total messages per second, and the 5 busiest topics
  - the 5 groups with the most lag, and groups that are Empty, Dead or
    rebalancing
  - each lane's freshness and last error
- Keep the payload small (top-N lists, not every row). It is catalog data, so
  no privilege beyond seeing the cluster.

### UI

- Replace the redirect with an Overview page, and add an "Overview" sidebar
  entry above Topics.
- Tiles for each area. Every item links to its detail page (topic, group,
  broker).
- Refresh from the updates stream, like the other pages.
- Leave room for sections that later briefs add: recent activity, disk use and
  growth, consumer group status. Do not build them here.

## Rules

- Healthy looks calm: only problems get warning or error tones.
- Pending data (a lane that has not polled yet) shows as pending, not as zero.

## Done when

- `/cluster/$cluster` shows the overview. Existing links to `/topics` still
  work.
- API tests cover the endpoint, using store fixtures with offline and
  under-replicated partitions and lagging groups.
- The page renders cleanly with an empty cluster and with a lane still pending.

## Out of scope

- Activity feed, sizes, group status and reassignments. Other briefs add them.

## Needs first

- Sidebar groups and section ordering (brief "Sidebar groups, privileges and
  badge tones"). Check `web/src/lib/sections.ts` supports placing a section
  before Topics. If it does not, add just that and nothing else from that brief.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 3. Offset and watermark history

A short in-memory history of committed offsets and high watermarks, reused by
group status, trends and catch-up estimates.

````markdown
# klens: Offset and watermark history

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Every page reads a background projection of each cluster that ingest lanes
refresh on their own schedules (`src/kafka/ingest/`, `src/kafka/store/`).

Two lanes matter here:

- The watermark lane (`src/kafka/ingest/watermarks.rs`) reads every partition's
  low and high watermark every `tuning.ingest.watermark` (3s).
  `rates_between` computes each partition's high watermark delta, then sums
  them per topic. `RateStore` (`src/kafka/store/rates.rs`) keeps only the latest
  total per topic.
- The offset lane (`src/kafka/ingest/offsets.rs`) reads committed offsets:
  every `tuning.ingest.fast_offset` (2s) for groups someone has open, tracked by
  interest leases (`src/kafka/store/interest.rs`), and every
  `tuning.ingest.slow_offset` (20s) otherwise. Each wave publishes a
  `GroupOffsetsWave` on the store's change bus (`src/kafka/store/bus.rs`).

Nothing keeps past values, so klens cannot say whether lag is growing or how
fast a group consumes.

## Goal

Keep a short, bounded history per partition of high watermarks and of each
group's committed offset, with helpers to answer rate and trend questions.
Other briefs build features on it. This one adds no UI.

## What to build

- A history structure on `ClusterStore`:
  - per topic partition: timestamped high watermark samples
  - per group, topic and partition: timestamped committed offset samples
- Record samples where the lanes commit their tables, so none are lost to a
  lagging bus subscriber.
- A bounded window, e.g. `tuning.ingest.history_window` (default 15m), plus a
  cap on samples per series. Drop series when their topic, partition or group
  disappears (`TopologyDelta` lists removals).
- Helpers, each with unit tests:
  - produce rate of a partition over a window
  - consume rate of a group on a partition over a window
  - lag trend over a window: growing, shrinking or flat, robust to one noisy
    sample
  - how long the committed offset has been unchanged
  - estimated time to catch up (lag divided by consume rate minus produce
    rate), or "never" when the group falls behind
- Log truncation and a reset committed offset must not produce negative rates;
  treat them as a break in the series.

## Rules

- Memory matters: recent commits trimmed what the lanes and tables hold. State
  the worst-case memory for 10k partitions and 1k groups in the pull request,
  and pick defaults that keep it modest.
- Slow-polled groups have sparse samples (one per 20s). Helpers must say when
  there are too few samples to answer, rather than guess.

## Done when

- The history fills from both lanes and respects the window and the sample cap.
- Tests cover every helper, including truncation, resets, sparse series and
  removed topics or groups.
- The new setting is documented in the README tuning block.

## Out of scope

- Any API or UI. Consumer group status and trends are separate briefs.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 4. Records at an offset and record links

Open a partition at a given offset, and share a link to one record.

````markdown
# klens: Records at an offset and record links

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Users have per-cluster privileges; reading records needs `records`, checked
through `session.cluster(name)?.records()?` (`src/app/context.rs`), which hands
back a `Granted<RecordsCap>` whose `read` runs the scan. Obfuscation rules hide
parts of records inside the scan, before filters run and before anything is
rendered.

`GET /api/clusters/{cluster}/topics/{topic}/records` takes the parameters in
`RecordParams` (`src/app/records/types.rs`): partitions, `order`, a `from`/`to`
time range, `contains`, `schemaId`, `limit` and a `cursor`. There is no way to
start at an offset. Paging uses `RecordCursor` (`src/kafka/scan/cursor.rs`) and
`plan_windows` (`src/kafka/scan/plan.rs`), which already support starting
positions per partition (`Remaining::From`).

In the UI, the Data tab of a topic (`web/src/routes/cluster/$cluster/topics_.$topic.tsx`)
renders `RecordBrowser` (`web/src/components/records/`). A selected record opens
in a drawer in `record-view.tsx`. Topic page search params are validated in
`web/src/lib/route-search.ts` (`topicDetailSearch`).

## Goal

People usually arrive with a partition and an offset from an error log or a
dead-letter header. Let them open that record directly, and share a link to
it.

## What to build

### Backend

- An `offset` parameter on the records endpoint. It requires exactly one
  partition and reads forward from that offset, with normal paging after it.
- A reusable way to read the one record at a partition and offset through the
  same scan pipeline, so decoding and obfuscation apply. Other briefs reuse it,
  e.g. to read the timestamp of the record at a consumer's committed offset.
- Clear outcomes when the record is not there:
  - below the low watermark: deleted by retention
  - at or above the high watermark: not written yet
  - inside the range but missing (compacted away, or a transaction marker):
    return the next record and say that the exact offset is gone

### UI

- A "Go to offset" control in the record browser: partition and offset.
- Record links: `?tab=data&partition=3&offset=1234` on the topic page opens the
  Data tab at that record with its drawer open. The drawer gets a "Copy link"
  button.
- Show the not-there outcomes above as messages, not errors.

## Rules

- Everything stays behind the `records` privilege. A user without it who opens
  a link sees the same thing as for any record URL today.
- Validate the new search params in `web/src/lib/route-search.ts`.

## Done when

- API tests cover a normal offset, below low, at or above high, a missing offset
  in a compacted topic, and the privilege check.
- Opening a record link in a fresh tab lands on that record with the drawer
  open.

## Out of scope

- Searching by key (a separate brief).

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 5. Consumer group status and the stuck record

Say whether each group is fine, catching up, falling behind or stuck, and point
at the record it is stuck on.

````markdown
# klens: Consumer group status and the stuck record

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Every page reads a background projection of each cluster that ingest lanes
refresh on their own schedules (`src/kafka/ingest/`, `src/kafka/store/`). Users
have per-cluster privileges (`records`, `configs`, `schema_text`, `acls`),
checked in `src/app/context.rs`. Without a privilege a user still sees the
catalog, including groups and lag.

Committed offsets are polled every 2s for groups someone has open (interest
leases, `src/kafka/store/interest.rs`) and every 20s otherwise. Groups are
served by `src/app/groups.rs` (`GroupRow`, `GroupDetail`, `GroupOffset` in
`src/app/groups/types.rs`) and shown in `web/src/routes/cluster/$cluster/groups.tsx`
and `groups_.$group.tsx`. Lag today is a message count only.

## Goal

For every group, say what state it is in and, when it is stuck, which record it
is stuck on and how far behind it is in time.

## What to build

### Status per partition and per group

Compute a status for each partition a group has committed offsets on, from the
offset and watermark history:

| Status | Meaning |
| --- | --- |
| OK | lag is small and not growing |
| Catching up | lag is shrinking |
| Falling behind | lag is growing |
| Stalled | committed offset unchanged for a while, new messages still arriving, and the group has members |
| Idle | nothing new to consume |

- Roll the worst partition status up to the group.
- Add the status to `GroupRow` and `GroupOffset`, show it as a column on the
  groups list and on the group's offsets tab, and make it filterable.
- Add "time to catch up" per group: "clears in ~6 min", or "never, falling
  behind at 120 msg/s".
- A status needs enough samples. Show "not enough data yet" instead of guessing,
  especially for slow-polled groups.

### Time behind and the stuck record

- For a partition, "time behind" is now minus the timestamp of the record at
  the committed offset. Show "4m 12s behind" next to the message lag.
- Read that record only for groups with an interest lease, and cache the result
  until the committed offset moves.
- For a stalled partition, link to the record at the committed offset: it is
  almost always the one the consumer keeps failing on.
- Both need the `records` privilege, because they read a record. Users without
  it see the status and message lag only.

## Rules

- Thresholds (how long unchanged counts as stalled, what counts as "small" lag)
  are named constants or `tuning` settings, documented in the README.
- No extra Kafka load for groups nobody is looking at, beyond what the offset
  lane already does.

## Done when

- Unit tests cover each status, including sparse samples, a reset offset and a
  truncated log.
- API tests cover the new fields, and show that time behind is absent without
  `records`.
- A group that stops committing while its topic keeps receiving messages shows
  as Stalled, with a working link to the record at its committed offset.

## Needs first

- The offset and watermark history (brief "Offset and watermark history"): a
  bounded history on `ClusterStore` with rate and trend helpers.
- Reading one record at an offset (brief "Records at an offset and record
  links"), plus record links on the topic page.

Check both exist before starting. If either is missing, stop and report it
rather than building a partial copy.

## Open questions

- Should time behind be visible to catalog-only users? It reveals a timestamp,
  no payload. Default to no; note the choice in the pull request.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 6. Activity page

A timeline of what changed in the cluster, built from changes klens already
detects.

````markdown
# klens: Activity page

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Every page reads a background projection of each cluster that ingest lanes
refresh on their own schedules (`src/kafka/ingest/`, `src/kafka/store/`). Users
have per-cluster privileges (`records`, `configs`, `schema_text`, `acls`),
checked in `src/app/context.rs`. Without a privilege a user still sees the
catalog.

Each lane compares its new snapshot with the previous one (`diff` in
`LaneSource`, `src/kafka/ingest/runner.rs`) and publishes a `Change` on the
store's bus (`src/kafka/store/bus.rs`): `TopologyDelta` (topics and groups
added, removed or changed, broker changes), `ConfigsDelta`, `SubjectsDelta`,
watermark ticks and group offset waves. The deltas carry names only, not what
changed. The topology lane runs every 10s, configs every 60s and subjects every
30s. Nothing is kept once published.

## Goal

When something breaks, the first question is "what changed?". Keep a timeline
of cluster changes and show it.

```
14:02  payments-7   in-sync replicas shrank [1,2,3] → [1,2]
14:03  billing      rebalanced, 4 → 3 members (billing-7f9c left)
14:05  orders       retention.ms 7d → 1d
14:06  orders-value schema v12 registered
```

## What to build

### Backend

- Build entries where the lanes compute their diffs, because both the previous
  and the next table are available there. Cover at least:
  - topics created or deleted, partition count changes
  - leader changes, in-sync replica sets shrinking or growing, partitions going
    offline
  - brokers joining or leaving, controller changes
  - groups created or deleted, state changes, members joining or leaving
  - topic config changes, with the key and the old and new values
  - new schema subjects and versions
- Keep entries in a bounded in-memory ring buffer per cluster (a `tuning`
  setting for its size).
- `GET /api/clusters/{cluster}/activity` with filters for topic, group, broker
  and time, and push new entries over the existing updates stream
  (`src/app/updates.rs`).

### UI

- An "Activity" page in the sidebar's Insights group, newest first, filterable.
- The same entries filtered on each topic and group page.
- A sidebar badge with the number of changes in the last hour, in a warning tone
  when any entry is a problem (offline partition, shrinking replicas).

## Rules

- Config values are `configs` data. Without that privilege, an entry says that
  a topic's configuration changed, without keys or values.
- Entries only name clusters the user can see; go through
  `session.cluster(name)?` like every other endpoint.
- Only add entries for real changes. A lane that reconnects must not flood the
  log.

## Done when

- Tests cover each entry kind from table diffs, the privilege filtering of
  config entries, and the ring buffer bound.
- Changing a topic's retention shows up on the page within one config lane
  period.

## Out of scope

- Keeping entries across restarts. Say in the UI that the log starts when klens
  started.
- Who made a change: Kafka does not report it.

## Needs first

- Sidebar groups and badge tones (brief "Sidebar groups, privileges and badge
  tones"). Check `web/src/lib/sections.ts` supports an Insights group and badge
  tones. If it does not, stop and report it.

## Open questions

- With several klens replicas, each keeps its own log. Is that acceptable, or
  does the page need to say so?

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 7. Transactions page

Find open transactions that block `read_committed` consumers.

````markdown
# klens: Transactions page

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Every page reads a background projection of each cluster that ingest lanes
refresh on their own schedules (`src/kafka/ingest/`, `src/kafka/store/`). Users
have per-cluster privileges, checked in `src/app/context.rs`. Without a
privilege a user still sees the catalog.

krafka (pinned by git rev in `Cargo.toml`) has admin calls klens does not use
yet, in its `src/admin/transactions.rs`:

- `list_transactions(state_filters, producer_id_filters, duration_filter, transactional_id_pattern)`:
  transactional ids with their producer id and state
- `describe_transactions(&[&str])`: per id, the state, `timeout_ms`,
  `start_time_ms`, producer id and epoch, and the partitions in the current
  transaction
- `describe_producers(&[(&str, &[i32])])`: per partition, the active producers
  with `producer_id`, `last_timestamp` and `current_txn_start_offset` (`-1` when
  no transaction is open)

## Goal

An open transaction that never finishes pins a partition's last stable offset.
Every consumer reading with `read_committed` then stops at that offset: its lag
grows and nothing in its logs explains why. This page finds those transactions.
It is the UI version of `kafka-transactions.sh find-hanging`.

## What to build

### Backend

- A lane (`tuning.ingest.transactions`, e.g. 30s) that lists transactions in
  `Ongoing`, `PrepareCommit` and `PrepareAbort` and describes them.
- On demand from the page, cached briefly, a `describe_producers` sweep over
  partitions (batched per broker) to find partitions with an open transaction.
- A partition is hanging when its open transaction started longer ago than the
  broker's `transaction.max.timeout.ms` (default 15 minutes), or when the
  coordinator does not know about it.
- `GET /api/clusters/{cluster}/transactions` returning:
  - open transactions: id, producer id and epoch, state, age, timeout,
    partitions
  - hanging partitions: topic, partition, producer id, the offset the
    transaction started at, and the consumer groups reading that topic

### UI

- A "Transactions" page in the sidebar's Insights group: open transactions, with
  those past their timeout flagged, and a "Hanging partitions" section at the
  top when there are any.
- Each hanging partition says "blocks read_committed consumers at offset N" and
  links to the topic and to the affected groups.
- A sidebar badge in a warning tone with the number of hanging partitions.
- The group page links here when a stalled partition has an open transaction.

## Rules

- Kafka permissions: `DESCRIBE` on transactional ids for listing and
  describing, and `READ` on topics for `describe_producers`. When klens's Kafka
  user lacks them, the page says so instead of showing an empty list.
- Treat this as catalog data (no klens privilege beyond seeing the cluster),
  unless you find a reason not to; note it in the pull request.
- Add the new calls to `ClusterSession` and the fake cluster.

## Done when

- Tests cover the hanging rules (past timeout, unknown to the coordinator, not
  hanging), the permission-denied case and the endpoint.
- With a local broker, a producer that begins a transaction and never commits
  shows up as hanging after the timeout.

## Out of scope

- Aborting transactions (a write).

## Needs first

- Sidebar groups and badge tones (brief "Sidebar groups, privileges and badge
  tones"). Check `web/src/lib/sections.ts` supports an Insights group and badge
  tones. If it does not, stop and report it.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 8. Key lookup

Find records by exact key, scanning only the partitions the key can be in.

````markdown
# klens: Key lookup

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Reading records needs the `records` privilege, checked through
`session.cluster(name)?.records()?` (`src/app/context.rs`). Obfuscation rules
hide parts of records inside the scan, before filters run.

`GET /api/clusters/{cluster}/topics/{topic}/records` takes `RecordParams`
(`src/app/records/types.rs`). The only content filter is `contains`: one
case-insensitive substring match over key and value together
(`src/kafka/scan/filter.rs`). The live tail (`.../records/tail`) takes the same
filter parameters. The UI's record browser is in
`web/src/components/records/`.

## Goal

Find every record with a given key, much faster than a substring search, by
scanning only the partition the key hashes to.

## What to build

- A `key` parameter on the records endpoint and the tail: exact match on the
  key.
- Partition pruning. Clients disagree on how a key picks a partition:

  | Client | Default partitioner for keyed records |
  | --- | --- |
  | Java, franz-go | murmur2 |
  | librdkafka (the Confluent clients) | CRC32 (`consistent_random`) |
  | Sarama | FNV-1a |

  Compute the candidate partition under each of the three (at most three
  partitions), scan those first, and fall back to every partition when nothing
  matches. Implement each formula exactly as its client does, with test vectors
  taken from each client.
- A per-topic override in the cluster config, e.g. `partitioner: murmur2`, to
  skip the guessing.
- Keys framed by a schema registry (magic byte and schema id) cannot be
  reproduced from typed text, so pruning does not apply to them: match the
  decoded key text and scan every partition.
- UI: a key field in the record browser, and "Find records with this key" in
  the record drawer.

## Rules

- Custom partitioners exist, and adding partitions moves keys: older records of
  a key may sit in another partition. The fallback scan covers both. Say in the
  UI when the result came from the fallback.
- The key filter matches the obfuscated view, like `contains`. A hashed key
  matches its `kx:` token, never the cleartext.

## Done when

- Test vectors for all three hashes match each client's reference output.
- Tests cover a key found by pruning, found only by fallback, a framed key, an
  obfuscated key, and the tail.

## Out of scope

- Key history (a separate brief, built on this one).

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 9. Sizes on Topics, partitions and Brokers

Sizes on disk on the pages that already list topics, partitions and brokers,
instead of a separate page.

````markdown
# klens: Sizes on Topics, partitions and Brokers

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Every page reads a background projection of each cluster that ingest lanes
refresh on their own schedules (`src/kafka/ingest/`, `src/kafka/store/`). Users
have per-cluster privileges (`records`, `configs`, `schema_text`, `acls`),
checked in `src/app/context.rs`. Without a privilege a user still sees the
catalog. Broker configs, including `log.dirs`, need `configs`.

klens shows message counts but no sizes. krafka (pinned by git rev in
`Cargo.toml`) has `describe_log_dirs(topics: Option<Vec<DescribableLogDirTopic>>)`
in its `src/admin/partitions.rs`, returning `LogDirInfo` per broker log
directory (`src/admin/mod.rs`):

- `broker_id`, `log_dir` (path), `error`
- `total_bytes` and `usable_bytes` of the volume (`-1` when unknown; Kafka 3.3+)
- a cordoned flag on newer brokers
- per partition replica: `partition_size`, `offset_lag` (behind the high
  watermark) and `is_future_key` (a replica being moved)

Pages involved: `web/src/routes/cluster/$cluster/topics.tsx` (list),
`topics_.$topic.tsx` (header facts and partitions tab), `nodes.tsx` (brokers
list) and `nodes_.$id.tsx` (broker detail: facts and configs only). API types:
`TopicRow`, `TopicDetail`, `PartitionRow` (`src/app/topics/types.rs`),
`BrokerRow` (`src/app/brokers/types.rs`). `formatBytes` exists in
`web/src/lib/format.ts`.

## Goal

Show each size on the page that already lists what it belongs to.

## What to build

### Backend

- `describe_log_dirs` on `ClusterSession` and the fake cluster.
- A slow lane, `tuning.ingest.log_dirs` (default 60s, like configs), with its
  table in the store. Until its first poll, sizes are pending, the same way
  retention waits for topic configs.
- Sizes on the existing API types:
  - `TopicRow` and `TopicDetail`: size of one copy (summed from each partition's
    leader replica) and disk used across all replicas
  - `PartitionRow`: size, and per replica its size, offset lag and whether it is
    being moved
  - `BrokerRow`: bytes used, and total and usable bytes when known
- `GET /api/clusters/{cluster}/brokers/{id}/log-dirs`: each directory's use,
  error and cordoned flag, plus the replicas the broker hosts. Paths only with
  `configs`.

### UI

| Page | What to add |
| --- | --- |
| Topics list | sortable `Size` column; tooltip with disk used across replicas |
| Topic header | "4.2 GiB, 12.6 GiB on disk" next to the message count |
| Partitions tab | `Size` column; replica pill tooltips with size, offset lag and a "moving" mark |
| Brokers list | `Disk` column with a usage bar when total bytes are known |
| Broker detail | disk use in the header; a `Log dirs` section; a `Partitions` tab listing hosted replicas, largest first, with leader or follower and offset lag |

- When a topic sets `retention.bytes`, show how full each partition is on the
  partitions tab, and flag the topic on the Topics list when a partition is
  close.
- A broker disk above a threshold, or a log dir with an error, gets a warning
  tone on the Brokers list, the broker page and the Brokers sidebar badge.
- "Bytes per message": partition size divided by retained messages, shown on the
  partitions tab.

## Rules

- `retention.bytes` applies per partition: compare it with each partition, never
  with the topic total.
- `describe_log_dirs` counts only local segments. On topics with
  `remote.storage.enable=true`, label the size as local.
- Sizes are catalog data, visible to every user. Log dir paths describe the
  broker's filesystem, so show them only with `configs`.
- klens's Kafka user needs `DESCRIBE` on the cluster. Without it, sizes stay
  blank with a tooltip saying why.
- A broker's answer lists every replica it hosts. If it can exceed
  `tuning.kafka.max_response_mib`, ask for topics in batches.

## Done when

- Lane tests cover a normal poll, a log dir error, unknown volume size, and a
  moving replica.
- API tests cover sizes on topics, partitions and brokers, and log dir paths
  hidden without `configs`.
- All five pages show sizes against a local broker, and the pending state
  before the first poll.

## Out of scope

- Growth over time. It needs a size history; keep the lane's data shaped so one
  can be added.

## Open questions

- What disk usage turns a broker's warning on: fixed 80% and 90%, or a `tuning`
  setting?

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 10. Flow page

A graph of which groups read which topics and, where it can be known, who
writes them.

````markdown
# klens: Flow page

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Every page reads a background projection of each cluster that ingest lanes
refresh on their own schedules (`src/kafka/ingest/`, `src/kafka/store/`). Users
have per-cluster privileges, checked in `src/app/context.rs`. Without a
privilege a user still sees the catalog.

The consumer side is already known exactly: each group's members, their client
ids and assigned partitions (`GroupInfo` in `src/kafka/store/tables.rs`), and
the topics each group has committed offsets on.

The producer side is not. Research on what Kafka can tell us:

- krafka's `describe_producers` (in its `src/admin/transactions.rs`) returns,
  per partition, active producers with `producer_id`, `producer_epoch`,
  `last_sequence`, `last_timestamp`, `coordinator_epoch` and
  `current_txn_start_offset`. There is no client id, principal or host.
- Only idempotent or transactional producers get an id. Java clients 3.0+ are
  idempotent by default; librdkafka (under the Confluent Python, Go, .NET and
  Node clients) and Sarama default to not idempotent, so their producers do not
  appear at all.
- An id is one producer instance for one run: ten replicas are ten ids, and a
  redeploy gives new ids. Brokers keep an idle producer's state until
  `producer.id.expiration.ms` (1 day by default on Kafka 3.4+).
- It is queried per partition leader: batch it per broker, in a slow lane. It
  needs `READ` on each topic.

Ways to name producers, most reliable first:

1. `list_transactions` maps each `transactional.id`, usually a meaningful name,
   to its producer id. Kafka Streams apps with exactly-once use transactional
   ids starting with their `application.id`, which is also their consumer group
   id: a real "topic A → app → topic B" chain.
2. Kafka Streams internal topics, `<application.id>-…-changelog` and
   `<application.id>-…-repartition`, give write edges from their names alone.
3. A per-cluster setting naming a header producers stamp, e.g.
   `producer_header: x-service`; klens samples the latest record per partition
   and reads it. This works for every client.
4. Naming guesses: group `billing` probably matches transactional id
   `billing-0`.

krafka parses each record batch's producer id (`producer_id` in its
`src/protocol/record.rs`) but `ConsumerRecord` does not expose it, so klens
cannot yet tie an unnamed producer id to a header value.

## Goal

A graph of topics, consumer groups and known producers, coloured by lag. It
answers "who reads this topic?" before someone changes its retention or deletes
it, and shows as much of the producer side as can be known, marked by
confidence.

## What to build

### Backend

- A slow lane: `describe_producers` over all partitions, batched per broker, and
  `list_transactions` for the id-to-name map. Keep edges seen recently, with a
  last-seen time.
- `GET /api/clusters/{cluster}/flow`: nodes (topics, groups, named producers,
  unnamed producer groups) and edges, each edge marked exact or inferred, with
  lag and rate where known, plus coverage: the share of recently written
  partitions whose producers are named.
- Sources for this version: exact consumer edges; transactional producers named
  through `list_transactions`; Kafka Streams naming; unnamed producer ids
  grouped by the set of topics they write.

### UI

- A "Flow" page in the sidebar's Insights group.
- Solid edges for exact links, dashed for inferred ones. Unnamed producers shown
  as one node per topic set: "3 unnamed producers, last write 2s ago".
- A coverage line, so nobody reads the map as complete.
- Focus on one topic or group and its neighbours; the whole cluster can have
  thousands of nodes.
- Open from a topic or group page ("Show in flow").

## Rules

- Never draw an inferred link as exact.
- Without `READ` on topics or `DESCRIBE` on transactional ids, show the consumer
  side and say what is missing.

## Done when

- Tests cover edge building from fixtures: exact consumer edges, a named
  transactional producer, a Streams app, unnamed producers grouped, and the
  coverage figure.
- The page stays readable when focused on a topic with dozens of groups.

## Out of scope

- The producer header setting and the krafka change to expose batch producer
  ids. Follow-ups once this lands.

## Needs first

- Sidebar groups (brief "Sidebar groups, privileges and badge tones"). Check
  `web/src/lib/sections.ts` supports an Insights group. If it does not, stop and
  report it.

## Open questions

- Which layout library: `@xyflow/react`, `elkjs`, or `@dagrejs/dagre`? Pick one
  that stays readable with hundreds of nodes and justify it in the pull request.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 11. Audit log

Log who read which records, as structured events.

````markdown
# klens: Audit log

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Users sign in with OIDC (`src/app/auth/`) or, when auth is off, use it
anonymously. They have per-cluster privileges (`records`, `configs`,
`schema_text`, `acls`), checked in `src/app/context.rs`, and obfuscation rules
hide parts of records inside the scan. Logging is set up in `src/telemetry.rs`.

Record reads happen in `src/app/records.rs` (pages) and
`src/app/records/tail.rs` (live tail). Admins can impersonate a user
(`POST /api/auth/impersonate`, `src/app/auth.rs`). An earlier, unmerged pull
request for offset resets logged every write attempt under the `klens::audit`
tracing target; follow that shape.

## Goal

Teams that use privileges and obfuscation will ask who looked at what. Emit one
structured event per sensitive action.

## What to build

- Events under the `klens::audit` target for: a record page read, a tail started
  and ended, an impersonation, and a denied privilege check on those routes.
- Fields: who (the signed-in subject and email, or `anonymous` with the client
  address when auth is off), who they impersonate if anyone, cluster, topic,
  partitions, whether a filter was used, how many records were returned, and
  whether the topic is obfuscated.
- A README section describing the events and their fields, and how to route the
  target to its own sink with the log filter.

## Rules

- Never log a filter's value, only that one was used and its length: a search
  for a card number would otherwise put the number in the log.
- Never log record contents.
- Events go out even when the normal log level is `warn`.

## Done when

- Tests capture events (there is a capture helper in `src/telemetry.rs`) for
  each action, with and without auth, and check that filter values never
  appear.

## Out of scope

- Shipping events anywhere but the log.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 12. API tokens

Let scripts, a CLI and agents call the API without a browser session.

````markdown
# klens: API tokens

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Auth is optional OIDC with the authorization code flow and cookie sessions
(axum-login, `src/app/auth/`). The auth middleware is applied to the API routes
in `src/app.rs`. Roles are named privilege sets (`records`, `configs`,
`schema_text`, `acls`), bound to IdP groups and optionally scoped to clusters
(`auth.roles` in the config; see the README's Authentication section). Secrets
in the config name where to read them: `{value: …}`, `{env: NAME}` or
`{file: PATH}`.

## Goal

Non-browser clients (scripts, a CLI, the planned MCP server) need to call the API
with the same privilege model as signed-in users.

## What to build

- Service tokens in the config, e.g.:

  ```yaml
  auth:
    tokens:
      - name: ci-reader
        token: { env: KLENS_CI_TOKEN }
        role: viewer
        clusters: [staging]
  ```

- Requests with `Authorization: Bearer <token>` authenticate as that token's
  role and cluster scope. Compare in constant time, and keep only a digest in
  memory.
- The identity shows up in `whoami` and in logs by the token's name, never its
  value.
- README documentation next to the Authentication section.

## Rules

- Tokens get exactly the privileges of their role, on their clusters. A cluster
  outside the scope is reported as unknown, as for users.
- Cookie and bearer auth must not mix on one request.
- A config with a token that is too short (under 32 bytes) fails at startup.

## Done when

- Tests cover a valid token, a wrong token, a token scoped away from a cluster,
  a missing privilege, and startup validation.

## Out of scope

- Tokens minted from the UI (they would need storage klens does not have).
- Accepting OIDC access tokens as bearer tokens. A possible follow-up.

## Open questions

- Should `auth.tokens` work without OIDC configured (a tokens-only mode)?

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 13. MCP server

Let an engineer's agent debug Kafka through klens, with the same privileges and
obfuscation as the UI.

````markdown
# klens: MCP server

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Users have per-cluster privileges (`records`, `configs`, `schema_text`, `acls`),
checked in `src/app/context.rs`; without a privilege a user still sees the
catalog. Obfuscation rules hide parts of records inside the scan, before
filters run, so a filter can only match what the page shows. Record reads are
capped by `tuning.records`.

The API already covers what an agent needs: clusters, search
(`src/app/search.rs`), topics, groups with lag, brokers, subjects, ACLs and
record pages.

## Goal

Expose klens as a Model Context Protocol server, so an agent can answer "why is
billing lagging?" through klens instead of with direct cluster credentials. The
privilege checks and server-side obfuscation klens already has are what make
this safe.

## What to build

- An MCP endpoint over streamable HTTP at `/api/mcp`, for example with the
  official Rust SDK, `rmcp`.
- Read-only tools, each a thin layer over the existing handlers and `Granted`
  calls:
  - `list_clusters`, `search`
  - `describe_topic`, `describe_group` (with lag, and status if the group status
    feature exists), `describe_broker`
  - `read_records` (topic, partitions, time range, filter, limit) and
    `latest_records`
  - `get_schema`
- Each tool checks the same privilege as its page and returns the same
  obfuscated view.
- Tool descriptions written for a model: what the tool is for, what it returns,
  and its limits.
- Every call goes to the audit log.
- README documentation, including how to connect a client.

## Rules

- No write tools.
- Cap output: record tools default to a small limit and never exceed
  `tuning.records.max_limit`; truncate long payloads and say so.
- Authenticate like the API: a session cookie or a service token.

## Done when

- Tests cover each tool's privilege check and obfuscation, and output caps.
- An MCP client (for example `npx @modelcontextprotocol/inspector`) can list and
  call the tools against a local klens.

## Needs first

- The audit log (brief "Audit log"): events under `klens::audit`.
- Service tokens (brief "API tokens"): `Authorization: Bearer` support.

Check both exist before starting. If either is missing, stop and report it.

## Open questions

- A separate listener, or routes under `/api`? Default to `/api/mcp`.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 14. Trace page

Follow a message across topics by key or correlation header.

````markdown
# klens: Trace page

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Reading records needs the `records` privilege, checked through
`session.cluster(name)?.records()?` (`src/app/context.rs`). Obfuscation rules
hide parts of records inside the scan, before filters run. Hashed fields become
deterministic `kx:` tokens, so equal values give equal tokens.

Record pages seek by timestamp (`from`/`to` in `RecordParams`,
`src/app/records/types.rs`), which turns a time window into a short offset range
per partition. `ClusterSession::offsets_for_times` exists
(`src/kafka/session.rs`).

## Goal

Answer "where did `order-123` go?": find records with the same key or the same
correlation header across topics, within a time window, and show them in time
order, e.g. `orders → payments → payments.dlq`.

## What to build

### Backend

- `GET /api/clusters/{cluster}/trace` with: a key or a header (`name` and
  `value`), a centre timestamp, a window (default ±5 minutes), and a topic
  pattern (exact or trailing `*`).
- For each matching topic, turn the window into offset ranges, skip empty ones,
  and scan with key pruning when searching by key.
- Results ordered by timestamp, with a `complete` flag and a resume cursor when
  the deadline hits, like record pages.
- A cap on topics per trace (`tuning`), and the same scan deadline as record
  pages.

### UI

- A "Trace" page in the sidebar's Insights group, hidden without `records`.
- A timeline of matches grouped by topic, each opening the record.
- "Trace this record" in the record drawer, prefilled with its key, headers and
  timestamp.
- Clicking a `kx:` token in the drawer starts a trace for it.

## Rules

- Filters match the obfuscated view, like every other filter.
- Only clusters and topics the user can see are searched.

## Done when

- Tests cover key and header traces across topics, empty windows skipped, the
  topic cap, obfuscated tokens, and a partial result with a cursor.

## Needs first

- Sidebar groups and privilege rules (brief "Sidebar groups, privileges and
  badge tones").
- Records at an offset and record links (brief "Records at an offset and record
  links").
- Key lookup with partition pruning (brief "Key lookup").

Check all three exist before starting. If any is missing, stop and report it.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 15. Key history

Every version of a key, with a diff between versions.

````markdown
# klens: Key history

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Reading records needs the `records` privilege (`src/app/context.rs`).
Obfuscation rules hide parts of records inside the scan; hashed fields become
deterministic `kx:` tokens, so equal values give equal tokens. Values decoded
through a schema registry are JSON. The record drawer is in
`web/src/components/records/record-view.tsx`.

## Goal

For state, change-data-capture and event-sourced topics, a key's versions are
the entity's audit trail. From any record, show every version of its key.

## What to build

- "History of this key" in the record drawer.
- A view listing each version oldest to newest: offset, partition, timestamp,
  and a JSON diff against the previous version (fields added, removed,
  changed). Deletions (null values) marked as such.
- Backed by key lookup, reading every match across pages, with the scan's usual
  deadline and a resume cursor.
- Non-JSON values fall back to a text diff.

## Rules

- Diffs run on the obfuscated view. A hashed field shows *that* it changed
  (different tokens) without revealing either value; a masked field never shows
  a change.
- On compacted topics, older versions may be gone. Say so in the view.

## Done when

- Tests cover the diff (nested objects, arrays, type changes, deletions) and the
  obfuscated cases.
- The view works on a compacted topic and on a topic with the key spread over
  two partitions after a partition increase.

## Needs first

- Key lookup (brief "Key lookup"): a `key` parameter with partition pruning.
- Records at an offset and record links (brief "Records at an offset and record
  links").

Check both exist before starting. If either is missing, stop and report it.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 16. Hot partitions and last write

Per-partition rates, skew warnings, and when each partition was last written.

````markdown
# klens: Hot partitions and last write

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
The watermark lane (`src/kafka/ingest/watermarks.rs`) reads every partition's
watermarks every 3s. `rates_between` computes each partition's high watermark
delta, then sums them per topic; `RateStore` (`src/kafka/store/rates.rs`) keeps
only the latest topic total, published on the bus as a `WatermarksTick`.
`PartitionRow` (`src/app/topics/types.rs`) has no rate. The partitions tab is
in `web/src/routes/cluster/$cluster/topics_.$topic.tsx`.

## Goal

Show which partitions take the traffic, warn when one takes most of it (usually
a bad key), and show when each partition was last written.

## What to build

- Keep per-partition rates alongside the topic totals, and add `rate` to
  `PartitionRow`.
- Record when each partition's high watermark last moved, and add `lastWriteAt`
  to `PartitionRow`. It is unknown until klens has seen a move since it started;
  say "no writes since klens started".
- A skew flag on the topic when one partition's share of traffic is far above a
  fair share and the topic is busy enough to matter. Put both thresholds in
  named constants or `tuning`.
- UI: `Msg/s` and `Last write` columns on the partitions tab, a warning in the
  topic header when skewed, and a filter for skewed topics on the Topics list.

## Rules

- Idle topics are never "skewed".
- Existing topic rate behaviour and the updates stream payload stay compatible.

## Done when

- Tests cover per-partition rates, last write tracking across restarts of the
  lane, and the skew rule's edges (idle topic, one-partition topic, even
  traffic).

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 17. Lag and throughput trends

Small trend lines that show whether lag and traffic are rising or falling.

````markdown
# klens: Lag and throughput trends

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Groups show a single current lag (`GroupRow`, `GroupDetail` in
`src/app/groups/types.rs`) and topics a single current rate. Live updates arrive
over `/api/clusters/{cluster}/updates` (`src/app/updates.rs`,
`web/src/lib/api/updates.ts`).

## Goal

Show at a glance whether a group is catching up or falling further behind, and
whether a topic's traffic is rising.

## What to build

- API: recent lag per group and recent rate per topic as short series from the
  offset and watermark history, e.g. one point per 30s over the history window.
  Either a field on existing rows or a separate endpoint; keep list payloads
  small.
- UI: a sparkline next to lag on the groups list and group page, and next to
  the rate on the topics list and topic header. Hovering shows the value and
  time.
- The series grows live from the updates stream rather than refetching.

## Rules

- Readable in light and dark themes, using the existing tone tokens.
- Sparse series (slow-polled groups) render as sparse, never interpolated into
  something that looks precise.

## Done when

- Tests cover the series built from history, including gaps.
- A group whose lag grows shows a rising line within a minute.

## Needs first

- The offset and watermark history (brief "Offset and watermark history"): a
  bounded history on `ClusterStore`. Check it exists before starting. If it does
  not, stop and report it.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 18. Topic profile

What a topic's recent records look like: top keys and, for JSON, the fields.

````markdown
# klens: Topic profile

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Reading records needs the `records` privilege (`src/app/context.rs`).
Obfuscation rules hide parts of records inside the scan, before anything else
sees them. Values decoded through a schema registry are JSON; schemaless topics
may carry raw JSON text. The topic page has Data, Partitions, Groups and
Configuration tabs (`web/src/routes/cluster/$cluster/topics_.$topic.tsx`).

## Goal

Describe a topic from a sample of its recent records, so people understand an
unfamiliar topic in seconds.

## What to build

- `GET /api/clusters/{cluster}/topics/{topic}/profile`: sample the newest N
  records across partitions (a `tuning` setting, e.g. 1000) through the normal
  scan, and return:
  - the most frequent keys with their share ("customer 42 is 38% of messages"),
    and how many distinct keys the sample had
  - record size distribution (smallest, median, largest)
  - for JSON values: each field path with how often it appears and its types
  - header names and how often they appear
- A "Profile" tab on the topic page, behind `records`.
- Cache a profile briefly, and show when it was sampled and how many records it
  covers.

## Rules

- The profile runs on the obfuscated view. Masked keys group as `***`; hashed
  keys stay tokens.
- Bounded by the scan deadline and `tuning.records` limits; a partial sample is
  labelled as partial.

## Done when

- Tests cover key counting, field inference across nested objects and arrays,
  mixed types, non-JSON values, and obfuscated fields.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 19. Field filters

Filter records by key, value or header, and by conditions on decoded JSON.

````markdown
# klens: Field filters

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Reading records needs the `records` privilege (`src/app/context.rs`).
Obfuscation rules hide parts of records inside the scan, before filters run: a
filter can only match what the page shows. That rule exists so a filter cannot
recover a hidden value one guess at a time.

The only content filter today is `contains` (`src/kafka/scan/filter.rs`): one
case-insensitive substring match over key and value together, compiled with
aho-corasick. It is evaluated in stages: `on_raw` checks raw bytes and returns
`Verdict::NeedsPayload` when a field is registry-framed, and `on_payload` checks
the decoded text. Record pages and the live tail take the same parameters
(`RecordParams`, `src/app/records/types.rs`). The record browser's filter UI is
in `web/src/components/records/record-view.tsx` (`RecordFilter`).

## Goal

Let people narrow records precisely: by where the text is, and by conditions on
decoded values, e.g. `value.status = "FAILED"`.

## What to build

- Scoped text filters: key only, value only, or a named header.
- Conditions on decoded JSON values: a field path with `=`, `!=`, `<`, `>`,
  `contains` and `exists`, combined with AND. Paths use the same syntax as
  obfuscation field rules (`card.number`; an array on the path fans out).
- Compile once per query, and keep the staged evaluation: cheap raw checks first,
  decoding only when needed.
- The same parameters on record pages and the tail.
- UI: filter chips in the record browser, one per condition, next to the
  existing search.

## Rules

- Every condition runs on the obfuscated view. A condition on a masked field can
  never match anything but `***`; on a hashed field, only its token.
- Invalid expressions are 400 errors that name the problem.

## Done when

- Tests cover each operator, array fan-out, missing fields, type mismatches,
  framed payloads, the tail, and obfuscated fields (including that a condition
  cannot reveal a masked value).

## Open questions

- A text syntax typed into one box, or structured chips only? Chips are safer to
  start with.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 20. Export records

Download a filtered range of records as NDJSON.

````markdown
# klens: Export records

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Reading records needs the `records` privilege (`src/app/context.rs`).
Obfuscation rules hide parts of records inside the scan. Record pages
(`GET /api/clusters/{cluster}/topics/{topic}/records`, `RecordParams` in
`src/app/records/types.rs`) are capped by `tuning.records.max_limit` (500) and
page with a cursor. Today the UI can only download one record at a time, from
the record drawer (`web/src/components/records/record-view.tsx`).

## Goal

Download everything a filter matches in a range, for sharing or offline
analysis.

## What to build

- `GET /api/clusters/{cluster}/topics/{topic}/records/export` with the same
  parameters as a record page, streaming NDJSON (one record per line, the same
  shape as `Record` in the API) with a `Content-Disposition` filename.
- Pages through the scan until the range ends or a cap is hit
  (`tuning.records.max_export`, e.g. 10000). The last line says whether the
  export is complete.
- An "Export" button in the record browser that uses the current filters.
- An audit event per export: who, cluster, topic, filters used (never their
  values), record count.

## Rules

- Same privilege and the same obfuscated view as a record page.
- Streams as it reads; never buffer the whole export in memory.

## Done when

- Tests cover a complete export, the cap, filters, obfuscation, the privilege
  check, and the audit event.

## Needs first

- The audit log (brief "Audit log"): events under `klens::audit`. Check it
  exists before starting. If it does not, stop and report it.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 21. Prometheus metrics

A `/metrics` endpoint for klens's own health, and optionally consumer lag.

````markdown
# klens: Prometheus metrics

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Every page reads a background projection of each cluster that ingest lanes
refresh (`src/kafka/ingest/`), each with a `LaneHealth` (last update, last
error, last poll duration; `src/kafka/store/lane.rs`). Record scans report
`complete: false` when they hit their deadline. Live tails are capped by
`tuning.tail.max_live`. Scan consumers are pooled (`tuning.scan.pool_*`). klens
computes lag for every group. `/health` and `/ready` are public
(`src/app/health.rs`); everything else can sit behind OIDC. There are no metrics
today.

## Goal

Let operators monitor klens itself, and optionally use it as a lightweight lag
exporter.

## What to build

- A Prometheus text endpoint, off by default, on its own listener, e.g.
  `metrics: { bind: 0.0.0.0:9090 }`, so it is never exposed with the UI.
- Metrics:
  - per cluster and lane: age of the last successful poll, poll duration, errors
  - record scans: count, duration, and how many hit the deadline
  - live tails running, and tails refused at the cap
  - scan consumer pool size
  - HTTP requests by route and status
- Optional per-group lag (`metrics.group_lag: true`), labelled by cluster, group
  and topic.
- README documentation with every metric and label.

## Rules

- The metrics listener has no auth, so it must not expose anything the catalog
  would hide; group lag stays opt-in because group and topic names are catalog
  data.
- Pick a crate (e.g. `prometheus-client`, or `metrics` with
  `metrics-exporter-prometheus`) that passes `cargo audit` and `cargo machete`.
- Keep label cardinality bounded: routes as templates, never raw paths.

## Done when

- Tests scrape the endpoint and check each metric family and label.
- Disabled by default, with no listener bound.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 22. Schema version diff

Compare two versions of a subject's schema.

````markdown
# klens: Schema version diff

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Schema bodies need the `schema_text` privilege. The subjects API
(`src/app/subjects.rs`) lists each subject's `versions` and returns one version's
schema with `GET /api/clusters/{cluster}/subjects/{subject}?version=N`. Avro,
JSON Schema and Protobuf are supported. The page is
`web/src/routes/cluster/$cluster/schemas.tsx`.

## Goal

When a compatibility check fails or a consumer breaks after a deploy, show what
changed between two versions of a schema.

## What to build

- On a subject, pick two versions (default: latest and the one before) and show
  the difference.
- First a line diff of both schemas pretty-printed the same way, so formatting
  never shows up as a change.
- For Avro, a field-level summary on top: fields added, removed, retyped, and
  defaults changed.
- Probably no backend change: both versions come from the existing endpoint.

## Rules

- Behind `schema_text`, like the schema body itself.

## Done when

- Tests cover the diff for each schema type and the Avro field summary (nested
  records, unions, defaults).

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 23. Compare clusters

Topic configs and partition counts side by side across clusters.

````markdown
# klens: Compare clusters

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
It connects to several clusters at once. Every page reads a background
projection of each cluster; the config lane fetches every topic's configs every
60s. Users have per-cluster privileges (`records`, `configs`, `schema_text`,
`acls`), checked in `src/app/context.rs`. A cluster no role binding covers is
reported as unknown, never as forbidden, so nobody can probe for clusters they
may not know about.

## Goal

Catch drift between clusters that should match, e.g. staging and prod:
"`orders`: retention 7d on prod, 1d on staging; 12 vs 6 partitions; missing on
dr".

## What to build

- `GET /api/clusters/{cluster}/compare/{other}`: topics present on only one
  side, partition count and replication factor differences, and differences in
  the topic configs that matter (`retention.ms`, `retention.bytes`,
  `cleanup.policy`, `min.insync.replicas`, `max.message.bytes`,
  `compression.type`, and any explicitly set config).
- A "Compare" page in the sidebar's Insights group, shown only when the user can
  see more than one cluster: pick the other cluster, filter by topic, show only
  differences by default.

## Rules

- Both clusters go through `session.cluster(name)?`; an unknown or hidden other
  cluster is a 404, like any cluster.
- Topic names and partition counts are catalog data. Config values need
  `configs` on both clusters; without it, show only catalog differences.
- Internal topics are excluded unless asked for.

## Done when

- Tests cover missing topics, partition and config differences, the privilege
  rules on each side, and a hidden other cluster.

## Needs first

- Sidebar groups and conditions (brief "Sidebar groups, privileges and badge
  tones"). Check `web/src/lib/sections.ts` supports an Insights group and a
  "more than one cluster" condition. If it does not, stop and report it.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 24. Quotas page

Producer and consumer quotas per user and client id.

````markdown
# klens: Quotas page

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
Users have per-cluster privileges (`records`, `configs`, `schema_text`, `acls`),
checked in `src/app/context.rs`. krafka (pinned by git rev in `Cargo.toml`) has
`describe_client_quotas`, which klens does not use yet. The ACLs page
(`src/app/acls.rs`, `web/src/routes/cluster/$cluster/acls.tsx`) is a good model:
a live Kafka call behind a privilege.

## Goal

Show which users and clients are throttled, and by how much.

## What to build

- `describe_client_quotas` on `ClusterSession` and the fake cluster.
- `GET /api/clusters/{cluster}/quotas`, fetched live and cached briefly: each
  entity (user, client id, IP, and defaults) with its quotas
  (`producer_byte_rate`, `consumer_byte_rate`, `request_percentage`,
  `controller_mutation_rate`).
- A "Quotas" page in the sidebar's Insights group, filterable by entity.

## Rules

- Behind the `configs` privilege.
- klens's Kafka user needs `DESCRIBE_CONFIGS` on the cluster. Without it, the
  page says so.

## Done when

- Tests cover each entity type, defaults, the privilege check and the
  permission-denied case.

## Needs first

- Sidebar groups and privilege rules (brief "Sidebar groups, privileges and
  badge tones"). Check `web/src/lib/sections.ts` supports an Insights group and
  a `configs` requirement. If it does not, stop and report it.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````

## 25. Produce and replay messages

Write a message, or copy records from a dead-letter topic back to their source.

````markdown
# klens: Produce and replay messages

## Context

klens is a Kafka UI: a Rust service (axum, krafka) with a React app in `web/`.
It is read-only today and describes itself as "not a Kafka platform", so writes
must be deliberate and opt-in. Users have per-cluster privileges (`records`,
`configs`, `schema_text`, `acls`), checked in `src/app/context.rs`
(`Privilege` in `src/app/auth/access.rs`). Obfuscation rules hide parts of
records inside the scan, per topic.

An earlier pull request for resetting and deleting consumer group offsets was
closed without merging. Its design is the reference for any write:

- writes are off unless a cluster lists them under `writes`
- signed-in users also need a role granting each write privilege
- a dry run where it applies, and destructive actions require repeating the
  target's name
- every write attempt is logged under the `klens::audit` target
- cross-site requests and unknown body fields are rejected

## Goal

Let authorised users produce a message to a topic, and replay records from one
topic to another, typically from a dead-letter topic back to its source.

## What to build

- A `produce` write, enabled per cluster (`writes: [produce]`) and granted by a
  new `produce` privilege.
- `POST /api/clusters/{cluster}/topics/{topic}/produce`: key, value, headers,
  optional partition. Keys and values as text or base64 bytes.
- `POST /api/clusters/{cluster}/topics/{topic}/replay`: copy a selection of
  records (partition and offsets, or a filter over a range) to a target topic,
  keeping key, value and headers, and adding a header naming the source offset.
- UI: a "Produce" dialog on the topic page, and "Replay to…" on selected
  records, both only with the privilege.

## Rules

- **Replay must not leak obfuscated data.** The server copies wire bytes the
  user may not be allowed to see. Refuse to replay from an obfuscated topic
  unless the target topic is covered by the same rule.
- Every attempt, allowed or refused, goes to the audit log.
- Encoding values through the schema registry is out of scope for the first
  version; say so in the dialog.

## Done when

- Tests cover the opt-in at both levels, the privilege, produce, replay, the
  obfuscation refusal, cross-site rejection and the audit events.

## Needs first

- The audit log (brief "Audit log"): events under `klens::audit`. Check it
  exists before starting. If it does not, stop and report it.

## Working in klens

- Checks: `cargo fmt --all`,
  `cargo clippy --workspace --all-targets --all-features -- -D warnings` and
  `cargo nextest run --workspace`; in `web/`, `bunx vp check` and
  `bun run build`. Warnings are errors.
- CI runs `cargo mutants` on the lines a pull request changes, so tests must
  fail when the new logic is mutated.
- Tests sit next to the code (`tests.rs` per module). Helpers:
  `src/app/harness.rs`, `src/kafka/store/fixtures.rs`, and the fake session in
  `src/kafka/testing.rs`.
- Kafka calls go through the `ClusterSession` trait (`src/kafka/session.rs`),
  implemented in `src/kafka/client.rs` and faked in `src/kafka/testing.rs`.
- New API types derive `ts_rs::TS`: export them from `src/app/typescript.rs`,
  list them in `xtask/src/tasks/types.rs`, then run `cargo xtask types`. Never
  edit `web/src/api/types.gen.ts` by hand.
- New settings go under `tuning` (`src/config/tuning.rs`) and into the README
  with their default.
- Commit subjects look like `feat(scope): Capitalized imperative summary`.
````
