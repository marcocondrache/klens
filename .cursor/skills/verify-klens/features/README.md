# klens verification map

This directory is the maintained source for verifying user-facing klens behavior. Read this index, then use the matching feature file as the recipe.

## Baseline preconditions

- Launch with `.cursor/skills/verify-klens/helpers/launch.sh`.
- Doctor with `.cursor/skills/verify-klens/helpers/doctor.sh`. Require `clusters` to include `local` with topology `updatedAt` set, no `lastError`, and URL `http://127.0.0.1:18080` unless `KLENS_VERIFY_PORT` changed it.
- Seed topic `klens-verify-topics` exists with key `verify-1` and value `hello-from-verify-klens`.
- Auth is off. `/api/auth/me` reports `"enabled": false`. Custom roles and the header user menu stay hidden. `whoami.subject` is null.
- Never drive an instance this run did not start.

## Driving conventions

- Start every recipe from `/` unless the feature file says otherwise.
- Prefer headings, placeholders, button names, and route paths over CSS position.
- Treat topic names and the seed payload as literals.
- Run browser steps through Playwright (`helpers/drive-topics.mjs` for Topics, `helpers/drive-command-palette.mjs` for the palette). Use `curl` only to corroborate `/api`.
- Drive ACLs before opening topic Data. A records timeout can poison later live RPCs.
- Default topology lane interval is 10s. If doctor fails with `list_consumer_groups` after a few polls, relaunch with `KLENS_VERIFY_TOPOLOGY_SECS=600`. That is session harness, not a `launch.sh` default. `KLENS_TOPOLOGY_LANE_INTERVAL` and `KLENS_CATALOG_POLL_INTERVAL` are ignored.
- Leave `klens-verify-topics` in place across features in one session. Cleanup does not delete Kafka data unless this run started the broker.

## Proof and skip reporting

- Capture the action and the resulting state. A final screenshot without the click or filter that produced it is incomplete.
- UI proof includes an ARIA snapshot and a screenshot with the `klens` wordmark visible.
- API proof is a response body, not a status code alone.
- Record the feature ID and the URL used with every artifact.
- `clusters.topology.lastError` with `updatedAt == null` is `verified-unreachable` for catalog features. Quote the `Cluster unreachable` alert. A later poll failure with `updatedAt` set shows `Topology lane failing` and is not a catalog pass.
- Do not report a skipped entry point as verified through a different path.

## Feature entry contract

Each feature file starts with an H1 and one paragraph. It then uses exactly four H2 sections in this order.

1. `Sub-features` lists short IDs with one line each.
2. `How to get to it (user POV)` lists every user entry point.
3. `Driving it with Playwright` starts with `Preconditions:` and pairs each user action with a command and an observable result.
4. `Gotchas` lists traps that waste or invalidate a run.

## Features

- [Topics](./topics.md) covers the default landing catalog, search, internal-topic toggle, and opening a topic.
- [Topic records](./topic-records.md) covers the topic page Data tab and record payload.
- [Consumer groups](./consumer-groups.md) covers the groups catalog and a group row.
- [Schema registry](./schema-registry.md) covers the subject catalog and the subject sheet.
- [Brokers](./brokers.md) covers the broker list and the controller badge.
- [ACLs](./acls.md) covers the live ACL list and the ENABLED empty list on the verify broker.
- [Command palette](./command-palette.md) covers search from the header button, `/`, and `Meta+K` / `Control+K`.
