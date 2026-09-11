# klens verification map

This directory is the maintained source for verifying user-facing klens behavior. Read this index, then use the matching feature file as the recipe.

## Baseline preconditions

- Launch with `.cursor/skills/verify-klens/helpers/launch.sh`.
- Doctor with `.cursor/skills/verify-klens/helpers/doctor.sh`. Require cluster `local`, status `HEALTHY`, and URL `http://127.0.0.1:18080` unless `KLENS_VERIFY_PORT` changed it.
- Seed topic `klens-verify-topics` exists with key `verify-1` and value `hello-from-verify-klens`.
- Auth is off. `/auth/me` reports `"enabled": false`.
- Never drive an instance this run did not start.

## Driving conventions

- Start every recipe from `/` unless the feature file says otherwise.
- Prefer headings, placeholders, button names, and route paths over CSS position.
- Treat topic names and the seed payload as literals.
- Run browser steps through Playwright (`helpers/drive-topics.mjs` for Topics). Use `curl` only to corroborate GraphQL.
- Leave `klens-verify-topics` in place across features in one session. Cleanup does not delete Kafka data unless this run started the broker.

## Proof and skip reporting

- Capture the action and the resulting state. A final screenshot without the click or filter that produced it is incomplete.
- UI proof includes an ARIA snapshot and a screenshot with the `klens` wordmark visible.
- GraphQL proof is a response body, not a status code alone.
- Record the feature ID and the URL used with every artifact.
- An `OFFLINE` cluster is `verified-unreachable` for catalog features. Quote the `Cluster unreachable` alert.
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
- [Brokers](./brokers.md) covers the broker list and the controller badge.
- [Command palette](./command-palette.md) covers search from the header button, `/`, and `Meta+K`.
