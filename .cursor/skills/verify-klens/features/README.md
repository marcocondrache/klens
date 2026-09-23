# klens verification map

This directory is the maintained source for verifying the klens web UI. Read the index, then use the matching feature file.

## Baseline preconditions

- Launch with `.cursor/skills/verify-klens/helpers/launch.sh`.
- The UI is `http://127.0.0.1:18080` unless `KLENS_VERIFY_PORT` says otherwise.
- The run directory is `.cursor/skills/verify-klens/run` unless `KLENS_VERIFY_RUN_DIR` says otherwise.
- The cluster name is `local`. Brokers are `127.0.0.1:9092`.
- Topic `klens-verify-topics` has a record whose key is `verify-1` and whose value is `hello-from-verify-klens`.
- `helpers/doctor.sh` passes before a drive.
- Never drive an instance that this run did not start.

## Driving conventions

- Start from the baseline unless a feature names another precondition.
- Prefer ARIA roles, accessible names, and placeholders over CSS selectors or coordinates.
- Run the Topics proof with `node .cursor/skills/verify-klens/helpers/drive-topics.mjs`.
- Drive the other features with Playwright against `CHROME` or `/usr/bin/google-chrome`.
- Use the same `KLENS_VERIFY_RUN_DIR` for launch, doctor, drive, and cleanup.
- Cleanup deletes the run directory and keeps `artifacts/`.

## Proof and skip reporting

- Capture the user action and the resulting state, not only the final screen.
- UI proof includes an ARIA snapshot and a screenshot with the `klens` wordmark visible.
- Corroborate with the JSON body from `GET /api/...` and the URL after navigation.
- Record the feature id and the entry point with every artifact.
- Report an unreachable path with the command you ran and the unmet precondition.
- Do not report a skipped entry point as verified through a different path.

## Feature entry contract

Each feature file starts with an H1 and one paragraph. It then uses exactly four H2 sections in this order.

1. `Sub-features`
2. `How to get to it (user POV)`
3. `Driving it with Playwright`
4. `Gotchas`

## Features

- [Topics](./topics.md) lists topics, filters the table, and opens a topic.
- [Topic records](./topic-records.md) reads records on the topic Data tab.
- [Consumer groups](./consumer-groups.md) lists groups and opens one group.
- [Schema registry](./schema-registry.md) lists subjects and opens a schema.
- [Brokers](./brokers.md) lists brokers and opens broker configuration.
- [ACLs](./acls.md) lists ACL bindings, or says authorization is disabled.
- [Command palette](./command-palette.md) jumps to a section or a catalog hit.

The partition control lives on the topic Data tab. There is no separate Follow control in the UI.
