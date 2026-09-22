---
name: verify-klens
description: Drive the klens Kafka web UI the way a user does. Use when proving a UI or API change, or when you need a local launch, doctor, drive, and cleanup recipe for klens.
---

# Verify klens

klens is a single-process Kafka inspector. The user-facing surface is the web UI served by the Rust process (embedded `ui` feature). JSON routes under `GET /api` are the data API the UI calls. Live catalog updates are `GET /api/clusters/{cluster}/updates` (`text/event-stream`). There is no product CLI or TUI.

Read `features/README.md` before a drive. Drive one mapped feature end to end. Do not treat an API-only check as UI proof.

## Launch

Use the helpers. They write state under the run directory and refuse to start a second instance in the same directory.

```sh
.cursor/skills/verify-klens/helpers/launch.sh
```

What that does:

1. Builds `web/` into `static/`, then `cargo build --locked --features ui`.
2. Writes an isolated config (`bind: 127.0.0.1:18080`, cluster `local` at `127.0.0.1:9092`).
3. Starts Redpanda only when port 9092 is closed. Prefers `mise kafka:up` (same task `.cursor/environment/bootstrap.sh start` runs after Docker). Falls back to `rpk container start` with the ports in `mise.toml`.
4. Creates topic `klens-verify-topics` and produces one record (`verify-1` / `hello-from-verify-klens`).
5. Starts `target/debug/klens` with `KLENS_CONFIG_PATH` pointing at that config.

Ready when `GET /health` returns 204 and `GET /` returns HTML titled `klens`. The process log line is `listening`.

This checkout's Cursor environment install is `mise install`, `mise run types`, and `cargo fetch --locked`. Its start is Docker, then `mise kafka:up`. Verify still builds and binds its own klens process.

Overrides, all optional:

- `KLENS_VERIFY_PORT` (default `18080`)
- `KLENS_VERIFY_RUN_DIR` (default `.cursor/skills/verify-klens/run`)
- `KLENS_VERIFY_BROKERS` (default `127.0.0.1:9092`)
- `RPK` (path to `rpk`; default `rpk` on `PATH`)
- `KLENS_VERIFY_TOPOLOGY_SECS` (harness only, seconds, default `10`). Written into the generated cluster as `ingest.topology_secs`. The topology lane refreshes metadata and consumer-group membership. A short interval can fail `ListGroups` against Redpanda and set `clusters.topology.lastError`, which fails doctor. For a multi-feature drive set `600` so the lane stays quiet. That override is session harness, not a product default. The old `KLENS_TOPOLOGY_LANE_INTERVAL` and `KLENS_CATALOG_POLL_INTERVAL` names are ignored.

Do not use `mise web:dev` / `vp dev` for verification. `web/vite.config.ts` proxies `/api` and `/health` to `http://localhost:8080`, so a Vite session cannot bind a private port.

Do not start a second verify instance in the same run directory. Two instances can run only with distinct `KLENS_VERIFY_PORT` and `KLENS_VERIFY_RUN_DIR` values. They may share one Kafka broker. Never drive an instance this run did not start.

Teardown is `helpers/cleanup.sh`. It kills only the PID recorded at launch.

## Doctor

```sh
.cursor/skills/verify-klens/helpers/doctor.sh
```

Read-only. Fail if any check misses:

- The PID file exists and that PID is alive.
- `/proc/<pid>/comm` is `klens`.
- The recorded port is held by that PID.
- `GET /health` is 204.
- `GET /api/auth/me` is `{"enabled":false,"user":null}` (verify configs omit OIDC).
- `GET /` includes `<title>klens</title>`.
- `GET /api/clusters` includes `local`, has topology `updatedAt` set, and has no topology `lastError`.

If doctor fails, stop driving. Relaunch or fix the unmet check.

## Drive

Prefer the helper for the feature under test. Topics is the seeded proof path:

```sh
.cursor/skills/verify-klens/helpers/drive-topics.mjs
```

That script uses Playwright against `/usr/bin/google-chrome` (override with `CHROME`). It opens the UI, waits for the Topics heading, checks the table has no `Rows per page` footer, toggles `Show internal`, filters to the seeded topic, opens the topic, and writes evidence.

Drive the live ACL page before opening a topic Data tab. A records fetch timeout can poison the shared krafka client so later `acls` RPCs also time out.

If you drive by hand, use these handles from this repo. Prefer them over coordinates.

| Control | Handle |
|---|---|
| Home redirect | `/` → `/cluster/<name>/topics` when a cluster exists (`web/src/routes/index.tsx`) |
| Topics page | `/cluster/local/topics`, heading `Topics` |
| Topic search | `input[data-search-hotkey]` placeholder `Search topics…` |
| Internal topics | label `Show internal` |
| Sidebar | links `Topics`, `Consumer Groups`, `Schema Registry`, `Brokers`, `ACLs` |
| Command palette | header button visible label `Search` (accessible name `Search Ctrl+K`), or `Meta+K` / `Control+K`; `/` opens the dialog only when no `data-search-hotkey` field is visible; dialog title `Search klens` |
| Catalog alerts | `Cluster unreachable` when `topology.lastError` is set and `topology.updatedAt` is null; `Topology lane failing` when both are set |
| Topic row | table cell with the topic name; click opens `/cluster/local/topics/<name>` |
| Topic tabs | `Data`, `Partitions`, `Consumer groups`, `Configuration` |
| Schema Registry page | `/cluster/local/schemas`, heading `Schema registry` |
| Schema search | `input[data-search-hotkey]` placeholder `Search subjects…` |
| ACLs page | `/cluster/local/acls`, heading `ACLs` |
| ACL search | `input[data-search-hotkey]` placeholder `Search ACLs…` |
| Auth off | `/login` redirects to `/`; no `Continue with SSO` |

The JSON API the UI uses (corroborate, do not substitute for the UI path):

```sh
curl -sS "$KLENS_VERIFY_URL/api/clusters/local/topics"
```

## Evidence

Write proof under `.cursor/skills/verify-klens/artifacts/<run-id>/`. Cleanup must not delete that directory.

A pass captures the user action and the resulting state:

- Screenshot of the page after the action, with the `klens` sidebar wordmark visible.
- An ARIA snapshot (Playwright `ariaSnapshot`) of the same page.
- The JSON body that backs the view (topics list, topic, groups, or brokers).
- The URL after navigation.
- For a produce or seed, a second read of the topic (UI row or record key) after the write.

Proof standards:

- Exercise the real UI route. A 200 from `/api` alone is not UI proof.
- Mocks stop at Kafka and Schema Registry. Do not stub `/api` or `/api/auth/me`.
- If `clusters.topology.lastError` is set and `updatedAt` is null, that is a verified-unreachable catalog (`Cluster unreachable`), not a Topics pass. Record the alert text and stop. `Topology lane failing` (stale `updatedAt` plus a later `lastError`) is also not a catalog pass.
- Dry-run does not apply. klens always talks to the configured brokers.

## Cleanup

```sh
.cursor/skills/verify-klens/helpers/cleanup.sh
```

Sends SIGTERM to the recorded klens PID, waits, then SIGKILL if needed. Removes the run directory (config, pid, log). Leaves `artifacts/` in place.

If launch wrote `started_kafka=1`, cleanup runs `rpk container stop`. If Kafka was already listening on 9092, leave it.

Never `pkill klens`. Never kill by binary name.

## Helpers

All scripts are executable. Run them from any cwd. They resolve the repo root from their own path.

| Script | Purpose |
|---|---|
| `helpers/launch.sh` | Build, config, optional Kafka, seed, start, wait for `/health` |
| `helpers/doctor.sh` | Read-only liveness, port owner, auth, API health |
| `helpers/drive-topics.mjs` | Playwright proof of the Topics feature |
| `helpers/drive-command-palette.mjs` | Playwright proof of palette search and arrow keys |
| `helpers/cleanup.sh` | Stop the PID this run started |

Install the browser driver once per machine:

```sh
cd .cursor/skills/verify-klens/helpers && npm install
```
