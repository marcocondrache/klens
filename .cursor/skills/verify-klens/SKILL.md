---
name: verify-klens
description: Drive the klens Kafka web UI the way a user does. Use when proving a browser change or a JSON /api change, or when you need a local launch, doctor, drive, and cleanup recipe for klens.
---

# Verify klens

klens is one Rust process with a web UI. The user path is the browser. The UI reads JSON under `/api`. There is no product CLI.

Read `features/README.md` before a drive. Drive one mapped feature end to end. UI proof is the browser path.

## Launch

```sh
.cursor/skills/verify-klens/helpers/launch.sh
```

The helper does five things.

1. It builds `web/` into `static/`, then runs `cargo build --locked --features ui`. `mise run web:build` is used when `mise` is on `PATH`. Otherwise it uses `bun` or `npm` in `web/`. The `ui` feature embeds `static/` when `src/server/web.rs` compiles, so launch touches that file for the build and restores it afterward.
2. It writes a config with no `auth` key. The process binds `127.0.0.1:18080`. The cluster name is `local` and the brokers are `127.0.0.1:9092`.
3. It starts Redpanda only when port 9092 is closed. It prefers `mise run kafka:up` when `mise` and Docker or Podman are available. Otherwise it runs the same `rpk container start` flags as `mise.toml`. If there is no container runtime, and `rpk redpanda start` exists, it starts a local Redpanda process and records that PID.
4. It creates topic `klens-verify-topics` and produces one record. The key is `verify-1`. The value is `hello-from-verify-klens`.
5. It starts `target/debug/klens` with `KLENS_CONFIG_PATH` pointed at the run config.

The process is ready when `GET /health` and `GET /ready` both return 204. The log line is `listening`.

Do not use `mise run web:dev` for verification. `web/vite.config.ts` proxies `/api` to `http://localhost:8080`, so a Vite session cannot bind a private port.

A second launch in the same run directory exits if that klens PID is still alive. Two runs need different `KLENS_VERIFY_PORT` and `KLENS_VERIFY_RUN_DIR` values. They may share one Kafka broker. Never drive an instance this run did not start.

Optional environment:

- `KLENS_VERIFY_PORT` defaults to `18080`.
- `KLENS_VERIFY_RUN_DIR` defaults to `.cursor/skills/verify-klens/run`.
- `KLENS_VERIFY_BROKERS` defaults to `127.0.0.1:9092`.
- `KLENS_VERIFY_SKIP_BUILD=1` reuses `target/debug/klens` and `static/index.html` when both already exist.
- `CHROME` is the browser binary for the drive. The default is `/usr/bin/google-chrome`.

Verify configs omit `auth`. `GET /api/auth/me` is `{"enabled":false,"user":null}`. `GET /api/auth/login` is 404.

When a deployment does set `auth.oidc`, an unsigned visit is a full page load of `/api/auth/login`, and that route redirects to the identity provider. The `/login` page then shows **Sign in** and **Continue with SSO**. A signed-in user who opens `/login` is sent home. The helpers never turn that mode on. The topics drive proves the auth-off `/login` path in the browser.

Schema Registry is added to the config only when port 8081 is already open. `mise run kafka:up` starts it. The binary Redpanda fallback in this helper does not. With no registry, the Schema registry page lists zero subjects.

Teardown is `helpers/cleanup.sh`.

## Doctor

```sh
.cursor/skills/verify-klens/helpers/doctor.sh
```

The check is read-only. It fails if any item misses.

- The PID file exists and that PID is alive.
- `/proc/<pid>/comm` is `klens`.
- The recorded port is held by that PID.
- `GET /health` and `GET /ready` are 204.
- `GET /` and `GET /login` return the shell whose title is `klens`. The `/login` redirect runs in the browser, so curl still receives the shell.
- `GET /api/auth/me` is enabled `false` and user `null`.
- `GET /api/auth/login` is 404.
- `GET /api/clusters` includes `local`, `ready` true, `topology.updatedAt` set, and `topology.lastError` null.
- `GET /api/clusters/local/topics` includes `klens-verify-topics`.

If doctor fails, stop driving.

## Drive

Install the browser driver once per machine.

```sh
cd .cursor/skills/verify-klens/helpers && npm install
```

Topics is the seeded proof.

```sh
node .cursor/skills/verify-klens/helpers/drive-topics.mjs
```

The script uses Playwright against `CHROME` or `/usr/bin/google-chrome`. It opens `/login`, waits until the app leaves that page, filters to the seeded topic, opens the row, and writes evidence.

Use the same `KLENS_VERIFY_RUN_DIR` you passed to launch.

Handles from this repo, for a drive you run by hand:

| Control | Handle |
|---|---|
| Home | `/` redirects to `/cluster/<first>/topics` after the catalog is ready |
| Topics | `/cluster/local/topics`, heading `Topics` |
| Topic search | placeholder `Search topics…` |
| Internal topics | switch `Show internal` |
| Topic filters | button `Add filter`, then `Policy`, `Health`, or `Activity` |
| Sidebar | links `Topics`, `Consumer Groups`, `Schema Registry`, `Brokers`, `ACLs` |
| Command palette | button `Search`, or `Control+K`. Dialog name `Search klens` |
| Topic row | row whose name includes the topic. Click opens `/cluster/local/topics/<name>` |
| Topic tabs | `Data`, `Partitions`, `Consumer groups`, `Configuration` |
| Record search | placeholder `Search key or value…` |
| Record time | `From timestamp`, `To timestamp` |
| Partition select | visible value `All partitions`, options `Partition N` |
| Record order | visible value `Newest` or `Oldest` |
| Consumer groups | `/cluster/local/groups`, heading `Consumer groups` |
| Schema registry | `/cluster/local/schemas`, heading `Schema registry` |
| Brokers | `/cluster/local/nodes`, heading `Brokers` |
| ACLs | `/cluster/local/acls`, heading `ACLs` |
| Auth off | `/login` leaves the login page. `Continue with SSO` is absent |

`/` on a list page focuses the visible `data-search-hotkey` input. It opens the command palette only when that input is not on the page.

`Cluster unreachable` means `topology.updatedAt` is null and `topology.lastError` is set. `Topology lane failing` means both are set. Either alert means the catalog drive failed.

JSON the UI calls, to corroborate a view:

```sh
curl -sS "$URL/api/clusters/local/topics"
curl -sS "$URL/api/clusters/local/topics/klens-verify-topics/records?order=NEWEST&limit=50"
```

## Evidence

Write proof under `.cursor/skills/verify-klens/artifacts/<run-id>/`. Cleanup must not delete that directory. Launch stores the run id in the run directory. The topics drive reads it.

A pass captures the action and the resulting state.

- A screenshot after the action, with the `klens` sidebar wordmark visible.
- An ARIA snapshot of the same page.
- The URL after navigation.
- The JSON body behind the view, from `GET /api/...`.
- For the seeded topic, the record key and value on the Data tab and again in the records JSON.

Proof standards:

- Use the real UI route. Do not stub `/api` or `/api/auth/me`.
- Kafka and Schema Registry are the external boundary. Do not mock them inside klens.
- If `Cluster unreachable` or `Topology lane failing` is on screen, record the alert text and stop. That is not a Topics pass.
- klens always talks to the configured brokers. There is no dry-run mode.

## Cleanup

```sh
.cursor/skills/verify-klens/helpers/cleanup.sh
```

The helper sends SIGTERM to the recorded klens PID, waits, then SIGKILL if needed. If launch started a container, cleanup runs `mise run kafka:down` or `rpk container stop`. If launch started a Redpanda binary, cleanup kills that PID and its children. It then deletes the run directory. It leaves `artifacts/` in place.

If port 9092 was already open, cleanup leaves that broker alone.

A second cleanup, after the run directory is gone, exits 0.

Never `pkill klens`. Never kill by binary name.

## Helpers

Run the scripts from any working directory. They resolve the repo from their own path. Pass the same `KLENS_VERIFY_RUN_DIR` to each one.

| Script | Purpose |
|---|---|
| `helpers/launch.sh` | Build, config, optional Kafka, seed, start, wait for `/ready` |
| `helpers/doctor.sh` | Read-only liveness, port owner, auth, catalog, seeded topic |
| `helpers/drive-topics.mjs` | Playwright proof of Topics, including auth-off `/login` |
| `helpers/cleanup.sh` | Stop the PID this run started |

```sh
.cursor/skills/verify-klens/helpers/launch.sh
.cursor/skills/verify-klens/helpers/doctor.sh
node .cursor/skills/verify-klens/helpers/drive-topics.mjs
.cursor/skills/verify-klens/helpers/cleanup.sh
```
