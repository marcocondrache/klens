# Proof artifacts

Each launch creates `artifacts/<run-id>/`. Cleanup deletes the run directory and leaves this tree.

## 20260911T181720Z

First live pass of this skill. Feature: Topics (`features/topics.md`).

- Launch: `target/debug/klens` on `127.0.0.1:18080`, cluster `local` `HEALTHY`
- Doctor: `/health` 204, `/auth/me` `enabled: false`, GraphQL clusters `HEALTHY`
- Drive: `/` → `/cluster/local/topics` → filter `klens-verify-topics` → topic page
- Cleanup: PID 23172 gone, port 18080 closed, these files still here

See `topics/NOTES.txt`, `topics/landing.png`, `topics/filtered.png`, `topics/open.png`, and `topics/topics.json`.
