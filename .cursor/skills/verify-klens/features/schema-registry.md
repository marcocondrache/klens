# Schema registry

Schema registry lists subjects for the cluster and opens one subject to read its schema text.

## Sub-features

- `schemas-list` shows subjects, or an empty list when no registry is configured.
- `schemas-search` keeps subjects whose names contain the query.
- `schemas-open` opens a sheet with the schema text.

## How to get to it (user POV)

- Choose **Schema Registry** in the sidebar.
- Choose **Schema Registry** under **Go to** in the command palette.
- Open `/cluster/local/schemas`.

## Driving it with Playwright

Preconditions:

- `helpers/doctor.sh` passed.
- Read `run/schema-registry`. `1` means the klens config points at `http://127.0.0.1:8081`. `0` means the config omits `schema_registry`.

- **Open the page.** Click the sidebar link named `Schema Registry`. The URL is `/cluster/local/schemas`. The heading is `Schema registry`.
- **No registry.** When `run/schema-registry` is `0`, the description starts with `0 subjects` and the table says `No results.`. `GET /api/clusters/local/subjects` agrees.
- **Search.** Fill the placeholder `Search subjects…`. The visible subject names contain the query.
- **Open.** Click a subject row. A dialog title shows the subject name. The schema body is visible when the role has schema text. Auth-off sessions do.
- **Proof.** Save a screenshot, an ARIA snapshot, the URL, and `GET /api/clusters/local/subjects`.

## Gotchas

- The sidebar label is `Schema Registry`. The page heading is `Schema registry`.
- Omitting `schema_registry` in the klens config yields an empty subject list and no subjects error. A configured registry that is down sets `subjects.lastError` on `GET /api/clusters`.
- Command palette hits for a subject go to `/cluster/local/schemas?q=<subject>`. They do not open the sheet by themselves.
- A role without `schema_text` sees `Schema text is not available for your role.` Auth-off does not hit that branch.
