# Consumer groups

Consumer groups lists groups in the cluster, their state and lag, and opens one group.

## Sub-features

- `groups-list` shows the group table.
- `groups-search` keeps groups whose ids contain the query.
- `groups-filter` narrows rows by state or lag.
- `groups-open` opens one group on the offsets tab.

## How to get to it (user POV)

- Choose **Consumer Groups** in the sidebar.
- Choose **Consumer groups** under **Go to** in the command palette.
- Open `/cluster/local/groups`.

## Driving it with Playwright

Preconditions:

- `helpers/doctor.sh` passed.
- The browser is signed in the auth-off sense, on cluster `local`.

- **Open the page.** Click the sidebar link named `Consumer Groups`. The URL is `/cluster/local/groups`. The heading is `Consumer groups`.
- **Search.** Fill the placeholder `Search consumer groups…`. Rows that remain contain that text in the group id.
- **Filter.** Click `Add filter`, then `State` or `Lag`. The URL query uses `state` or `lag`.
- **Empty.** If the table says `No results.`, `GET /api/clusters/local/groups` has an empty `rows` array. That is a real empty catalog, not a failed load.
- **Open.** Click a row. The URL is `/cluster/local/groups/<id>`. The heading is the group id. The `Offsets` tab is selected.
- **Proof.** Save a screenshot, an ARIA snapshot, the URL, and `GET /api/clusters/local/groups`.

## Gotchas

- The sidebar label is `Consumer Groups`. The page heading is `Consumer groups`.
- Group ids can contain slashes. The app encodes each path segment. Assert the heading text, not a hand-built URL.
- Lag sort is the default, descending. A group with incomplete watermarks shows the title `Some partitions have no watermark yet`.
- The launch seed does not create a consumer. An empty table can still be a pass when the JSON `rows` array is empty and no error alert is shown.
