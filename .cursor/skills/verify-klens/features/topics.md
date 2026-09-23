# Topics

Topics lists every topic in the selected cluster, hides internal topics until asked, and opens a topic from a row.

## Sub-features

- `topics-login-off` leaves `/login` when authentication is off.
- `topics-list` shows the topic table after the catalog is ready.
- `topics-search` keeps rows whose names contain the query.
- `topics-internal` shows or hides topics marked internal.
- `topics-filter` narrows rows by policy, health, or activity.
- `topics-open` opens the topic page from a row.

## How to get to it (user POV)

- Open `/`. The app selects the first cluster and lands on Topics.
- Choose **Topics** in the sidebar.
- Choose **Topics** under **Go to** in the command palette.

## Driving it with Playwright

Preconditions:

- `helpers/doctor.sh` passed.
- `klens-verify-topics` exists on cluster `local`.
- Authentication is off, so `/api/auth/me` has `enabled` false.

Run the whole recipe with `node .cursor/skills/verify-klens/helpers/drive-topics.mjs`.

- **Auth-off login.** Open `/login`. Wait until the URL matches `/cluster/local/topics`. The heading is `Topics`. The text `Continue with SSO` is absent.
- **Wordmark.** The sidebar link named `klens` is visible. `Cluster unreachable` is absent.
- **Search.** Fill the placeholder `Search topics…` with `klens-verify-topics`. The row named `klens-verify-topics` is visible.
- **Open.** Click that row. The URL is `/cluster/local/topics/klens-verify-topics`. The heading contains that name. The `Data` tab is visible. A cell reads `verify-1` and a cell reads `hello-from-verify-klens`.
- **Internal switch.** The switch named `Show internal` is on the Topics page. Turning it on adds internal topic names such as those that start with `_`.
- **Filter.** Click the button named `Add filter`, then `Policy`, then `delete`. The URL query contains `policy=delete`. `klens-verify-topics` stays if its policy is delete.
- **Proof.** Save a screenshot and `body` ARIA snapshot for the filtered list and for the opened topic. Save `GET /api/clusters/local/topics` beside them.

## Gotchas

- `GET /login` from curl returns the HTML shell. The redirect to Topics runs in the browser after `/api/auth/me`.
- The sidebar label is `Topics`. The page heading is also `Topics`.
- Search is client-side over the rows already loaded. It does not call `/api/clusters/local/search`.
- `/` focuses `Search topics…` because that input has `data-search-hotkey`. It does not open the command palette.
- Internal topics stay hidden until **Show internal** is on. A search for `klens-verify-topics` does not need that switch.
- `Cluster unreachable` or `Topology lane failing` means the catalog drive failed. Record the alert and stop.
