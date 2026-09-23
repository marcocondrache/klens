# ACLs

ACLs lists allow and deny bindings for the cluster, or says that the broker has authorization disabled.

## Sub-features

- `acls-list` shows bindings when the authorizer is enabled.
- `acls-disabled` says authorization is disabled when the broker reports that.
- `acls-search` keeps bindings whose resource, principal, or host contains the query.
- `acls-filter` narrows rows by resource, operation, permission, or pattern.

## How to get to it (user POV)

- Choose **ACLs** in the sidebar.
- Choose **ACLs** under **Go to** in the command palette.
- Open `/cluster/local/acls`.

## Driving it with Playwright

Preconditions:

- `helpers/doctor.sh` passed.
- Auth is off, so the ACL privilege is present and the sidebar shows **ACLs**.

- **Open the page.** Click the sidebar link named `ACLs`. The URL is `/cluster/local/acls`. The heading is `ACLs`.
- **Disabled authorizer.** If the description is `Authorization is disabled on this cluster.`, `GET /api/clusters/local/acls` has `authorizer` `DISABLED` and `bindings` `[]`. That is a pass for a broker with no authorizer.
- **Bindings.** If the description is a binding count, the table rows match `bindings` in that JSON.
- **Search.** Fill the placeholder `Search ACLs…`. Visible rows contain the query in resource, principal, host, or operation.
- **Filter.** Click `Add filter`, then `Resource`, `Operation`, `Permission`, or `Pattern`.
- **Proof.** Save a screenshot, an ARIA snapshot, the URL, and `GET /api/clusters/local/acls`.

## Gotchas

- A role without `acls` sees the heading `ACLs` and the description `ACL bindings are not available for your role.` The sidebar hides the link in that case. Auth-off does not hit that branch.
- Redpanda started by the binary fallback often reports `authorizer` `DISABLED`. Do not treat that sentence as a failed page load.
- ACL data is read live from the broker. It is not part of the topology snapshot that gates `/ready`.
