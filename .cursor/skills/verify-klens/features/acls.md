# ACLs

ACLs is the live binding list. It describes every ACL the broker returns for the active cluster. Local verify Redpanda answers DescribeAcls with no rows (`authorizer: ENABLED`).

## Sub-features

- `acls-land` opens `/cluster/local/acls` with heading `ACLs`.
- `acls-empty` shows the empty binding list on the local verify broker.
- `acls-search` filters bindings from `Search ACLs…` and writes `?q=` on the URL.
- `acls-resource` filters by resource type and writes `?resource=` on the URL.

## How to get to it (user POV)

- Choose the `ACLs` sidebar link.
- Choose `ACLs` in the command palette Go to group.
- Open `/cluster/local/acls`.

## Driving it with Playwright

Preconditions:

- Doctor reports `clusters` includes `local` and `catalogHealth` has `updatedAt` with no `lastError`.
- Start from `/`.

- **Open list.** Click sidebar `ACLs`. URL is `/cluster/local/acls`. Heading is `ACLs`.
- **Empty list.** Local Redpanda answers DescribeAcls with no rows. The table empty state is `No ACL bindings.` The page is not an error. GraphQL `acls(cluster: "local") { authorizer bindings { principal } }` returns `authorizer: ENABLED` and `bindings: []` with no field errors.
- **Search and resource.** Those filters stay on the page. They only change visible rows when bindings exist. On this broker they keep the same empty state.
- **Proof.** Screenshot the page with the `ACLs` heading and the empty state. Save the GraphQL body above. There is no ACL badge in the sidebar and no `/acls/<id>` route.

## Gotchas

- An empty ENABLED list on the local verify broker is a pass. Quote the empty-state text and the GraphQL authorizer.
- A broker that returns SECURITY_DISABLED must show `Authorization is disabled on this cluster.` and GraphQL `authorizer: DISABLED`. That path is covered by FakeCluster tests. Do not treat it as `verified-unreachable`.
- A GraphQL field error on `acls` is a failed drive. Do not treat that as either empty state.
- The heading, sidebar, and palette label are all `ACLs`.
- Sidebar `ACLs` has no count badge. The command palette does not search ACL rows.
- Drive this page before topic Data. After a records timeout, `acls` can return `operation timed out: request` with code `CLIENT`. That is a poisoned client, not the ENABLED empty list.
