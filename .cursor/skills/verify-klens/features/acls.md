# ACLs

ACLs is the live binding list. It describes every ACL the broker returns for the active cluster, or shows a calm empty state when authorization is off.

## Sub-features

- `acls-land` opens `/cluster/local/acls` with heading `ACLs`.
- `acls-disabled` shows the authorizer-off empty state on the local verify broker.
- `acls-search` filters bindings from `Search ACLs…` and writes `?q=` on the URL.
- `acls-resource` filters by resource type and writes `?resource=` on the URL.

## How to get to it (user POV)

- Choose the `ACLs` sidebar link.
- Choose `ACLs` in the command palette Go to group.
- Open `/cluster/local/acls`.

## Driving it with Playwright

Preconditions:

- Doctor reports `local` `HEALTHY`.
- Start from `/`.

- **Open list.** Click sidebar `ACLs`. URL is `/cluster/local/acls`. Heading is `ACLs`.
- **Authorizer off.** Local Redpanda has no authorizer. The table empty state is `Authorization is disabled on this cluster.` The page is not an error. GraphQL `acls(cluster: "local") { authorizer bindings { principal } }` returns `authorizer: DISABLED` and `bindings: []` with no field errors.
- **Search and resource.** Those filters stay on the page. They only change visible rows when bindings exist. On this broker they keep the same empty state.
- **Proof.** Screenshot the page with the `ACLs` heading and the disabled empty state. Save the GraphQL body above. There is no ACL badge in the sidebar and no `/acls/<id>` route.

## Gotchas

- Authorizer-off is a pass, not `verified-unreachable`. Quote the empty-state text and the GraphQL `DISABLED` authorizer.
- A GraphQL field error on `acls` is a failed drive. Do not treat that as the disabled empty state.
- The heading, sidebar, and palette label are all `ACLs`.
- Sidebar `ACLs` has no count badge. The command palette does not search ACL rows.
