# Authentication

By default the UI and JSON API are open to anyone who can reach the process.

## OIDC login

To require a login, add an OIDC provider to `config.yaml`. klens uses the
authorization code flow with PKCE. Sessions use
[axum-login](https://github.com/maxcountryman/axum-login). Auth, catalog, and
live routes are under `/api`. `/health` and `/ready` stay public. A process
restart drops in-memory sessions and requires a new login.

Set `auth.session_key` (or `KLENS_SESSION_KEY`, which takes precedence) to a
base64 or plain secret of at least 32 bytes so the session cookie survives a
restart. Without one, klens generates a key per boot and every deploy logs
everyone out.

```yaml
auth:
  oidc:
    issuer: https://keycloak.example.com/realms/klens
    client_id: klens
    client_secret: "..."
    redirect_uri: http://localhost:8080/api/auth/callback
```

Register `redirect_uri` with the identity provider. Without `roles`, any
authenticated user has the same access as an open deployment.

## Roles

To restrict what signed-in users may do, add `roles`. A role is nothing but a
name for a set of privileges, defined by you: there are no built-in roles. The
privileges are `records`, `configs`, `schema_text`, and `acls`; a role that
lists none still sees the catalog (clusters, topics, groups, lag) but no
payloads, live configs, schema bodies, or ACL bindings. Bindings map IdP groups
from the `claim` to those roles, and unmatched users cannot sign in. Omit
`clusters` on a binding to allow every configured cluster.

Bindings are evaluated per cluster: a user's privileges on a cluster are the
union of the roles bound to their groups **whose scope covers that cluster**.
Two roles need not be comparable — an `operator` and an `auditor` binding
combine into both privilege sets — but a role scoped to `prod` contributes
nothing to `payments`, so a wide `admin` binding never lifts a narrow one
elsewhere. A cluster no binding covers is invisible: it is reported as unknown
rather than forbidden, so nobody can probe for clusters they are not allowed to
know about.

```yaml
auth:
  oidc:
    issuer: https://keycloak.example.com/realms/klens
    client_id: klens
    client_secret: "..."
    redirect_uri: http://localhost:8080/api/auth/callback
  roles:
    # claim: groups
    definitions:
      admin: [records, configs, schema_text, acls]
      viewer: [] # catalog only
      operator: [records, configs]
      auditor: [acls, schema_text]
    bindings:
      - groups: [klens-admins]
        role: admin
      - groups: [klens-viewers]
        role: viewer
      - groups: [kafka-operators]
        role: operator
        clusters: [staging, dev]
      - groups: [security-team]
        role: auditor
```
