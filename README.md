# klens

A web UI for Kafka. Browse topics, records, consumer groups, brokers, schemas,
and ACLs across one or more clusters.

klens is one Rust binary that serves the UI and a JSON API. It can tail a topic
live, hide fields in records, and restrict access by OIDC group.

Responses are compressed with zstd, brotli, or gzip, whichever the client
accepts. Live streams are not, so each event goes out as soon as it is ready.

## Install

Images are published to GHCR on each release from `main`:

```sh
docker pull ghcr.io/marcocondrache/klens:latest
```

Copy [config/clusters.example.yaml](config/clusters.example.yaml) to `config.yaml`, then run with that file mounted:

```sh
docker run --rm -p 8080:8080 \
  -v "$PWD/config.yaml:/config.yaml:ro" \
  ghcr.io/marcocondrache/klens:latest
```

The Helm chart is published to the same registry:

```sh
helm install klens oci://ghcr.io/marcocondrache/charts/klens \
  -n klens --create-namespace -f my-values.yaml
```

`config` is the same YAML the process loads here. The chart also lives in
[`charts/klens`](charts/klens) if you want to install from a checkout.

## Configuration

klens loads `config.yaml` from its working directory. Set `KLENS_CONFIG_PATH`
to load another file. klens reads no other environment variable, apart from
the ones a secret names with `{env: NAME}`.

Omitted keys take their defaults, and unknown keys stop startup. So does a bad
value, with its line and column, for example
`must be at least 1s at line 12, column 15`.

```yaml
bind: 0.0.0.0:8080
allowed_hosts: [localhost, 127.0.0.1, "::1"] # see Allowed hosts
log_level: info # off, error, warn, info, debug, or trace
clusters: {} # by name, shown in the UI in this order
# auth: see Authentication
# mcp: see MCP
# tuning: see Tuning
```

### Clusters

A cluster needs its `bootstrap_servers`. TLS and SASL are each on when their
block is present, so `tls: {}` is enough to connect over TLS. klens then trusts
the Mozilla root CAs it ships with, or only `ca_cert` when that is set.

```yaml
clusters:
  prod:
    bootstrap_servers: [broker-1:9093, broker-2:9093]
    client_id: klens-prod # default: klens-<cluster name>
    writable: false # default: klens changes nothing on this cluster
    tls:
      ca_cert: /tls/ca.pem
      client: { cert: /tls/client.pem, key: /tls/client.key }
      insecure_skip_verify: false
    sasl:
      mechanism: SCRAM-SHA-512 # PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512
      username: klens
      password: { env: KAFKA_PASSWORD }
```

A cluster can also point at a [Schema Registry](#schema-registry) and hide
record contents with [obfuscation](#obfuscation) rules.

klens changes a cluster only when it sets `writable: true`, and then only for
users who hold the privilege the change needs (see
[Authentication](#authentication)). Without `auth`, everyone who reaches klens
holds every privilege, and klens logs a warning at startup that names each
writable cluster.

### Allowed hosts

Without `auth`, klens answers only requests addressed to a host in
`allowed_hosts`. Any other host gets `403 HOST_NOT_ALLOWED`, and so does a
request that names no host. This stops DNS rebinding, where a web page points
its own domain at klens and then reads clusters or sends writes from a
visitor's browser. The list binds browsers only, since any other client can
name an allowed host. With `auth`, klens ignores the list, because such a page
never holds the session cookie.

`/health` and `/ready` answer every host, because Kubernetes probes and load
balancer health checks name an IP address. Point a load balancer health check
at one of them, not at `/`.

An entry is a host name or an IP address, with an optional port. An entry
without a port matches any port, and one with a port matches only that port.
Browsers leave out `:80` and `:443`, so list a host on those ports without a
port. Names match in any case. An IPv6 address takes brackets when a port
follows, and YAML needs quotes around it, as in `"[::1]:8080"`. An empty list
stops startup, and so does an entry with a scheme, a path, a wildcard, a bad
port, or a short IPv4 form such as `10.1`.

The default names only the local machine. A deployment that people reach by
another name, such as an ingress host, must list it. A proxy in front of klens
must pass on the browser's host, as nginx does with
`proxy_set_header Host $host`. A proxy that always sends the same name, such as
`127.0.0.1:8080`, defeats the check unless it refuses other hosts itself. klens
logs the list at startup when auth is off, and each refused host at debug
level.

```yaml
allowed_hosts: [localhost, 127.0.0.1, "::1", klens.example.com]
```

### Secrets

Every secret names where to read it: `{value: ...}` inline, `{env: NAME}` from
an environment variable, or `{file: PATH}` from a file such as a mounted
Kubernetes secret. A trailing newline in a secret file is dropped, and an
inline value YAML would read as a number must be quoted. A plain string where
a secret belongs fails at startup, and the error does not repeat it.

### Tuning

Timeouts, pool sizes, limits, and lane cadence live under `tuning`, the same
for every cluster. Durations are strings such as `250ms`, `10s`, `1h 30m`, or
ISO 8601 `PT10S`. This block lists the defaults:

```yaml
tuning:
  kafka:
    connect_timeout: 10s
    request_timeout: 10s # raised to connect_timeout if smaller
    consume_timeout: 5s # how long one record page or tail open may read
    max_in_flight_requests: 32 # per broker connection
    max_response_mib: 32 # largest broker response frame
  schema_registry:
    timeout: 5s
    subject_fetch_concurrency: 8
    missing_schema_ttl: 60s # how long an unknown schema id stays cached
  scan:
    pool_per_topic: 2 # idle scan consumers kept per topic
    pool_total: 16 # idle scan consumers kept across all topics
    pool_idle_ttl: 60s
    poll_wait: 100ms # longest single scan poll
  records:
    max_limit: 500 # most records one page may request
    min_window: 4 # fewest offsets read from each partition
    window_multiplier: 2
    search_window_multiplier: 8 # used while a `contains` or `schemaId` filter runs
  tail:
    batch_limit: 100
    interval: 250ms
    poll_wait: 500ms
    max_live: 32
  ingest:
    topology: 10s
    high_watermark: 3s
    low_watermark: 30s # how often the watermark lane rereads low watermarks
    config: 60s
    subjects: 30s
    log_dirs: 60s
    acls: 60s
    quotas: 60s
    scram_users: 60s
    offset_tick: 1s # how often the offset lane checks which groups are due
    fast_offset: 2s # groups someone is looking at
    slow_offset: 20s # every other group
    offset_fetch_concurrency: 32
    interest_ttl: 30s # how long a viewed group stays on fast_offset
    idle_heartbeat: 15s # an idle topic's rate drops to zero after this
    max_sample_gap: 15s # older watermark samples do not count toward a rate
  mcp:
    max_concurrent_calls: 16 # MCP tool calls served at once, across every client
    live_calls_per_minute: 30 # MCP calls that make klens read more from Kafka or change it
```

Every page reads a background projection of each cluster, refreshed by the
independent lanes `ingest` paces. A lane that needs another lane's table
starts as soon as that table exists. A lane's period and
`scan.pool_idle_ttl` must be at least `1s`. Counts must be at least 1, except `records.min_window`,
`records.window_multiplier`, `records.search_window_multiplier`, and
`tail.max_live`.

## Live tail

`GET /api/clusters/{cluster}/topics/{topic}/records/tail` follows a topic from
its current end as a server-sent event stream. It takes the same `partition`,
`contains`, and `schemaId` parameters as a record page. It needs the `records`
privilege, and it applies obfuscation the same way a page does. The first frame
is `ready` and names each partition's start offset. After that, `records` frames
arrive oldest first.

A tail samples a busy topic rather than streaming all of it. Each frame carries
at most `tuning.tail.batch_limit` (100) of the newest records, and frames are
at least `tuning.tail.interval` (`250ms`) apart. A partition that falls too
far behind skips ahead. `skipped` counts what was passed over. Each tail holds
its own consumer, and `tuning.tail.max_live` (32) caps how many run at once.
Past that cap, a new tail gets `503 TOO_MANY_TAILS`.

## Storage

klens asks every broker to describe its log dirs every
`tuning.ingest.log_dirs` (`60s`), and again as soon as a topic or broker
appears. A topic's size counts the largest replica
of each partition, and the topic page adds the bytes across every replica. A
broker's size counts every log in its dirs. Its disk use is that of the
fullest dir's volume, which Kafka reports from 3.3 on. The broker page lists
each dir, and flags one that is offline or cordoned.

Sizes need the `Describe` operation on the `Cluster` resource. On a cluster
that does not grant it, or does not serve `DescribeLogDirs`, sizes stay blank
and the rest of klens works as before.

## Quotas

klens describes client quotas every `tuning.ingest.quotas` (`60s`). The quotas
page lists each user, client ID, and IP quota, including the defaults, and
needs the `broker_configs` privilege. Reading quotas needs the `DescribeConfigs`
operation on the `Cluster` resource. On a cluster that does not grant it, the
page says so.

## SCRAM users

klens describes SCRAM credentials every `tuning.ingest.scram_users` (`60s`).
The users page lists each Kafka user that has one, with the mechanism and
iteration count of each credential, and needs the `acls` privilege. Kafka never
returns a password or salt. Reading credentials needs Kafka 2.7 or later and the
`Describe` operation on the `Cluster` resource. On a cluster that does not grant
it, the page says so.

On a writable cluster, a role with `set_scram_credentials` adds users and sets
their passwords, and a role with `delete_scram_credentials` deletes credentials
from the users page. klens salts the password itself with a random 32-byte salt
and sends Kafka only the salted hash, so the password never reaches the broker
or the log. Kafka takes 4096 to 16384
iterations, and klens defaults to 4096. Changing credentials needs the `Alter`
operation on the `Cluster` resource.

## Authentication

By default the UI and JSON API are open to anyone who can reach the process,
and browsers reach them only through the [allowed hosts](#allowed-hosts).

To require a login, add an OIDC provider to `config.yaml`. klens uses the
authorization code flow with PKCE. Sessions use
[axum-login](https://github.com/maxcountryman/axum-login). Auth, catalog, and
live routes are under `/api`. `/health` and `/ready` stay public. A process
restart drops in-memory sessions and requires a new login.

Set `auth.session.key` to a secret of at least 32 bytes so the session cookie
survives a restart. Without one, klens generates a key per boot and every
deploy logs everyone out.

A login must come back from the provider within `auth.session.login_timeout`
(`10m`). A session ends when the ID token expires or after
`auth.session.max_age` (`12h`), whichever comes first.

```yaml
auth:
  oidc:
    issuer: https://keycloak.example.com/realms/klens
    client_id: klens
    client_secret: { env: OIDC_CLIENT_SECRET }
    redirect_uri: http://localhost:8080/api/auth/callback
  session:
    key: { env: KLENS_SESSION_KEY }
```

Register `redirect_uri` with the identity provider. Session cookies are
`Secure` when it is `https`. Without `roles`, any authenticated user has the
same access as an open deployment.

To restrict what signed-in users may do, add `roles`. A role is nothing but a
name for a set of privileges, defined by you: there are no built-in roles. The
privileges are `records`, `topic_configs`, `broker_configs`, `schema_text`, and
`acls` to read, and one privilege per change on a writable cluster:
`create_topics`, `delete_topics`, `alter_topic_configs`, `add_partitions`,
`delete_records`, `produce`, `reset_offsets`, `delete_offsets`,
`delete_groups`, `register_schemas`, `set_compatibility`, `delete_schemas`,
`create_acls`, `delete_acls`, `alter_quotas`, `set_scram_credentials`,
`delete_scram_credentials`, and `alter_broker_configs`. A role that lists none
still sees the catalog (clusters, topics, groups, lag, sizes) but no payloads,
live configs, schema bodies, or ACL bindings. A role's `bindings` name the IdP groups that
hold it, read from the ID token claim that `oidc.groups_claim` names (default
`groups`). Unmatched users cannot sign in. Omit `clusters` on a binding to
allow every configured cluster.

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
    client_secret: { env: OIDC_CLIENT_SECRET }
    redirect_uri: http://localhost:8080/api/auth/callback
    # groups_claim: groups
  roles:
    admin:
      privileges:
        [records, topic_configs, broker_configs, schema_text, acls,
         create_topics, delete_topics, alter_topic_configs, add_partitions, delete_records, produce,
         reset_offsets, delete_offsets, delete_groups,
         register_schemas, set_compatibility, delete_schemas,
         create_acls, delete_acls, alter_quotas, set_scram_credentials, delete_scram_credentials,
         alter_broker_configs]
      bindings:
        - groups: [klens-admins]
    viewer:
      privileges: [] # catalog only
      bindings:
        - groups: [klens-viewers]
    operator:
      privileges: [records, topic_configs, broker_configs]
      bindings:
        - groups: [kafka-operators]
          clusters: [staging, dev]
    auditor:
      privileges: [acls, schema_text]
      bindings:
        - groups: [security-team]
```

## MCP

With an `mcp` block in `config.yaml`, klens serves tools to AI agents over the
[Model Context Protocol](https://modelcontextprotocol.io) at `/mcp`. Without
the block, there is no `/mcp`. An agent in Claude Code, VS Code, or Cursor
reads the same background projection as the UI, so most calls cost Kafka
nothing.

```yaml
mcp: {}
```

[Connect an agent](#connect-an-agent) shows how to add klens to each client.

`klens_clusters` lists clusters with their health, `klens_search` finds topics,
groups, brokers, and schema subjects by name, and `klens_access_explain` tells
the agent what it may do on each cluster. `klens_topics_list` filters and sorts
a cluster's topics, and `klens_topic_describe` shows one topic's partitions, the
consumer groups that read it with their lag, the schema subjects named after it,
and the configs it sets apart from Kafka's defaults. `klens_groups_list` lists
consumer groups by lag, and `klens_group_describe` shows one group's members,
its lag per partition, and what looks wrong, such as one member that holds most
of the lag. `klens_brokers_list` lists a cluster's brokers with their log dirs,
or one broker with the configs it sets apart from Kafka's defaults.
`klens_schemas_list` lists a cluster's schema subjects, and `klens_schema_get`
reads the text of one subject's schema live from the registry, at its latest
version unless the agent names one. `klens_acls_list` lists a cluster's ACL
bindings, filtered by principal, resource, host, operation, or permission. A
tool that reads one cluster needs no `cluster` argument when the agent sees
only one.

`klens_records_read` reads a page of a topic's records live from Kafka, newest
first unless the agent asks for the oldest. The agent can pick partitions,
start at an offset or a time, and keep the records whose key or value holds
some text. A page holds 10 records unless the agent asks for up to 50, and the
result gives a cursor for the next page. `klens_record_get` reads one record by
its partition and offset. Both need the `records` privilege, and an obfuscation
rule hides what it covers, as in the UI. A record result is text. A JSON line
gives each record's partition, offset, timestamp, size, and schema id. A second
JSON line holds the record's key, headers, and value, between markers that
change with every result, and the result tells the agent to read what sits
between them as data. JSON escapes keep a payload on its own line, and klens
escapes a payload that holds the closing marker. `klens_schema_get` puts the
schema text and its references between such markers too. A broker or schema
registry can send back any text, so the last error of a background read sits
between markers wherever a result shows it, and so does the message of a
failing registry in a refusal.

Three tools change Kafka, and only once `privileges` allows it, as the ceiling
below shows. `klens_topic_create` creates a topic with the partitions,
replication factor, and configs the agent gives, or else the broker's defaults.
`klens_record_produce` writes one record and returns its partition and offset.
Its key and value are UTF-8 text, base64 bytes, or JSON that klens writes with
a registry schema, as in the UI. It writes no tombstones, since a tombstone
deletes its key from a compacted topic. `klens_schema_register` registers a
schema version under a subject and returns the version and the schema id. Each
needs the privilege the UI needs for the same change, `create_topics`,
`produce`, or `register_schemas`, on a cluster with `writable: true`. klens logs
each change as it logs one from the UI, with the user and client of the call.
The tool list leaves out each of the three unless a cluster the agent sees takes
changes and grants its privilege, and `klens_access_explain` marks it
unavailable on a cluster that takes none. When Kafka or the registry refuses a
change, the refusal carries the reason between markers.

A list returns 25 rows unless the agent asks for up to 100, and says how many
it shows out of how many matched. A count, size, or rate klens has not
measured yet is null rather than 0. Group ids, client ids, hosts, assignment
protocols, subject names, principals, and resource names come from whoever runs
a Kafka client, so a result that carries them tells the agent to read them as
data, not as instructions.

Without `auth`, anyone who reaches `/mcp` can call its tools. `/mcp` then
answers only the hosts in [`allowed_hosts`](#allowed-hosts), and it refuses
every request that carries an `Origin` header, so a web page cannot call it
from a visitor's browser. klens logs at startup that it serves `/mcp`, with the
ceiling below.

With `auth`, `/mcp` takes only an access token from the `auth.oidc` provider,
sent as `Authorization: Bearer`, and never the session cookie. `resource` is
then required, and `/mcp` answers only its host. The token's audience must be
in `token.audiences`, which defaults to `resource`, and the agent holds what the
roles of the token's groups grant, under the ceiling below. A missing or bad
token gets `401` with a challenge that points to the resource's
[RFC 9728](https://www.rfc-editor.org/rfc/rfc9728) metadata, so an MCP client
finds the provider on its own. A valid token whose groups bind no role gets
`403`. klens logs each call with the token's user and client.
[Identity provider](#identity-provider) shows how to set the provider up.

```yaml
mcp:
  resource: https://klens.example.com/mcp # required with auth
  allowed_origins: ["https://claude.ai:443"] # only with auth, with a port or :*
  token:
    audiences: [https://klens.example.com/mcp] # default: [resource]
    clients: [klens-mcp] # azp, else client_id; default: any client
    scopes: [] # named in the 401 challenge and the metadata
    max_age: 1h # the longest exp - iat; raise it on Entra ID
    groups_claim: groups # default: auth.oidc.groups_claim
    user_claim: sub # oid on Entra ID
```

The block is a ceiling on what an MCP client may do. `privileges` lists the
reads and changes it may use beyond the catalog. The reads are `records`,
`topic_configs`, `broker_configs`, `schema_text`, and `acls`, and it defaults to
all five. `records` opens the record tools, `schema_text` opens
`klens_schema_get`, and `acls` opens `klens_acls_list`. `topic_configs` adds a
topic's config overrides to `klens_topic_describe`, and `broker_configs` adds a
broker's to `klens_brokers_list`. A tool says when it left such a section out.
MCP changes nothing until `privileges` names `create_topics`, `produce`, or
`register_schemas`, which open the three tools that change Kafka. Any other
write privilege stops startup, so a later klens cannot switch on a new write
tool by itself. `clusters` limits MCP to the clusters it names. When it is
omitted, MCP reaches every cluster, and an empty list reaches none. A name that
is not a configured cluster stops startup. The tool list an agent gets leaves
out each tool that needs a privilege the agent holds on none of the clusters it
sees, so without `records` it never sees the record tools.

```yaml
mcp:
  privileges: [topic_configs, schema_text] # no record payloads or ACLs
  clusters: [dev, staging]
```

The UI and MCP share each cluster's `writable` flag, so name the clusters an
agent may reach in `clusters` before you add a write privilege, and leave
production out. With `auth`, `token.clients` is then required, so a token that
another client of the provider gets cannot change Kafka. klens warns at startup
when `auth` is off and `privileges` names a write, since anyone who reaches
`/mcp` could then change a writable cluster. It also warns when `privileges`
names `records` or `schema_text` beside a write. An agent that reads text a
producer or schema author chose may follow instructions hidden in it, and a
write gives it a way to copy private data out.

```yaml
mcp:
  privileges: [topic_configs, create_topics, produce, register_schemas]
  clusters: [dev, staging] # keep writes off production
  resource: https://klens.example.com/mcp
  token:
    clients: [klens-mcp] # required with a write privilege
```

`tuning.mcp.max_concurrent_calls` (16) caps the tool calls klens serves at
once, across every client. Past it, a call fails with `RATE_LIMITED` rather
than waiting. A request body holds at most 64 KiB, and a search query at most
256 characters. A result holds at most 24,000 bytes, counting both the text and
the structured copy it carries. A longer one keeps its first rows and says how
many it left out. `klens_group_describe` also shortens the topic and partition
lists of each member and finding, and a detailed `klens_schemas_list` shows the
newest 10 versions of each subject. `klens_brokers_list` takes the last broker
id it showed as `after` to list the brokers that did not fit.
`klens_group_describe` cuts a name a client chose, such as a client id, after
256 characters and ends it with `…`. A config value longer than 500 characters
is cut and marked `cut`. A record result instead cuts long keys, headers, and
values to one length, and shows no more headers of a record than that length,
so a page keeps every record and its cursor. It says which records it cut.
When the cursor of a topic with many partitions leaves too little room,
`klens_records_read` fails with `INVALID_REQUEST` and asks for fewer records or
partitions. A refused call returns its error code and a hint for the next call,
and klens logs the tool and the code at info level, never the arguments. klens
never logs the `rmcp` library below error, because rmcp logs tool arguments and
results at debug level.

`klens_group_describe` makes klens read the group's offsets every
`tuning.ingest.fast_offset` for `tuning.ingest.interest_ttl`, as opening the
group in the UI does. The record tools read Kafka on every call,
`klens_schema_get` reads the schema registry, `klens_brokers_list` reads Kafka
when it reads a broker's configs, and the three write tools change Kafka or the
registry. `tuning.mcp.live_calls_per_minute` (30) caps how often agents may call
these tools. With `auth`, each user has a budget of their own. Without it, every
client shares one. A call past it fails with `RATE_LIMITED`.

Every result reaches the agent's model provider, record payloads, configs,
schema text, and ACLs included. Set `privileges` and `clusters` to what you
would share with it, and leave out `records` to keep payloads from it.

### Identity provider

With `auth`, an MCP client signs the user in at the `auth.oidc` provider and
sends klens the access token it gets back. klens checks that token itself, so
the provider must issue it as a JWT signed with a public key, such as RS256 or
ES256, and four settings must line up.

The token's audience must be `mcp.resource`, unless `token.audiences` names
another, as Dex and Entra ID need. klens writes the resource the way a URL
parser does, with a lowercase host, no `:443`, and a `/` after a bare host,
and logs it at startup. Use that exact text as the audience, and give users the
same URL for their client.

The access token must list the user's groups in the claim that
`token.groups_claim` names. It defaults to `auth.oidc.groups_claim`, the claim
the UI reads from the ID token, so the same role bindings apply to both.

MCP clients share one public OAuth client at the provider. It has no secret,
uses PKCE, and lists the redirect URI of every MCP client your users run, as
[Connect an agent](#connect-an-agent) gives them. Each user gives their MCP
client the id of this OAuth client.

`token.clients` names that client, so a token that another client of the
provider gets for the same audience does not open `/mcp`. klens logs a warning
at startup while the list is empty, and refuses to start without it when
`mcp.privileges` names a write.

Keycloak fits. In the realm that `auth.oidc.issuer` names, create an OpenID
Connect client such as `klens-mcp`. Turn Client authentication off, keep
Standard flow on, require PKCE with S256, and list the redirect URIs under
Valid redirect URIs. In the client's dedicated scope, add an Audience mapper
with Included Custom Audience set to `mcp.resource` and Add to access token on,
so only this client's tokens carry the audience. Add a Group Membership mapper
there too, with Token Claim Name `groups`, Full group path set as for the UI's
client, and Add to access token on. Keycloak names the client in `azp`, so set
`token.clients: [klens-mcp]`.

Auth0 fits. Under Settings, Advanced, turn on the Resource Parameter
Compatibility Profile, so the `resource` parameter that MCP clients send
selects the API. Under Applications, APIs, create an API whose identifier is
`mcp.resource`, signed with RS256. Set its Maximum Access Token Lifetime to 3600
seconds, because the default of 86,400 seconds is longer than `token.max_age`
allows. Create a Native application, which has no secret, list the redirect
URIs under Allowed Callback URLs, and put its client id in `token.clients`.
Auth0 has no groups claim, so a post-login Action adds the user's roles to the
access token, and `token.groups_claim` names that claim.

```js
exports.onExecutePostLogin = async (event, api) => {
  const roles = event.authorization?.roles ?? [];
  api.accessToken.setCustomClaim("https://klens.example.com/groups", roles);
};
```

Claude Code can send a `resource` other than the one klens advertises
([anthropics/claude-code#52871](https://github.com/anthropics/claude-code/issues/52871)),
and Auth0 then shows an error page instead of a sign-in. If that happens, turn
the profile off and set the tenant's Default Audience, under Settings, General,
to `mcp.resource`. Every application in the tenant that names no audience then
gets a token for klens, and `token.clients` keeps those tokens out of `/mcp`.

Okta and Dex probably fit, and neither has been verified. Okta sets the
audience of an access token only on a custom authorization server, so
`auth.oidc.issuer` must name that server, with `mcp.resource` as its audience
and a groups claim on its access tokens. Okta names the client in `cid`, which
klens does not read, so leave `token.clients` empty there, which keeps MCP to
reads. Give klens an authorization server of its own instead, and assign its
access policy only to the MCP client and the UI's client.

Dex issues access tokens shaped like its ID tokens, with the id of the client
that asked as the audience. Create a static public client for MCP apart from
the UI's, put its id in `token.audiences`, and set `token.scopes` to
`[openid, groups]` so Dex adds the groups. The audience already names the
client, so `token.clients` can stay empty while MCP serves only reads. Dex
gives an access token the lifetime of its ID tokens, 24 hours unless
`expiry.idTokens` says otherwise, so set that to `1h` or raise `token.max_age`
to match.

Entra ID can work, but it needs more setup and breaks easily. Create an app
registration for MCP apart from the UI's, because `token.audiences` may not
name `auth.oidc.client_id`. Set its Application ID URI to `mcp.resource`, which
Entra accepts only on a domain the tenant has verified, expose a scope, and
list the redirect URIs under Mobile and desktop applications. Set
`requestedAccessTokenVersion` to 2 in its manifest, so its tokens carry the
v2.0 issuer the UI signs in with. Such a token names the app's application id
as its audience and in `azp`, so put that id in `token.audiences` and
`token.clients`. Put the full scope, such as
`https://klens.example.com/mcp/access`, in `token.scopes`, because Entra issues
a token for another audience unless the client asks for it. Set
`token.user_claim: oid`, because Entra's `sub` differs per app. Its tokens last
up to 90 minutes, and their `iat` can lie 5 minutes before they are issued, so
raise `token.max_age` to `100m`.

Entra's groups claim lists group object ids, and klens refuses a token with
more than 64 groups, so app roles in the `roles` claim, with
`token.groups_claim: roles`, often fit better. Entra's discovery document names
no PKCE methods, and the MCP specification tells a client to refuse such a
provider, so a strict client may not sign in at all. Entra also refuses a
sign-in with AADSTS9010010 when the client's `resource` is not the Application
ID URI, so the Claude Code bug that affects Auth0 affects Entra too.

Google cannot work. Its access tokens are opaque, so klens has nothing to
check. A provider such as Keycloak or Dex can sign users in through Google and
stand in front of both the UI and `/mcp`.

### Connect an agent

Without `auth`, a user on the machine that runs klens adds it to Claude Code
with one command.

```sh
claude mcp add --transport http klens http://localhost:8080/mcp
```

With `auth`, a client signs the user in the first time it calls `/mcp`, and it
needs the id of the public client from [Identity provider](#identity-provider).
Give it the URL exactly as klens logs `mcp.resource`. Claude Code sends the
user back to `http://localhost:<port>/callback`, so fix the port with
`--callback-port` and list that URI, here `http://localhost:8765/callback`.
Then run `/mcp` in Claude Code to sign in.

```sh
claude mcp add --transport http --client-id klens-mcp --callback-port 8765 \
  klens https://klens.example.com/mcp
```

VS Code reads the client id from `oauth.clientId` in `.vscode/mcp.json`. Its
client metadata lists `http://127.0.0.1:33418/` and
`https://vscode.dev/redirect` as its redirect URIs.

```json
{
  "servers": {
    "klens": {
      "type": "http",
      "url": "https://klens.example.com/mcp",
      "oauth": { "clientId": "klens-mcp" }
    }
  }
}
```

Cursor reads it from `auth.CLIENT_ID` in `.cursor/mcp.json`, and its desktop
app redirects to `http://localhost:8787/callback`. Unless `auth.scopes` names
scopes, Cursor asks for the ones the provider's metadata lists, so copy
`token.scopes` there when you set it.

```json
{
  "mcpServers": {
    "klens": {
      "url": "https://klens.example.com/mcp",
      "auth": { "CLIENT_ID": "klens-mcp" }
    }
  }
}
```

Without `auth`, leave out `oauth` and `auth`, and use
`http://localhost:8080/mcp`.

claude.ai and Claude Desktop connectors call klens from Anthropic's cloud, at
addresses in `160.79.104.0/21`. They reach klens only when klens is reachable
from the internet, and they reach the provider's discovery document and token
endpoint from there too. Add klens as a custom connector with its `/mcp` URL
and your own OAuth client id, and list `https://claude.ai/api/mcp/auth_callback`
on the OAuth client. If klens answers them with `403` and logs no
`bearer token refused: no matching role` line, list `https://claude.ai:443` in
`mcp.allowed_origins`.

klens has not been tested with any of these clients. The commands, keys, and
redirect URIs come from each client's own documentation.

## Schema Registry

Each cluster can optionally point at a Confluent-compatible Schema Registry.
When omitted, the Schemas page is empty for that cluster. Framed Avro, JSON, and
Protobuf payloads decode to JSON when a registry is configured.

```yaml
schema_registry:
  url: http://localhost:8081
  # auth:
  #   username: user
  #   password: { env: SCHEMA_REGISTRY_PASSWORD }
```

Credentials may only travel over plaintext `http://` when the host is loopback.
A remote registry that needs a username and password must be reached over
`https://`.

## Obfuscation

A cluster can hide parts of its records from everyone browsing it, so a topic
stays debuggable without showing card numbers or emails in cleartext. Rules are
per topic, compiled at boot, and applied inside the scan, before filters and
before anything is rendered.

```yaml
obfuscation:
  # Keys the hash strategy: 32 bytes or more, used as written.
  secret: { env: KLENS_OBFUSCATION_SECRET }
  rules:
    # Field rules walk the JSON a registry decode produced.
    - topics: ["payments.*"] # exact name, or a trailing-* prefix
      fields:
        - path: card.number
          strategy: hash # deterministic token: kx:3f9a2c481b7d6e05
        - path: card.cvv
          strategy: drop # the field disappears
        - path: customer.email
          strategy: mask # ***
      # A value that never decoded cannot be walked. Default: mask it whole.
      unparsed: mask # mask | allow
    # Whole-field rules, for topics browsed as raw text.
    - topics: ["audit.raw"]
      key: mask
      value: hash
      headers: ["x-user-id"] # header values to mask, by name
    # Pattern rules, for schemaless text topics no registry decodes.
    - topics: ["app.logs"]
      patterns:
        - regex: '\b\d{13,19}\b'
          strategy: hash
        - regex: '[\w.+-]+@[\w-]+\.[\w.]+'
          strategy: mask
```

A path met by an array fans out over its elements, so `items.sku` covers every
element's `sku`. `hash` is `HMAC-SHA256` truncated to 64 bits: equal values
render as equal tokens, so records stay correlatable, but the token cannot be
enumerated back without the secret. Rotating the secret changes every token.

Field rules only reach JSON a registry decode produced. Pattern rules reach the
rendered text of key and value instead, which is all a schemaless topic ever
has: every match is replaced by its strategy's output, and `drop` deletes the
match. They only run on topics a rule names, never by detection — and they are
the weaker of the two, because a value written in an unexpected format slips
past the regex. Use field rules wherever a schema exists.

Filters see the obfuscated record, not the wire record: a `contains`
filter over a protected field matches the token, never the value behind it.
That is deliberate — a filter that searched the cleartext would recover a
hidden value one character at a time. What the page can show is what a query
can search.

Rules apply to every session, including admins. A topic must be covered by at
most one rule, and the secret must be at least 32 bytes. Both are boot-time
errors rather than a silently weaker policy. Offsets, timestamps, `sizeBytes`,
and schema ids keep describing the wire record. A record page reports
`obfuscated: true` for a covered topic, which is what the UI badges.
