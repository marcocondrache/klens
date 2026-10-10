use std::fmt::{self, Display, Formatter};
use std::time::Duration;

use axum::http::Uri;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use url::Url;

use super::{Privilege, at_least_one_second};

const READS: [Privilege; 5] = [
    Privilege::Records,
    Privilege::TopicConfigs,
    Privilege::BrokerConfigs,
    Privilege::SchemaText,
    Privilege::Acls,
];

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Mcp {
    /// Browser origins `/mcp` answers while `auth` is on, each with its port
    /// or `:*` for any port. Every other request that carries an `Origin`
    /// header gets 403. It must stay empty without `auth`.
    pub allowed_origins: Vec<AllowedOrigin>,
    /// The most an MCP client may read or change beyond the catalog, whatever
    /// its roles grant. Omitted, it holds the five reads, so MCP changes
    /// nothing until it names `create_topics`, `produce` or
    /// `register_schemas`.
    #[serde(deserialize_with = "tool_privileges")]
    pub privileges: Vec<Privilege>,
    /// Omitted, MCP reaches every cluster. A list, even an empty one, limits
    /// it to those clusters.
    pub clusters: Option<Vec<String>>,
    /// The URL MCP clients reach `/mcp` at, such as
    /// `https://klens.example.com/mcp`. Required with `auth`, and refused
    /// without it. `/mcp` then answers only its host, and a token must name
    /// it as its audience unless `token.audiences` says otherwise.
    #[serde(deserialize_with = "resource")]
    pub resource: Option<Url>,
    /// Which access tokens from the `auth.oidc` provider open `/mcp`. Only
    /// with `auth`.
    pub token: Token,
}

impl Default for Mcp {
    fn default() -> Self {
        Self {
            allowed_origins: Vec::new(),
            privileges: READS.to_vec(),
            clusters: None,
            resource: None,
            token: Token::default(),
        }
    }
}

impl Mcp {
    pub const WRITES: &[Privilege] = &[
        Privilege::CreateTopics,
        Privilege::Produce,
        Privilege::RegisterSchemas,
    ];

    pub fn writes(&self) -> bool {
        self.privileges
            .iter()
            .any(|privilege| Self::WRITES.contains(privilege))
    }

    pub fn reads_private_text(&self) -> bool {
        self.privileges
            .iter()
            .any(|privilege| matches!(privilege, Privilege::Records | Privilege::SchemaText))
    }

    pub fn audiences(&self) -> Vec<String> {
        match &self.token.audiences {
            Some(audiences) => audiences.clone(),
            None => self.resource.iter().map(Url::to_string).collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Token {
    /// A token's `aud` must name one of these. Omitted, it must name
    /// `resource`. It may never name `auth.oidc.client_id`, whose ID tokens
    /// would then open `/mcp`.
    #[serde(deserialize_with = "audiences")]
    pub audiences: Option<Vec<String>>,
    /// When set, a token's `azp`, or else its `client_id`, must be one of
    /// these. A Keycloak audience mapper on a default scope puts klens'
    /// audience on the tokens of every client in the realm.
    pub clients: Vec<String>,
    /// The scopes an MCP client asks the provider for. Entra ID needs the
    /// API's scope here, or it issues a token for another audience, and Dex
    /// needs `openid` and `groups`.
    #[serde(deserialize_with = "scopes")]
    pub scopes: Vec<String>,
    /// Refuses a token whose `exp` lies further than this past its `iat`,
    /// which bounds how long a token outlives a group the provider removed.
    /// Entra ID often issues tokens that last longer than an hour, so raise
    /// it there.
    #[serde(deserialize_with = "at_least_one_second")]
    pub max_age: Duration,
    /// The claim that lists a token's groups. Omitted, `auth.oidc.groups_claim`.
    pub groups_claim: Option<String>,
    /// The claim that names the user on log lines and keys their live-call
    /// budget, such as `oid` on Entra ID. A token without it is named by its
    /// `sub`.
    pub user_claim: String,
}

impl Default for Token {
    fn default() -> Self {
        Self {
            audiences: None,
            clients: Vec::new(),
            scopes: Vec::new(),
            max_age: Duration::from_secs(60 * 60),
            groups_claim: None,
            user_claim: "sub".to_owned(),
        }
    }
}

/// A browser origin as rmcp matches it: a scheme, a host, and a port or `*`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
pub struct AllowedOrigin(String);

impl TryFrom<String> for AllowedOrigin {
    type Error = String;

    fn try_from(entry: String) -> Result<Self, String> {
        let any_port = entry.strip_suffix(":*");
        let base = any_port.unwrap_or(&entry);
        let origin: Uri = base
            .parse()
            .map_err(|_| format!("'{entry}' is not an origin such as https://claude.ai:443"))?;
        // The parsed URI reads a missing path as `/`, so only the entry's
        // text shows whether anything follows the authority.
        if !matches!(origin.scheme_str(), Some("http" | "https"))
            || origin.authority().is_none_or(|authority| {
                !base.ends_with(authority.as_str())
                    || authority.as_str().contains('@')
                    || authority.host().is_empty()
            })
        {
            return Err(format!(
                "'{entry}' must be an http or https scheme, a host and a port, with nothing after \
                 them"
            ));
        }
        match (origin.port_u16(), any_port) {
            // rmcp lets a portless entry match any port, and plans to match
            // only the scheme's default port instead.
            (None, None) => Err(format!(
                "'{entry}' must name its port, as in https://claude.ai:443, or end in :* for \
                 any port"
            )),
            (Some(_), Some(_)) => Err(format!(
                "'{entry}' must name its port or end in :*, not both"
            )),
            _ => Ok(Self(entry)),
        }
    }
}

impl Display for AllowedOrigin {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A later release may add a tool, a destructive one above all, and an
/// existing config must not switch it on.
fn tool_privileges<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Privilege>, D::Error> {
    let privileges = Vec::<Privilege>::deserialize(deserializer)?;
    if privileges
        .iter()
        .any(|privilege| !READS.contains(privilege) && !Mcp::WRITES.contains(privilege))
    {
        return Err(D::Error::custom(
            "MCP tools use only records, topic_configs, broker_configs, schema_text, acls, \
             create_topics, produce, and register_schemas, so privileges may name no other",
        ));
    }
    Ok(privileges)
}

fn resource<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Url>, D::Error> {
    let url = Url::deserialize(deserializer)?;
    if !matches!(url.scheme(), "http" | "https")
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(D::Error::custom(
            "must be an http or https URL with no user, query or fragment",
        ));
    }
    // klens routes the resource's metadata under its path, and axum 0.8
    // refuses a route segment that starts with `:` or `*`.
    if url
        .path_segments()
        .into_iter()
        .flatten()
        .any(|segment| segment.starts_with([':', '*']))
    {
        return Err(D::Error::custom(
            "must not have a path segment that starts with : or *",
        ));
    }
    Ok(Some(url))
}

fn audiences<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Vec<String>>, D::Error> {
    let audiences = Vec::<String>::deserialize(deserializer)?;
    if audiences.is_empty() {
        return Err(D::Error::custom(
            "must name at least one audience, or be left out to take resource",
        ));
    }
    Ok(Some(audiences))
}

/// A scope goes into the `WWW-Authenticate` header, so it holds only the
/// characters RFC 6749 allows there.
fn scopes<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<String>, D::Error> {
    let scopes = Vec::<String>::deserialize(deserializer)?;
    if let Some(scope) = scopes.iter().find(|scope| {
        scope.is_empty()
            || !scope
                .bytes()
                .all(|byte| matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
    }) {
        return Err(D::Error::custom(format!(
            "'{scope}' is not a scope, which is printable ASCII with no space, quote or backslash"
        )));
    }
    Ok(scopes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{yaml, yaml_err};

    #[test]
    fn an_empty_block_serves_every_read_on_every_cluster() {
        let mcp: Mcp = yaml("{}");

        assert_eq!(mcp.privileges, READS);
        assert_eq!(mcp.clusters, None);
        assert_eq!(mcp, Mcp::default());
        assert!(!mcp.writes());
    }

    #[test]
    fn reads_privileges_and_clusters() {
        let some: Mcp = yaml("{privileges: [records], clusters: [dev, staging]}");
        let none: Mcp = yaml("{privileges: [], clusters: []}");

        assert_eq!(some.privileges, [Privilege::Records]);
        assert_eq!(
            some.clusters.as_deref(),
            Some(&["dev".into(), "staging".into()][..])
        );
        assert!(none.privileges.is_empty());
        assert_eq!(none.clusters.as_deref(), Some(&[][..]));
    }

    #[test]
    fn reads_the_three_write_privileges_beside_the_reads() {
        let mcp: Mcp =
            yaml("privileges: [topic_configs, create_topics, produce, register_schemas]");

        assert_eq!(
            mcp.privileges,
            [
                Privilege::TopicConfigs,
                Privilege::CreateTopics,
                Privilege::Produce,
                Privilege::RegisterSchemas
            ]
        );
        assert!(mcp.writes());
        assert!(!mcp.reads_private_text());
    }

    #[test]
    fn any_other_write_privilege_stops_the_load() {
        for privilege in [
            "delete_topics",
            "set_scram_credentials",
            "set_compatibility",
        ] {
            let error = yaml_err::<Mcp>(&format!("privileges: [produce, {privilege}]"));

            assert_eq!(
                error,
                "MCP tools use only records, topic_configs, broker_configs, schema_text, acls, \
                 create_topics, produce, and register_schemas, so privileges may name no other at \
                 line 1, column 13",
                "{privilege}"
            );
        }
    }

    #[test]
    fn records_and_schema_text_are_the_private_text() {
        for (privileges, private) in [
            ("[records]", true),
            ("[schema_text]", true),
            ("[topic_configs, broker_configs, acls, produce]", false),
        ] {
            let mcp: Mcp = yaml(&format!("privileges: {privileges}"));

            assert_eq!(mcp.reads_private_text(), private, "{privileges}");
        }
    }

    #[test]
    fn a_token_takes_the_resource_as_its_audience_for_an_hour_unless_told_otherwise() {
        let mcp: Mcp = yaml("resource: https://KLENS.example.com:443/mcp");

        assert_eq!(mcp.audiences(), ["https://klens.example.com/mcp"]);
        assert_eq!(mcp.token, Token::default());
        assert_eq!(mcp.token.max_age, Duration::from_secs(3_600));
        assert_eq!(mcp.token.user_claim, "sub");
        assert_eq!(mcp.token.groups_claim, None);
    }

    #[test]
    fn reads_every_token_key() {
        let mcp: Mcp = yaml(
            "
            allowed_origins: ['https://claude.ai:443', 'http://localhost:*']
            resource: https://klens.example.com/mcp
            token:
              audiences: [api://klens, https://klens.example.com/mcp]
              clients: [claude-code, vscode]
              scopes: [api://klens/mcp.read, openid]
              max_age: 15m
              groups_claim: roles
              user_claim: oid
            ",
        );

        assert_eq!(
            mcp.allowed_origins
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            ["https://claude.ai:443", "http://localhost:*"]
        );
        assert_eq!(
            mcp.audiences(),
            ["api://klens", "https://klens.example.com/mcp"]
        );
        assert_eq!(mcp.token.clients, ["claude-code", "vscode"]);
        assert_eq!(mcp.token.scopes, ["api://klens/mcp.read", "openid"]);
        assert_eq!(mcp.token.max_age, Duration::from_secs(900));
        assert_eq!(mcp.token.groups_claim.as_deref(), Some("roles"));
        assert_eq!(mcp.token.user_claim, "oid");
    }

    #[test]
    fn rejects_a_token_rule_it_cannot_apply() {
        for (source, expected) in [
            (
                "resource: klens.example.com/mcp",
                "relative URL without a base",
            ),
            (
                "resource: ftp://klens.example.com/mcp",
                "must be an http or https URL with no user, query or fragment",
            ),
            (
                "resource: https://klens.example.com/mcp?tenant=a",
                "must be an http or https URL with no user, query or fragment",
            ),
            (
                "resource: https://klens.example.com/mcp#a",
                "must be an http or https URL with no user, query or fragment",
            ),
            (
                "resource: https://admin@klens.example.com/mcp",
                "must be an http or https URL with no user, query or fragment",
            ),
            (
                "resource: 'https://klens.example.com/:mcp'",
                "must not have a path segment that starts with : or *",
            ),
            (
                "resource: 'https://klens.example.com/klens/*'",
                "must not have a path segment that starts with : or *",
            ),
            (
                "token: {audiences: []}",
                "must name at least one audience, or be left out to take resource",
            ),
            (
                "token: {scopes: ['mcp read']}",
                "'mcp read' is not a scope, which is printable ASCII with no space, quote or \
                 backslash",
            ),
            (
                "token: {scopes: ['']}",
                "'' is not a scope, which is printable ASCII with no space, quote or backslash",
            ),
            ("token: {max_age: 0s}", "must be at least 1s"),
            ("token: {issuer: x}", "unknown field `issuer`"),
        ] {
            let error = yaml_err::<Mcp>(source);
            assert!(error.contains(expected), "{source}: {error}");
        }
    }

    #[test]
    fn an_allowed_origin_names_its_scheme_host_and_port() {
        for entry in [
            "https://claude.ai:443",
            "http://localhost:6274",
            "http://localhost:*",
            "http://[::1]:*",
        ] {
            let origin: AllowedOrigin = yaml(&format!("'{entry}'"));
            assert_eq!(origin.to_string(), entry);
        }

        for (entry, expected) in [
            (
                "https://claude.ai",
                "'https://claude.ai' must name its port, as in https://claude.ai:443, or end in \
                 :* for any port",
            ),
            (
                "https://claude.ai:443:*",
                "'https://claude.ai:443:*' must name its port or end in :*, not both",
            ),
            (
                "claude.ai:443",
                "'claude.ai:443' must be an http or https scheme, a host and a port",
            ),
            (
                "https://claude.ai:443/",
                "'https://claude.ai:443/' must be an http or https scheme, a host and a port",
            ),
            (
                "https://user@claude.ai:443",
                "'https://user@claude.ai:443' must be an http or https scheme, a host and a port",
            ),
            (
                "null",
                "'null' must be an http or https scheme, a host and a port",
            ),
            (
                "*",
                "'*' must be an http or https scheme, a host and a port",
            ),
            (
                "https://claude ai:443",
                "'https://claude ai:443' is not an origin such as https://claude.ai:443",
            ),
        ] {
            let error = yaml_err::<AllowedOrigin>(&format!("'{entry}'"));
            assert!(error.starts_with(expected), "{entry}: {error}");
        }
    }
}
