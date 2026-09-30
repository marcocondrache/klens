use std::time::Duration;

use indexmap::IndexMap;
use openidconnect::{IssuerUrl, RedirectUrl};
use serde::Deserialize;

use super::{KeyMaterial, Secret, duration};

/// An OpenID Connect login in front of the UI and API.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Auth {
    pub oidc: Oidc,
    #[serde(default)]
    pub session: Session,
    /// Keyed by role name. Without roles, every signed-in user may do
    /// everything; with them, a user no binding matches cannot sign in.
    pub roles: Option<IndexMap<String, Role>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Oidc {
    /// Kept as written: discovery compares it to the provider's issuer byte
    /// for byte.
    pub issuer: IssuerUrl,
    pub client_id: String,
    pub client_secret: Secret,
    /// Where the provider sends the browser back. Session cookies are
    /// `Secure` when it is https.
    pub redirect_uri: RedirectUrl,
    #[serde(default = "default_scopes")]
    pub scopes: Vec<String>,
    /// The ID token claim that lists a user's groups.
    #[serde(default = "default_groups_claim")]
    pub groups_claim: String,
}

fn default_scopes() -> Vec<String> {
    ["openid", "email", "profile"].map(str::to_owned).to_vec()
}

fn default_groups_claim() -> String {
    "groups".to_owned()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Session {
    /// Signs session cookies. Without it, each restart signs everyone out.
    pub key: Option<KeyMaterial>,
    /// How long a login may take at the provider.
    #[serde(deserialize_with = "duration")]
    pub login_timeout: Duration,
    /// How long a session lasts at most, even if its ID token lasts longer.
    #[serde(deserialize_with = "duration")]
    pub max_age: Duration,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            key: None,
            login_timeout: Duration::from_secs(10 * 60),
            max_age: Duration::from_secs(12 * 60 * 60),
        }
    }
}

/// A set of privileges, and the groups that hold it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Role {
    /// Granted on top of the catalog, which every role sees.
    pub privileges: Vec<Privilege>,
    pub bindings: Vec<Binding>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub groups: Vec<String>,
    /// Omitted, the binding covers every cluster.
    pub clusters: Option<Vec<String>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Privilege {
    /// Record pages and live tails.
    Records,
    /// Live topic and broker configs.
    Configs,
    /// Schema bodies.
    SchemaText,
    /// ACL bindings.
    Acls,
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;
    use crate::config::parse;

    const OIDC: &str = "
oidc:
  issuer: https://idp.example.com
  client_id: klens
  client_secret: {value: oidc-secret}
  redirect_uri: https://klens.example.com/api/auth/callback
";

    fn auth(extra: &str) -> anyhow::Result<Auth> {
        parse(&format!("{OIDC}{extra}"))
    }

    #[test]
    fn an_oidc_block_is_all_it_takes() {
        let auth = auth("").unwrap();

        assert_eq!(auth.oidc.issuer.as_str(), "https://idp.example.com");
        assert_eq!(auth.oidc.client_id, "klens");
        assert_eq!(auth.oidc.client_secret.expose_secret(), "oidc-secret");
        assert_eq!(
            auth.oidc.redirect_uri.as_str(),
            "https://klens.example.com/api/auth/callback"
        );
        assert_eq!(auth.oidc.scopes, ["openid", "email", "profile"]);
        assert_eq!(auth.oidc.groups_claim, "groups");
        assert!(auth.session.key.is_none());
        assert_eq!(auth.session.login_timeout, Duration::from_secs(600));
        assert_eq!(auth.session.max_age, Duration::from_secs(43_200));
        assert!(auth.roles.is_none());
    }

    #[test]
    fn reads_the_session_and_oidc_overrides() {
        let auth = auth(
            "  scopes: [openid, groups]
  groups_claim: roles
session:
  key: {value: 0123456789abcdef0123456789abcdef}
  login_timeout: 5m
  max_age: 1h
",
        )
        .unwrap();

        assert_eq!(auth.oidc.scopes, ["openid", "groups"]);
        assert_eq!(auth.oidc.groups_claim, "roles");
        assert_eq!(
            auth.session.key.unwrap().as_bytes(),
            b"0123456789abcdef0123456789abcdef"
        );
        assert_eq!(auth.session.login_timeout, Duration::from_secs(300));
        assert_eq!(auth.session.max_age, Duration::from_secs(3_600));
    }

    #[test]
    fn reads_roles_in_file_order() {
        let auth = auth(
            "roles:
  viewer:
    privileges: []
    bindings: [{groups: [everyone]}]
  operator:
    privileges: [records, configs, schema_text, acls]
    bindings:
      - groups: [ops, sre]
        clusters: [staging]
",
        )
        .unwrap();
        let roles = auth.roles.unwrap();

        assert_eq!(roles.keys().collect::<Vec<_>>(), ["viewer", "operator"]);
        assert!(roles["viewer"].privileges.is_empty());
        assert_eq!(roles["viewer"].bindings[0].groups, ["everyone"]);
        assert!(roles["viewer"].bindings[0].clusters.is_none());
        assert_eq!(
            roles["operator"].privileges,
            [
                Privilege::Records,
                Privilege::Configs,
                Privilege::SchemaText,
                Privilege::Acls
            ]
        );
        assert_eq!(roles["operator"].bindings[0].groups, ["ops", "sre"]);
        assert_eq!(
            roles["operator"].bindings[0].clusters.as_deref(),
            Some(&["staging".to_owned()][..])
        );
    }

    #[test]
    fn rejects_a_url_that_does_not_parse() {
        let error = parse::<Oidc>(
            "{issuer: idp, client_id: k, client_secret: {value: s}, redirect_uri: /cb}",
        )
        .unwrap_err();

        assert!(
            error.to_string().starts_with("relative URL without a base"),
            "{error}"
        );
    }
}
