use std::net::{Ipv4Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, anyhow};
use indexmap::IndexMap;
use serde::de::{DeserializeOwned, Error as _};
use serde::{Deserialize, Deserializer};

mod auth;
mod cluster;
mod hosts;
mod mcp;
pub mod obfuscation;
mod secret;
mod tuning;

pub use auth::{Auth, Binding, Oidc, Privilege, Role, Session};
pub use cluster::{BasicAuth, ClientCert, Cluster, Sasl, SaslMechanism, SchemaRegistry, Tls};
pub use hosts::AllowedHost;
pub use mcp::Mcp;
pub use secret::{KeyMaterial, Secret};
pub use tuning::{
    IngestTuning, KafkaTuning, McpTuning, RecordLimits, ScanTuning, SchemaRegistryTuning,
    TailTuning, Tuning,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub bind: SocketAddr,
    /// Hosts klens answers while `auth` is off, by name or IP address and
    /// optionally port. Any other `Host` gets 403 everywhere but `/health`
    /// and `/ready`, so a web page that points its own domain at klens cannot
    /// use it from a visitor's browser.
    #[serde(deserialize_with = "hosts::at_least_one")]
    pub allowed_hosts: Vec<AllowedHost>,
    pub log_level: LogLevel,
    /// Keyed by the name the UI shows, in the order it shows them.
    pub clusters: IndexMap<String, Cluster>,
    /// Without it, the UI and API are open to anyone who can reach them.
    pub auth: Option<Auth>,
    /// Serves MCP tools at `/mcp`. Without it, there is no `/mcp`.
    pub mcp: Option<Mcp>,
    pub tuning: Tuning,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from((Ipv4Addr::UNSPECIFIED, 8080)),
            allowed_hosts: AllowedHost::loopback(),
            log_level: LogLevel::default(),
            clusters: IndexMap::new(),
            auth: None,
            mcp: None,
            tuning: Tuning::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Off,
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let yaml = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        parse(&yaml)
            .and_then(|config: Self| config.check().map(|()| config))
            .with_context(|| format!("invalid config {}", path.display()))
    }

    fn check(&self) -> anyhow::Result<()> {
        let Some(mcp) = &self.mcp else {
            return Ok(());
        };
        if let Some(unknown) = mcp
            .clusters
            .iter()
            .flatten()
            .find(|name| !self.clusters.contains_key(*name))
        {
            anyhow::bail!("mcp.clusters names '{unknown}', which is not a configured cluster");
        }
        if mcp.bind == self.bind {
            anyhow::bail!(
                "mcp.bind is {}, the address of bind, so give MCP a port of its own",
                self.bind
            );
        }
        anyhow::ensure!(
            self.auth.is_none() || mcp.secret.is_some(),
            "mcp.secret is required with auth, since /mcp leaves sign-in to a gateway that sends it"
        );
        Ok(())
    }

    pub fn mcp_warnings(&self) -> Vec<&'static str> {
        let Some(mcp) = &self.mcp else {
            return Vec::new();
        };
        let mut warnings = Vec::new();
        if mcp.secret.is_none() && mcp.writes() {
            warnings.push(
                "mcp.secret is unset and mcp.privileges names a write, so anyone who reaches \
                 mcp.bind can change a writable cluster",
            );
        }
        if mcp.writes() && mcp.reads_private_text() {
            warnings.push(
                "mcp.privileges names records or schema_text beside a write, so text an agent \
                 reads from Kafka could steer it to copy private data out through that write",
            );
        }
        warnings
    }

    pub fn writable_without_auth(&self) -> Vec<&str> {
        if self.auth.is_some() {
            return Vec::new();
        }
        self.clusters
            .iter()
            .filter(|(_, cluster)| cluster.writable)
            .map(|(name, _)| name.as_str())
            .collect()
    }
}

pub(crate) fn parse<T: DeserializeOwned>(yaml: &str) -> anyhow::Result<T> {
    ::config::Config::builder()
        .add_source(::config::File::from_str(yaml, ::config::FileFormat::Yaml))
        .build()
        .and_then(::config::Config::try_deserialize)
        .map_err(|error| anyhow!(error))
}

fn duration<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    jiff::fmt::serde::unsigned_duration::required::deserialize(deserializer)
}

fn at_least_one_second<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let duration = duration(deserializer)?;
    if duration < Duration::from_secs(1) {
        return Err(D::Error::custom("must be at least 1s"));
    }
    Ok(duration)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{temp_file, yaml, yaml_err};

    #[test]
    fn every_key_has_a_default() {
        let config: Config = yaml("{}");

        assert_eq!(config.bind, "0.0.0.0:8080".parse().unwrap());
        assert_eq!(
            config
                .allowed_hosts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            ["localhost", "127.0.0.1", "[::1]"]
        );
        assert_eq!(config.log_level, LogLevel::Info);
        assert!(config.clusters.is_empty());
        assert!(config.auth.is_none());
        assert!(config.mcp.is_none());
        assert_eq!(config.tuning, Tuning::default());
    }

    #[test]
    fn the_example_config_loads() {
        let example: Config = yaml(include_str!("../config/clusters.example.yaml"));

        assert_eq!(example.clusters.keys().collect::<Vec<_>>(), ["local"]);
    }

    #[test]
    fn clusters_keep_file_order() {
        let config: Config = yaml(
            "
            bind: 127.0.0.1:3000
            log_level: debug
            clusters:
              zeta: {bootstrap_servers: [zeta:9092]}
              alpha: {bootstrap_servers: [alpha:9092]}
            ",
        );

        assert_eq!(config.bind, "127.0.0.1:3000".parse().unwrap());
        assert_eq!(config.log_level, LogLevel::Debug);
        assert_eq!(
            config.clusters.keys().collect::<Vec<_>>(),
            ["zeta", "alpha"]
        );
    }

    #[test]
    fn cluster_names_keep_their_case() {
        let config: Config = yaml("clusters:\n  Prod-EU: {bootstrap_servers: [a:9092]}");

        assert_eq!(config.clusters.keys().collect::<Vec<_>>(), ["Prod-EU"]);
    }

    #[test]
    fn rejects_what_it_cannot_read_where_it_is() {
        for (source, expected) in [
            ("bogus: true", "unknown field `bogus`, expected one of"),
            (
                "log_level: verbose",
                "enum LogLevel does not have variant constructor verbose for key `log_level`",
            ),
            (
                "bind: nowhere",
                "invalid socket address syntax for key `bind`",
            ),
            (
                "allowed_hosts: []",
                "must name at least one host for key `allowed_hosts`",
            ),
            (
                "allowed_hosts: [localhost, 'https://klens.example.com']",
                "'https://klens.example.com' must be a host and an optional port, \
                 with no scheme, path or user for key `allowed_hosts[1]`",
            ),
            (
                "clusters:\n  a: {bootstrap_servers: [a:9092]}\n  a: {bootstrap_servers: [b:9092]}",
                "String(\"a\"): duplicated key in mapping",
            ),
        ] {
            let error = yaml_err::<Config>(source);
            assert!(error.starts_with(expected), "{source}: {error}");
        }
    }

    #[test]
    fn load_names_the_file_it_could_not_use() {
        let missing = Config::load("/nonexistent/klens.yaml").unwrap_err();
        assert_eq!(
            missing.to_string(),
            "failed to read /nonexistent/klens.yaml"
        );

        let file = temp_file("bogus: true");
        let invalid = Config::load(file.path()).unwrap_err();
        assert_eq!(
            invalid.to_string(),
            format!("invalid config {}", file.path().display())
        );
        assert!(
            format!("{invalid:#}").contains("unknown field `bogus`, expected one of"),
            "{invalid:#}"
        );
    }

    #[test]
    fn writable_clusters_without_auth_are_named_in_file_order() {
        let clusters = "
            clusters:
              zeta: {bootstrap_servers: [zeta:9092], writable: true}
              beta: {bootstrap_servers: [beta:9092]}
              alpha: {bootstrap_servers: [alpha:9092], writable: true}
            ";
        let open: Config = yaml(clusters);
        let guarded: Config = yaml(&format!(
            "{clusters}
            auth:
              oidc:
                issuer: https://idp.example.com
                client_id: klens
                client_secret: {{value: oidc-secret}}
                redirect_uri: https://klens.example.com/api/auth/callback
            "
        ));

        assert_eq!(open.writable_without_auth(), ["zeta", "alpha"]);
        assert!(guarded.writable_without_auth().is_empty());
    }

    const AUTH: &str = "
            auth:
              oidc:
                issuer: https://idp.example.com
                client_id: klens
                client_secret: {value: oidc-secret}
                redirect_uri: https://klens.example.com/api/auth/callback
            ";

    const SECRET: &str = "secret: {value: gateway-secret-of-at-least-32-bytes}";
    const MCP_BIND: &str = "bind: 0.0.0.0:8090";

    #[test]
    fn mcp_with_auth_needs_a_secret() {
        let with_auth =
            |mcp: &str| yaml::<Config>(&format!("{AUTH}\n            mcp: {{{MCP_BIND}, {mcp}}}"));

        assert!(with_auth(SECRET).check().is_ok());
        assert_eq!(
            with_auth("privileges: [records]")
                .check()
                .unwrap_err()
                .to_string(),
            "mcp.secret is required with auth, since /mcp leaves sign-in to a gateway that sends it"
        );
        assert!(
            yaml::<Config>(&format!("mcp: {{{MCP_BIND}}}"))
                .check()
                .is_ok()
        );
    }

    #[test]
    fn mcp_warns_of_each_way_a_write_reaches_too_far() {
        const OPEN: &str = "mcp.secret is unset and mcp.privileges names a write, so anyone \
                            who reaches mcp.bind can change a writable cluster";
        const PRIVATE: &str = "mcp.privileges names records or schema_text beside a write, so \
                               text an agent reads from Kafka could steer it to copy private \
                               data out through that write";
        let with_auth = |mcp: &str| {
            yaml::<Config>(&format!(
                "{AUTH}\n            mcp: {{{MCP_BIND}, {SECRET}, {mcp}}}"
            ))
        };
        let without_auth = |mcp: &str| yaml::<Config>(&format!("mcp: {{{MCP_BIND}, {mcp}}}"));

        for (config, expected) in [
            (yaml::<Config>("{}"), vec![]),
            (without_auth(""), vec![]),
            (with_auth("privileges: [records]"), vec![]),
            (with_auth("privileges: [acls, produce]"), vec![]),
            (
                with_auth("privileges: [schema_text, register_schemas]"),
                vec![PRIVATE],
            ),
            (without_auth("privileges: [create_topics]"), vec![OPEN]),
            (
                without_auth("privileges: [records, produce]"),
                vec![OPEN, PRIVATE],
            ),
            (without_auth(SECRET), vec![]),
            (
                without_auth(&format!("privileges: [create_topics], {SECRET}")),
                vec![],
            ),
        ] {
            assert_eq!(config.mcp_warnings(), expected, "{:?}", config.mcp);
        }
    }

    #[test]
    fn mcp_bind_must_differ_from_bind() {
        let config = |bind: &str| yaml::<Config>(&format!("mcp: {{bind: '{bind}'}}"));

        assert_eq!(
            config("0.0.0.0:8080").check().unwrap_err().to_string(),
            "mcp.bind is 0.0.0.0:8080, the address of bind, so give MCP a port of its own"
        );
        assert!(config("0.0.0.0:8090").check().is_ok());
    }

    #[test]
    fn mcp_clusters_must_be_configured_clusters() {
        let config = |mcp: &str| {
            yaml::<Config>(&format!(
                "
                clusters:
                  dev: {{bootstrap_servers: [dev:9092]}}
                  prod: {{bootstrap_servers: [prod:9092]}}
                mcp: {{{MCP_BIND}, clusters: {mcp}}}
                "
            ))
        };

        assert!(config("[dev, prod]").check().is_ok());
        assert!(config("[]").check().is_ok());
        assert_eq!(
            config("[dev, staging]").check().unwrap_err().to_string(),
            "mcp.clusters names 'staging', which is not a configured cluster"
        );
    }

    #[test]
    fn load_checks_the_rules_that_span_blocks() {
        let file = temp_file(&format!("mcp: {{{MCP_BIND}, clusters: [ghost]}}"));

        let invalid = Config::load(file.path()).unwrap_err();

        assert_eq!(
            format!("{invalid:#}"),
            format!(
                "invalid config {}: mcp.clusters names 'ghost', which is not a configured cluster",
                file.path().display()
            )
        );
    }

    #[test]
    fn load_reads_the_file() {
        let file = temp_file("log_level: warn");

        assert_eq!(Config::load(file.path()).unwrap().log_level, LogLevel::Warn);
    }
}
