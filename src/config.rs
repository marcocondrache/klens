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
        anyhow::ensure!(
            self.auth.is_none(),
            "mcp cannot be served together with auth yet; remove one of the two blocks"
        );
        if let Some(unknown) = mcp
            .clusters
            .iter()
            .flatten()
            .find(|name| !self.clusters.contains_key(*name))
        {
            anyhow::bail!("mcp.clusters names '{unknown}', which is not a configured cluster");
        }
        Ok(())
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

/// Snippets stay off because they quote the lines around an error, and those
/// can hold a secret.
pub(crate) fn parse<T: DeserializeOwned>(yaml: &str) -> anyhow::Result<T> {
    serde_saphyr::from_str_with_options(yaml, serde_saphyr::options! { with_snippet: false })
        .map_err(|error| anyhow!(error.render_with_formatter(&serde_saphyr::UserMessageFormatter)))
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
    fn rejects_what_it_cannot_read_where_it_is() {
        for (source, expected) in [
            ("bogus: true", "unknown field `bogus`"),
            (
                "log_level: verbose",
                "unknown variant `verbose`, expected one of off, error, warn, info, debug, trace",
            ),
            (
                "bind: nowhere",
                "invalid socket address syntax at line 1, column 7",
            ),
            (
                "allowed_hosts: []",
                "must name at least one host at line 1, column 16",
            ),
            (
                "allowed_hosts: [localhost, 'https://klens.example.com']",
                "'https://klens.example.com' must be a host and an optional port, \
                 with no scheme, path or user at line 1, column 28",
            ),
            (
                "clusters:\n  a: {bootstrap_servers: [a:9092]}\n  a: {bootstrap_servers: [b:9092]}",
                "duplicate mapping key: a not allowed here at line 3, column 3",
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
            format!("{invalid:#}").contains("unknown field `bogus`"),
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

    #[test]
    fn mcp_runs_only_without_auth_for_now() {
        let both: Config = yaml(
            "
            mcp: {}
            auth:
              oidc:
                issuer: https://idp.example.com
                client_id: klens
                client_secret: {value: oidc-secret}
                redirect_uri: https://klens.example.com/api/auth/callback
            ",
        );

        assert_eq!(
            both.check().unwrap_err().to_string(),
            "mcp cannot be served together with auth yet; remove one of the two blocks"
        );
        assert!(yaml::<Config>("mcp: {}").check().is_ok());
    }

    #[test]
    fn mcp_clusters_must_be_configured_clusters() {
        let config = |mcp: &str| {
            yaml::<Config>(&format!(
                "
                clusters:
                  dev: {{bootstrap_servers: [dev:9092]}}
                  prod: {{bootstrap_servers: [prod:9092]}}
                mcp: {mcp}
                "
            ))
        };

        assert!(config("{clusters: [dev, prod]}").check().is_ok());
        assert!(config("{clusters: []}").check().is_ok());
        assert_eq!(
            config("{clusters: [dev, staging]}")
                .check()
                .unwrap_err()
                .to_string(),
            "mcp.clusters names 'staging', which is not a configured cluster"
        );
    }

    #[test]
    fn load_checks_the_rules_that_span_blocks() {
        let file = temp_file("mcp: {clusters: [ghost]}");

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
