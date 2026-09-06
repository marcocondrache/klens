use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

pub const DEFAULT_PATH: &str = "config.yaml";

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file {}: {source}", path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config file {}: {source}", path.display())]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_yaml_ng::Error,
    },
    #[error("invalid configuration for cluster '{cluster}': {reason}")]
    InvalidCluster { cluster: String, reason: String },
}

impl ConfigError {
    pub(crate) fn invalid_cluster(cluster: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidCluster {
            cluster: cluster.into(),
            reason: reason.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_bind", deserialize_with = "deserialize_bind")]
    pub bind: SocketAddr,
    #[serde(default = "default_log")]
    pub log: String,
    #[serde(default)]
    pub clusters: Vec<ClusterConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            log: default_log(),
            clusters: Vec::new(),
        }
    }
}

fn default_bind() -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], 8080))
}

fn default_log() -> String {
    String::from("info")
}

fn deserialize_bind<'de, D>(deserializer: D) -> Result<SocketAddr, D::Error>
where
    D: serde::Deserializer<'de>,
{
    String::deserialize(deserializer)?
        .parse()
        .map_err(serde::de::Error::custom)
}

impl Config {
    pub fn path() -> PathBuf {
        std::env::var_os("CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_PATH))
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;

        let config: Self = serde_yaml_ng::from_str(&raw).map_err(|source| ConfigError::Parse {
            path: path.to_owned(),
            source,
        })?;

        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut seen = HashSet::with_capacity(self.clusters.len());

        for cluster in &self.clusters {
            cluster.validate()?;

            let name = cluster.name.trim().to_owned();
            if !seen.insert(name.clone()) {
                return Err(ConfigError::invalid_cluster(name, "duplicate cluster name"));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub name: String,
    pub bootstrap_servers: Vec<String>,
    #[serde(default)]
    pub security: Option<SecurityConfig>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

impl ClusterConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        let fail = |reason: &str| Err(ConfigError::invalid_cluster(self.name.clone(), reason));

        if self.name.trim().is_empty() {
            return fail("name must not be empty");
        }

        if self.bootstrap_servers.is_empty() {
            return fail("bootstrap_servers must not be empty");
        }

        if let Some(security) = &self.security {
            let needs_sasl = matches!(
                security.protocol,
                SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
            );

            if needs_sasl && security.sasl.is_none() {
                return fail("sasl settings are required for SASL protocols");
            }

            if let Some(tls) = &security.tls
                && tls.client_cert.is_some() != tls.client_key.is_some()
            {
                return fail("client_cert and client_key must be set together");
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum SecurityProtocol {
    #[serde(rename = "PLAINTEXT")]
    Plaintext,
    #[serde(rename = "SSL")]
    Ssl,
    #[serde(rename = "SASL_PLAINTEXT")]
    SaslPlaintext,
    #[serde(rename = "SASL_SSL")]
    SaslSsl,
}

impl SecurityProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plaintext => "PLAINTEXT",
            Self::Ssl => "SSL",
            Self::SaslPlaintext => "SASL_PLAINTEXT",
            Self::SaslSsl => "SASL_SSL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum SaslMechanism {
    #[serde(rename = "PLAIN")]
    Plain,
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
}

impl SaslMechanism {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::ScramSha256 => "SCRAM-SHA-256",
            Self::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityConfig {
    pub protocol: SecurityProtocol,
    #[serde(default)]
    pub sasl: Option<SaslConfig>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaslConfig {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    #[serde(default)]
    pub ca_cert: Option<PathBuf>,
    #[serde(default)]
    pub client_cert: Option<PathBuf>,
    #[serde(default)]
    pub client_key: Option<PathBuf>,
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_cluster(yaml: &str) -> Result<ClusterConfig, serde_yaml_ng::Error> {
        serde_yaml_ng::from_str(yaml)
    }

    fn parse_config(yaml: &str) -> Result<Config, serde_yaml_ng::Error> {
        serde_yaml_ng::from_str(yaml)
    }

    #[test]
    fn parses_root_config() {
        let config = parse_config(
            "
            clusters:
              - name: local
                bootstrap_servers:
                  - localhost:9092
              - name: staging
                bootstrap_servers:
                  - broker-1:9092
                  - broker-2:9092
            ",
        )
        .unwrap();

        assert_eq!(config.clusters.len(), 2);
        assert_eq!(config.clusters[0].name, "local");
        assert_eq!(config.clusters[1].name, "staging");
        assert_eq!(config.bind, default_bind());
        assert_eq!(config.log, "info");
        config.validate().unwrap();
    }

    #[test]
    fn parses_bind_and_log() {
        let config = parse_config(
            "
            bind: 127.0.0.1:3000
            log: debug
            clusters: []
            ",
        )
        .unwrap();

        assert_eq!(config.bind, "127.0.0.1:3000".parse().unwrap());
        assert_eq!(config.log, "debug");
    }

    #[test]
    fn rejects_invalid_bind() {
        assert!(
            parse_config(
                "
            bind: not-an-address
            clusters: []
            "
            )
            .is_err()
        );
    }

    #[test]
    fn parses_minimal_cluster() {
        let config = parse_cluster(
            "
            name: local
            bootstrap_servers:
              - localhost:9092
            ",
        )
        .unwrap();

        assert_eq!(config.name, "local");
        assert_eq!(config.bootstrap_servers, vec!["localhost:9092"]);
        assert_eq!(config.security, None);
        assert!(config.properties.is_empty());
    }

    #[test]
    fn parses_bootstrap_server_list() {
        let config = parse_cluster(
            "
            name: local
            bootstrap_servers:
              - broker-1:9092
              - broker-2:9092
            ",
        )
        .unwrap();

        assert_eq!(
            config.bootstrap_servers,
            vec!["broker-1:9092", "broker-2:9092"]
        );
    }

    #[test]
    fn parses_full_security_settings() {
        let config = parse_cluster(
            "
            name: secure
            bootstrap_servers:
              - broker:9092
            security:
              protocol: SASL_SSL
              sasl:
                mechanism: SCRAM-SHA-512
                username: admin
                password: secret
              tls:
                ca_cert: /etc/ca.pem
                client_cert: /etc/client.pem
                client_key: /etc/client.key
                insecure_skip_verify: true
            properties:
              request.timeout.ms: '10000'
            ",
        )
        .unwrap();

        let security = config.security.unwrap();
        assert_eq!(security.protocol, SecurityProtocol::SaslSsl);
        assert_eq!(security.sasl.unwrap().mechanism, SaslMechanism::ScramSha512);
        assert_eq!(security.tls.unwrap().ca_cert, Some("/etc/ca.pem".into()));
        assert_eq!(
            config
                .properties
                .get("request.timeout.ms")
                .map(String::as_str),
            Some("10000")
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        assert!(
            parse_cluster(
                "
            name: local
            bootstrap_servers:
              - localhost:9092
            bogus: true
            "
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_unknown_root_fields() {
        assert!(
            parse_config(
                "
            clusters: []
            bogus: true
            "
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_scalar_bootstrap_servers() {
        assert!(
            parse_cluster(
                "
            name: local
            bootstrap_servers: localhost:9092
            "
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_empty_bootstrap_servers() {
        let config = parse_cluster(
            "
            name: local
            bootstrap_servers: []
            ",
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("bootstrap_servers must not be empty")
        );
    }

    #[test]
    fn validation_accepts_plaintext_without_sasl() {
        let config = ClusterConfig {
            name: "local".to_owned(),
            bootstrap_servers: vec!["localhost:9092".to_owned()],
            security: None,
            properties: HashMap::new(),
        };

        config.validate().unwrap();
    }

    #[test]
    fn validation_requires_sasl_for_sasl_protocols() {
        let config = parse_cluster(
            "
            name: local
            bootstrap_servers:
              - localhost:9092
            security:
              protocol: SASL_PLAINTEXT
            ",
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("sasl settings are required"));
    }

    #[test]
    fn validation_requires_client_cert_and_key_together() {
        let config = parse_cluster(
            "
            name: local
            bootstrap_servers:
              - localhost:9092
            security:
              protocol: SSL
              tls:
                client_cert: /etc/client.pem
            ",
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("must be set together"));
    }

    #[test]
    fn validation_rejects_duplicate_cluster_names() {
        let config = parse_config(
            "
            clusters:
              - name: local
                bootstrap_servers:
                  - localhost:9092
              - name: local
                bootstrap_servers:
                  - localhost:9093
            ",
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("duplicate cluster name"));
    }
}
