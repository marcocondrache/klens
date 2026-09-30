use std::path::PathBuf;

use serde::Deserialize;
use url::Url;

use super::Secret;
use super::obfuscation::Obfuscation;

/// A Kafka cluster: how to reach it, and what klens layers on top of it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cluster {
    pub bootstrap_servers: Vec<String>,
    /// Defaults to `klens-<cluster name>`.
    pub client_id: Option<String>,
    /// Connects over TLS when present. `tls: {}` trusts the system roots.
    pub tls: Option<Tls>,
    /// Authenticates with SASL when present.
    pub sasl: Option<Sasl>,
    /// Decodes framed payloads and lists the cluster's schemas.
    pub schema_registry: Option<SchemaRegistry>,
    /// Hides parts of records from everyone who browses the cluster.
    pub obfuscation: Option<Obfuscation>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Tls {
    /// PEM bundle to trust instead of the system roots.
    pub ca_cert: Option<PathBuf>,
    /// PEM certificate and key to present to the brokers.
    pub client: Option<ClientCert>,
    pub insecure_skip_verify: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientCert {
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sasl {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: Secret,
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

/// A Confluent-compatible Schema Registry.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistry {
    pub url: Url,
    pub auth: Option<BasicAuth>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BasicAuth {
    pub username: String,
    pub password: Secret,
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;
    use crate::config::parse;

    #[test]
    fn a_bare_cluster_is_plaintext_with_nothing_on_top() {
        let cluster: Cluster = parse("bootstrap_servers: [localhost:9092]").unwrap();

        assert_eq!(cluster.bootstrap_servers, ["localhost:9092"]);
        assert!(cluster.client_id.is_none());
        assert!(cluster.tls.is_none());
        assert!(cluster.sasl.is_none());
        assert!(cluster.schema_registry.is_none());
        assert!(cluster.obfuscation.is_none());
    }

    #[test]
    fn reads_every_connection_setting() {
        let cluster: Cluster = parse(
            "
            bootstrap_servers: [broker-1:9092, broker-2:9092]
            client_id: browser
            tls:
              ca_cert: /etc/ca.pem
              client: {cert: /etc/client.pem, key: /etc/client.key}
              insecure_skip_verify: true
            sasl:
              mechanism: SCRAM-SHA-512
              username: admin
              password: {value: kafka-secret}
            schema_registry:
              url: https://registry.example.com
              auth: {username: klens, password: {value: registry-secret}}
            ",
        )
        .unwrap();

        assert_eq!(
            cluster.bootstrap_servers,
            ["broker-1:9092", "broker-2:9092"]
        );
        assert_eq!(cluster.client_id.as_deref(), Some("browser"));

        let tls = cluster.tls.unwrap();
        assert_eq!(tls.ca_cert, Some("/etc/ca.pem".into()));
        let client = tls.client.unwrap();
        assert_eq!(client.cert, PathBuf::from("/etc/client.pem"));
        assert_eq!(client.key, PathBuf::from("/etc/client.key"));
        assert!(tls.insecure_skip_verify);

        let sasl = cluster.sasl.unwrap();
        assert_eq!(sasl.mechanism, SaslMechanism::ScramSha512);
        assert_eq!(sasl.username, "admin");
        assert_eq!(sasl.password.expose_secret(), "kafka-secret");

        let registry = cluster.schema_registry.unwrap();
        assert_eq!(registry.url.as_str(), "https://registry.example.com/");
        let auth = registry.auth.unwrap();
        assert_eq!(auth.username, "klens");
        assert_eq!(auth.password.expose_secret(), "registry-secret");
    }

    #[test]
    fn an_empty_tls_block_verifies_against_the_system_roots() {
        let cluster: Cluster = parse("{bootstrap_servers: [kafka:9093], tls: {}}").unwrap();
        let tls = cluster.tls.unwrap();

        assert!(tls.ca_cert.is_none());
        assert!(tls.client.is_none());
        assert!(!tls.insecure_skip_verify);
    }

    #[test]
    fn reads_each_sasl_mechanism_by_its_kafka_name() {
        for (name, mechanism) in [
            ("PLAIN", SaslMechanism::Plain),
            ("SCRAM-SHA-256", SaslMechanism::ScramSha256),
            ("SCRAM-SHA-512", SaslMechanism::ScramSha512),
        ] {
            let sasl: Sasl = parse(&format!(
                "{{mechanism: {name}, username: u, password: {{value: p}}}}"
            ))
            .unwrap();
            assert_eq!(sasl.mechanism, mechanism);
        }
    }

    #[test]
    fn a_client_cert_needs_its_key() {
        let error = parse::<Tls>("client: {cert: /etc/client.pem}").unwrap_err();

        assert!(
            error.to_string().starts_with("missing field `key`"),
            "{error}"
        );
    }
}
