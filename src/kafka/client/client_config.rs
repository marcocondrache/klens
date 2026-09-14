use std::time::Duration;

use krafka::auth::{AuthConfig, TlsConfig as KrafkaTlsConfig};

use crate::config::{ClusterConfig, SaslMechanism, SecurityConfig, SecurityProtocol, TlsConfig};
use crate::environment::{CLIENT_ID_PREFIX, SOCKET_CONNECTION_SETUP_TIMEOUT_MS};
use crate::kafka::error::KafkaError;

/// Connection settings krafka needs from a cluster YAML node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct KrafkaConnect {
    pub bootstrap: String,
    pub client_id: String,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
}

impl KrafkaConnect {
    pub(super) fn from_cluster(
        cluster: &ClusterConfig,
        request_timeout: Duration,
    ) -> Result<Self, KafkaError> {
        let mut connect = Self {
            bootstrap: cluster.bootstrap_servers.join(","),
            client_id: format!("{CLIENT_ID_PREFIX}-{}", cluster.name),
            request_timeout,
            connect_timeout: Duration::from_millis(u64::from(*SOCKET_CONNECTION_SETUP_TIMEOUT_MS)),
        };

        for (key, value) in &cluster.properties {
            match key.as_str() {
                "bootstrap.servers" => connect.bootstrap = value.clone(),
                "client.id" => connect.client_id = value.clone(),
                "request.timeout.ms" | "api.version.request.timeout.ms" => {
                    connect.request_timeout = duration_ms(key, value)?;
                }
                "socket.connection.setup.timeout.ms" => {
                    connect.connect_timeout = duration_ms(key, value)?;
                }
                _ => {
                    return Err(KafkaError::Admin(format!(
                        "cluster '{}' sets unknown Kafka property '{key}'",
                        cluster.name
                    )));
                }
            }
        }

        if connect.request_timeout < connect.connect_timeout {
            connect.request_timeout = connect.connect_timeout;
        }
        Ok(connect)
    }
}

fn duration_ms(key: &str, value: &str) -> Result<Duration, KafkaError> {
    let millis = value.parse::<u64>().map_err(|_| {
        KafkaError::Admin(format!(
            "cluster property '{key}' is not a millisecond count"
        ))
    })?;
    Ok(Duration::from_millis(millis))
}

/// `None` is plaintext. A SASL protocol with no `sasl` block is an error.
pub(super) fn krafka_auth(cluster: &ClusterConfig) -> Result<Option<AuthConfig>, KafkaError> {
    let Some(security) = &cluster.security else {
        return Ok(None);
    };

    Ok(Some(match security.protocol {
        SecurityProtocol::Plaintext => return Ok(None),
        SecurityProtocol::Ssl => AuthConfig::ssl(krafka_tls(security.tls.as_ref())),
        SecurityProtocol::SaslPlaintext => krafka_sasl(cluster, security, None)?,
        SecurityProtocol::SaslSsl => {
            krafka_sasl(cluster, security, Some(krafka_tls(security.tls.as_ref())))?
        }
    }))
}

fn krafka_sasl(
    cluster: &ClusterConfig,
    security: &SecurityConfig,
    tls: Option<KrafkaTlsConfig>,
) -> Result<AuthConfig, KafkaError> {
    let sasl = security.sasl.as_ref().ok_or_else(|| {
        KafkaError::Admin(format!(
            "cluster '{}' sets a SASL security protocol with no sasl block",
            cluster.name
        ))
    })?;

    Ok(match (sasl.mechanism, tls) {
        (SaslMechanism::Plain, None) => AuthConfig::sasl_plain(&sasl.username, &sasl.password)?,
        (SaslMechanism::Plain, Some(tls)) => {
            AuthConfig::sasl_plain_ssl(&sasl.username, &sasl.password, tls)?
        }
        (SaslMechanism::ScramSha256, None) => {
            AuthConfig::sasl_scram_sha256(&sasl.username, &sasl.password)
        }
        (SaslMechanism::ScramSha256, Some(tls)) => {
            AuthConfig::sasl_scram_sha256_ssl(&sasl.username, &sasl.password, tls)
        }
        (SaslMechanism::ScramSha512, None) => {
            AuthConfig::sasl_scram_sha512(&sasl.username, &sasl.password)
        }
        (SaslMechanism::ScramSha512, Some(tls)) => {
            AuthConfig::sasl_scram_sha512_ssl(&sasl.username, &sasl.password, tls)
        }
    })
}

fn krafka_tls(tls: Option<&TlsConfig>) -> KrafkaTlsConfig {
    let tls = tls.cloned().unwrap_or_default();
    let mut krafka_tls = if tls.insecure_skip_verify {
        KrafkaTlsConfig::insecure()
    } else {
        KrafkaTlsConfig::new()
    };

    if let Some(ca_cert) = &tls.ca_cert {
        krafka_tls = krafka_tls.with_ca_cert(ca_cert.to_string_lossy());
    }

    if let (Some(cert), Some(key)) = (&tls.client_cert, &tls.client_key) {
        krafka_tls = krafka_tls.with_client_cert(cert.to_string_lossy(), key.to_string_lossy());
    }

    krafka_tls
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn cluster(yaml: &str) -> ClusterConfig {
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    #[test]
    fn connect_settings_use_bootstrap_and_timeouts() {
        let cluster = cluster(
            "
            name: local
            bootstrap_servers:
              - broker-1:9092
              - broker-2:9092
            properties:
              request.timeout.ms: '10000'
            ",
        );

        let connect = KrafkaConnect::from_cluster(&cluster, Duration::from_secs(8)).unwrap();
        assert_eq!(connect.bootstrap, "broker-1:9092,broker-2:9092");
        assert_eq!(connect.client_id, "klens-local");
        assert_eq!(connect.request_timeout, Duration::from_millis(10_000));
        assert_eq!(connect.connect_timeout, Duration::from_secs(10));
    }

    #[test]
    fn properties_override_connection_timeouts() {
        let cluster = cluster(
            "
            name: local
            bootstrap_servers:
              - localhost:9092
            properties:
              socket.connection.setup.timeout.ms: '30000'
            ",
        );

        let connect = KrafkaConnect::from_cluster(&cluster, Duration::from_secs(8)).unwrap();
        assert_eq!(connect.connect_timeout, Duration::from_secs(30));
        assert_eq!(connect.request_timeout, Duration::from_secs(30));
    }

    #[test]
    fn unknown_properties_are_rejected() {
        let cluster = cluster(
            "
            name: local
            bootstrap_servers:
              - localhost:9092
            properties:
              queued.min.messages: '2000'
            ",
        );

        let error = KrafkaConnect::from_cluster(&cluster, Duration::from_secs(8)).unwrap_err();
        assert!(matches!(error, KafkaError::Admin(_)));
    }

    #[test]
    fn from_config_keeps_cluster_order() {
        let config: Config = serde_yaml_ng::from_str(
            "
            bind: 127.0.0.1:8080
            clusters:
              - name: local
                bootstrap_servers:
                  - localhost:9092
              - name: staging
                bootstrap_servers:
                  - staging:9092
            ",
        )
        .unwrap();

        let derived: Vec<_> = config
            .clusters
            .iter()
            .map(|cluster| KrafkaConnect::from_cluster(cluster, Duration::from_secs(10)).unwrap())
            .collect();
        assert_eq!(derived[0].client_id, "klens-local");
        assert_eq!(derived[1].client_id, "klens-staging");
    }

    #[test]
    fn krafka_auth_is_none_for_plaintext() {
        let cluster = cluster(
            "
            name: local
            bootstrap_servers:
              - localhost:9092
            ",
        );

        assert!(krafka_auth(&cluster).unwrap().is_none());
    }

    #[test]
    fn krafka_auth_builds_scram_ssl_settings() {
        let cluster = cluster(
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
            ",
        );

        let auth = krafka_auth(&cluster).unwrap().expect("sasl ssl auth");

        assert_eq!(
            auth.sasl_mechanism(),
            Some(&krafka::auth::SaslMechanism::ScramSha512)
        );
        let credentials = auth.scram_credentials().expect("scram credentials");
        assert_eq!(credentials.username, "admin");
        assert_eq!(credentials.password, "secret");

        let tls = auth.tls_config().expect("tls config");
        assert_eq!(tls.ca_cert_path(), Some("/etc/ca.pem"));
        assert_eq!(tls.client_cert_path(), Some("/etc/client.pem"));
        assert_eq!(tls.client_key_path(), Some("/etc/client.key"));
        assert!(!tls.verify_server_cert());
    }

    #[test]
    fn krafka_auth_builds_ssl_only_settings() {
        let cluster = cluster(
            "
            name: secure
            bootstrap_servers:
              - broker:9092
            security:
              protocol: SSL
              tls:
                ca_cert: /etc/ca.pem
            ",
        );

        let auth = krafka_auth(&cluster).unwrap().expect("ssl auth");
        assert_eq!(auth.sasl_mechanism(), None);
        assert_eq!(
            auth.tls_config().and_then(|tls| tls.ca_cert_path()),
            Some("/etc/ca.pem")
        );
    }

    #[test]
    fn krafka_auth_rejects_sasl_protocol_without_sasl_block() {
        let cluster = cluster(
            "
            name: broken
            bootstrap_servers:
              - broker:9092
            security:
              protocol: SASL_PLAINTEXT
            ",
        );

        let error = krafka_auth(&cluster).unwrap_err();
        assert!(matches!(error, KafkaError::Admin(_)));
    }
}
