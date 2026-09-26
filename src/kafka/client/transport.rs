use std::time::Duration;

use krafka::admin::AdminClient as KrafkaAdmin;
use krafka::auth::{AuthConfig, TlsConfig as KrafkaTlsConfig};
use krafka::client::KrafkaClient as KrafkaSharedClient;
use krafka::network::TransportConfig;

use crate::config::{ClusterConfig, SaslMechanism, SecurityConfig, SecurityProtocol, TlsConfig};
use crate::environment::{
    CLIENT_ID_PREFIX, MAX_IN_FLIGHT_REQUESTS, MAX_RESPONSE_BYTES, REQUEST_TIMEOUT,
    SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
};
use crate::kafka::error::KafkaError;

pub(super) struct Transport {
    pub(super) client: KrafkaSharedClient,
    pub(super) admin: KrafkaAdmin,
    pub(super) connector: Connector,
}

#[derive(Clone)]
pub(super) struct Connector {
    bootstrap_servers: String,
    client_id: String,
    request_timeout: Duration,
    connect_timeout: Duration,
    transport: TransportConfig,
    auth: Option<AuthConfig>,
}

impl Connector {
    fn new(config: &ClusterConfig) -> Result<Self, KafkaError> {
        let properties = &config.properties;
        let connect_timeout = Duration::from_millis(
            properties
                .connect_timeout_ms
                .unwrap_or(u64::from(*SOCKET_CONNECTION_SETUP_TIMEOUT_MS)),
        );
        let request_timeout = properties
            .request_timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(*REQUEST_TIMEOUT)
            .max(connect_timeout);
        let client_id = properties
            .client_id
            .clone()
            .unwrap_or_else(|| format!("{CLIENT_ID_PREFIX}-{}", config.name));

        Ok(Self {
            bootstrap_servers: config.bootstrap_servers.join(","),
            client_id,
            request_timeout,
            connect_timeout,
            transport: TransportConfig::builder()
                .max_in_flight_requests(*MAX_IN_FLIGHT_REQUESTS)
                .max_response_size(*MAX_RESPONSE_BYTES)
                .tcp_nodelay(true)
                .build()?,
            auth: krafka_auth(config)?,
        })
    }

    pub(super) async fn connect(&self) -> Result<KrafkaSharedClient, KafkaError> {
        let mut builder = KrafkaSharedClient::builder(self.bootstrap_servers.clone())
            .client_id(self.client_id.clone())
            .request_timeout(self.request_timeout)
            .connect_timeout(self.connect_timeout)
            .transport(self.transport.clone());

        if let Some(auth) = &self.auth {
            builder = builder.auth(auth.clone());
        }

        Ok(builder.build().await?)
    }
}

pub(super) async fn connect(config: &ClusterConfig) -> Result<Transport, KafkaError> {
    let connector = Connector::new(config)?;
    let client = connector.connect().await?;
    let admin = KrafkaAdmin::builder()
        .with_client(&client)
        .request_timeout(connector.request_timeout)
        .connect_timeout(connector.connect_timeout)
        .build()
        .await?;

    Ok(Transport {
        client,
        admin,
        connector,
    })
}

fn krafka_auth(cluster: &ClusterConfig) -> Result<Option<AuthConfig>, KafkaError> {
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

    fn cluster(yaml: &str) -> ClusterConfig {
        serde_yaml_ng::from_str(yaml).unwrap()
    }

    #[tokio::test]
    async fn properties_configure_the_shared_transport() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        let mut cluster = cluster(
            "
            name: local
            bootstrap_servers:
              - localhost:9092
            properties:
              client_id: custom-client
              request_timeout_ms: 8000
              connect_timeout_ms: 30000
            ",
        );

        cluster.bootstrap_servers = vec![broker.bootstrap_servers()];
        // Building succeeds only if request_timeout is raised to the connect timeout.
        let transport = connect(&cluster).await.unwrap();
        assert!(
            broker
                .requests()
                .iter()
                .all(|request| request.client_id.as_deref() == Some("custom-client"))
        );
        assert_eq!(transport.admin.request_timeout(), Duration::from_secs(30));
        transport.admin.close().await;
        transport.client.pool().close_all().await;
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
