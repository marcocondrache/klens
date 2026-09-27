use std::time::Duration;

use krafka::admin::AdminClient as KrafkaAdmin;
use krafka::auth::{AuthConfig, TlsConfig as KrafkaTlsConfig};
use krafka::client::KrafkaClient as KrafkaSharedClient;
use krafka::network::TransportConfig;
use secrecy::ExposeSecret;

use crate::config::{
    ClusterConfig, ClusterName, KafkaTuning, SaslConfig, SaslMechanism, SecurityConfig, TlsConfig,
};
use crate::kafka::error::KafkaError;

const CLIENT_ID_PREFIX: &str = "klens";

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
    fn new(
        name: &ClusterName,
        config: &ClusterConfig,
        tuning: &KafkaTuning,
    ) -> Result<Self, KafkaError> {
        let properties = &config.properties;
        let connect_timeout = properties
            .connect_timeout
            .unwrap_or(tuning.connect_timeout)
            .get();
        let request_timeout = properties
            .request_timeout
            .unwrap_or(tuning.request_timeout)
            .get()
            .max(connect_timeout);
        let client_id = properties
            .client_id
            .clone()
            .unwrap_or_else(|| format!("{CLIENT_ID_PREFIX}-{name}"));

        Ok(Self {
            bootstrap_servers: config.bootstrap_servers.join(","),
            client_id,
            request_timeout,
            connect_timeout,
            transport: TransportConfig::builder()
                .max_in_flight_requests(tuning.max_in_flight_requests.get())
                .max_response_size(tuning.max_response_bytes())
                .tcp_nodelay(true)
                .build()?,
            auth: krafka_auth(&config.security)?,
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

pub(super) async fn connect(
    name: &ClusterName,
    config: &ClusterConfig,
    tuning: &KafkaTuning,
) -> Result<Transport, KafkaError> {
    let connector = Connector::new(name, config, tuning)?;
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

fn krafka_auth(security: &SecurityConfig) -> Result<Option<AuthConfig>, KafkaError> {
    Ok(match security {
        SecurityConfig::Plaintext {} => None,
        SecurityConfig::Ssl { tls } => Some(AuthConfig::ssl(krafka_tls(tls))),
        SecurityConfig::SaslPlaintext { sasl } => Some(krafka_sasl(sasl, None)?),
        SecurityConfig::SaslSsl { sasl, tls } => Some(krafka_sasl(sasl, Some(krafka_tls(tls)))?),
    })
}

fn krafka_sasl(sasl: &SaslConfig, tls: Option<KrafkaTlsConfig>) -> Result<AuthConfig, KafkaError> {
    Ok(match (sasl.mechanism, tls) {
        (SaslMechanism::Plain, None) => {
            AuthConfig::sasl_plain(&sasl.username, sasl.password.expose_secret())?
        }
        (SaslMechanism::Plain, Some(tls)) => {
            AuthConfig::sasl_plain_ssl(&sasl.username, sasl.password.expose_secret(), tls)?
        }
        (SaslMechanism::ScramSha256, None) => {
            AuthConfig::sasl_scram_sha256(&sasl.username, sasl.password.expose_secret())
        }
        (SaslMechanism::ScramSha256, Some(tls)) => {
            AuthConfig::sasl_scram_sha256_ssl(&sasl.username, sasl.password.expose_secret(), tls)
        }
        (SaslMechanism::ScramSha512, None) => {
            AuthConfig::sasl_scram_sha512(&sasl.username, sasl.password.expose_secret())
        }
        (SaslMechanism::ScramSha512, Some(tls)) => {
            AuthConfig::sasl_scram_sha512_ssl(&sasl.username, sasl.password.expose_secret(), tls)
        }
    })
}

fn krafka_tls(tls: &TlsConfig) -> KrafkaTlsConfig {
    let mut krafka_tls = if tls.insecure_skip_verify {
        KrafkaTlsConfig::insecure()
    } else {
        KrafkaTlsConfig::new()
    };

    if let Some(ca_cert) = &tls.ca_cert {
        krafka_tls = krafka_tls.with_ca_cert(ca_cert.to_string_lossy());
    }

    if let Some(client) = &tls.client {
        krafka_tls = krafka_tls
            .with_client_cert(client.cert.to_string_lossy(), client.key.to_string_lossy());
    }

    krafka_tls
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Period;

    fn cluster(yaml: &str) -> ClusterConfig {
        serde_saphyr::from_str(yaml).unwrap()
    }

    #[tokio::test]
    async fn properties_configure_the_shared_transport() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        let mut cluster = cluster(
            "
            bootstrap_servers:
              - localhost:9092
            properties:
              client_id: custom-client
              request_timeout: 8s
              connect_timeout: 30s
            ",
        );

        cluster.bootstrap_servers = vec![broker.bootstrap_servers()].try_into().unwrap();
        // Building succeeds only if request_timeout is raised to the connect timeout.
        let transport = connect(&"local".parse().unwrap(), &cluster, &KafkaTuning::default())
            .await
            .unwrap();
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

    fn timeouts(properties: &str, tuning: KafkaTuning) -> (Duration, Duration) {
        let connector = Connector::new(
            &"local".parse().unwrap(),
            &cluster(&format!(
                "
                bootstrap_servers: [localhost:9092]
                properties: {{{properties}}}
                "
            )),
            &tuning,
        )
        .unwrap();
        (connector.connect_timeout, connector.request_timeout)
    }

    #[test]
    fn cluster_timeouts_override_the_tuning_defaults() {
        let tuning = KafkaTuning {
            connect_timeout: Period::from_secs(5),
            request_timeout: Period::from_secs(20),
            ..KafkaTuning::default()
        };

        assert_eq!(
            timeouts("", tuning),
            (Duration::from_secs(5), Duration::from_secs(20))
        );
        assert_eq!(
            timeouts("request_timeout: 8s, connect_timeout: 2s", tuning),
            (Duration::from_secs(2), Duration::from_secs(8))
        );
    }

    #[test]
    fn the_request_timeout_is_raised_to_the_connect_timeout() {
        let tuning = KafkaTuning {
            connect_timeout: Period::from_secs(30),
            request_timeout: Period::from_secs(10),
            ..KafkaTuning::default()
        };

        assert_eq!(
            timeouts("", tuning),
            (Duration::from_secs(30), Duration::from_secs(30))
        );
        assert_eq!(
            timeouts("request_timeout: 8s", tuning),
            (Duration::from_secs(30), Duration::from_secs(30))
        );
        assert_eq!(
            timeouts("request_timeout: 8s, connect_timeout: 250ms", tuning),
            (Duration::from_millis(250), Duration::from_secs(8))
        );
    }

    #[test]
    fn the_default_client_id_uses_the_cluster_name() {
        let cluster = cluster(
            "
            bootstrap_servers:
              - localhost:9092
            ",
        );

        let connector =
            Connector::new(&"local".parse().unwrap(), &cluster, &KafkaTuning::default()).unwrap();
        assert_eq!(connector.client_id, "klens-local");
        assert_eq!(
            "  local  ".parse::<ClusterName>(),
            Err("cluster name '  local  ' must not start or end with whitespace".to_owned())
        );
    }

    #[test]
    fn krafka_auth_is_none_for_plaintext() {
        let cluster = cluster(
            "
            bootstrap_servers:
              - localhost:9092
            ",
        );

        assert!(krafka_auth(&cluster.security).unwrap().is_none());
    }

    #[test]
    fn krafka_auth_builds_scram_ssl_settings() {
        let cluster = cluster(
            "
            bootstrap_servers:
              - broker:9092
            security:
              protocol: SASL_SSL
              sasl:
                mechanism: SCRAM-SHA-512
                username: admin
                password: {value: secret}
              tls:
                ca_cert: /etc/ca.pem
                client:
                  cert: /etc/client.pem
                  key: /etc/client.key
                insecure_skip_verify: true
            ",
        );

        let auth = krafka_auth(&cluster.security)
            .unwrap()
            .expect("sasl ssl auth");

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
            bootstrap_servers:
              - broker:9092
            security:
              protocol: SSL
              tls:
                ca_cert: /etc/ca.pem
            ",
        );

        let auth = krafka_auth(&cluster.security).unwrap().expect("ssl auth");
        assert_eq!(auth.sasl_mechanism(), None);
        assert_eq!(
            auth.tls_config().and_then(|tls| tls.ca_cert_path()),
            Some("/etc/ca.pem")
        );
    }
}
