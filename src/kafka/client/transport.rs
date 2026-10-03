use std::time::Duration;

use krafka::admin::AdminClient as KrafkaAdmin;
use krafka::auth::{AuthConfig, TlsConfig as KrafkaTlsConfig};
use krafka::client::KrafkaClient as KrafkaSharedClient;
use krafka::network::TransportConfig;
use secrecy::ExposeSecret;

use crate::config::{self, KafkaTuning, Sasl, SaslMechanism, Tls};
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
        name: &str,
        cluster: &config::Cluster,
        tuning: &KafkaTuning,
    ) -> Result<Self, KafkaError> {
        Ok(Self {
            bootstrap_servers: cluster.bootstrap_servers.join(","),
            client_id: cluster
                .client_id
                .clone()
                .unwrap_or_else(|| format!("{CLIENT_ID_PREFIX}-{name}")),
            request_timeout: tuning.request_timeout.max(tuning.connect_timeout),
            connect_timeout: tuning.connect_timeout,
            transport: TransportConfig::builder()
                .max_in_flight_requests(tuning.max_in_flight_requests.get())
                .max_response_size(tuning.max_response_bytes())
                .tcp_nodelay(true)
                .build()?,
            auth: krafka_auth(cluster)?,
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
    name: &str,
    cluster: &config::Cluster,
    tuning: &KafkaTuning,
) -> Result<Transport, KafkaError> {
    let connector = Connector::new(name, cluster, tuning)?;
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

fn krafka_auth(cluster: &config::Cluster) -> Result<Option<AuthConfig>, KafkaError> {
    let tls = cluster.tls.as_ref().map(krafka_tls);
    Ok(match (&cluster.sasl, tls) {
        (None, None) => None,
        (None, Some(tls)) => Some(AuthConfig::ssl(tls)),
        (Some(sasl), tls) => Some(krafka_sasl(sasl, tls)?),
    })
}

fn krafka_sasl(sasl: &Sasl, tls: Option<KrafkaTlsConfig>) -> Result<AuthConfig, KafkaError> {
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

fn krafka_tls(tls: &Tls) -> KrafkaTlsConfig {
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
    use crate::kafka::client::testing::Broker;
    use crate::testing::yaml;

    fn cluster(source: &str) -> config::Cluster {
        yaml(source)
    }

    fn tuning(connect_timeout: u64, request_timeout: u64) -> KafkaTuning {
        KafkaTuning {
            connect_timeout: Duration::from_secs(connect_timeout),
            request_timeout: Duration::from_secs(request_timeout),
            ..KafkaTuning::default()
        }
    }

    #[tokio::test]
    async fn the_cluster_and_tuning_configure_the_shared_transport() {
        let broker = Broker::start().await;
        let cluster = cluster(&format!(
            "{{bootstrap_servers: ['{}'], client_id: custom-client}}",
            broker.bootstrap_servers()
        ));

        // Building succeeds only if request_timeout is raised to the connect timeout.
        let transport = connect("local", &cluster, &tuning(30, 8)).await.unwrap();
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
    fn the_request_timeout_is_raised_to_the_connect_timeout() {
        let timeouts = |tuning: KafkaTuning| {
            let connector = Connector::new(
                "local",
                &cluster("bootstrap_servers: [kafka:9092]"),
                &tuning,
            )
            .unwrap();
            (connector.connect_timeout, connector.request_timeout)
        };

        assert_eq!(
            timeouts(tuning(5, 20)),
            (Duration::from_secs(5), Duration::from_secs(20))
        );
        assert_eq!(
            timeouts(tuning(30, 10)),
            (Duration::from_secs(30), Duration::from_secs(30))
        );
    }

    #[test]
    fn the_default_client_id_uses_the_cluster_name() {
        let connector = Connector::new(
            "local",
            &cluster("bootstrap_servers: [a:9092, b:9092]"),
            &KafkaTuning::default(),
        )
        .unwrap();

        assert_eq!(connector.client_id, "klens-local");
        assert_eq!(connector.bootstrap_servers, "a:9092,b:9092");
    }

    #[test]
    fn a_cluster_without_tls_or_sasl_connects_in_plaintext() {
        let auth = krafka_auth(&cluster("bootstrap_servers: [kafka:9092]")).unwrap();

        assert!(auth.is_none());
    }

    #[test]
    fn a_tls_block_alone_connects_over_ssl() {
        let auth = krafka_auth(&cluster(
            "{bootstrap_servers: [kafka:9093], tls: {ca_cert: /etc/ca.pem}}",
        ))
        .unwrap()
        .expect("ssl auth");

        assert_eq!(auth.sasl_mechanism(), None);
        let tls = auth.tls_config().expect("tls config");
        assert_eq!(tls.ca_cert_path(), Some("/etc/ca.pem"));
        assert!(tls.verify_server_cert());
    }

    #[test]
    fn a_sasl_block_alone_authenticates_in_plaintext() {
        let auth = krafka_auth(&cluster(
            "
            bootstrap_servers: [kafka:9092]
            sasl: {mechanism: PLAIN, username: admin, password: {value: secret}}
            ",
        ))
        .unwrap()
        .expect("sasl auth");

        assert_eq!(
            auth.sasl_mechanism(),
            Some(&krafka::auth::SaslMechanism::Plain)
        );
        assert!(auth.tls_config().is_none());
    }

    #[test]
    fn sasl_and_tls_together_authenticate_over_ssl() {
        let auth = krafka_auth(&cluster(
            "
            bootstrap_servers: [broker:9092]
            tls:
              ca_cert: /etc/ca.pem
              client: {cert: /etc/client.pem, key: /etc/client.key}
              insecure_skip_verify: true
            sasl:
              mechanism: SCRAM-SHA-512
              username: admin
              password: {value: secret}
            ",
        ))
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
}
