use std::time::Duration;

use krafka::Kafka;
use krafka::admin::AdminClient as KrafkaAdmin;
use krafka::auth::{AuthConfig, TlsConfig as KrafkaTlsConfig};
use krafka::consumer::Consumer;
use krafka::producer::Producer;
use secrecy::ExposeSecret;

use crate::config::{self, KafkaTuning, Sasl, SaslMechanism, Tls};
use crate::kafka::error::KafkaError;

const CLIENT_ID_PREFIX: &str = "klens";

pub(super) struct Transport {
    pub(super) kafka: Kafka,
    pub(super) admin: KrafkaAdmin,
    /// krafka shows the metadata cache its clients share only through a
    /// consumer. This one is never assigned a partition, so it fetches nothing.
    pub(super) metadata: Consumer,
    pub(super) connector: Connector,
}

#[derive(Clone)]
pub(super) struct Connector {
    bootstrap_servers: String,
    client_id: String,
    request_timeout: Duration,
    connect_timeout: Duration,
    max_in_flight_requests: usize,
    max_response_size: usize,
    auth: Option<AuthConfig>,
}

impl Connector {
    fn new(name: &str, cluster: &config::Cluster, tuning: &KafkaTuning) -> Self {
        Self {
            bootstrap_servers: cluster.bootstrap_servers.join(","),
            client_id: cluster
                .client_id
                .clone()
                .unwrap_or_else(|| format!("{CLIENT_ID_PREFIX}-{name}")),
            request_timeout: tuning.request_timeout.max(tuning.connect_timeout),
            connect_timeout: tuning.connect_timeout,
            max_in_flight_requests: tuning.max_in_flight_requests.get(),
            max_response_size: tuning.max_response_bytes(),
            auth: krafka_auth(cluster),
        }
    }

    pub(super) async fn connect(&self) -> Result<Kafka, KafkaError> {
        let mut builder = Kafka::builder(self.bootstrap_servers.clone())
            .client_id(self.client_id.clone())
            .request_timeout(self.request_timeout)
            .connect_timeout(self.connect_timeout)
            .max_in_flight_requests(self.max_in_flight_requests)
            .max_response_size(self.max_response_size)
            .tcp_nodelay(true);

        if let Some(auth) = &self.auth {
            builder = builder.security(auth.clone());
        }

        Ok(builder.connect().await?)
    }
}

impl Transport {
    pub(super) async fn connect(
        name: &str,
        cluster: &config::Cluster,
        tuning: &KafkaTuning,
    ) -> Result<Self, KafkaError> {
        let connector = Connector::new(name, cluster, tuning);
        let kafka = connector.connect().await?;
        let admin = kafka
            .admin()
            .default_api_timeout(connector.request_timeout)
            .metrics_push(false);
        let metadata = kafka
            .consumer_without_group()
            .metrics_push(false)
            .build()
            .await?;

        Ok(Self {
            kafka,
            admin,
            metadata,
            connector,
        })
    }

    pub(super) async fn producer(&self) -> Result<Producer, KafkaError> {
        let request_timeout = self.connector.request_timeout;
        // krafka retries a retriable error until the delivery timeout, and
        // only idempotence keeps a resend from storing the record twice.
        Ok(self
            .kafka
            .producer()
            .linger(Duration::ZERO)
            .max_block(request_timeout)
            .delivery_timeout(request_timeout)
            .idempotent(true)
            .metrics_push(false)
            .build()
            .await?)
    }
}

fn krafka_auth(cluster: &config::Cluster) -> Option<AuthConfig> {
    let tls = cluster.tls.as_ref().map(krafka_tls);
    match (&cluster.sasl, tls) {
        (None, None) => None,
        (None, Some(tls)) => Some(AuthConfig::ssl(tls)),
        (Some(sasl), None) => Some(krafka_sasl(sasl)),
        (Some(sasl), Some(tls)) => Some(krafka_sasl(sasl).with_tls(tls)),
    }
}

fn krafka_sasl(sasl: &Sasl) -> AuthConfig {
    let password = sasl.password.expose_secret();
    match sasl.mechanism {
        SaslMechanism::Plain => AuthConfig::sasl_plain(&sasl.username, password),
        SaslMechanism::ScramSha256 => AuthConfig::sasl_scram_sha256(&sasl.username, password),
        SaslMechanism::ScramSha512 => AuthConfig::sasl_scram_sha512(&sasl.username, password),
    }
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
        Transport::connect("local", &cluster, &tuning(30, 8))
            .await
            .unwrap();
        assert!(
            broker
                .requests()
                .iter()
                .all(|request| request.client_id.as_deref() == Some("custom-client"))
        );
    }

    #[test]
    fn the_request_timeout_is_raised_to_the_connect_timeout() {
        let timeouts = |tuning: KafkaTuning| {
            let connector = Connector::new(
                "local",
                &cluster("bootstrap_servers: [kafka:9092]"),
                &tuning,
            );
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
        );

        assert_eq!(connector.client_id, "klens-local");
        assert_eq!(connector.bootstrap_servers, "a:9092,b:9092");
    }

    #[test]
    fn a_cluster_without_tls_or_sasl_connects_in_plaintext() {
        let auth = krafka_auth(&cluster("bootstrap_servers: [kafka:9092]"));

        assert!(auth.is_none());
    }

    #[test]
    fn a_tls_block_alone_connects_over_ssl() {
        let auth = krafka_auth(&cluster(
            "{bootstrap_servers: [kafka:9093], tls: {ca_cert: /etc/ca.pem}}",
        ))
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
