use std::time::Duration;

use async_trait::async_trait;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;

use crate::kafka::config::{ClusterConfig, SaslConfig, SecurityConfig, TlsConfig};
use crate::kafka::error::KafkaError;
use crate::kafka::metadata::{BrokerInfo, ClusterInfo, MetadataApi};

#[derive(Debug)]
pub struct ClusterClient {
    config: ClusterConfig,
    admin: AdminClient<DefaultClientContext>,
}

impl ClusterClient {
    pub fn from_config(config: ClusterConfig) -> Result<Self, KafkaError> {
        config.validate()?;

        let mut client = ClientConfig::new();
        client.set("bootstrap.servers", config.bootstrap_servers.join(","));
        client.set("client.id", format!("klens-{}", config.name));

        if let Some(security) = &config.security {
            apply_security(&mut client, security);
        }

        for (key, value) in &config.properties {
            client.set(key, value);
        }

        let admin = client.create()?;

        Ok(Self { config, admin })
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }
}

#[async_trait]
impl MetadataApi for ClusterClient {
    async fn describe_cluster(&self, timeout: Duration) -> Result<ClusterInfo, KafkaError> {
        let metadata = self.admin.inner().fetch_metadata(None, timeout)?;

        let brokers = metadata
            .brokers()
            .iter()
            .map(|broker| BrokerInfo {
                id: broker.id(),
                host: broker.host().to_owned(),
                port: broker.port(),
            })
            .collect();

        Ok(ClusterInfo { brokers })
    }
}

fn apply_security(client: &mut ClientConfig, security: &SecurityConfig) {
    client.set("security.protocol", security.protocol.as_str());

    if let Some(sasl) = &security.sasl {
        apply_sasl(client, sasl);
    }

    if let Some(tls) = &security.tls {
        apply_tls(client, tls);
    }
}

fn apply_sasl(client: &mut ClientConfig, sasl: &SaslConfig) {
    client.set("sasl.mechanisms", sasl.mechanism.as_str());
    client.set("sasl.username", &sasl.username);
    client.set("sasl.password", &sasl.password);
}

fn apply_tls(client: &mut ClientConfig, tls: &TlsConfig) {
    if let Some(ca_cert) = &tls.ca_cert {
        client.set("ssl.ca.location", ca_cert.to_string_lossy().as_ref());
    }

    if let Some(client_cert) = &tls.client_cert {
        client.set(
            "ssl.certificate.location",
            client_cert.to_string_lossy().as_ref(),
        );
    }

    if let Some(client_key) = &tls.client_key {
        client.set("ssl.key.location", client_key.to_string_lossy().as_ref());
    }

    if tls.insecure_skip_verify {
        client.set("enable.ssl.certificate.verification", "false");
    }
}
