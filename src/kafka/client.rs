use std::time::Duration;

use async_trait::async_trait;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;

use crate::config::ClusterConfig;
use crate::kafka::config::KafkaClusterConfig;
use crate::kafka::error::KafkaError;
use crate::kafka::metadata::{BrokerInfo, ClusterInfo, MetadataApi};

#[derive(Debug)]
pub struct ClusterClient {
    config: ClusterConfig,
    admin: AdminClient<DefaultClientContext>,
}

impl ClusterClient {
    pub fn from_config(config: ClusterConfig) -> Result<Self, KafkaError> {
        let kafka = KafkaClusterConfig::try_from(&config)?;
        let admin = kafka.into_client_config().create()?;

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
