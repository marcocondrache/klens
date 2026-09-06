use std::sync::atomic::{AtomicU64, Ordering};

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, StreamConsumer};

use crate::config::ClusterConfig;
use crate::kafka::config::KafkaClusterConfig;
use crate::kafka::error::KafkaError;

const BROWSE_GROUP_PREFIX: &str = "klens.internal.browse";

/// Builds typed rdkafka clients from a cluster's shared settings.
#[derive(Clone)]
pub struct ClientFactory {
    cluster: String,
    client: ClientConfig,
}

impl std::fmt::Debug for ClientFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientFactory")
            .field("cluster", &self.cluster)
            .finish_non_exhaustive()
    }
}

impl ClientFactory {
    pub fn new(config: &ClusterConfig) -> Result<Self, KafkaError> {
        Ok(Self {
            cluster: config.name.trim().to_owned(),
            client: KafkaClusterConfig::try_from(config)?.into_client_config(),
        })
    }

    pub fn admin(&self) -> Result<AdminClient<DefaultClientContext>, KafkaError> {
        Ok(self.client.create()?)
    }

    pub fn offset_consumer(&self, group_id: &str) -> Result<BaseConsumer, KafkaError> {
        Ok(self.consumer_config(group_id, "offsets", false).create()?)
    }

    pub fn browser(&self) -> Result<StreamConsumer, KafkaError> {
        Ok(self
            .consumer_config(&browse_group_id(&self.cluster), "browse", true)
            .create()?)
    }

    fn consumer_config(&self, group_id: &str, role: &str, partition_eof: bool) -> ClientConfig {
        let mut client = self.client.clone();
        client.set("client.id", format!("klens-{}-{role}", self.cluster));
        client.set("group.id", group_id);
        client.set("enable.auto.commit", "false");
        client.set("enable.auto.offset.store", "false");
        client.set("allow.auto.create.topics", "false");
        client.set("auto.offset.reset", "error");
        client.set(
            "enable.partition.eof",
            if partition_eof { "true" } else { "false" },
        );
        client
    }
}

fn browse_group_id(cluster: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    format!(
        "{BROWSE_GROUP_PREFIX}.{cluster}.{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browse_group_ids_are_internal_and_unique() {
        let first = browse_group_id("local");
        let second = browse_group_id("local");

        assert!(first.starts_with("klens.internal.browse.local."));
        assert_ne!(first, second);
    }
}
