use std::num::NonZeroU16;

use crate::app::auth::access::{
    AddPartitionsCap, AlterTopicConfigsCap, CreateTopicsCap, DeleteTopicsCap,
};
use crate::kafka::KafkaError;
use crate::kafka::model::{ConfigEdit, ConfigSource, NewTopic};

use super::Granted;

impl Granted<'_, CreateTopicsCap> {
    pub(crate) async fn create_topic(&self, topic: &NewTopic) -> Result<(), KafkaError> {
        self.cluster.session.create_topic(topic).await?;
        tracing::info!(cluster = %self.cluster.store.name(), topic = %topic.name, "created topic");
        self.settle(&self.cluster.store.topology, |topology| {
            topology.topics.contains_key(topic.name.as_str())
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, AlterTopicConfigsCap> {
    pub(crate) async fn alter_topic_configs(
        &self,
        topic: &str,
        edit: &ConfigEdit,
    ) -> Result<(), KafkaError> {
        self.writable_topic(topic, |_| ())?;
        self.cluster
            .session
            .alter_topic_configs(topic, edit)
            .await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            topic,
            set = ?edit.set.keys().collect::<Vec<_>>(),
            reset = ?edit.reset,
            "altered topic configs"
        );
        self.settle(&self.cluster.store.configs, |table| {
            table
                .get(topic)
                .is_some_and(|entries| edit.shows_in(entries, ConfigSource::DynamicTopic))
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, AddPartitionsCap> {
    pub(crate) async fn add_partitions(
        &self,
        topic: &str,
        count: NonZeroU16,
    ) -> Result<(), KafkaError> {
        self.writable_topic(topic, |_| ())?;
        self.cluster.session.add_partitions(topic, count).await?;
        tracing::info!(cluster = %self.cluster.store.name(), topic, count, "added partitions");
        let count = usize::from(count.get());
        self.settle(&self.cluster.store.topology, |known| {
            known
                .topics
                .get(topic)
                .is_some_and(|info| info.partitions.len() >= count)
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, DeleteTopicsCap> {
    pub(crate) async fn delete_topic(&self, topic: &str) -> Result<(), KafkaError> {
        self.writable_topic(topic, |_| ())?;
        self.cluster.session.delete_topic(topic).await?;
        tracing::info!(cluster = %self.cluster.store.name(), topic, "deleted topic");
        self.settle(&self.cluster.store.topology, |known| {
            !known.topics.contains_key(topic)
        })
        .await;
        Ok(())
    }
}
