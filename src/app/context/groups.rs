use crate::app::auth::access::{DeleteGroupsCap, DeleteOffsetsCap, ResetOffsetsCap};
use crate::kafka::KafkaError;
use crate::kafka::model::{CommittedOffset, OffsetMove, OffsetReset};
use crate::kafka::store::TopicInfo;

use super::Granted;

impl Granted<'_, ResetOffsetsCap> {
    pub(crate) async fn plan_reset(
        &self,
        reset: &OffsetReset,
    ) -> Result<Vec<OffsetMove>, KafkaError> {
        self.known_group(&reset.group, |_| ())?;
        self.plan(reset).await
    }

    pub(crate) async fn reset_offsets(
        &self,
        reset: &OffsetReset,
    ) -> Result<Vec<OffsetMove>, KafkaError> {
        self.stopped_group(&reset.group)?;
        let moves = self.plan(reset).await?;
        if moves.is_empty() {
            return Ok(moves);
        }
        let committed: Vec<CommittedOffset> = moves.iter().map(OffsetMove::committed).collect();
        self.cluster
            .session
            .alter_group_offsets(&reset.group, &committed)
            .await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            group = reset.group.as_str(),
            topic = ?reset.topic,
            partitions = committed.len(),
            to = ?reset.to,
            "reset group offsets"
        );
        let _watching = self.cluster.store.interest.lease_group(&reset.group);
        self.settle(&self.cluster.store.offsets, |table| {
            table
                .get(&reset.group)
                .is_some_and(|offsets| offsets.shows(&committed))
        })
        .await;
        Ok(moves)
    }

    async fn plan(&self, reset: &OffsetReset) -> Result<Vec<OffsetMove>, KafkaError> {
        let Some(topic) = &reset.topic else {
            return self.cluster.plan_reset(&reset.group, None, reset.to).await;
        };
        let known = self.writable_topic(topic, TopicInfo::partition_ids)?;
        if let Some(&partition) = reset
            .partitions
            .iter()
            .find(|partition| !known.contains(partition))
        {
            return Err(self.unknown_partition(topic, partition));
        }
        let partitions = if reset.partitions.is_empty() {
            &known
        } else {
            &reset.partitions
        };
        self.cluster
            .plan_reset(&reset.group, Some((topic, partitions)), reset.to)
            .await
    }
}

impl Granted<'_, DeleteGroupsCap> {
    pub(crate) async fn delete_group(&self, group: &str) -> Result<(), KafkaError> {
        self.stopped_group(group)?;
        self.cluster.session.delete_group(group).await?;
        tracing::info!(cluster = %self.cluster.store.name(), group, "deleted group");
        self.settle(&self.cluster.store.topology, |known| {
            !known.groups.contains_key(group)
        })
        .await;
        Ok(())
    }
}

impl Granted<'_, DeleteOffsetsCap> {
    pub(crate) async fn delete_offsets(&self, group: &str, topic: &str) -> Result<(), KafkaError> {
        if self.known_group(group, |info| info.consumes(topic))? {
            return Err(KafkaError::ConsumedTopic {
                group: group.to_owned(),
                topic: topic.to_owned(),
            });
        }
        let partitions: Vec<i32> = self
            .cluster
            .session
            .committed_offsets(group, None)
            .await?
            .into_iter()
            .filter(|offset| offset.topic == topic)
            .map(|offset| offset.partition)
            .collect();
        if partitions.is_empty() {
            return Ok(());
        }
        self.cluster
            .session
            .delete_group_offsets(group, topic, &partitions)
            .await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            group,
            topic,
            partitions = partitions.len(),
            "deleted group offsets"
        );
        let _watching = self.cluster.store.interest.lease_group(group);
        self.settle(&self.cluster.store.offsets, |table| {
            table.get(group).is_none_or(|offsets| {
                offsets
                    .partitions()
                    .all(|(committed, _)| committed != topic)
            })
        })
        .await;
        Ok(())
    }
}
