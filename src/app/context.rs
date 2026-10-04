use std::num::NonZeroU16;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::AppState;
use crate::app::auth::SessionGuard;
use crate::app::auth::access::{
    AccessError, ClusterAccess, ConfigsCap, EffectiveAccess, ManageGroupsCap, ManageTopicsCap,
    ProduceCap, RecordsCap, SchemaTextCap,
};
use crate::kafka::model::{
    CommittedOffset, ConfigEdit, FoundRecord, NewRecord, NewTopic, OffsetMove, OffsetReset,
    ProducedRecord, RecordAt, RecordDeletion, RegisteredSchema,
};
use crate::kafka::store::{ClusterStore, GroupInfo, Lane, TopicInfo};
use crate::kafka::{
    Cluster, ConfigEntry, Export, KafkaError, RecordPage, RecordQuery, Tail, TailLimits, TailQuery,
};

use super::error::ApiError;

const SETTLE: Duration = Duration::from_secs(10);

pub(crate) struct Session {
    pub state: AppState,
    pub access: EffectiveAccess,
    pub guard: SessionGuard,
}

pub(crate) struct ClusterHandle<'a> {
    pub access: ClusterAccess<'a>,
    pub store: &'a Arc<ClusterStore>,
    cluster: &'a Cluster,
    limits: TailLimits,
}

impl<'a> ClusterHandle<'a> {
    pub(crate) fn name(&self) -> &str {
        self.access.cluster()
    }

    pub(crate) fn records(&self) -> Result<Granted<'a, RecordsCap>, AccessError> {
        self.access.records().map(|cap| self.grant(cap))
    }

    pub(crate) fn configs(&self) -> Result<Granted<'a, ConfigsCap>, AccessError> {
        self.access.configs().map(|cap| self.grant(cap))
    }

    pub(crate) fn schema_text(&self) -> Result<Granted<'a, SchemaTextCap>, AccessError> {
        self.access.schema_text().map(|cap| self.grant(cap))
    }

    pub(crate) fn manage_topics(&self) -> Result<Granted<'a, ManageTopicsCap>, AccessError> {
        self.writable()?;
        self.access.manage_topics().map(|cap| self.grant(cap))
    }

    pub(crate) fn produce(&self) -> Result<Granted<'a, ProduceCap>, AccessError> {
        self.writable()?;
        self.access.produce().map(|cap| self.grant(cap))
    }

    pub(crate) fn manage_groups(&self) -> Result<Granted<'a, ManageGroupsCap>, AccessError> {
        self.writable()?;
        self.access.manage_groups().map(|cap| self.grant(cap))
    }

    fn writable(&self) -> Result<(), AccessError> {
        if self.cluster.writable {
            Ok(())
        } else {
            Err(AccessError::ReadOnlyCluster(self.name().to_owned()))
        }
    }

    fn grant<Cap>(&self, cap: Cap) -> Granted<'a, Cap> {
        Granted {
            _cap: cap,
            cluster: self.cluster,
            limits: self.limits,
        }
    }
}

/// A privilege checked on one cluster. The live calls it guards hang off
/// it, so none of them can run without the check.
pub(crate) struct Granted<'a, Cap> {
    _cap: Cap,
    cluster: &'a Cluster,
    limits: TailLimits,
}

impl Granted<'_, RecordsCap> {
    pub(crate) async fn read(&self, query: RecordQuery) -> Result<RecordPage, KafkaError> {
        self.cluster.records(query, self.limits.records).await
    }

    pub(crate) async fn record(&self, at: RecordAt) -> Result<FoundRecord, KafkaError> {
        self.cluster.record(at, self.limits.records).await
    }

    pub(crate) async fn export(&self, query: RecordQuery) -> Result<Export, KafkaError> {
        self.cluster.export(query, self.limits.records).await
    }

    pub(crate) async fn tail(&self, query: TailQuery) -> Result<Tail, KafkaError> {
        self.cluster.tail(query, self.limits).await
    }
}

impl Granted<'_, ConfigsCap> {
    pub(crate) async fn broker_configs(&self, id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
        self.cluster.broker_configs(id).await
    }
}

impl Granted<'_, SchemaTextCap> {
    pub(crate) async fn subject_schema(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        self.cluster.session.subject_schema(subject, version).await
    }
}

impl Granted<'_, ManageTopicsCap> {
    pub(crate) async fn create_topic(&self, topic: &NewTopic) -> Result<(), KafkaError> {
        self.cluster.session.create_topic(topic).await?;
        tracing::info!(cluster = %self.cluster.store.name(), topic = %topic.name, "created topic");
        self.settle(&self.cluster.store.topology, |topology| {
            topology.topics.contains_key(topic.name.as_str())
        })
        .await;
        Ok(())
    }

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
                .is_some_and(|entries| edit.shows_in(entries))
        })
        .await;
        Ok(())
    }

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

    /// Without `partitions` the deletion covers every partition, and without
    /// `before` it deletes every record.
    pub(crate) async fn delete_records(
        &self,
        topic: &str,
        partitions: &[i32],
        before: Option<i64>,
    ) -> Result<(), KafkaError> {
        let known = self.writable_topic(topic, TopicInfo::partition_ids)?;
        if let Some(&partition) = partitions
            .iter()
            .find(|partition| !known.contains(partition))
        {
            return Err(self.unknown_partition(topic, partition));
        }
        let deletion = RecordDeletion {
            topic: topic.to_owned(),
            before: if partitions.is_empty() {
                &known
            } else {
                partitions
            }
            .iter()
            .map(|&partition| (partition, before))
            .collect(),
        };
        let lows = self.cluster.session.delete_records(&deletion).await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            topic,
            partitions = ?deletion.before.keys().collect::<Vec<_>>(),
            before,
            "deleted records"
        );
        self.settle(&self.cluster.store.watermarks, |marks| {
            lows.iter().all(|(&partition, &low)| {
                marks
                    .get(topic, partition)
                    .is_some_and(|mark| mark.low >= low)
            })
        })
        .await;
        Ok(())
    }

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

impl Granted<'_, ProduceCap> {
    pub(crate) async fn produce(&self, record: &NewRecord) -> Result<ProducedRecord, KafkaError> {
        let topic = record.topic.as_str();
        let partitions = self.writable_topic(topic, TopicInfo::partition_ids)?;
        if let Some(partition) = record.partition
            && !partitions.contains(&partition)
        {
            return Err(self.unknown_partition(topic, partition));
        }
        let produced = self.cluster.session.produce(record).await?;
        tracing::info!(
            cluster = %self.cluster.store.name(),
            topic,
            partition = produced.partition,
            offset = produced.offset,
            "produced record"
        );
        Ok(produced)
    }
}

impl Granted<'_, ManageGroupsCap> {
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
            group = %reset.group,
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

    pub(crate) async fn delete_group(&self, group: &str) -> Result<(), KafkaError> {
        self.stopped_group(group)?;
        self.cluster.session.delete_group(group).await?;
        tracing::info!(cluster = %self.cluster.store.name(), group = %group, "deleted group");
        self.settle(&self.cluster.store.topology, |known| {
            !known.groups.contains_key(group)
        })
        .await;
        Ok(())
    }

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
            group = %group,
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

    fn stopped_group(&self, group: &str) -> Result<(), KafkaError> {
        if self.known_group(group, |info| info.state.has_members())? {
            return Err(KafkaError::ActiveGroup {
                group: group.to_owned(),
            });
        }
        Ok(())
    }

    fn known_group<T>(
        &self,
        group: &str,
        read: impl FnOnce(&GroupInfo) -> T,
    ) -> Result<T, KafkaError> {
        self.cluster
            .store
            .topology
            .load()
            .as_ref()
            .and_then(|known| known.group(group))
            .map(read)
            .ok_or_else(|| KafkaError::UnknownGroup {
                cluster: self.cluster.store.name().to_owned(),
                group: group.to_owned(),
            })
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

impl<Cap> Granted<'_, Cap> {
    fn unknown_partition(&self, topic: &str, partition: i32) -> KafkaError {
        KafkaError::UnknownPartition {
            cluster: self.cluster.store.name().to_owned(),
            topic: topic.to_owned(),
            partition,
        }
    }

    fn writable_topic<T>(
        &self,
        topic: &str,
        read: impl FnOnce(&TopicInfo) -> T,
    ) -> Result<T, KafkaError> {
        let topology = self.cluster.store.topology.load();
        match topology.as_ref().and_then(|known| known.topics.get(topic)) {
            None => Err(KafkaError::UnknownTopic {
                cluster: self.cluster.store.name().to_owned(),
                topic: topic.to_owned(),
            }),
            Some(info) if info.internal => Err(KafkaError::InternalTopic(topic.to_owned())),
            Some(info) => Ok(read(info)),
        }
    }

    async fn settle<T>(&self, lane: &Lane<T>, done: impl Fn(&T) -> bool) {
        if tokio::time::timeout(SETTLE, lane.refresh_until(done))
            .await
            .is_err()
        {
            tracing::debug!(
                cluster = %self.cluster.store.name(),
                "the store did not show a change in time"
            );
        }
    }
}

impl Session {
    pub(crate) fn cluster<'a>(&'a self, name: &'a str) -> Result<ClusterHandle<'a>, ApiError> {
        let access = self.access.cluster(name)?;
        let cluster = self.state.clusters.get(access.cluster())?;
        Ok(ClusterHandle {
            access,
            store: &cluster.store,
            cluster,
            limits: self.state.limits,
        })
    }
}

impl FromRequestParts<AppState> for Session {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let Some(access) = parts.extensions.remove::<EffectiveAccess>() else {
            return Err(ApiError::Unauthorized);
        };
        let Some(guard) = parts.extensions.remove::<SessionGuard>() else {
            return Err(ApiError::Unauthorized);
        };
        Ok(Self {
            state: state.clone(),
            access,
            guard,
        })
    }
}
