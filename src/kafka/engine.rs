//! Live Kafka facade.
//!
//! [`QueryEngine`] looks up a [`ClusterSession`], assembles a
//! [`ClusterSnapshot`] via `assemble_catalog`, and serves records, configs, a
//! single consumer group, and schema subjects.

use std::collections::HashMap;

use futures::future::join_all;
use indexmap::IndexMap;

use crate::config::Config;
use crate::environment::OFFSET_FETCH_BATCH;
use crate::kafka::adapter::ClusterHandle;
use crate::kafka::broker::Broker;
use crate::kafka::catalog::{CatalogAssemble, CatalogReuse, ClusterSnapshot};
use crate::kafka::cluster::{ClusterIdentity, ClusterOverview};
use crate::kafka::error::KafkaError;
use crate::kafka::group::{ConsumerGroup, GroupSnapshot};
use crate::kafka::limits::RecordLimits;
use crate::kafka::metadata::MetadataSnapshot;
use crate::kafka::record::RecordPage;
use crate::kafka::record::page::{fetch_one_page, fill_filtered_page};
use crate::kafka::record::plan::apply_timestamp_bounds;
use crate::kafka::record::query::RecordQuery;
use crate::kafka::registry::SchemaSubject;
use crate::kafka::session::ClusterSession;
use crate::kafka::topic::{Topic, groups_for_topic};
use crate::kafka::topic_config::ConfigEntry;
use crate::kafka::watermarks::Watermarks;

/// Assembles the catalog snapshot and serves live Kafka I/O from [`ClusterSession`]s.
pub struct QueryEngine<S: ?Sized> {
    registry: IndexMap<String, Box<S>>,
    limits: RecordLimits,
}

impl QueryEngine<dyn ClusterSession> {
    pub fn from_config(config: &Config) -> Result<Self, KafkaError> {
        Ok(Self::from_sessions(
            config
                .clusters
                .iter()
                .map(ClusterHandle::from_config)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }

    pub fn from_sessions(sessions: Vec<impl ClusterSession>) -> Self {
        let mut registry = IndexMap::with_capacity(sessions.len());
        for session in sessions {
            registry.insert(
                session.identity().name.clone(),
                Box::new(session) as Box<dyn ClusterSession>,
            );
        }

        Self {
            registry,
            limits: RecordLimits::from_env(),
        }
    }
}

impl<S: ClusterSession + ?Sized> QueryEngine<S> {
    pub fn names(&self) -> Vec<&str> {
        self.registry.keys().map(String::as_str).collect()
    }

    pub fn identities(&self) -> Vec<ClusterIdentity> {
        self.registry
            .values()
            .map(|session| session.identity().clone())
            .collect()
    }

    pub fn session(&self, name: &str) -> Result<&S, KafkaError> {
        self.registry
            .get(name)
            .map(Box::as_ref)
            .ok_or_else(|| KafkaError::UnknownCluster(name.to_owned()))
    }

    pub async fn broker_configs(
        &self,
        cluster: &str,
        id: i32,
    ) -> Result<Vec<ConfigEntry>, KafkaError> {
        let session = self.session(cluster)?;
        if session.metadata().await?.broker(id).is_none() {
            return Err(KafkaError::UnknownBroker {
                cluster: cluster.to_owned(),
                id,
            });
        }
        session.broker_configs(id).await
    }

    pub async fn catalog(&self, cluster: &str) -> Result<ClusterSnapshot, KafkaError> {
        Ok(self.assemble_catalog(cluster, None, true).await?.snapshot)
    }

    pub async fn assemble_catalog(
        &self,
        cluster: &str,
        reuse: Option<&CatalogReuse>,
        fetch_configs: bool,
    ) -> Result<CatalogAssemble, KafkaError> {
        let session = self.session(cluster)?;
        let meta = session.metadata().await?;
        let names = meta.topic_names();
        let mut groups = session.consumer_groups().await?;
        let metadata_hash = metadata_lane_hash(&meta, &groups);
        let watermark_names = catalog_watermark_names(&names, &groups);
        let watermark_partitions = meta.topic_partition_pairs(&watermark_names);

        let watermarks_fut = session.watermarks_many(&watermark_partitions);
        let hydrate = Self::hydrate_committed_offsets(session, &mut groups);
        let (configs, fetched_configs, watermarks) = if fetch_configs {
            let configs_fut = session.topics_configs(&names);
            let (configs, watermarks, ()) = tokio::join!(configs_fut, watermarks_fut, hydrate);
            match configs {
                Ok(configs) => (configs, true, watermarks),
                Err(_) => (
                    reuse.map(|lane| lane.configs.clone()).unwrap_or_default(),
                    false,
                    watermarks,
                ),
            }
        } else {
            let (watermarks, ()) = tokio::join!(watermarks_fut, hydrate);
            (
                reuse.map(|lane| lane.configs.clone()).unwrap_or_default(),
                false,
                watermarks,
            )
        };
        let ends = ends_from_watermarks(&watermarks);

        if let Some(previous) = reuse.and_then(|lane| {
            (lane.metadata_hash == metadata_hash)
                .then_some(lane.snapshot.as_ref())
                .flatten()
        }) {
            let topics = previous
                .topics
                .iter()
                .map(|topic| {
                    topic
                        .with_watermarks(watermarks.get(&topic.name).unwrap_or(&HashMap::new()))
                        .with_config(configs.get(&topic.name).map(Vec::as_slice))
                })
                .collect();
            let groups = groups
                .iter()
                .map(|group| ConsumerGroup::assemble(group, &ends))
                .collect();
            return Ok(CatalogAssemble {
                snapshot: ClusterSnapshot::assemble(
                    topics,
                    groups,
                    previous.brokers.clone(),
                    previous.overview.clone(),
                ),
                metadata_hash,
                configs,
                fetched_configs,
                reused_topology: true,
            });
        }

        let topics = meta
            .topics
            .iter()
            .map(|topic| {
                Topic::assemble(
                    topic,
                    watermarks.get(&topic.name).unwrap_or(&HashMap::new()),
                    configs.get(&topic.name).map(Vec::as_slice),
                    groups_for_topic(&topic.name, &groups),
                )
            })
            .collect();
        let group_count = groups.len() as i32;
        let groups = groups
            .iter()
            .map(|group| ConsumerGroup::assemble(group, &ends))
            .collect();
        let brokers = Broker::assemble_all(&meta);
        let overview = ClusterOverview::assemble(session.identity().clone(), &meta, group_count);

        Ok(CatalogAssemble {
            snapshot: ClusterSnapshot::assemble(topics, groups, brokers, overview),
            metadata_hash,
            configs,
            fetched_configs,
            reused_topology: false,
        })
    }

    pub async fn topic_configs(
        &self,
        cluster: &str,
        name: &str,
    ) -> Result<Vec<ConfigEntry>, KafkaError> {
        let session = self.session(cluster)?;
        if session.metadata().await?.topic(name).is_none() {
            return Err(KafkaError::UnknownTopic {
                cluster: cluster.to_owned(),
                topic: name.to_owned(),
            });
        }

        session
            .topics_configs(&[name])
            .await
            .map(|mut configs| configs.remove(name).unwrap_or_default())
    }

    async fn hydrate_committed_offsets(session: &S, groups: &mut [GroupSnapshot]) {
        for chunk in groups.chunks_mut(*OFFSET_FETCH_BATCH) {
            let fetches = chunk.iter().enumerate().filter_map(|(offset, group)| {
                let partitions = group.assigned_partitions();
                if partitions.is_empty() {
                    return None;
                }

                let group_id = group.id.clone();
                Some(async move {
                    let committed = session
                        .committed_offsets(&group_id, &partitions)
                        .await
                        .unwrap_or_default();
                    (offset, committed)
                })
            });

            for (offset, committed) in join_all(fetches).await {
                chunk[offset].committed = committed;
            }
        }
    }

    pub async fn consumer_group(
        &self,
        cluster: &str,
        id: &str,
    ) -> Result<ConsumerGroup, KafkaError> {
        let session = self.session(cluster)?;
        let mut snapshot = session.consumer_group(id).await?;
        Self::hydrate_committed_offsets(session, std::slice::from_mut(&mut snapshot)).await;
        let ends = Self::end_offsets(session, std::slice::from_ref(&snapshot)).await;
        Ok(ConsumerGroup::assemble(&snapshot, &ends))
    }

    async fn end_offsets(session: &S, groups: &[GroupSnapshot]) -> HashMap<(String, i32), i64> {
        ends_from_watermarks(&session.watermarks_many(&group_end_partitions(groups)).await)
    }

    pub async fn records(
        &self,
        cluster: &str,
        query: RecordQuery,
    ) -> Result<RecordPage, KafkaError> {
        let session = self.session(cluster)?;
        let partitions = self.resolve_partitions(cluster, session, &query).await?;

        let limit = self.limits.clamp_limit(query.limit)?;
        query.timestamps.validate()?;

        let watermarks = Self::window_watermarks(session, &query, &partitions).await?;

        if query.filter.is_some() {
            fill_filtered_page(
                session,
                &query,
                &partitions,
                &watermarks,
                limit,
                self.limits,
            )
            .await
        } else {
            fetch_one_page(
                session,
                &query,
                &partitions,
                &watermarks,
                limit,
                self.limits,
            )
            .await
        }
    }

    async fn resolve_partitions(
        &self,
        cluster: &str,
        session: &S,
        query: &RecordQuery,
    ) -> Result<Vec<i32>, KafkaError> {
        let meta = session.metadata().await?;
        let topic = meta
            .topic(&query.topic)
            .ok_or_else(|| KafkaError::UnknownTopic {
                cluster: cluster.to_owned(),
                topic: query.topic.clone(),
            })?;

        match query.partition {
            Some(id) if topic.partition(id).is_none() => Err(KafkaError::UnknownPartition {
                cluster: cluster.to_owned(),
                topic: query.topic.clone(),
                partition: id,
            }),
            Some(id) => Ok(vec![id]),
            None => Ok(topic.partition_ids()),
        }
    }

    async fn window_watermarks(
        session: &S,
        query: &RecordQuery,
        partitions: &[i32],
    ) -> Result<HashMap<i32, Watermarks>, KafkaError> {
        let pairs: Vec<(String, i32)> = partitions
            .iter()
            .map(|partition| (query.topic.clone(), *partition))
            .collect();
        let mut watermarks = session
            .watermarks_many(&pairs)
            .await
            .remove(&query.topic)
            .unwrap_or_default();

        let start = query.timestamps.start_seek();
        let end = query.timestamps.end_seek();
        if start.is_none() && end.is_none() {
            return Ok(watermarks);
        }

        let seek = |timestamp: Option<i64>| async move {
            match timestamp {
                Some(timestamp) => session
                    .offsets_for_times(&query.topic, partitions, timestamp)
                    .await
                    .map(Some),
                None => Ok(None),
            }
        };
        let (from_offsets, to_offsets) = tokio::try_join!(seek(start), seek(end))?;

        apply_timestamp_bounds(&mut watermarks, from_offsets.as_ref(), to_offsets.as_ref());
        Ok(watermarks)
    }

    pub async fn schema_subjects(&self, cluster: &str) -> Result<Vec<SchemaSubject>, KafkaError> {
        self.session(cluster)?.schema_subjects().await
    }
}

fn metadata_lane_hash(meta: &MetadataSnapshot, groups: &[GroupSnapshot]) -> u64 {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    meta.cluster_id.hash(&mut hasher);
    for broker in &meta.brokers {
        broker.id.hash(&mut hasher);
        broker.host.hash(&mut hasher);
        broker.port.hash(&mut hasher);
    }
    for topic in &meta.topics {
        topic.name.hash(&mut hasher);
        topic.internal.hash(&mut hasher);
        for partition in &topic.partitions {
            partition.id.hash(&mut hasher);
            partition.leader.hash(&mut hasher);
            partition.replicas.hash(&mut hasher);
            partition.isr.hash(&mut hasher);
        }
    }
    for group in groups {
        group.id.hash(&mut hasher);
        std::mem::discriminant(&group.state).hash(&mut hasher);
        group.protocol.hash(&mut hasher);
        group.coordinator.hash(&mut hasher);
        for member in &group.members {
            member.id.hash(&mut hasher);
            member.client_id.hash(&mut hasher);
            member.host.hash(&mut hasher);
            for assignment in &member.assignments {
                assignment.topic.hash(&mut hasher);
                assignment.partitions.hash(&mut hasher);
            }
        }
    }
    hasher.finish()
}

fn group_end_partitions(groups: &[GroupSnapshot]) -> Vec<(String, i32)> {
    let mut partitions = Vec::new();
    for group in groups {
        partitions.extend(group.assigned_partitions());
        partitions.extend(
            group
                .committed
                .iter()
                .map(|offset| (offset.topic.clone(), offset.partition)),
        );
    }
    partitions.sort();
    partitions.dedup();
    partitions
}

fn catalog_watermark_names(topic_names: &[&str], groups: &[GroupSnapshot]) -> Vec<String> {
    let mut names: Vec<String> = topic_names.iter().map(|name| (*name).to_owned()).collect();
    names.extend(GroupSnapshot::consumed_topic_names(groups));
    names.sort();
    names.dedup();
    names
}

fn ends_from_watermarks(
    watermarks: &HashMap<String, HashMap<i32, Watermarks>>,
) -> HashMap<(String, i32), i64> {
    let mut ends = HashMap::new();
    for (topic, marks) in watermarks {
        for (partition, Watermarks { high, .. }) in marks {
            ends.insert((topic.clone(), *partition), *high);
        }
    }
    ends
}

#[cfg(test)]
mod tests;
