use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use moka::future::Cache;
use rdkafka::admin::{
    AdminClient, AdminOptions, ConfigSource as RdConfigSource, OwnedResourceSpecifier,
    ResourceSpecifier,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::Consumer;
use rdkafka::metadata::Metadata;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use crate::config::ClusterConfig;
use crate::environment::{
    ADMIN_TIMEOUT, BLOCKING_SLACK, CONFIG_BATCH, CONSUME_TIMEOUT, INTERNAL_GROUP_PREFIX,
    METADATA_TIMEOUT, METADATA_TTL, WATERMARK_BATCH, WATERMARK_TIMEOUT,
};
use crate::kafka::assignment::parse_consumer_assignment;
use crate::kafka::browse;
use crate::kafka::decode::PayloadDecoder;
use crate::kafka::error::KafkaError;
use crate::kafka::factory::ClientFactory;
use crate::kafka::model::{
    BrokerMetadata, ClusterIdentity, CommittedOffset, ConfigEntry, ConfigSource, FetchPlan,
    GroupMember, GroupSnapshot, GroupState, MetadataSnapshot, PartitionMetadata, Record,
    SchemaSubject, TopicMetadata, Watermarks, is_internal_group, is_internal_topic,
};
use crate::kafka::schema::SchemaRegistryClient;
use crate::kafka::session::ClusterSession;

#[derive(Clone, Copy)]
struct Timeouts {
    metadata: Duration,
    watermark: Duration,
    admin: Duration,
    consume: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            metadata: *METADATA_TIMEOUT,
            watermark: *WATERMARK_TIMEOUT,
            admin: *ADMIN_TIMEOUT,
            consume: *CONSUME_TIMEOUT,
        }
    }
}

/// Live Kafka connection for one configured cluster.
pub(crate) struct ClusterHandle {
    identity: ClusterIdentity,
    factory: ClientFactory,
    admin: Arc<AdminClient<DefaultClientContext>>,
    metadata: Cache<(), MetadataSnapshot>,
    groups: Cache<(), Vec<GroupSnapshot>>,
    subjects: Cache<(), Vec<SchemaSubject>>,
    schema_registry: Option<PayloadDecoder>,
    timeouts: Timeouts,
}

impl std::fmt::Debug for ClusterHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterHandle")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl ClusterHandle {
    pub(crate) fn from_config(config: &ClusterConfig) -> Result<Self, KafkaError> {
        let identity = ClusterIdentity::from(config);
        let factory = ClientFactory::new(config);
        let admin = Arc::new(factory.admin()?);
        let schema_registry = config
            .schema_registry
            .as_ref()
            .map(|registry| {
                SchemaRegistryClient::new(identity.name.clone(), registry).map(PayloadDecoder::new)
            })
            .transpose()?;

        Ok(Self {
            identity,
            factory,
            admin,
            metadata: snapshot_cache(*METADATA_TTL),
            groups: snapshot_cache(*METADATA_TTL),
            subjects: snapshot_cache(*METADATA_TTL),
            schema_registry,
            timeouts: Timeouts::default(),
        })
    }

    fn admin_options(&self) -> AdminOptions {
        AdminOptions::new().operation_timeout(Some(self.timeouts.admin))
    }
}

#[async_trait]
impl ClusterSession for ClusterHandle {
    fn identity(&self) -> &ClusterIdentity {
        &self.identity
    }

    fn consume_timeout(&self) -> Duration {
        self.timeouts.consume
    }

    async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
        self.metadata
            .try_get_with(
                (),
                Self::fetch_metadata(Arc::clone(&self.admin), self.timeouts.metadata),
            )
            .await
            .map_err(into_kafka_error)
    }

    async fn watermarks_many(&self, topics: &[&str]) -> HashMap<String, HashMap<i32, Watermarks>> {
        let Ok(meta) = self.metadata().await else {
            return HashMap::new();
        };
        let partitions = meta.topic_partition_pairs(topics);
        if partitions.is_empty() {
            return HashMap::new();
        }

        let factory = self.factory.clone();
        let group_id = format!("{INTERNAL_GROUP_PREFIX}list-offsets.{}", self.identity.name);
        let timeout = self.timeouts.watermark;
        let batch = (*WATERMARK_BATCH).max(1);

        run_blocking(timeout + timeout + *BLOCKING_SLACK, move || {
            let consumer = factory.offset_consumer(&group_id)?;
            let mut beginning = HashMap::new();
            let mut end = HashMap::new();
            for chunk in partitions.chunks(batch) {
                beginning.extend(list_offsets(&consumer, chunk, Offset::Beginning, timeout)?);
                end.extend(list_offsets(&consumer, chunk, Offset::End, timeout)?);
            }
            Ok(merge_watermark_offsets(&beginning, &end))
        })
        .await
        .unwrap_or_default()
    }

    async fn offsets_for_times(
        &self,
        topic: &str,
        partitions: &[i32],
        timestamp: i64,
    ) -> Result<HashMap<i32, Option<i64>>, KafkaError> {
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }

        let factory = self.factory.clone();
        let group_id = format!("{INTERNAL_GROUP_PREFIX}list-offsets.{}", self.identity.name);
        let topic = topic.to_owned();
        let partitions = partitions.to_vec();
        let timeout = self.timeouts.watermark;

        run_blocking(timeout + *BLOCKING_SLACK, move || {
            let consumer = factory.offset_consumer(&group_id)?;
            let mut tpl = TopicPartitionList::new();
            for partition in &partitions {
                tpl.add_partition_offset(&topic, *partition, Offset::Offset(timestamp))?;
            }

            let listed = consumer.offsets_for_times(tpl, timeout)?;
            let mut out: HashMap<i32, Option<i64>> = partitions
                .iter()
                .copied()
                .map(|partition| (partition, None))
                .collect();
            for element in listed.elements() {
                let offset = match element.offset() {
                    Offset::Offset(offset) if offset >= 0 => Some(offset),
                    _ => None,
                };
                out.insert(element.partition(), offset);
            }
            Ok(out)
        })
        .await
    }

    async fn topic_configs(
        &self,
        topics: &[&str],
    ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
        let mut out = HashMap::new();

        for chunk in topics.chunks(*CONFIG_BATCH) {
            let specs: Vec<ResourceSpecifier<'_>> = chunk
                .iter()
                .copied()
                .map(ResourceSpecifier::Topic)
                .collect();
            let results = self
                .admin
                .describe_configs(&specs, &self.admin_options())
                .await?;

            for result in results {
                let Ok(resource) = result else {
                    continue;
                };
                if let OwnedResourceSpecifier::Topic(name) = resource.specifier {
                    out.insert(
                        name,
                        resource
                            .entries
                            .into_iter()
                            .map(ConfigEntry::from)
                            .collect(),
                    );
                }
            }
        }

        Ok(out)
    }

    async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
        let spec = ResourceSpecifier::Broker(broker_id);
        let results = self
            .admin
            .describe_configs(&[spec], &self.admin_options())
            .await?;

        match results.into_iter().next() {
            Some(Ok(resource)) => Ok(resource
                .entries
                .into_iter()
                .map(ConfigEntry::from)
                .collect()),
            Some(Err(error)) => Err(KafkaError::Admin(error.to_string())),
            None => Ok(Vec::new()),
        }
    }

    async fn consumer_groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
        self.groups
            .try_get_with(
                (),
                Self::fetch_group_list(Arc::clone(&self.admin), self.timeouts.admin),
            )
            .await
            .map_err(into_kafka_error)
    }

    async fn committed_offsets(
        &self,
        group_id: &str,
        partitions: &[(String, i32)],
    ) -> Result<Vec<CommittedOffset>, KafkaError> {
        if partitions.is_empty() {
            return Ok(Vec::new());
        }

        let factory = self.factory.clone();
        let group_id = group_id.to_owned();
        let partitions = partitions.to_vec();
        let timeout = self.timeouts.admin;

        run_blocking(timeout + *BLOCKING_SLACK, move || {
            let consumer = factory.offset_consumer(&group_id)?;
            let mut tpl = TopicPartitionList::new();
            for (topic, partition) in &partitions {
                tpl.add_partition(topic, *partition);
            }

            let committed = consumer.committed_offsets(tpl, timeout)?;
            let mut offsets = Vec::new();
            for element in committed.elements() {
                if let Offset::Offset(offset) = element.offset() {
                    offsets.push(CommittedOffset {
                        topic: element.topic().to_owned(),
                        partition: element.partition(),
                        offset,
                    });
                }
            }
            Ok(offsets)
        })
        .await
    }

    async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError> {
        browse::consume(
            &self.factory,
            plan,
            self.timeouts.consume,
            self.schema_registry.as_ref(),
        )
        .await
    }

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        let Some(decoder) = self.schema_registry.clone() else {
            return Ok(Vec::new());
        };

        self.subjects
            .try_get_with((), async move { decoder.client().subjects().await })
            .await
            .map_err(into_kafka_error)
    }
}

impl ClusterHandle {
    async fn fetch_metadata(
        admin: Arc<AdminClient<DefaultClientContext>>,
        timeout: Duration,
    ) -> Result<MetadataSnapshot, KafkaError> {
        run_blocking(timeout + timeout + *BLOCKING_SLACK, move || {
            let client = admin.inner();
            let metadata = client.fetch_metadata(None, timeout)?;
            let cluster_id = client.fetch_cluster_id(timeout);
            Ok(MetadataSnapshot::from_rdkafka(&metadata, cluster_id))
        })
        .await
    }

    async fn fetch_group_list(
        admin: Arc<AdminClient<DefaultClientContext>>,
        timeout: Duration,
    ) -> Result<Vec<GroupSnapshot>, KafkaError> {
        run_blocking(timeout + *BLOCKING_SLACK, move || {
            let list = admin.inner().fetch_group_list(None, timeout)?;
            Ok(list
                .groups()
                .iter()
                .filter(|group| !is_internal_group(group.name()))
                .map(GroupSnapshot::from_rdkafka)
                .collect())
        })
        .await
    }
}

async fn run_blocking<T, F>(limit: Duration, work: F) -> Result<T, KafkaError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, KafkaError> + Send + 'static,
{
    tokio::time::timeout(limit, tokio::task::spawn_blocking(work))
        .await
        .map_err(|_| KafkaError::Admin("kafka request timed out".into()))?
        .map_err(KafkaError::from)?
}

fn snapshot_cache<V>(ttl: Duration) -> Cache<(), V>
where
    V: Clone + Send + Sync + 'static,
{
    Cache::builder().max_capacity(1).time_to_live(ttl).build()
}

fn into_kafka_error(err: Arc<KafkaError>) -> KafkaError {
    Arc::try_unwrap(err).unwrap_or_else(|err| KafkaError::Admin(err.to_string()))
}

fn list_offsets(
    consumer: &impl Consumer,
    partitions: &[(String, i32)],
    timestamp: Offset,
    timeout: Duration,
) -> Result<HashMap<(String, i32), Option<i64>>, KafkaError> {
    let mut tpl = TopicPartitionList::new();
    for (topic, partition) in partitions {
        tpl.add_partition_offset(topic, *partition, timestamp)?;
    }

    let listed = consumer.offsets_for_times(tpl, timeout)?;
    Ok(listed
        .elements()
        .into_iter()
        .map(|element| {
            let offset = match element.offset() {
                Offset::Offset(offset) if offset >= 0 => Some(offset),
                _ => None,
            };
            ((element.topic().to_owned(), element.partition()), offset)
        })
        .collect())
}

fn merge_watermark_offsets(
    beginning: &HashMap<(String, i32), Option<i64>>,
    end: &HashMap<(String, i32), Option<i64>>,
) -> HashMap<String, HashMap<i32, Watermarks>> {
    let mut out: HashMap<String, HashMap<i32, Watermarks>> = HashMap::new();
    for (key, high) in end {
        let Some(high) = *high else {
            continue;
        };
        // Empty partitions often return only the last offset.
        let low = beginning.get(key).copied().flatten().unwrap_or(high);
        if high < low {
            continue;
        }
        out.entry(key.0.clone())
            .or_default()
            .insert(key.1, Watermarks { low, high });
    }
    out
}

impl MetadataSnapshot {
    fn from_rdkafka(metadata: &Metadata, cluster_id: Option<String>) -> Self {
        Self {
            cluster_id,
            brokers: metadata
                .brokers()
                .iter()
                .map(|broker| BrokerMetadata {
                    id: broker.id(),
                    host: broker.host().to_owned(),
                    port: broker.port(),
                })
                .collect(),
            topics: metadata
                .topics()
                .iter()
                .map(|topic| {
                    let name = topic.name().to_owned();
                    TopicMetadata {
                        internal: is_internal_topic(&name),
                        name,
                        partitions: topic
                            .partitions()
                            .iter()
                            .map(|partition| PartitionMetadata {
                                id: partition.id(),
                                leader: partition.leader(),
                                replicas: partition.replicas().to_vec(),
                                isr: partition.isr().to_vec(),
                            })
                            .collect(),
                    }
                })
                .collect(),
        }
    }
}

impl GroupSnapshot {
    fn from_rdkafka(info: &rdkafka::groups::GroupInfo) -> Self {
        Self {
            id: info.name().to_owned(),
            state: GroupState::parse(info.state()),
            protocol: info.protocol().to_owned(),
            coordinator: 0,
            members: info
                .members()
                .iter()
                .map(|member| GroupMember {
                    id: member.id().to_owned(),
                    client_id: member.client_id().to_owned(),
                    host: member.client_host().trim_start_matches('/').to_owned(),
                    assignments: member
                        .assignment()
                        .map(parse_consumer_assignment)
                        .unwrap_or_default(),
                })
                .collect(),
            committed: Vec::new(),
        }
    }
}

impl From<rdkafka::admin::ConfigEntry> for ConfigEntry {
    fn from(entry: rdkafka::admin::ConfigEntry) -> Self {
        Self {
            name: entry.name,
            value: entry.value,
            source: ConfigSource::from(entry.source),
            read_only: entry.is_read_only,
            sensitive: entry.is_sensitive,
        }
    }
}

impl From<RdConfigSource> for ConfigSource {
    fn from(source: RdConfigSource) -> Self {
        match source {
            RdConfigSource::DynamicTopic => Self::DynamicTopic,
            RdConfigSource::DynamicBroker => Self::DynamicBroker,
            RdConfigSource::StaticBroker => Self::StaticBroker,
            RdConfigSource::Unknown
            | RdConfigSource::DynamicDefaultBroker
            | RdConfigSource::Default => Self::Default,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::task::JoinSet;

    use super::*;

    fn snapshot() -> MetadataSnapshot {
        MetadataSnapshot {
            cluster_id: Some("id".into()),
            brokers: Vec::new(),
            topics: Vec::new(),
        }
    }

    #[tokio::test]
    async fn expires_metadata_after_ttl() {
        let cache = snapshot_cache(Duration::from_millis(20));
        let fetches = Arc::new(AtomicUsize::new(0));

        let first = {
            let fetches = Arc::clone(&fetches);
            cache
                .try_get_with((), async move {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, KafkaError>(snapshot())
                })
                .await
                .unwrap()
        };
        assert_eq!(first.cluster_id.as_deref(), Some("id"));
        assert_eq!(fetches.load(Ordering::SeqCst), 1);

        cache
            .try_get_with((), async {
                fetches.fetch_add(1, Ordering::SeqCst);
                Ok::<_, KafkaError>(snapshot())
            })
            .await
            .unwrap();
        assert_eq!(fetches.load(Ordering::SeqCst), 1);

        tokio::time::sleep(Duration::from_millis(30)).await;

        cache
            .try_get_with((), async {
                fetches.fetch_add(1, Ordering::SeqCst);
                Ok::<_, KafkaError>(snapshot())
            })
            .await
            .unwrap();
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn coalesces_concurrent_misses() {
        let cache = Arc::new(snapshot_cache(Duration::from_secs(5)));
        let fetches = Arc::new(AtomicUsize::new(0));
        let mut joins = JoinSet::new();

        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let fetches = Arc::clone(&fetches);
            joins.spawn(async move {
                cache
                    .try_get_with((), async move {
                        fetches.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok::<_, KafkaError>(snapshot())
                    })
                    .await
            });
        }

        while let Some(result) = joins.join_next().await {
            result.unwrap().unwrap();
        }

        assert_eq!(fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn does_not_cache_errors() {
        let cache: Cache<(), MetadataSnapshot> = snapshot_cache(Duration::from_secs(5));
        let fetches = Arc::new(AtomicUsize::new(0));

        let failed = {
            let fetches = Arc::clone(&fetches);
            cache
                .try_get_with((), async move {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    Err(KafkaError::Admin("boom".into()))
                })
                .await
                .map_err(into_kafka_error)
        };
        assert!(failed.is_err());

        let recovered = {
            let fetches = Arc::clone(&fetches);
            cache
                .try_get_with((), async move {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, KafkaError>(snapshot())
                })
                .await
                .unwrap()
        };
        assert_eq!(recovered.cluster_id.as_deref(), Some("id"));
        assert_eq!(fetches.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn merge_watermark_offsets_keeps_empty_skips_inverted_and_partial() {
        let beginning = HashMap::from([
            (("orders".into(), 0), Some(0)),
            (("orders".into(), 1), Some(10)),
            (("orders".into(), 2), Some(4)),
            (("payments".into(), 0), Some(1)),
            (("payments".into(), 1), None),
            (("logs".into(), 0), Some(3)),
        ]);
        let end = HashMap::from([
            (("orders".into(), 0), Some(0)),
            (("orders".into(), 1), Some(5)),
            (("orders".into(), 2), Some(12)),
            (("payments".into(), 0), None),
            (("payments".into(), 1), Some(9)),
            (("logs".into(), 0), Some(9)),
        ]);

        assert_eq!(
            merge_watermark_offsets(&beginning, &end),
            HashMap::from([
                (
                    "orders".into(),
                    HashMap::from([
                        (0, Watermarks { low: 0, high: 0 }),
                        (2, Watermarks { low: 4, high: 12 }),
                    ]),
                ),
                (
                    "payments".into(),
                    HashMap::from([(1, Watermarks { low: 9, high: 9 })]),
                ),
                (
                    "logs".into(),
                    HashMap::from([(0, Watermarks { low: 3, high: 9 })]),
                ),
            ])
        );
    }
}
