mod blocking;
mod browse;
mod client_config;
mod convert;
mod factory;
mod offsets;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use moka::future::Cache;
use rdkafka::admin::{AdminClient, AdminOptions, OwnedResourceSpecifier, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::topic_partition_list::Offset;

use crate::config::ClusterConfig;
use crate::environment::{
    ADMIN_TIMEOUT, BLOCKING_SLACK, CONFIG_BATCH, CONFIG_CONCURRENCY, CONSUME_TIMEOUT,
    INTERNAL_GROUP_PREFIX, METADATA_TIMEOUT, METADATA_TTL, WATERMARK_TIMEOUT, WATERMARK_TTL,
};
use crate::kafka::cluster::ClusterIdentity;
use crate::kafka::error::KafkaError;
use crate::kafka::group::{CommittedOffset, GroupSnapshot, is_internal_group};
use crate::kafka::metadata::MetadataSnapshot;
use crate::kafka::record::Record;
use crate::kafka::record::plan::FetchPlan;
use crate::kafka::registry::SchemaSubject;
use crate::kafka::session::ClusterSession;
use crate::kafka::topic_config::ConfigEntry;
use crate::kafka::watermarks::Watermarks;

use blocking::{into_kafka_error, run_blocking, snapshot_cache};
use factory::ClientFactory;
use offsets::{list_offsets, merge_watermark_offsets};

use crate::kafka::registry::client::SchemaRegistryClient;
use crate::kafka::registry::decode::PayloadDecoder;

pub use client_config::KafkaClusterConfig;

/// Per-call deadlines, defaulted from [`crate::environment`].
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

pub(crate) struct ClusterHandle {
    identity: ClusterIdentity,
    factory: ClientFactory,
    admin: Arc<AdminClient<DefaultClientContext>>,
    metadata: Cache<(), MetadataSnapshot>,
    groups: Cache<(), Vec<GroupSnapshot>>,
    subjects: Cache<(), Vec<SchemaSubject>>,
    watermarks: Cache<(), HashMap<String, HashMap<i32, Watermarks>>>,
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
            watermarks: snapshot_cache(*WATERMARK_TTL),
            schema_registry,
            timeouts: Timeouts::default(),
        })
    }

    fn admin_options(&self) -> AdminOptions {
        AdminOptions::new().operation_timeout(Some(self.timeouts.admin))
    }

    /// Consumer group used for this cluster's `ListOffsets` probes.
    fn offsets_group_id(&self) -> String {
        format!("{INTERNAL_GROUP_PREFIX}list-offsets.{}", self.identity.name)
    }

    async fn load_watermarks(
        &self,
        topics: Vec<String>,
    ) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError> {
        let meta = self.metadata().await?;
        let names: Vec<&str> = topics.iter().map(String::as_str).collect();
        let partitions = meta.topic_partition_pairs(&names);
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }

        let factory = self.factory.clone();
        let group_id = self.offsets_group_id();
        let timeout = self.timeouts.watermark;

        run_blocking(timeout + timeout + *BLOCKING_SLACK, move || {
            let consumer = factory.offset_consumer(&group_id)?;
            let beginning = list_offsets(&consumer, &partitions, Offset::Beginning, timeout)?;
            let end = list_offsets(&consumer, &partitions, Offset::End, timeout)?;
            Ok(merge_watermark_offsets(&beginning, &end))
        })
        .await
    }

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
        if topics.is_empty() {
            return HashMap::new();
        }

        if let Some(cached) = self.watermarks.get(&()).await {
            return select_watermarks(cached, topics);
        }

        let Ok(meta) = self.metadata().await else {
            return HashMap::new();
        };
        let all = meta.topic_names();
        let fetch_all = !all.is_empty() && topics.len() >= all.len();
        let requested: Vec<String> = if fetch_all {
            all.into_iter().map(str::to_owned).collect()
        } else {
            topics.iter().map(|name| (*name).to_owned()).collect()
        };

        if fetch_all {
            match self
                .watermarks
                .try_get_with((), self.load_watermarks(requested))
                .await
            {
                Ok(cached) => select_watermarks(cached, topics),
                Err(_) => HashMap::new(),
            }
        } else {
            self.load_watermarks(requested).await.unwrap_or_default()
        }
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
        let group_id = self.offsets_group_id();
        let topic = topic.to_owned();
        let partitions = partitions.to_vec();
        let timeout = self.timeouts.watermark;

        run_blocking(timeout + *BLOCKING_SLACK, move || {
            let consumer = factory.offset_consumer(&group_id)?;
            let pairs: Vec<(String, i32)> = partitions
                .iter()
                .map(|partition| (topic.clone(), *partition))
                .collect();
            let listed = list_offsets(&consumer, &pairs, Offset::Offset(timestamp), timeout)?;

            // Every requested partition gets an entry; the broker may omit some.
            let mut out: HashMap<i32, Option<i64>> = partitions
                .iter()
                .copied()
                .map(|partition| (partition, None))
                .collect();
            out.extend(
                listed
                    .into_iter()
                    .map(|((_, partition), offset)| (partition, offset)),
            );
            Ok(out)
        })
        .await
    }

    async fn topic_configs(
        &self,
        topics: &[&str],
    ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
        if topics.is_empty() {
            return Ok(HashMap::new());
        }

        let chunk_size = (*CONFIG_BATCH).max(1);
        let concurrency = (*CONFIG_CONCURRENCY).max(1);
        let names: Vec<String> = topics.iter().map(|name| (*name).to_owned()).collect();
        let chunks: Vec<Vec<String>> = names
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect();
        let admin = Arc::clone(&self.admin);
        let timeout = self.timeouts.admin;

        let results = stream::iter(chunks)
            .map(|chunk| {
                let admin = Arc::clone(&admin);
                async move { describe_topic_configs(&admin, &chunk, timeout).await }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;

        let mut out = HashMap::new();
        for result in results {
            out.extend(result?);
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
            use rdkafka::consumer::Consumer;
            use rdkafka::topic_partition_list::TopicPartitionList;

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

fn select_watermarks(
    mut cached: HashMap<String, HashMap<i32, Watermarks>>,
    topics: &[&str],
) -> HashMap<String, HashMap<i32, Watermarks>> {
    if topics.len() == cached.len() {
        return cached;
    }

    let mut out = HashMap::with_capacity(topics.len());
    for name in topics {
        if let Some(marks) = cached.remove(*name) {
            out.insert((*name).to_owned(), marks);
        }
    }
    out
}

async fn describe_topic_configs(
    admin: &AdminClient<DefaultClientContext>,
    names: &[String],
    timeout: Duration,
) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
    let specs: Vec<ResourceSpecifier<'_>> = names
        .iter()
        .map(|name| ResourceSpecifier::Topic(name.as_str()))
        .collect();
    let options = AdminOptions::new().operation_timeout(Some(timeout));
    let results = admin.describe_configs(&specs, &options).await?;

    let mut out = HashMap::new();
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
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_watermarks_keeps_requested_topics() {
        let cached = HashMap::from([
            (
                "orders".into(),
                HashMap::from([(0, Watermarks { low: 0, high: 8 })]),
            ),
            (
                "payments".into(),
                HashMap::from([(0, Watermarks { low: 1, high: 4 })]),
            ),
        ]);

        let selected = select_watermarks(cached.clone(), &["orders"]);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected["orders"][&0], Watermarks { low: 0, high: 8 });

        let all = select_watermarks(cached, &["orders", "payments"]);
        assert_eq!(all.len(), 2);
    }
}
