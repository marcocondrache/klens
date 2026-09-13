//! Long-lived rdkafka wrapper.
//!
//! One [`KafkaClient`] per cluster. Callers use domain types only; rdkafka
//! stays inside this module. Production [`ClusterSession`] is this type.

mod blocking;
mod browse;
mod client_config;
mod convert;
mod deadline;
mod group_offsets;
mod offsets;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rdkafka::admin::{AdminClient, AdminOptions, OwnedResourceSpecifier, ResourceSpecifier};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, StreamConsumer};
use rdkafka::topic_partition_list::Offset;

use crate::config::ClusterConfig;
use crate::environment::{
    ADMIN_TIMEOUT, BROWSE_GROUP_PREFIX, CLIENT_ID_PREFIX, CONSUME_TIMEOUT, INTERNAL_GROUP_PREFIX,
    METADATA_TIMEOUT, WATERMARK_TIMEOUT,
};
use crate::kafka::cluster::ClusterIdentity;
use crate::kafka::error::KafkaError;
use crate::kafka::group::{CommittedOffset, GroupSnapshot, is_internal_group};
use crate::kafka::metadata::MetadataSnapshot;
use crate::kafka::record::Record;
use crate::kafka::record::plan::FetchPlan;
use crate::kafka::registry::SchemaSubject;
use crate::kafka::registry::client::SchemaRegistryClient;
use crate::kafka::registry::decode::PayloadDecoder;
use crate::kafka::session::ClusterSession;
use crate::kafka::topic_config::ConfigEntry;
use crate::kafka::watermarks::Watermarks;

use blocking::run_blocking;
use deadline::Deadline;
use group_offsets::NativeQueue;
use offsets::{list_offsets, merge_watermark_offsets, partition_time_offsets};

pub use client_config::KafkaClusterConfig;

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

/// Process-lifetime Kafka handle. All broker I/O for a cluster goes through here.
pub struct KafkaClient {
    identity: ClusterIdentity,
    timeouts: Timeouts,
    base: ClientConfig,
    admin: Arc<AdminClient<DefaultClientContext>>,
    /// Shared ListConsumerGroupOffsets result queue. One poller at a time.
    offset_queue: Arc<Mutex<NativeQueue>>,
    /// Reused for ListOffsets (watermarks and offsets-for-times).
    log: Arc<BaseConsumer>,
    schema_registry: Option<PayloadDecoder>,
}

impl std::fmt::Debug for KafkaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaClient")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl KafkaClient {
    pub fn connect(config: &ClusterConfig) -> Result<Self, KafkaError> {
        let identity = ClusterIdentity::from(config);
        let base = KafkaClusterConfig::from(config).into_client_config();
        let admin: AdminClient<DefaultClientContext> = base.create()?;
        let offset_queue = NativeQueue::new(admin.inner().native_ptr())?;
        let log = log_consumer(&base, &identity.name)?;
        let schema_registry = config
            .schema_registry
            .as_ref()
            .map(|registry| {
                SchemaRegistryClient::new(identity.name.clone(), registry).map(PayloadDecoder::new)
            })
            .transpose()?;

        Ok(Self {
            identity,
            timeouts: Timeouts::default(),
            base,
            admin: Arc::new(admin),
            offset_queue: Arc::new(Mutex::new(offset_queue)),
            log: Arc::new(log),
            schema_registry,
        })
    }

    pub fn identity(&self) -> &ClusterIdentity {
        &self.identity
    }

    pub fn consume_timeout(&self) -> Duration {
        self.timeouts.consume
    }

    pub async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
        let admin = Arc::clone(&self.admin);
        let timeout = self.timeouts.metadata;
        run_blocking(timeout + timeout, move || {
            let client = admin.inner();
            let metadata = client.fetch_metadata(None, timeout)?;
            let cluster_id = client.fetch_cluster_id(timeout);
            Ok(MetadataSnapshot::from_rdkafka(&metadata, cluster_id))
        })
        .await
    }

    pub async fn groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
        let admin = Arc::clone(&self.admin);
        let timeout = self.timeouts.admin;
        run_blocking(timeout, move || {
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

    pub async fn group(&self, id: &str) -> Result<GroupSnapshot, KafkaError> {
        let admin = Arc::clone(&self.admin);
        let timeout = self.timeouts.admin;
        let requested = id.to_owned();
        run_blocking(timeout, move || {
            let list = admin.inner().fetch_group_list(Some(&requested), timeout)?;
            Ok(list
                .groups()
                .iter()
                .filter(|group| !is_internal_group(group.name()))
                .find(|group| group.name() == requested)
                .map(GroupSnapshot::from_rdkafka))
        })
        .await?
        .ok_or_else(|| KafkaError::UnknownGroup {
            cluster: self.identity.name.clone(),
            id: id.to_owned(),
        })
    }

    /// Committed offsets for a group we are not a member of.
    pub async fn committed_offsets(
        &self,
        group_id: &str,
        partitions: &[(String, i32)],
    ) -> Result<Vec<CommittedOffset>, KafkaError> {
        group_offsets::list(
            &self.admin,
            &self.offset_queue,
            group_id,
            partitions,
            Deadline::from(self.timeouts.admin),
        )
        .await
    }

    /// Low and high watermarks. Does not refetch cluster metadata.
    pub async fn watermarks(
        &self,
        partitions: &[(String, i32)],
    ) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError> {
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }

        let log = Arc::clone(&self.log);
        let partitions = partitions.to_vec();
        let timeout = self.timeouts.watermark;
        run_blocking(timeout + timeout, move || {
            let beginning = list_offsets(&*log, &partitions, Offset::Beginning, timeout)?;
            let end = list_offsets(&*log, &partitions, Offset::End, timeout)?;
            Ok(merge_watermark_offsets(&beginning, &end))
        })
        .await
    }

    pub async fn offsets_for_times(
        &self,
        topic: &str,
        partitions: &[i32],
        timestamp: i64,
    ) -> Result<HashMap<i32, Option<i64>>, KafkaError> {
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }

        let log = Arc::clone(&self.log);
        let topic = topic.to_owned();
        let partitions = partitions.to_vec();
        let timeout = self.timeouts.watermark;
        run_blocking(timeout, move || {
            let pairs: Vec<(&str, i32)> = partitions
                .iter()
                .map(|partition| (topic.as_str(), *partition))
                .collect();
            let listed = list_offsets(&*log, &pairs, Offset::Offset(timestamp), timeout)?;
            Ok(partition_time_offsets(listed))
        })
        .await
    }

    pub async fn topic_configs(
        &self,
        topics: &[&str],
    ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
        if topics.is_empty() {
            return Ok(HashMap::new());
        }

        let specs: Vec<ResourceSpecifier<'_>> = topics
            .iter()
            .copied()
            .map(ResourceSpecifier::Topic)
            .collect();
        let results = self
            .admin
            .describe_configs(&specs, &self.admin_options())
            .await?;

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

    pub async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
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
            Some(Err(error)) => Err(KafkaError::BrokerConfigs {
                id: broker_id,
                message: error.to_string(),
            }),
            None => Ok(Vec::new()),
        }
    }

    /// Fully scan the plan's half-open windows. Incomplete scans return
    /// [`KafkaError::Timeout`], not a partial page.
    pub async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError> {
        if plan.windows.is_empty() || plan.limit == 0 {
            return Ok(Vec::new());
        }
        let consumer = self.browser()?;
        browse::consume(
            consumer,
            plan,
            self.timeouts.consume,
            self.schema_registry.as_ref(),
        )
        .await
    }

    pub async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        let Some(decoder) = self.schema_registry.clone() else {
            return Ok(Vec::new());
        };
        decoder.client().subjects().await
    }

    fn admin_options(&self) -> AdminOptions {
        AdminOptions::new().operation_timeout(Some(self.timeouts.admin))
    }

    fn browser(&self) -> Result<StreamConsumer, KafkaError> {
        Ok(consumer_config(
            &self.base,
            &self.identity.name,
            &browse_group_id(&self.identity.name),
            "browse",
            true,
        )
        .create()?)
    }
}

#[async_trait]
impl ClusterSession for KafkaClient {
    fn identity(&self) -> &ClusterIdentity {
        KafkaClient::identity(self)
    }

    fn consume_timeout(&self) -> Duration {
        KafkaClient::consume_timeout(self)
    }

    async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
        KafkaClient::metadata(self).await
    }

    async fn watermarks(
        &self,
        partitions: &[(String, i32)],
    ) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError> {
        KafkaClient::watermarks(self, partitions).await
    }

    async fn offsets_for_times(
        &self,
        topic: &str,
        partitions: &[i32],
        timestamp: i64,
    ) -> Result<HashMap<i32, Option<i64>>, KafkaError> {
        KafkaClient::offsets_for_times(self, topic, partitions, timestamp).await
    }

    async fn topic_configs(
        &self,
        topics: &[&str],
    ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
        KafkaClient::topic_configs(self, topics).await
    }

    async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
        KafkaClient::broker_configs(self, broker_id).await
    }

    async fn groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
        KafkaClient::groups(self).await
    }

    async fn group(&self, id: &str) -> Result<GroupSnapshot, KafkaError> {
        KafkaClient::group(self, id).await
    }

    async fn committed_offsets(
        &self,
        group_id: &str,
        partitions: &[(String, i32)],
    ) -> Result<Vec<CommittedOffset>, KafkaError> {
        KafkaClient::committed_offsets(self, group_id, partitions).await
    }

    async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError> {
        KafkaClient::records(self, plan).await
    }

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        KafkaClient::schema_subjects(self).await
    }
}

fn log_consumer(base: &ClientConfig, cluster: &str) -> Result<BaseConsumer, KafkaError> {
    let group_id = format!("{INTERNAL_GROUP_PREFIX}list-offsets.{cluster}");
    Ok(consumer_config(base, cluster, &group_id, "offsets", false).create()?)
}

fn consumer_config(
    base: &ClientConfig,
    cluster: &str,
    group_id: &str,
    role: &str,
    partition_eof: bool,
) -> ClientConfig {
    let mut client = base.clone();
    client.set("client.id", format!("{CLIENT_ID_PREFIX}-{cluster}-{role}"));
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
    use rdkafka::config::ClientConfig;
    use rdkafka::mocking::MockCluster;
    use rdkafka::producer::{FutureProducer, FutureRecord};

    use crate::config::ClusterConfig;
    use crate::kafka::record::plan::PartitionWindow;
    use crate::kafka::record::query::RecordOrder;

    #[test]
    fn browse_group_ids_are_internal_and_unique() {
        let first = browse_group_id("local");
        let second = browse_group_id("local");

        assert!(first.starts_with(&format!("{BROWSE_GROUP_PREFIX}.local.")));
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn client_reads_metadata_watermarks_and_records() {
        let mock = MockCluster::new(1).expect("mock cluster");
        mock.create_topic("orders", 1, 1).expect("topic");
        let bootstrap = mock.bootstrap_servers();

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap)
            .create()
            .expect("producer");
        producer
            .send(
                FutureRecord::to("orders").payload("hello").key("k"),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");

        let client = KafkaClient::connect(&ClusterConfig {
            name: "test".into(),
            bootstrap_servers: vec![bootstrap],
            security: None,
            schema_registry: None,
            properties: HashMap::new(),
        })
        .expect("client");

        let meta = client.metadata().await.expect("metadata");
        assert!(
            meta.topics
                .iter()
                .any(|topic| topic.name == "orders" && topic.partitions.len() == 1)
        );

        let marks = client
            .watermarks(&[("orders".into(), 0)])
            .await
            .expect("watermarks");
        assert_eq!(marks["orders"][&0], Watermarks { low: 0, high: 1 });

        let records = client
            .records(&FetchPlan {
                topic: "orders".into(),
                windows: vec![PartitionWindow {
                    partition: 0,
                    start: 0,
                    end: 1,
                }],
                filter: None,
                limit: 10,
                order: RecordOrder::Oldest,
                schema_id: None,
            })
            .await
            .expect("records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[0].value.as_deref(), Some("hello"));
    }
}
