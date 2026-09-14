//! Long-lived broker adapter.
//!
//! One [`KafkaClient`] per cluster. Callers use domain types only. Production
//! [`ClusterSession`] is this type. Metadata already goes through krafka.
//! Other methods still use rdkafka.

mod blocking;
mod browse;
mod client_config;
mod convert;
mod offsets;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use krafka::client::KrafkaClient as KrafkaSharedClient;
use rdkafka::admin::{
    AdminClient, AdminOptions, ConsumerGroupState, OwnedResourceSpecifier, ResourceSpecifier,
};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::StreamConsumer;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use crate::config::ClusterConfig;
use crate::environment::{
    ADMIN_TIMEOUT, BROWSE_GROUP_PREFIX, CLIENT_ID_PREFIX, CONSUME_TIMEOUT, METADATA_TIMEOUT,
    QUEUED_MIN_MESSAGES, WATERMARK_TIMEOUT,
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
use crate::kafka::session::{ClusterSession, RecordBrowse};
use crate::kafka::topic_config::ConfigEntry;
use crate::kafka::watermarks::Watermarks;

use blocking::run_blocking;
use client_config::krafka_auth;
use convert::committed_from_tpl;
use offsets::{from_list_infos, merge_watermark_offsets, partition_time_offsets};

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
///
/// `krafka` connects on first use so tests that still talk to an rdkafka
/// mock broker do not also need a krafka-reachable listener.
pub struct KafkaClient {
    identity: ClusterIdentity,
    timeouts: Timeouts,
    base: ClientConfig,
    admin: Arc<AdminClient<DefaultClientContext>>,
    config: ClusterConfig,
    krafka: tokio::sync::OnceCell<KrafkaSharedClient>,
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
            config: config.clone(),
            krafka: tokio::sync::OnceCell::new(),
            schema_registry,
        })
    }

    async fn krafka_client(&self) -> Result<&KrafkaSharedClient, KafkaError> {
        self.krafka
            .get_or_try_init(|| async {
                let mut builder =
                    KrafkaSharedClient::builder(self.config.bootstrap_servers.join(","))
                        .client_id(format!("{CLIENT_ID_PREFIX}-{}", self.identity.name))
                        .request_timeout(self.timeouts.admin);
                if let Some(auth) = krafka_auth(&self.config)? {
                    builder = builder.auth(auth);
                }
                Ok(builder.build().await?)
            })
            .await
    }

    pub fn identity(&self) -> &ClusterIdentity {
        &self.identity
    }

    pub fn consume_timeout(&self) -> Duration {
        self.timeouts.consume
    }

    pub async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
        tokio::time::timeout(self.timeouts.metadata + self.timeouts.metadata, async {
            let cache = self.krafka_client().await?.metadata();
            cache.refresh().await?;
            Ok(MetadataSnapshot::from_krafka(cache))
        })
        .await
        .map_err(|_| KafkaError::Timeout)?
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
        match self.describe_group(id).await {
            Ok(snapshot) => Ok(snapshot),
            Err(KafkaError::UnknownGroup { .. }) => self.group_from_list(id).await,
            Err(error) => Err(error),
        }
    }

    async fn describe_group(&self, id: &str) -> Result<GroupSnapshot, KafkaError> {
        let results = self
            .admin
            .describe_consumer_groups(&[id], &self.admin_options())
            .await?;
        let description = results
            .into_iter()
            .find_map(Result::ok)
            .filter(|description| {
                description.state != ConsumerGroupState::Dead
                    && !is_internal_group(&description.group_id)
            })
            .ok_or_else(|| KafkaError::UnknownGroup {
                cluster: self.identity.name.clone(),
                id: id.to_owned(),
            })?;
        Ok(GroupSnapshot::from_description(description))
    }

    async fn group_from_list(&self, id: &str) -> Result<GroupSnapshot, KafkaError> {
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
        if partitions.is_empty() {
            return Ok(Vec::new());
        }

        let listed = self
            .admin
            .list_consumer_group_offsets(
                group_id,
                Some(&topic_partitions(partitions, None)?),
                &self.admin_options(),
            )
            .await?;
        Ok(committed_from_tpl(&listed))
    }

    /// Low and high watermarks. Does not refetch cluster metadata.
    pub async fn watermarks(
        &self,
        partitions: &[(String, i32)],
    ) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError> {
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }

        let opts = self.list_offset_options();
        let (beginning, end) = tokio::try_join!(
            list_partition_offsets(&self.admin, &opts, partitions, Offset::Beginning),
            list_partition_offsets(&self.admin, &opts, partitions, Offset::End),
        )?;
        Ok(merge_watermark_offsets(&beginning, &end))
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

        let pairs: Vec<(String, i32)> = partitions
            .iter()
            .map(|partition| (topic.to_owned(), *partition))
            .collect();
        let listed = list_partition_offsets(
            &self.admin,
            &self.list_offset_options(),
            &pairs,
            Offset::Offset(timestamp),
        )
        .await?;
        Ok(partition_time_offsets(listed))
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
        let browse = self.open_browse()?;
        let result = browse.fetch(plan).await;
        browse.close().await;
        result
    }

    pub async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        let Some(decoder) = self.schema_registry.clone() else {
            return Ok(Vec::new());
        };
        decoder.client().subjects().await
    }

    fn admin_options(&self) -> AdminOptions {
        AdminOptions::new()
            .request_timeout(Some(self.timeouts.admin))
            .operation_timeout(Some(self.timeouts.admin))
    }

    fn list_offset_options(&self) -> AdminOptions {
        AdminOptions::new().request_timeout(Some(self.timeouts.watermark))
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

    fn open_browse(&self) -> Result<browse::KafkaBrowse<'_>, KafkaError> {
        Ok(browse::KafkaBrowse::new(
            self.browser()?,
            self.timeouts.consume,
            self.schema_registry.as_ref(),
        ))
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

    async fn open_browse(&self) -> Result<Box<dyn RecordBrowse + '_>, KafkaError> {
        Ok(Box::new(KafkaClient::open_browse(self)?))
    }

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        KafkaClient::schema_subjects(self).await
    }
}

async fn list_partition_offsets(
    admin: &AdminClient<DefaultClientContext>,
    opts: &AdminOptions,
    partitions: &[(String, i32)],
    query: Offset,
) -> Result<HashMap<(String, i32), Option<i64>>, KafkaError> {
    let infos = admin
        .list_offsets(&topic_partitions(partitions, Some(query))?, opts)
        .await?;
    Ok(from_list_infos(infos))
}

fn topic_partitions(
    partitions: &[(String, i32)],
    offset: Option<Offset>,
) -> Result<TopicPartitionList, KafkaError> {
    let mut tpl = TopicPartitionList::new();
    for (topic, partition) in partitions {
        if let Some(offset) = offset {
            tpl.add_partition_offset(topic, *partition, offset)?;
        } else {
            tpl.add_partition(topic, *partition);
        }
    }
    Ok(tpl)
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
    client.set("queued.min.messages", QUEUED_MIN_MESSAGES.to_string());
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
    use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
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
    async fn empty_partitions_skip_kafka() {
        let mock = MockCluster::new(1).expect("mock cluster");
        let client = kafka_client(&mock.bootstrap_servers());
        let offsets = client.committed_offsets("unused", &[]).await.unwrap();
        assert!(offsets.is_empty());
        assert!(client.watermarks(&[]).await.unwrap().is_empty());
        assert!(
            client
                .offsets_for_times("orders", &[], 0)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn metadata_reads_topics_and_partitions_from_broker() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 2));

        let client = kafka_client(&broker.bootstrap_servers());
        let meta = client.metadata().await.expect("metadata");

        let topic = meta.topic("orders").expect("orders topic");
        assert_eq!(topic.partition_ids(), vec![0, 1]);
        assert!(!topic.internal);
    }

    #[tokio::test]
    async fn client_reads_watermarks_and_records() {
        let mock = MockCluster::new(1).expect("mock cluster");
        mock.create_topic("orders", 1, 1).expect("topic");
        let bootstrap = mock.bootstrap_servers();
        produce(&bootstrap, "orders").await;

        let client = kafka_client(&bootstrap);

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

    #[tokio::test]
    async fn browse_handle_serves_multiple_fetches_from_one_consumer() {
        let mock = MockCluster::new(1).expect("mock cluster");
        mock.create_topic("orders", 1, 1).expect("topic");
        let bootstrap = mock.bootstrap_servers();
        for _ in 0..4 {
            produce(&bootstrap, "orders").await;
        }

        let client = kafka_client(&bootstrap);
        let browse = client.open_browse().expect("browse handle");

        let window = |start: i64, end: i64| FetchPlan {
            topic: "orders".into(),
            windows: vec![PartitionWindow {
                partition: 0,
                start,
                end,
            }],
            filter: None,
            limit: 10,
            order: RecordOrder::Oldest,
            schema_id: None,
        };

        let first = browse.fetch(&window(0, 2)).await.expect("first pass");
        let second = browse.fetch(&window(2, 4)).await.expect("second pass");
        browse.close().await;

        assert_eq!(
            first.iter().map(|record| record.offset).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            second
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[tokio::test]
    async fn lists_committed_offsets_on_a_mock_cluster() {
        let mock = MockCluster::new(1).expect("mock cluster");
        mock.create_topic("orders", 1, 1).expect("topic");

        let bootstrap = mock.bootstrap_servers();
        produce(&bootstrap, "orders").await;
        commit(&bootstrap, "orders-group", "orders", 1);

        let client = kafka_client(&bootstrap);
        let offsets = client
            .committed_offsets("orders-group", &[("orders".into(), 0)])
            .await
            .expect("offset fetch");

        assert_eq!(
            offsets,
            vec![CommittedOffset {
                topic: "orders".into(),
                partition: 0,
                offset: 1,
            }]
        );
    }

    #[tokio::test]
    async fn eight_groups_share_one_client() {
        let mock = MockCluster::new(1).expect("mock cluster");
        mock.create_topic("orders", 1, 1).expect("topic");
        let bootstrap = mock.bootstrap_servers();
        produce(&bootstrap, "orders").await;

        for index in 0..8 {
            commit(&bootstrap, &format!("g{index}"), "orders", 1);
        }

        let client = Arc::new(kafka_client(&bootstrap));
        let fetches = (0..8).map(|index| {
            let client = Arc::clone(&client);
            async move {
                client
                    .committed_offsets(&format!("g{index}"), &[("orders".into(), 0)])
                    .await
            }
        });
        let results = futures::future::join_all(fetches).await;
        assert_eq!(results.len(), 8);
        for result in results {
            assert_eq!(result.expect("offset fetch")[0].offset, 1);
        }
    }

    fn kafka_client(bootstrap: &str) -> KafkaClient {
        KafkaClient::connect(&ClusterConfig {
            name: "test".into(),
            bootstrap_servers: vec![bootstrap.to_owned()],
            security: None,
            schema_registry: None,
            properties: HashMap::new(),
        })
        .expect("kafka client")
    }

    async fn produce(bootstrap: &str, topic: &str) {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .create()
            .expect("producer");
        producer
            .send(
                FutureRecord::to(topic).payload("hello").key("k"),
                Duration::from_secs(5),
            )
            .await
            .expect("produce");
    }

    fn commit(bootstrap: &str, group: &str, topic: &str, offset: i64) {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .create()
            .expect("consumer");
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, 0, Offset::Offset(offset))
            .expect("offset");
        consumer.assign(&tpl).expect("assign");
        consumer.commit(&tpl, CommitMode::Sync).expect("commit");
    }
}
