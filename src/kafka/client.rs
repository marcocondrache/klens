//! Long-lived broker adapter.
//!
//! One [`KafkaClient`] per cluster. Callers use domain types only. Production
//! [`ClusterSession`] is this type. All broker I/O goes through krafka.

mod browse;
mod client_config;
mod convert;
mod groups;
mod offsets;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use krafka::admin::{
    AdminClient as KrafkaAdmin, ConfigResourceType, DescribeConfigsRequest,
    DescribeConfigsResource, OffsetSpec, OffsetVisibility,
};
use krafka::client::KrafkaClient as KrafkaSharedClient;
use krafka::consumer::{AutoOffsetReset, Consumer as KrafkaConsumer};

use crate::config::ClusterConfig;
use crate::environment::{
    ADMIN_TIMEOUT, CLIENT_ID_PREFIX, CONSUME_TIMEOUT, METADATA_TIMEOUT, WATERMARK_TIMEOUT,
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

use client_config::{KrafkaConnect, krafka_auth};
use convert::committed_from_krafka;
use groups::{
    fill_classic_assignments, group_listing, listed_group_ids, snapshots_from_descriptions,
};
use offsets::{from_list_offsets, merge_watermark_offsets, partition_time_offsets};

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
/// `krafka` connects on first use so `connect` can succeed without a live
/// listener. The first broker call opens the pool.
pub struct KafkaClient {
    identity: ClusterIdentity,
    timeouts: Timeouts,
    connect: KrafkaConnect,
    config: ClusterConfig,
    krafka: tokio::sync::OnceCell<KrafkaSharedClient>,
    admin: tokio::sync::OnceCell<KrafkaAdmin>,
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
        let timeouts = Timeouts::default();
        let connect = KrafkaConnect::from_cluster(config, timeouts.admin)?;
        let schema_registry = config
            .schema_registry
            .as_ref()
            .map(|registry| {
                SchemaRegistryClient::new(identity.name.clone(), registry).map(PayloadDecoder::new)
            })
            .transpose()?;

        Ok(Self {
            identity,
            timeouts,
            connect,
            config: config.clone(),
            krafka: tokio::sync::OnceCell::new(),
            admin: tokio::sync::OnceCell::new(),
            schema_registry,
        })
    }

    async fn krafka_client(&self) -> Result<&KrafkaSharedClient, KafkaError> {
        self.krafka
            .get_or_try_init(|| async {
                let mut builder = KrafkaSharedClient::builder(&self.connect.bootstrap)
                    .client_id(self.connect.client_id.clone())
                    .request_timeout(self.connect.request_timeout)
                    .connect_timeout(self.connect.connect_timeout);
                if let Some(auth) = krafka_auth(&self.config)? {
                    builder = builder.auth(auth);
                }
                Ok(builder.build().await?)
            })
            .await
    }

    async fn krafka_admin(&self) -> Result<&KrafkaAdmin, KafkaError> {
        self.admin
            .get_or_try_init(|| async {
                Ok(KrafkaAdmin::builder()
                    .with_client(self.krafka_client().await?)
                    .build()
                    .await?)
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
        tokio::time::timeout(self.timeouts.admin, async {
            let admin = self.krafka_admin().await?;
            let ids = listed_group_ids(admin.list_consumer_groups(&group_listing()).await?);
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            let mut snapshots =
                snapshots_from_descriptions(admin.describe_consumer_groups(ids).await?);
            fill_classic_assignments(self.krafka_client().await?, &mut snapshots).await?;
            Ok(snapshots)
        })
        .await
        .map_err(|_| KafkaError::Timeout)?
    }

    pub async fn group(&self, id: &str) -> Result<GroupSnapshot, KafkaError> {
        if is_internal_group(id) {
            return Err(KafkaError::UnknownGroup {
                cluster: self.identity.name.clone(),
                id: id.to_owned(),
            });
        }
        tokio::time::timeout(self.timeouts.admin, async {
            let described = self
                .krafka_admin()
                .await?
                .describe_consumer_groups(vec![id.to_owned()])
                .await?;
            let mut snapshots = snapshots_from_descriptions(described);
            let Some(mut snapshot) = snapshots.drain(..).find(|snapshot| snapshot.id == id) else {
                return Err(KafkaError::UnknownGroup {
                    cluster: self.identity.name.clone(),
                    id: id.to_owned(),
                });
            };
            fill_classic_assignments(
                self.krafka_client().await?,
                std::slice::from_mut(&mut snapshot),
            )
            .await?;
            Ok(snapshot)
        })
        .await
        .map_err(|_| KafkaError::Timeout)?
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

        let topics = partitions_by_topic(partitions);
        let query = list_offset_query(&topics);
        let listed = tokio::time::timeout(self.timeouts.admin, async {
            self.krafka_admin()
                .await?
                .describe_consumer_group_offsets(
                    group_id,
                    Some(&query),
                    OffsetVisibility::IncludeUnstable,
                )
                .await
                .map_err(KafkaError::from)
        })
        .await
        .map_err(|_| KafkaError::Timeout)??;
        Ok(committed_from_krafka(listed))
    }

    /// Low and high watermarks. Does not refetch cluster metadata.
    pub async fn watermarks(
        &self,
        partitions: &[(String, i32)],
    ) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError> {
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }

        let topics = partitions_by_topic(partitions);
        let query = list_offset_query(&topics);
        let (beginning, end) = tokio::time::timeout(self.timeouts.watermark, async {
            let admin = self.krafka_admin().await?;
            tokio::try_join!(
                admin.list_offsets(&query, OffsetSpec::Earliest),
                admin.list_offsets(&query, OffsetSpec::Latest),
            )
            .map_err(KafkaError::from)
        })
        .await
        .map_err(|_| KafkaError::Timeout)??;
        Ok(merge_watermark_offsets(
            &from_list_offsets(beginning.into_iter().map(list_offset_parts)),
            &from_list_offsets(end.into_iter().map(list_offset_parts)),
        ))
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

        let listed = tokio::time::timeout(self.timeouts.watermark, async {
            self.krafka_admin()
                .await?
                .list_offsets(&[(topic, partitions)], OffsetSpec::Timestamp(timestamp))
                .await
                .map_err(KafkaError::from)
        })
        .await
        .map_err(|_| KafkaError::Timeout)??;
        Ok(partition_time_offsets(from_list_offsets(
            listed.into_iter().map(list_offset_parts),
        )))
    }

    pub async fn topic_configs(
        &self,
        topics: &[&str],
    ) -> Result<HashMap<String, Vec<ConfigEntry>>, KafkaError> {
        if topics.is_empty() {
            return Ok(HashMap::new());
        }

        let request = DescribeConfigsRequest {
            resources: topics
                .iter()
                .map(|topic| DescribeConfigsResource {
                    resource_type: ConfigResourceType::Topic,
                    resource_name: (*topic).to_owned(),
                    config_names: None,
                })
                .collect(),
            include_synonyms: false,
            include_documentation: false,
        };
        let results = tokio::time::timeout(self.timeouts.admin, async {
            self.krafka_admin()
                .await?
                .describe_configs_per_resource(request)
                .await
                .map_err(KafkaError::from)
        })
        .await
        .map_err(|_| KafkaError::Timeout)??;

        let mut out = HashMap::new();
        for result in results {
            if !result.error_code.is_ok() {
                continue;
            }
            if result.resource_type == ConfigResourceType::Topic {
                out.insert(
                    result.resource_name,
                    result.configs.into_iter().map(ConfigEntry::from).collect(),
                );
            }
        }
        Ok(out)
    }

    pub async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
        let results = tokio::time::timeout(self.timeouts.admin, async {
            self.krafka_admin()
                .await?
                .describe_configs_per_resource(DescribeConfigsRequest::for_broker(broker_id))
                .await
                .map_err(KafkaError::from)
        })
        .await
        .map_err(|_| KafkaError::Timeout)??;

        match results.into_iter().next() {
            Some(result) if result.error_code.is_ok() => {
                Ok(result.configs.into_iter().map(ConfigEntry::from).collect())
            }
            Some(result) => Err(KafkaError::BrokerConfigs {
                id: broker_id,
                message: result
                    .error
                    .unwrap_or_else(|| format!("{:?}", result.error_code)),
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
        let browse = self.open_browse().await?;
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

    async fn open_browse(&self) -> Result<browse::KafkaBrowse<'_>, KafkaError> {
        let consumer = KrafkaConsumer::builder()
            .with_client(self.krafka_client().await?)
            .client_id(browse_client_id(&self.identity.name))
            .enable_auto_commit(false)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build()
            .await?;
        Ok(browse::KafkaBrowse::new(
            consumer,
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
        Ok(Box::new(KafkaClient::open_browse(self).await?))
    }

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        KafkaClient::schema_subjects(self).await
    }
}

fn partitions_by_topic(partitions: &[(String, i32)]) -> HashMap<String, Vec<i32>> {
    let mut topics: HashMap<String, Vec<i32>> = HashMap::new();
    for (topic, partition) in partitions {
        topics.entry(topic.clone()).or_default().push(*partition);
    }
    topics
}

fn list_offset_query(topics: &HashMap<String, Vec<i32>>) -> Vec<(&str, &[i32])> {
    topics
        .iter()
        .map(|(topic, partitions)| (topic.as_str(), partitions.as_slice()))
        .collect()
}

fn list_offset_parts(result: krafka::admin::ListOffsetResult) -> (String, i32, i64) {
    (result.topic, result.partition, result.offset)
}

fn browse_client_id(cluster: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(1);
    format!(
        "{CLIENT_ID_PREFIX}-{cluster}-browse-{}",
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::sync::{Arc, Mutex};

    use crate::config::ClusterConfig;
    use crate::kafka::record::plan::PartitionWindow;
    use crate::kafka::record::query::RecordOrder;

    #[derive(Clone, Default)]
    struct LogBuf(Arc<Mutex<Vec<u8>>>);

    impl io::Write for LogBuf {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("log buf").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl LogBuf {
        fn as_string(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("log buf")).into_owned()
        }
    }

    #[test]
    fn browse_client_ids_are_unique() {
        let first = browse_client_id("local");
        let second = browse_client_id("local");

        assert!(first.starts_with(&format!("{CLIENT_ID_PREFIX}-local-browse-")));
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn admin_calls_do_not_warn_about_missing_close() {
        let logs = LogBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::WARN)
            .with_target(true)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 1).await;

        let client = kafka_client(&broker.bootstrap_servers());
        client
            .watermarks(&[("orders".into(), 0)])
            .await
            .expect("watermarks");
        client
            .watermarks(&[("orders".into(), 0)])
            .await
            .expect("second watermarks");

        let text = logs.as_string();
        assert!(
            !text.contains("AdminClient dropped without close"),
            "admin close warn: {text}"
        );
    }

    #[tokio::test]
    async fn empty_partitions_skip_kafka() {
        let client = kafka_client("127.0.0.1:1");
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
    async fn client_reads_watermarks_and_time_offsets() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 1).await;

        let client = kafka_client(&broker.bootstrap_servers());
        let marks = client
            .watermarks(&[("orders".into(), 0)])
            .await
            .expect("watermarks");
        assert_eq!(marks["orders"][&0], Watermarks { low: 0, high: 1 });

        let offsets = client
            .offsets_for_times("orders", &[0], 0)
            .await
            .expect("time offsets");
        assert_eq!(offsets.get(&0), Some(&Some(0)));
    }

    #[tokio::test]
    async fn client_reads_records() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 1).await;

        let client = kafka_client(&broker.bootstrap_servers());
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
        assert_eq!(records[0].key.as_deref(), Some("k"));

        let past_high = client
            .records(&FetchPlan {
                topic: "orders".into(),
                windows: vec![PartitionWindow {
                    partition: 0,
                    start: 5,
                    end: 10,
                }],
                filter: None,
                limit: 10,
                order: RecordOrder::Oldest,
                schema_id: None,
            })
            .await
            .expect("empty past high watermark");
        assert!(past_high.is_empty());
    }

    #[tokio::test]
    async fn browse_handle_serves_multiple_fetches_from_one_consumer() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 4).await;

        let client = kafka_client(&broker.bootstrap_servers());
        let browse = client.open_browse().await.expect("browse handle");

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
    async fn lists_committed_offsets() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 1).await;

        let client = kafka_client(&broker.bootstrap_servers());
        commit_krafka(&client, "orders-group", "orders", 1).await;
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
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 1).await;

        let client = Arc::new(kafka_client(&broker.bootstrap_servers()));
        for index in 0..8 {
            commit_krafka(&client, &format!("g{index}"), "orders", 1).await;
        }

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

    async fn produce_krafka(bootstrap: &str, topic: &str, count: usize) {
        let producer = krafka::producer::Producer::builder()
            .bootstrap_servers(bootstrap)
            .build()
            .await
            .expect("producer");
        for _ in 0..count {
            let _metadata = producer
                .send(topic, Some(b"k"), Some(b"hello"))
                .await
                .expect("produce");
        }
    }

    async fn commit_krafka(client: &KafkaClient, group: &str, topic: &str, offset: i64) {
        client
            .krafka_admin()
            .await
            .expect("admin")
            .alter_consumer_group_offsets(group, &[(topic, &[(0, offset)])])
            .await
            .expect("commit");
    }
}
