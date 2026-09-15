//! Long-lived broker adapter.
//!
//! One [`KafkaClient`] per cluster. Callers use domain types only. Production
//! [`ClusterSession`] is this type. All broker I/O goes through krafka.

mod browse;
mod config;
mod convert;
mod groups;
mod offsets;

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use krafka::admin::{
    AclFilter, AdminClient as KrafkaAdmin, ConfigResourceType, DescribeConfigsRequest,
    DescribeConfigsResource, GroupListing, OffsetSpec, OffsetVisibility,
};
use krafka::client::KrafkaClient as KrafkaSharedClient;

use crate::config::ClusterConfig;
use crate::environment::{
    CLIENT_ID_PREFIX, CONSUME_TIMEOUT, REQUEST_TIMEOUT, SOCKET_CONNECTION_SETUP_TIMEOUT_MS,
};
use crate::kafka::acl::AclListing;
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

use config::krafka_auth;
use convert::committed_from_krafka;
use groups::{fill_classic_assignments, snapshots_from_descriptions};
use offsets::{from_list_offsets, merge_watermark_offsets, partition_time_offsets};

/// Process-lifetime Kafka handle. All broker I/O for a cluster goes through here.
///
/// Construction connects the shared transport and creates its admin client.
pub struct KafkaClient {
    identity: ClusterIdentity,
    consume_timeout: Duration,
    krafka: KrafkaSharedClient,
    admin: KrafkaAdmin,
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
    pub async fn new(config: &ClusterConfig) -> Result<Self, KafkaError> {
        let identity = ClusterIdentity::from(config);
        let schema_registry = config
            .schema_registry
            .as_ref()
            .map(|registry| {
                SchemaRegistryClient::new(identity.name.clone(), registry).map(PayloadDecoder::new)
            })
            .transpose()?;

        let properties = &config.properties;
        let connect_timeout = Duration::from_millis(
            properties
                .connect_timeout_ms
                .unwrap_or(u64::from(*SOCKET_CONNECTION_SETUP_TIMEOUT_MS)),
        );
        let request_timeout = properties
            .request_timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(*REQUEST_TIMEOUT)
            .max(connect_timeout);
        let client_id = properties
            .client_id
            .clone()
            .unwrap_or_else(|| format!("{CLIENT_ID_PREFIX}-{}", config.name));

        let mut builder = KrafkaSharedClient::builder(config.bootstrap_servers.join(","))
            .client_id(client_id)
            .request_timeout(request_timeout)
            .connect_timeout(connect_timeout);

        if let Some(auth) = krafka_auth(config)? {
            builder = builder.auth(auth);
        }

        let krafka = builder.build().await?;
        let admin = KrafkaAdmin::builder()
            .with_client(&krafka)
            .request_timeout(request_timeout)
            .connect_timeout(connect_timeout)
            .build()
            .await?;

        Ok(Self {
            identity,
            consume_timeout: *CONSUME_TIMEOUT,
            krafka,
            admin,
            schema_registry,
        })
    }
}

#[async_trait]
impl ClusterSession for KafkaClient {
    fn identity(&self) -> &ClusterIdentity {
        &self.identity
    }

    fn consume_timeout(&self) -> Duration {
        self.consume_timeout
    }

    async fn metadata(&self) -> Result<MetadataSnapshot, KafkaError> {
        let cache = self.krafka.metadata();
        cache.refresh().await?;
        Ok(MetadataSnapshot::from_krafka(cache))
    }

    async fn groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
        let listed = self
            .admin
            .list_consumer_groups(&GroupListing::all())
            .await?;
        let ids: Vec<String> = listed
            .into_iter()
            .map(|group| group.group_id)
            .filter(|id| !is_internal_group(id))
            .collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut snapshots =
            snapshots_from_descriptions(self.admin.describe_consumer_groups(ids).await?);
        fill_classic_assignments(&self.krafka, &mut snapshots).await?;
        Ok(snapshots)
    }

    async fn group(&self, id: &str) -> Result<GroupSnapshot, KafkaError> {
        if is_internal_group(id) {
            return Err(KafkaError::UnknownGroup {
                cluster: self.identity.name.clone(),
                id: id.to_owned(),
            });
        }
        let described = self
            .admin
            .describe_consumer_groups(vec![id.to_owned()])
            .await?;
        let Some(mut snapshot) = snapshots_from_descriptions(described)
            .into_iter()
            .find(|snapshot| snapshot.id == id)
        else {
            return Err(KafkaError::UnknownGroup {
                cluster: self.identity.name.clone(),
                id: id.to_owned(),
            });
        };
        fill_classic_assignments(&self.krafka, std::slice::from_mut(&mut snapshot)).await?;
        Ok(snapshot)
    }

    /// Committed offsets for a group we are not a member of.
    async fn committed_offsets(
        &self,
        group_id: &str,
        partitions: &[(String, i32)],
    ) -> Result<Vec<CommittedOffset>, KafkaError> {
        if partitions.is_empty() {
            return Ok(Vec::new());
        }

        let topics = partitions_by_topic(partitions);
        let query = list_offset_query(&topics);
        let listed = self
            .admin
            .describe_consumer_group_offsets(
                group_id,
                Some(&query),
                OffsetVisibility::IncludeUnstable,
            )
            .await?;
        Ok(committed_from_krafka(listed))
    }

    /// Low and high watermarks. Does not refetch cluster metadata.
    async fn watermarks(
        &self,
        partitions: &[(String, i32)],
    ) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError> {
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }

        let topics = partitions_by_topic(partitions);
        let query = list_offset_query(&topics);
        let (beginning, end) = tokio::try_join!(
            self.admin.list_offsets(&query, OffsetSpec::Earliest),
            self.admin.list_offsets(&query, OffsetSpec::Latest),
        )?;
        Ok(merge_watermark_offsets(
            &from_list_offsets(beginning.into_iter().map(list_offset_parts)),
            &from_list_offsets(end.into_iter().map(list_offset_parts)),
        ))
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

        let listed = self
            .admin
            .list_offsets(&[(topic, partitions)], OffsetSpec::Timestamp(timestamp))
            .await?;
        Ok(partition_time_offsets(from_list_offsets(
            listed.into_iter().map(list_offset_parts),
        )))
    }

    async fn topic_configs(
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
        let results = self.admin.describe_configs_per_resource(request).await?;

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

    async fn broker_configs(&self, broker_id: i32) -> Result<Vec<ConfigEntry>, KafkaError> {
        let results = self
            .admin
            .describe_configs_per_resource(DescribeConfigsRequest::for_broker(broker_id))
            .await?;

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
    async fn records(&self, plan: &FetchPlan) -> Result<Vec<Record>, KafkaError> {
        if plan.windows.is_empty() || plan.limit == 0 {
            return Ok(Vec::new());
        }
        let deadline = tokio::time::Instant::now() + self.consume_timeout;
        tokio::time::timeout_at(
            deadline,
            browse::fetch(&self.krafka, plan, self.schema_registry.as_ref(), deadline),
        )
        .await
        .map_err(|_| KafkaError::Timeout)?
    }

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        let Some(decoder) = &self.schema_registry else {
            return Ok(Vec::new());
        };
        decoder.client().subjects().await
    }

    async fn acls(&self) -> Result<AclListing, KafkaError> {
        AclListing::from_admin_result(
            &self.identity.name,
            self.admin.describe_acls(AclFilter::all()).await,
        )
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

        let client = kafka_client(&broker.bootstrap_servers()).await;
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
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        let client = kafka_client(&broker.bootstrap_servers()).await;
        broker.clear_requests();
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
        assert!(broker.requests().is_empty());
    }

    #[tokio::test]
    async fn broker_io_is_bounded_by_the_configured_request_timeout() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        let client = KafkaClient::new(&ClusterConfig {
            name: "test".into(),
            bootstrap_servers: vec![broker.bootstrap_servers()],
            security: None,
            schema_registry: None,
            properties: crate::config::KafkaProperties {
                request_timeout_ms: Some(100),
                connect_timeout_ms: Some(100),
                ..Default::default()
            },
        })
        .await
        .unwrap();
        assert_eq!(client.admin.request_timeout(), Duration::from_millis(100));
        broker.clear_requests();
        broker.on(krafka::protocol::ApiKey::Metadata, |_| {
            krafka::testing::Control::Silence
        });
        let error = tokio::time::timeout(Duration::from_secs(2), client.metadata())
            .await
            .expect("krafka must bound the request without an adapter timeout")
            .unwrap_err();
        assert!(matches!(error, KafkaError::Krafka(_)), "{error:?}");
        assert!(
            broker.request_count(krafka::protocol::ApiKey::Metadata) > 0,
            "{error:?}"
        );
        client.admin.close().await;
        client.krafka.pool().close_all().await;
    }

    #[tokio::test]
    async fn metadata_reads_topics_and_partitions_from_broker() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 2));

        let client = kafka_client(&broker.bootstrap_servers()).await;
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

        let client = kafka_client(&broker.bootstrap_servers()).await;
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

        let client = kafka_client(&broker.bootstrap_servers()).await;
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
    async fn consecutive_scans_share_transport_without_sharing_positions() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 4).await;

        let client = kafka_client(&broker.bootstrap_servers()).await;

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

        let first = client.records(&window(2, 4)).await.expect("first pass");
        let second = client.records(&window(0, 2)).await.expect("second pass");

        assert_eq!(
            first.iter().map(|record| record.offset).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(
            second
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        // Each scan only clamps its high watermark: initial_offsets avoids
        // looking up a reset position that would immediately be overwritten.
        assert_eq!(
            broker.request_count(krafka::protocol::ApiKey::ListOffsets),
            2
        );
        assert_eq!(broker.request_count(krafka::protocol::ApiKey::JoinGroup), 0);

        let first_plan = window(0, 2);
        let second_plan = window(2, 4);
        let (first, second) =
            tokio::try_join!(client.records(&first_plan), client.records(&second_plan),)
                .expect("independent concurrent scans");
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
    async fn scan_drains_prefetched_records_before_completing() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 550).await;
        let client = kafka_client(&broker.bootstrap_servers()).await;
        let records = client
            .records(&FetchPlan {
                topic: "orders".into(),
                windows: vec![PartitionWindow {
                    partition: 0,
                    start: 0,
                    end: 550,
                }],
                filter: None,
                limit: 550,
                order: RecordOrder::Oldest,
                schema_id: None,
            })
            .await
            .unwrap();
        assert_eq!(
            records
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            (0..550).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn record_deadline_bounds_broker_io() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 1));
        let mut client = kafka_client(&broker.bootstrap_servers()).await;
        broker.on(krafka::protocol::ApiKey::ListOffsets, |_| {
            krafka::testing::Control::Silence
        });
        client.consume_timeout = Duration::from_millis(50);
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            client.records(&FetchPlan {
                topic: "orders".into(),
                windows: vec![PartitionWindow {
                    partition: 0,
                    start: 0,
                    end: 1,
                }],
                filter: None,
                limit: 1,
                order: RecordOrder::Oldest,
                schema_id: None,
            }),
        )
        .await
        .expect("broker I/O must honor the scan deadline")
        .unwrap_err();
        assert!(matches!(error, KafkaError::Timeout));
    }

    #[tokio::test]
    async fn lists_committed_offsets() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 1).await;

        let client = kafka_client(&broker.bootstrap_servers()).await;
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

        let client = Arc::new(kafka_client(&broker.bootstrap_servers()).await);
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

    async fn kafka_client(bootstrap: &str) -> KafkaClient {
        KafkaClient::new(&ClusterConfig {
            name: "test".into(),
            bootstrap_servers: vec![bootstrap.to_owned()],
            security: None,
            schema_registry: None,
            properties: Default::default(),
        })
        .await
        .expect("kafka client")
    }

    pub(super) async fn produce_krafka(bootstrap: &str, topic: &str, count: usize) {
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
            .admin
            .alter_consumer_group_offsets(group, &[(topic, &[(0, offset)])])
            .await
            .expect("commit");
    }
}
