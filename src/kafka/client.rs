mod convert;
mod groups;
mod offsets;
mod pool;
mod scan;
mod tail;
mod transport;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use foldhash::{HashMap, HashMapExt};
use krafka::admin::{
    AclFilter, ConfigResourceType, DescribeConfigsRequest, DescribeConfigsResource, GroupListing,
    OffsetSpec, OffsetVisibility,
};

use crate::config::ClusterConfig;
use crate::environment::CONSUME_TIMEOUT;
use crate::kafka::acl::AclListing;
use crate::kafka::cluster::ClusterIdentity;
use crate::kafka::error::KafkaError;
use crate::kafka::group::{CommittedOffset, GroupSnapshot, is_internal_group};
use crate::kafka::metadata::{MetadataSnapshot, TopicMetadata, Watermarks};
use crate::kafka::model::{PartitionWindow, ScanConsumer, TailConsumer, TailPosition};
use crate::kafka::registry::client::SchemaRegistryClient;
use crate::kafka::registry::decode::PayloadDecoder;
use crate::kafka::registry::{RegisteredSchema, SchemaSubject};
use crate::kafka::scan::obfuscate::ObfuscationPolicy;
use crate::kafka::scan::payload::PayloadCodec;
use crate::kafka::session::ClusterSession;
use crate::kafka::topic_config::ConfigEntry;

use convert::committed_from_krafka;
use groups::snapshots_from_descriptions;
use offsets::{from_list_offsets, merge_watermark_offsets, partition_time_offsets};
use pool::ScanPool;
use tail::TailLease;

pub struct KafkaClient {
    identity: ClusterIdentity,
    consume_timeout: Duration,
    transport: transport::Transport,
    scans: Arc<ScanPool>,
    schema_registry: Option<Arc<PayloadDecoder>>,
    obfuscation: Option<Arc<ObfuscationPolicy>>,
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
                SchemaRegistryClient::new(identity.name.clone(), registry)
                    .map(|client| Arc::new(PayloadDecoder::new(client)))
            })
            .transpose()?;

        let obfuscation = config
            .obfuscation
            .as_ref()
            .map(|rules| {
                ObfuscationPolicy::compile(rules)
                    .map(Arc::new)
                    .map_err(|error| KafkaError::Obfuscation {
                        cluster: identity.name.clone(),
                        message: error.to_string(),
                    })
            })
            .transpose()?;

        let transport = transport::connect(config).await?;

        Ok(Self {
            identity,
            consume_timeout: *CONSUME_TIMEOUT,
            scans: ScanPool::spawn(&transport),
            transport,
            schema_registry,
            obfuscation,
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
        let cache = self.transport.client.metadata();
        cache.refresh().await?;
        Ok(MetadataSnapshot::from_krafka(cache))
    }

    async fn topic_metadata(&self, topic: &str) -> Result<TopicMetadata, KafkaError> {
        let cache = self.transport.client.metadata();
        cache.refresh_for_topics(Some(&[topic])).await?;
        cache
            .topic(topic)
            .map(TopicMetadata::from_krafka)
            .ok_or_else(|| KafkaError::UnknownTopic {
                cluster: self.identity.name.clone(),
                topic: topic.to_owned(),
            })
    }

    async fn groups(&self) -> Result<Vec<GroupSnapshot>, KafkaError> {
        let listed = self
            .transport
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
        Ok(snapshots_from_descriptions(
            self.transport.admin.describe_consumer_groups(ids).await?,
        ))
    }

    async fn committed_offsets(
        &self,
        group_id: &str,
        partitions: Option<&[(String, i32)]>,
    ) -> Result<Vec<CommittedOffset>, KafkaError> {
        let topics = partitions.map(partitions_by_topic);
        let query = topics.as_ref().map(list_offset_query);
        if query.as_ref().is_some_and(Vec::is_empty) {
            return Ok(Vec::new());
        }
        let listed = self
            .transport
            .admin
            .describe_consumer_group_offsets(
                group_id,
                query.as_deref(),
                OffsetVisibility::IncludeUnstable,
            )
            .await?;
        Ok(committed_from_krafka(listed))
    }

    async fn watermarks(
        &self,
        topics: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, Watermarks>>, KafkaError> {
        let query = list_offset_query(topics);
        if query.is_empty() {
            return Ok(HashMap::new());
        }

        let (beginning, end) = tokio::try_join!(
            self.transport
                .admin
                .list_offsets(&query, OffsetSpec::Earliest),
            self.transport
                .admin
                .list_offsets(&query, OffsetSpec::Latest),
        )?;
        Ok(merge_watermark_offsets(
            &from_list_offsets(beginning.into_iter().map(list_offset_parts)),
            end.into_iter().map(list_offset_parts),
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
            .transport
            .admin
            .list_offsets(&[(topic, partitions)], OffsetSpec::Timestamp(timestamp))
            .await?;
        Ok(partition_time_offsets(
            listed.into_iter().map(list_offset_parts),
        ))
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
        let results = self
            .transport
            .admin
            .describe_configs_per_resource(request)
            .await?;

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
            .transport
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

    async fn open_scan(
        &self,
        topic: &str,
        windows: &[PartitionWindow],
    ) -> Result<Box<dyn ScanConsumer>, KafkaError> {
        Ok(Box::new(self.scans.acquire(topic, windows).await?))
    }

    async fn open_tail(
        &self,
        topic: &str,
        start: &[TailPosition],
    ) -> Result<Box<dyn TailConsumer>, KafkaError> {
        Ok(Box::new(
            TailLease::open(&self.transport.connector, topic, start).await?,
        ))
    }

    fn payload_codec(&self) -> Option<Arc<dyn PayloadCodec>> {
        self.schema_registry
            .clone()
            .map(|decoder| decoder as Arc<dyn PayloadCodec>)
    }

    fn obfuscation(&self) -> Option<Arc<ObfuscationPolicy>> {
        self.obfuscation.clone()
    }

    async fn schema_subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        let Some(decoder) = &self.schema_registry else {
            return Ok(Vec::new());
        };
        decoder.client().subjects().await
    }

    async fn subject_schema(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        let Some(decoder) = &self.schema_registry else {
            return Err(KafkaError::UnknownSubject {
                cluster: self.identity.name.clone(),
                subject: subject.to_owned(),
                version,
            });
        };
        decoder
            .client()
            .schema_by_subject_version(subject, version)
            .await
    }

    async fn acls(&self) -> Result<AclListing, KafkaError> {
        AclListing::from_admin_result(
            &self.identity.name,
            self.transport.admin.describe_acls(AclFilter::all()).await,
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
        .filter(|(topic, _)| krafka::protocol::validate_topic_name(topic).is_ok())
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
    use crate::environment::SCAN_PACE_BOUND;
    use crate::kafka::group::MemberAssignment;
    use crate::kafka::model::{PartitionWindow, RecordOrder};
    use crate::kafka::scan::session::scan_once;

    fn window(partition: i32, start: i64, end: i64) -> PartitionWindow {
        PartitionWindow {
            partition,
            start,
            end,
        }
    }

    fn wanted(topic: &str, partitions: &[i32]) -> HashMap<String, Vec<i32>> {
        HashMap::from_iter([(topic.to_owned(), partitions.to_vec())])
    }

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
    async fn connecting_does_not_warn_about_the_connection_memory_ceiling() {
        let logs = LogBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::WARN)
            .without_time()
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        let client = kafka_client(&broker.bootstrap_servers()).await;
        client.metadata().await.expect("metadata");

        let text = logs.as_string();
        assert!(!text.contains("memory ceiling"), "{text}");
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
            .watermarks(&wanted("orders", &[0]))
            .await
            .expect("watermarks");
        client
            .watermarks(&wanted("orders", &[0]))
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
        let offsets = client.committed_offsets("unused", Some(&[])).await.unwrap();
        assert!(offsets.is_empty());
        assert!(client.watermarks(&HashMap::new()).await.unwrap().is_empty());
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
            obfuscation: None,
            properties: crate::config::KafkaProperties {
                request_timeout_ms: Some(100),
                connect_timeout_ms: Some(100),
                ..Default::default()
            },
            ingest: Default::default(),
        })
        .await
        .unwrap();
        assert_eq!(
            client.transport.admin.request_timeout(),
            Duration::from_millis(100)
        );
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
        client.transport.admin.close().await;
        client.transport.client.pool().close_all().await;
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
            .watermarks(&wanted("orders", &[0]))
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
    async fn an_illegal_topic_name_does_not_fail_the_other_watermarks() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 1).await;

        let client = kafka_client(&broker.bootstrap_servers()).await;
        let mut topics = wanted("orders", &[0]);
        topics.insert(String::new(), vec![0]);

        let marks = client.watermarks(&topics).await.expect("watermarks");
        assert_eq!(marks["orders"][&0], Watermarks { low: 0, high: 1 });
        assert!(!marks.contains_key(""));

        broker.clear_requests();
        assert!(
            client
                .watermarks(&wanted("", &[0]))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            client
                .committed_offsets("unused", Some(&[(String::new(), 0)]))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(broker.requests().is_empty());
    }

    #[tokio::test]
    async fn client_reads_records() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 1).await;

        let client = kafka_client(&broker.bootstrap_servers()).await;
        let records = scan_once(
            &client,
            "orders",
            &[window(0, 0, 1)],
            10,
            RecordOrder::Oldest,
        )
        .await
        .expect("records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[0].value.as_deref(), Some("hello"));
        assert_eq!(records[0].key.as_deref(), Some("k"));

        let past_high = scan_once(
            &client,
            "orders",
            &[window(0, 5, 10)],
            10,
            RecordOrder::Oldest,
        )
        .await
        .expect("empty past high watermark");
        assert!(past_high.is_empty());
    }

    #[tokio::test]
    async fn consecutive_scans_reuse_a_consumer_without_looking_offsets_up() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 4).await;

        let client = kafka_client(&broker.bootstrap_servers()).await;

        let pass = async |start: i64, end: i64| {
            scan_once(
                &client,
                "orders",
                &[window(0, start, end)],
                10,
                RecordOrder::Oldest,
            )
            .await
        };

        let first = pass(2, 4).await.expect("first pass");
        let second = pass(0, 2).await.expect("second pass");

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
        assert_eq!(
            broker.request_count(krafka::protocol::ApiKey::ListOffsets),
            0,
            "a window start is known before the consumer exists"
        );
        assert_eq!(broker.request_count(krafka::protocol::ApiKey::JoinGroup), 0);

        let (first, second) =
            tokio::try_join!(pass(0, 2), pass(2, 4)).expect("independent concurrent scans");
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
        let records = scan_once(
            &client,
            "orders",
            &[window(0, 0, 550)],
            550,
            RecordOrder::Oldest,
        )
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
    async fn a_round_trip_slower_than_the_poll_budget_still_delivers() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 2).await;
        let client = kafka_client(&broker.bootstrap_servers()).await;
        broker.on(krafka::protocol::ApiKey::Fetch, |_| {
            krafka::testing::Control::Delay(*SCAN_PACE_BOUND * 3)
        });

        let records = scan_once(
            &client,
            "orders",
            &[window(0, 0, 2)],
            2,
            RecordOrder::Oldest,
        )
        .await
        .expect("a slow round trip is not a timeout");

        assert_eq!(
            records
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[tokio::test]
    async fn record_deadline_bounds_broker_io() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 1).await;
        let mut client = kafka_client(&broker.bootstrap_servers()).await;
        broker.on(krafka::protocol::ApiKey::Fetch, |_| {
            krafka::testing::Control::Silence
        });
        client.consume_timeout = Duration::from_millis(50);
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            scan_once(
                &client,
                "orders",
                &[window(0, 0, 1)],
                1,
                RecordOrder::Oldest,
            ),
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
            .committed_offsets("orders-group", Some(&[("orders".into(), 0)]))
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
    async fn lists_every_committed_offset_without_a_partition_filter() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 1).await;

        let client = kafka_client(&broker.bootstrap_servers()).await;
        commit_krafka(&client, "orders-group", "orders", 1).await;
        let offsets = client
            .committed_offsets("orders-group", None)
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
    async fn describes_classic_group_member_assignments() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 2));

        let consumer = krafka::consumer::Consumer::builder()
            .bootstrap_servers(broker.bootstrap_servers())
            .group_id("orders-group")
            .request_timeout(Duration::from_secs(2))
            .connect_timeout(Duration::from_secs(2))
            .build()
            .await
            .expect("consumer");
        consumer.subscribe(&["orders"]).await.expect("subscribe");
        assert!(
            broker
                .wait_for_requests(
                    krafka::protocol::ApiKey::SyncGroup,
                    1,
                    Duration::from_secs(15)
                )
                .await,
            "the consumer must finish join and sync before describe"
        );

        let client = kafka_client(&broker.bootstrap_servers()).await;
        let described = client
            .transport
            .admin
            .describe_consumer_groups(vec!["orders-group".to_owned()])
            .await
            .expect("describe group");
        let group = snapshots_from_descriptions(described)
            .into_iter()
            .find(|group| group.id == "orders-group")
            .expect("orders-group");
        assert_eq!(
            group.members[0].assignments,
            vec![MemberAssignment {
                topic: "orders".into(),
                partitions: vec![0, 1],
            }]
        );
        consumer.close().await.expect("close consumer");
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
                    .committed_offsets(&format!("g{index}"), Some(&[("orders".into(), 0)]))
                    .await
            }
        });
        let results = futures::future::join_all(fetches).await;
        assert_eq!(results.len(), 8);
        for result in results {
            assert_eq!(result.expect("offset fetch")[0].offset, 1);
        }
    }

    pub(super) async fn kafka_client(bootstrap: &str) -> KafkaClient {
        KafkaClient::new(&ClusterConfig {
            name: "test".into(),
            bootstrap_servers: vec![bootstrap.to_owned()],
            security: None,
            schema_registry: None,
            obfuscation: None,
            properties: Default::default(),
            ingest: Default::default(),
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
            .transport
            .admin
            .alter_consumer_group_offsets(group, &[(topic, &[(0, offset)])])
            .await
            .expect("commit");
    }
}
