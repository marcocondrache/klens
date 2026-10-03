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

use crate::config::{self, Tuning};
use crate::kafka::acl::AclListing;
use crate::kafka::cluster::ClusterIdentity;
use crate::kafka::error::KafkaError;
use crate::kafka::group::{CommittedOffset, GroupSnapshot};
use crate::kafka::metadata::{MetadataSnapshot, NewTopic, TopicMetadata};
use crate::kafka::model::{PartitionWindow, ScanConsumer, TailConsumer, TailPosition};
use crate::kafka::quota::{DescribedQuota, QuotaListing};
use crate::kafka::registry::client::SchemaRegistryClient;
use crate::kafka::registry::decode::PayloadDecoder;
use crate::kafka::registry::{RegisteredSchema, SchemaSubject};
use crate::kafka::scan::obfuscate::ObfuscationPolicy;
use crate::kafka::scan::payload::PayloadCodec;
use crate::kafka::session::ClusterSession;
use crate::kafka::storage::LogDir;
use crate::kafka::topic_config::ConfigEntry;

use convert::{committed_from_krafka, refused};
use groups::{
    ACTIVE_GROUP_STATES, LISTED_GROUP_TYPES, snapshots_from_descriptions, split_empty_groups,
};
use offsets::{known_offsets, partition_time_offsets};
use pool::ScanPool;
use scan::ReaderConfig;
use tail::TailLease;

pub struct KafkaClient {
    identity: ClusterIdentity,
    consume_timeout: Duration,
    scan_poll_wait: Duration,
    tail_reader: ReaderConfig,
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
    pub async fn new(
        name: &str,
        cluster: &config::Cluster,
        tuning: &Tuning,
    ) -> Result<Self, KafkaError> {
        let identity = ClusterIdentity::new(name);
        let schema_registry = cluster
            .schema_registry
            .as_ref()
            .map(|registry| {
                SchemaRegistryClient::new(identity.name.clone(), registry, &tuning.schema_registry)
                    .map(|client| {
                        Arc::new(PayloadDecoder::new(
                            client,
                            tuning.schema_registry.missing_schema_ttl,
                        ))
                    })
            })
            .transpose()?;

        let obfuscation = cluster
            .obfuscation
            .as_ref()
            .map(|rules| Arc::new(ObfuscationPolicy::compile(rules)));

        let transport = transport::connect(name, cluster, &tuning.kafka).await?;
        let scan_client = transport.connector.connect().await?;

        Ok(Self {
            identity,
            consume_timeout: tuning.kafka.consume_timeout,
            scan_poll_wait: tuning.scan.poll_wait,
            tail_reader: ReaderConfig::new(tuning, tuning.tail.poll_wait),
            scans: ScanPool::spawn(
                scan_client,
                &tuning.scan,
                ReaderConfig::new(tuning, tuning.scan.poll_wait),
            ),
            transport,
            schema_registry,
            obfuscation,
        })
    }

    async fn list_offsets(
        &self,
        topics: &HashMap<String, Vec<i32>>,
        spec: OffsetSpec,
    ) -> Result<HashMap<String, HashMap<i32, i64>>, KafkaError> {
        let query = list_offset_query(topics);
        if query.is_empty() {
            return Ok(HashMap::new());
        }

        let listed = self.transport.admin.list_offsets(&query, spec).await?;
        Ok(known_offsets(listed.into_iter().map(list_offset_parts)))
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

    fn scan_poll_wait(&self) -> Duration {
        self.scan_poll_wait
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
        let admin = &self.transport.admin;
        let listing = GroupListing::all().of_types(LISTED_GROUP_TYPES);
        let active_listing = listing.clone().in_states(ACTIVE_GROUP_STATES);
        let (listed, active) = tokio::try_join!(
            admin.list_consumer_groups(&listing),
            admin.list_consumer_groups(&active_listing),
        )?;
        let (active, mut groups) = split_empty_groups(
            listed.into_iter().map(|group| group.group_id),
            active.into_iter().map(|group| group.group_id),
        );
        groups.extend(snapshots_from_descriptions(
            admin.describe_consumer_groups(active).await?,
        ));
        Ok(groups)
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

    async fn low_watermarks(
        &self,
        topics: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, i64>>, KafkaError> {
        self.list_offsets(topics, OffsetSpec::Earliest).await
    }

    async fn high_watermarks(
        &self,
        topics: &HashMap<String, Vec<i32>>,
    ) -> Result<HashMap<String, HashMap<i32, i64>>, KafkaError> {
        self.list_offsets(topics, OffsetSpec::Latest).await
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

    async fn log_dirs(&self) -> Result<Vec<LogDir>, KafkaError> {
        let dirs = self.transport.admin.describe_log_dirs(None).await?;
        Ok(dirs.into_iter().map(LogDir::from_krafka).collect())
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
            TailLease::open(&self.transport.connector, topic, start, self.tail_reader).await?,
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

    async fn client_quotas(&self) -> Result<QuotaListing, KafkaError> {
        let described = self
            .transport
            .admin
            .describe_client_quotas(&[], false)
            .await?;
        QuotaListing::from_describe(
            &self.identity.name,
            described.error.as_deref(),
            described.entries.into_iter().map(|entry| DescribedQuota {
                entity: entry
                    .entity
                    .into_iter()
                    .map(|part| (part.entity_type, part.entity_name))
                    .collect(),
                values: entry
                    .values
                    .into_iter()
                    .map(|value| (value.key, value.value))
                    .collect(),
            }),
        )
    }

    async fn create_topic(&self, topic: &NewTopic) -> Result<(), KafkaError> {
        let admin = &self.transport.admin;
        admin
            .create_topics(vec![topic.to_krafka()?], admin.request_timeout(), false)
            .await?
            .into_iter()
            .try_for_each(|created| refused(created.error))
    }

    async fn delete_topic(&self, topic: &str) -> Result<(), KafkaError> {
        let admin = &self.transport.admin;
        admin
            .delete_topics(vec![topic.to_owned()], admin.request_timeout())
            .await?
            .into_iter()
            .try_for_each(|deleted| refused(deleted.error))
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
    use std::collections::BTreeMap;
    use std::num::NonZeroU16;

    use super::*;
    use crate::kafka::group::MemberAssignment;
    use crate::kafka::metadata::Watermarks;
    use crate::kafka::model::{PartitionWindow, RecordOrder, RecordQuery, TimestampRange};
    use crate::kafka::scan::session::{fetch_page, scan_once};
    use crate::kafka::session::watermarks;
    use crate::testing::{LogCapture, yaml};

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

    #[tokio::test]
    async fn connecting_does_not_warn_about_the_connection_memory_ceiling() {
        let logs = LogCapture::at(tracing::Level::WARN);

        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        let client = kafka_client(&broker.bootstrap_servers()).await;
        client.metadata().await.expect("metadata");

        logs.assert_lacks("memory ceiling");
    }

    #[tokio::test]
    async fn admin_calls_do_not_warn_about_missing_close() {
        let logs = LogCapture::at(tracing::Level::WARN);

        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 1).await;

        let client = kafka_client(&broker.bootstrap_servers()).await;
        watermarks(&client, &wanted("orders", &[0]))
            .await
            .expect("watermarks");
        watermarks(&client, &wanted("orders", &[0]))
            .await
            .expect("second watermarks");

        logs.assert_lacks("AdminClient dropped without close");
    }

    #[tokio::test]
    async fn empty_partitions_skip_kafka() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        let client = kafka_client(&broker.bootstrap_servers()).await;
        broker.clear_requests();
        let offsets = client.committed_offsets("unused", Some(&[])).await.unwrap();
        assert!(offsets.is_empty());
        assert!(
            client
                .low_watermarks(&HashMap::new())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            client
                .high_watermarks(&HashMap::new())
                .await
                .unwrap()
                .is_empty()
        );
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
    async fn the_scan_poll_wait_comes_from_tuning() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        let mut tuning = Tuning::default();
        tuning.scan.poll_wait = Duration::from_millis(250);
        let client = KafkaClient::new("test", &cluster(&broker.bootstrap_servers()), &tuning)
            .await
            .unwrap();

        assert_eq!(client.scan_poll_wait(), Duration::from_millis(250));
        client.transport.admin.close().await;
        client.transport.client.pool().close_all().await;
    }

    #[tokio::test]
    async fn broker_io_is_bounded_by_the_configured_request_timeout() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        let mut tuning = Tuning::default();
        tuning.kafka.request_timeout = Duration::from_millis(100);
        tuning.kafka.connect_timeout = Duration::from_millis(100);
        let client = KafkaClient::new("test", &cluster(&broker.bootstrap_servers()), &tuning)
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
    async fn a_broker_that_does_not_serve_list_groups_fails_the_call() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        let client = kafka_client(&broker.bootstrap_servers()).await;
        client.metadata().await.expect("metadata");

        let error = client.groups().await.unwrap_err();

        assert!(matches!(error, KafkaError::Krafka(_)), "{error:?}");
    }

    #[tokio::test]
    async fn a_broker_that_does_not_serve_describe_log_dirs_fails_the_call() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        let client = kafka_client(&broker.bootstrap_servers()).await;
        client.metadata().await.expect("metadata");

        let error = client.log_dirs().await.unwrap_err();

        assert!(matches!(error, KafkaError::Krafka(_)), "{error:?}");
    }

    #[tokio::test]
    async fn a_broker_that_does_not_serve_describe_client_quotas_fails_the_call() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        let client = kafka_client(&broker.bootstrap_servers()).await;
        client.metadata().await.expect("metadata");

        let error = client.client_quotas().await.unwrap_err();

        assert!(matches!(error, KafkaError::Krafka(_)), "{error:?}");
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
        assert_eq!(
            topic
                .partitions
                .iter()
                .map(|partition| partition.id)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
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
        let marks = watermarks(&client, &wanted("orders", &[0]))
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
    async fn low_and_high_watermarks_list_one_end_each() {
        let broker = krafka::testing::FakeBroker::start()
            .await
            .expect("fake broker");
        assert!(broker.create_topic("orders", 2));
        broker.with_state(|state| {
            for (id, low, high) in [(0, 1, 5), (1, 0, 2)] {
                let partition = state.partition_mut("orders", id).expect("partition");
                partition.log_start_offset = low;
                partition.next_offset = high;
            }
        });
        let client = kafka_client(&broker.bootstrap_servers()).await;
        client.metadata().await.expect("metadata");
        let both = wanted("orders", &[0, 1]);

        broker.clear_requests();
        let lows = client.low_watermarks(&both).await.expect("low watermarks");
        assert_eq!(
            broker.request_count(krafka::protocol::ApiKey::ListOffsets),
            1
        );

        broker.clear_requests();
        let highs = client
            .high_watermarks(&both)
            .await
            .expect("high watermarks");
        assert_eq!(
            broker.request_count(krafka::protocol::ApiKey::ListOffsets),
            1
        );
        assert_eq!(lows["orders"], HashMap::from_iter([(0, 1), (1, 0)]));
        assert_eq!(highs["orders"], HashMap::from_iter([(0, 5), (1, 2)]));
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

        let marks = watermarks(&client, &topics).await.expect("watermarks");
        assert_eq!(marks["orders"][&0], Watermarks { low: 0, high: 1 });
        assert!(!marks.contains_key(""));

        broker.clear_requests();
        assert!(
            client
                .low_watermarks(&wanted("", &[0]))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            client
                .high_watermarks(&wanted("", &[0]))
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
    async fn a_round_trip_slower_than_the_poll_max_wait_still_delivers() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 1));
        produce_krafka(&broker.bootstrap_servers(), "orders", 2).await;
        let client = kafka_client(&broker.bootstrap_servers()).await;
        let slow = client.scan_poll_wait * 3;
        broker.on(krafka::protocol::ApiKey::Fetch, move |_| {
            krafka::testing::Control::Delay(slow)
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

    #[tokio::test]
    async fn create_topic_creates_it_once() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        let client = kafka_client(&broker.bootstrap_servers()).await;
        let topic = NewTopic {
            name: "orders".into(),
            partitions: NonZeroU16::new(3),
            replication_factor: None,
            configs: BTreeMap::new(),
        };

        client.create_topic(&topic).await.expect("created");
        let created = client.topic_metadata("orders").await.unwrap();
        assert_eq!(created.partitions.len(), 3);

        let error = client.create_topic(&topic).await.unwrap_err();
        assert!(
            matches!(&error, KafkaError::Refused(message) if message.contains("already exists")),
            "{error}"
        );
    }

    #[tokio::test]
    async fn delete_topic_deletes_it_once() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 2));
        let client = kafka_client(&broker.bootstrap_servers()).await;

        client.delete_topic("orders").await.expect("deleted");
        assert!(!broker.with_state(|state| state.topics.contains_key("orders")));

        let error = client.delete_topic("orders").await.unwrap_err();
        assert!(matches!(error, KafkaError::Refused(_)), "{error}");
    }

    pub(super) fn cluster(bootstrap: &str) -> config::Cluster {
        yaml(&format!("bootstrap_servers: ['{bootstrap}']"))
    }

    pub(super) async fn kafka_client(bootstrap: &str) -> KafkaClient {
        KafkaClient::new("test", &cluster(bootstrap), &Tuning::default())
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

    #[tokio::test]
    async fn a_finished_partition_read_ahead_does_not_starve_the_others() {
        let broker = krafka::testing::FakeBroker::start().await.unwrap();
        assert!(broker.create_topic("orders", 2));
        produce_to_partitions(&broker.bootstrap_servers(), "orders", 2, 2_000).await;
        let client = kafka_client(&broker.bootstrap_servers()).await;
        let watermarks: HashMap<i32, Watermarks> = (0..2)
            .map(|partition| {
                (
                    partition,
                    Watermarks {
                        low: 0,
                        high: 2_000,
                    },
                )
            })
            .collect();
        let query = RecordQuery {
            topic: "orders".into(),
            partitions: vec![0, 1],
            filter: None,
            timestamps: TimestampRange::UNBOUNDED,
            limit: 50,
            order: RecordOrder::Oldest,
            cursor: None,
            schema_id: None,
        };

        let started = std::time::Instant::now();
        let page = fetch_page(
            &client,
            &query,
            &[0, 1],
            &watermarks,
            50,
            Tuning::default().records,
        )
        .await
        .expect("page");

        assert!(
            page.complete,
            "a full buffer of a paused partition must not stop the others being fetched"
        );
        assert!(started.elapsed() < client.consume_timeout / 2);
        assert_eq!(page.records.len(), 50);
    }

    async fn produce_to_partitions(bootstrap: &str, topic: &str, partitions: i32, count: usize) {
        let producer = krafka::producer::Producer::builder()
            .bootstrap_servers(bootstrap)
            .build()
            .await
            .expect("producer");
        let mut acks = futures::stream::FuturesUnordered::new();
        for partition in 0..partitions {
            for _ in 0..count {
                let record = krafka::producer::ProducerRecord::new(topic, &b"hello"[..])
                    .with_partition(partition);
                acks.push(producer.enqueue(record).await.expect("enqueue"));
            }
        }
        while let Some(ack) = futures::StreamExt::next(&mut acks).await {
            let _metadata = ack.expect("produce");
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
