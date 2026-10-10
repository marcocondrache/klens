use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::num::{NonZeroU8, NonZeroU16};

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::app::auth::access::Privilege;
use crate::app::context::{ClusterHandle, Session};
use crate::app::error::ApiError;
use crate::app::mcp::args::{NameFilter, ResponseFormat, input, largest_first};
use crate::app::mcp::configs::{ConfigSection, Section};
use crate::app::mcp::ext::{ClusterExt as _, SessionExt as _};
use crate::app::mcp::gate::ToolGate;
use crate::app::mcp::reply::{Page, Rows, fit, reply};
use crate::app::mcp::server::{KlensMcp, ToolResult};
use crate::app::mcp::untrusted::Boundary;
use crate::app::mcp::{CLIENT_VALUES_NOTICE, MAX_ROWS};
use crate::app::topics::CreateTopic;
use crate::app::topics::types::{CleanupPolicy, TopicGroupRow};
use crate::kafka::KafkaError;
use crate::kafka::store::projections::{self, TopicDetail};
use crate::kafka::store::{TopicInfo, WatermarkTable};

use super::schemas::SubjectRow;

#[derive(Clone, Copy, Default, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[schemars(inline)]
enum TopicSort {
    #[default]
    Name,
    Size,
    Rate,
    Records,
    Partitions,
    Groups,
}

input! {
    struct TopicsQuery {
        /// Keeps topics whose name holds this text, in any case.
        name_contains: Option<String>,
        /// True keeps topics with a partition short of in-sync replicas, false keeps the others.
        under_replicated: Option<bool>,
        /// True keeps topics with no records, false keeps the others. Both leave out the topics klens has not measured, and `unmeasured` counts them.
        empty: Option<bool>,
        /// Also lists Kafka's internal topics, such as __consumer_offsets.
        #[serde(default)]
        include_internal: bool,
        /// NAME unless given.
        #[serde(default)]
        sort: TopicSort,
        /// How many topics to return: 25 unless given, at most 100.
        #[schemars(range(min = 1, max = MAX_ROWS))]
        limit: Option<usize>,
        /// CONCISE unless given.
        #[serde(default)]
        response_format: ResponseFormat,
    }
}

input! {
    struct TopicName {
        /// The topic's exact name.
        topic: String,
    }
}

input! {
    struct TopicToCreate {
        /// The new topic's name, of letters, digits, `.`, `_` and `-`.
        topic: String,
        /// The broker's num.partitions unless given.
        partitions: Option<NonZeroU16>,
        /// The broker's default.replication.factor unless given.
        replication_factor: Option<NonZeroU8>,
        /// Topic configs to set, such as cleanup.policy or retention.ms.
        #[serde(default)]
        configs: BTreeMap<String, String>,
    }
}

pub(super) const GATES: &[ToolGate] = &[
    ToolGate::open("klens_topics_list"),
    ToolGate::needing("klens_topic_create", Privilege::CreateTopics).not_idempotent(),
    ToolGate::open("klens_topic_describe")
        .with_sections(&[(Section::Configs, Privilege::TopicConfigs)]),
];

/// Watermarks default to zero, so a partition klens has not read would
/// otherwise count as empty. Whether every partition of the topic is read.
fn measured(watermarks: Option<&WatermarkTable>, name: &str, topic: &TopicInfo) -> bool {
    watermarks.is_some_and(|table| {
        topic
            .partitions
            .iter()
            .all(|partition| table.get(name, partition.id).is_some())
    })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TopicRow {
    name: String,
    partition_count: i32,
    retained_messages: Option<i64>,
    size_bytes: Option<i64>,
    rate: Option<f64>,
    group_count: i32,
    under_replicated: bool,
    #[serde(flatten)]
    detail: Option<TopicRowDetail>,
}

/// What a DETAILED list adds to each topic.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TopicRowDetail {
    internal: bool,
    replication_factor: i32,
    produced_total: Option<i64>,
    retention_ms: Option<i64>,
    cleanup_policy: CleanupPolicy,
}

impl TopicRow {
    fn new(row: projections::TopicRow, counted: bool, rate: Option<f64>, detailed: bool) -> Self {
        Self {
            detail: detailed.then(|| TopicRowDetail {
                internal: row.internal,
                replication_factor: row.replication_factor,
                produced_total: counted.then_some(row.produced_total),
                retention_ms: row.retention_ms,
                cleanup_policy: row.cleanup_policy.into(),
            }),
            name: row.name.to_string(),
            partition_count: row.partition_count,
            retained_messages: counted.then_some(row.retained_messages),
            size_bytes: row.size_bytes,
            rate,
            group_count: row.group_count,
            under_replicated: row.under_replicated,
        }
    }
}

impl TopicSort {
    fn order(self, a: &TopicRow, b: &TopicRow) -> Ordering {
        match self {
            Self::Name => Ordering::Equal,
            Self::Size => largest_first(a.size_bytes, b.size_bytes, i64::cmp),
            Self::Rate => largest_first(a.rate, b.rate, f64::total_cmp),
            Self::Records => largest_first(a.retained_messages, b.retained_messages, i64::cmp),
            Self::Partitions => b.partition_count.cmp(&a.partition_count),
            Self::Groups => b.group_count.cmp(&a.group_count),
        }
        .then_with(|| a.name.cmp(&b.name))
    }
}

impl TopicsQuery {
    /// The topics that match, sorted, and how many `empty` could not judge
    /// because klens has not measured them.
    fn matching(&self, cluster: &ClusterHandle<'_>) -> Result<(Vec<TopicRow>, usize), ApiError> {
        let topology = cluster.topology()?;
        let watermarks = cluster.store.watermarks.load();
        let names = NameFilter::new(self.name_contains.as_deref());
        let mut unmeasured = 0;
        let mut topics: Vec<TopicRow> = cluster
            .store
            .topic_rows()
            .into_iter()
            .filter(|row| {
                (self.include_internal || !row.internal)
                    && names.matches(&row.name)
                    && self
                        .under_replicated
                        .is_none_or(|wanted| row.under_replicated == wanted)
            })
            .map(|row| {
                let counted = topology
                    .topics
                    .get(&row.name)
                    .is_some_and(|topic| measured(watermarks.as_deref(), &row.name, topic));
                let rate = cluster.store.rates.get(&row.name);
                TopicRow::new(row, counted, rate, self.response_format.is_detailed())
            })
            .filter(|row| match (self.empty, row.retained_messages) {
                (None, _) => true,
                (Some(wanted), Some(records)) => (records == 0) == wanted,
                (Some(_), None) => {
                    unmeasured += 1;
                    false
                }
            })
            .collect();
        topics.sort_by(|a, b| self.sort.order(a, b));
        Ok((topics, unmeasured))
    }
}

#[derive(Serialize)]
struct TopicList {
    #[serde(flatten)]
    page: Page<TopicRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unmeasured: Option<usize>,
}

reply!(TopicList: page);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CreatedTopic<'a> {
    topic: &'a str,
    partitions: Option<usize>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TopicSummary {
    name: String,
    internal: bool,
    partition_count: usize,
    replication_factor: i32,
    retained_messages: Option<i64>,
    produced_total: Option<i64>,
    size_bytes: Option<i64>,
    disk_bytes: Option<i64>,
    rate: Option<f64>,
    retention_ms: Option<i64>,
    cleanup_policy: CleanupPolicy,
    under_replicated_partitions: usize,
    offline_partitions: usize,
}

impl TopicSummary {
    fn new(detail: &TopicDetail, counted: bool, rate: Option<f64>) -> Self {
        let count = |test: fn(&projections::PartitionRow) -> bool| {
            detail
                .partitions
                .iter()
                .filter(|partition| test(partition))
                .count()
        };
        Self {
            name: detail.name.to_string(),
            internal: detail.internal,
            partition_count: detail.partitions.len(),
            replication_factor: detail.replication_factor,
            retained_messages: counted.then_some(detail.retained_messages),
            produced_total: counted.then_some(detail.produced_total),
            size_bytes: detail.size_bytes,
            disk_bytes: detail.disk_bytes,
            rate,
            retention_ms: detail.retention_ms,
            cleanup_policy: detail.cleanup_policy.into(),
            under_replicated_partitions: count(projections::PartitionRow::under_replicated),
            offline_partitions: count(projections::PartitionRow::offline),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PartitionRow {
    partition: i32,
    leader: Option<i32>,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    under_replicated: bool,
    offline: bool,
    low_watermark: Option<i64>,
    high_watermark: Option<i64>,
    retained_messages: Option<i64>,
    size_bytes: Option<i64>,
}

impl PartitionRow {
    fn new(row: &projections::PartitionRow, counted: bool) -> Self {
        Self {
            partition: row.id,
            leader: (!row.offline()).then_some(row.leader),
            replicas: row.replicas.clone(),
            isr: row.isr.clone(),
            under_replicated: row.under_replicated(),
            offline: row.offline(),
            low_watermark: counted.then_some(row.low_watermark),
            high_watermark: counted.then_some(row.high_watermark),
            retained_messages: counted.then(|| row.retained()),
            size_bytes: row.size_bytes,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TopicDescription {
    #[serde(flatten)]
    topic: TopicSummary,
    #[serde(flatten)]
    configs: ConfigSection,
    groups: Rows<TopicGroupRow>,
    subjects: Option<Vec<SubjectRow>>,
    partitions: Rows<PartitionRow>,
    notice: String,
}

impl TopicDescription {
    fn read(cluster: &ClusterHandle<'_>, detail: &TopicDetail) -> Result<Self, ApiError> {
        let boundary = Boundary::new();
        let partitions = Self::partitions(cluster, detail);
        let counted = partitions
            .iter()
            .all(|partition| partition.retained_messages.is_some());
        let configs = Self::configs(cluster, &detail.name, &boundary)?;
        let notice = match configs.explained_by_error() {
            true => format!("{CLIENT_VALUES_NOTICE} {}", boundary.lane_error_notice()),
            false => CLIENT_VALUES_NOTICE.to_owned(),
        };
        Ok(Self {
            topic: TopicSummary::new(detail, counted, cluster.store.rates.get(&detail.name)),
            configs,
            groups: Rows::new("groups", Self::groups(cluster, &detail.name)),
            subjects: Self::subjects(cluster, &detail.name),
            partitions: Rows::new("partitions", partitions),
            notice,
        })
    }

    fn partitions(cluster: &ClusterHandle<'_>, detail: &TopicDetail) -> Vec<PartitionRow> {
        let watermarks = cluster.store.watermarks.load();
        detail
            .partitions
            .iter()
            .map(|partition| {
                let counted = watermarks
                    .as_deref()
                    .is_some_and(|table| table.get(&detail.name, partition.id).is_some());
                PartitionRow::new(partition, counted)
            })
            .collect()
    }

    /// The groups that read the topic, the largest lag on it first.
    fn groups(cluster: &ClusterHandle<'_>, topic: &str) -> Vec<TopicGroupRow> {
        let mut groups = cluster.store.topic_groups(topic);
        groups.sort_by(|a, b| {
            largest_first(a.lag_on_topic, b.lag_on_topic, i64::cmp).then_with(|| a.id.cmp(&b.id))
        });
        groups.into_iter().map(Into::into).collect()
    }

    fn configs(
        cluster: &ClusterHandle<'_>,
        topic: &str,
        boundary: &Boundary,
    ) -> Result<ConfigSection, ApiError> {
        if let Err(error) = cluster.access.topic_configs() {
            return ConfigSection::withheld(error);
        }
        Ok(match cluster.store.topic_configs(topic) {
            Some(entries) => ConfigSection::overrides(entries),
            None => ConfigSection::not_read(cluster.store.configs.health().last_error, boundary),
        })
    }

    /// The topic's key and value subjects, or nothing without a schema registry.
    fn subjects(cluster: &ClusterHandle<'_>, topic: &str) -> Option<Vec<SubjectRow>> {
        let table = cluster
            .has_schema_registry()
            .then(|| cluster.store.subjects.load())
            .flatten()?;
        let subjects = ["key", "value"].into_iter().filter_map(|part| {
            let subject = format!("{topic}-{part}");
            let info = table.get(&subject)?;
            Some(SubjectRow::concise(subject, info))
        });
        Some(subjects.collect())
    }
}

reply!(TopicDescription: configs, groups, partitions; "the counts above cover every partition");

#[tool_router(router = topic_tools, vis = "pub(super)")]
impl KlensMcp {
    /// Lists a cluster's topics with their partition count, records, size, produce rate in records per second, how many groups read them and whether a partition is under-replicated.
    /// `sort` NAME goes from A to Z, and the others put the largest first and unmeasured values last.
    /// `responseFormat` DETAILED adds whether a topic is internal, its replication factor, the records it ever received, its retention and its cleanup policy.
    #[tool(title = "List topics")]
    async fn klens_topics_list(
        &self,
        session: Session,
        Parameters(query): Parameters<TopicsQuery>,
    ) -> ToolResult {
        let cluster = session.cluster_or_only(query.cluster.as_deref())?;
        let (topics, unmeasured) = query.matching(&cluster)?;
        Ok(fit(TopicList {
            page: Page::new(
                "topics",
                topics,
                query.limit,
                Some("pass `nameContains` or a filter"),
            ),
            unmeasured: (unmeasured > 0).then_some(unmeasured),
        }))
    }

    /// Creates a topic with the partitions, replication factor and configs given, or else the broker's defaults. It changes Kafka, so calls to it are limited per minute.
    /// It needs a cluster that accepts changes, and fails with READ_ONLY_CLUSTER on any other. It fails with REFUSED when Kafka refuses, such as for a topic that exists.
    /// The result gives the topic's name and partition count, which is null when you gave no `partitions` and klens has not seen the topic yet.
    #[tool(title = "Create a topic")]
    async fn klens_topic_create(
        &self,
        session: Session,
        Parameters(request): Parameters<TopicToCreate>,
    ) -> ToolResult {
        let cluster = session.cluster_or_only(request.cluster.as_deref())?;
        let admin = cluster.create_topics()?;
        let topic = CreateTopic {
            name: request.topic,
            partitions: request.partitions,
            replication_factor: request.replication_factor,
            configs: request.configs,
        }
        .into_topic()?;
        self.live_call(&session)?;
        admin.create_topic(&topic).await?;
        let partitions = cluster
            .store
            .topic_detail(&topic.name)
            .map(|detail| detail.partitions.len())
            .or(topic.partitions.map(|count| usize::from(count.get())));
        Ok(CallToolResult::structured(json!(CreatedTopic {
            topic: &topic.name,
            partitions,
        })))
    }

    /// Describes one topic: its partitions with their replicas and watermarks, its records, size, produce rate in records per second, retention and cleanup policy.
    /// `groups` lists the consumer groups that read it, the largest lag on this topic first.
    /// `subjects` lists its `<topic>-key` and `<topic>-value` schema subjects, and is null when klens reads no schema registry for the cluster or has not read it yet.
    /// `configs` lists each config whose value is not Kafka's default. When it is null, `omitted` gives the privilege it `needs`, or `notRead` while klens has not read them.
    #[tool(title = "Describe a topic")]
    async fn klens_topic_describe(
        &self,
        session: Session,
        Parameters(query): Parameters<TopicName>,
    ) -> ToolResult {
        let cluster = session.cluster_or_only(query.cluster.as_deref())?;
        cluster.topology()?;
        let Some(detail) = cluster.store.topic_detail(&query.topic) else {
            return Err(KafkaError::UnknownTopic {
                cluster: cluster.name().to_owned(),
                topic: query.topic,
            }
            .into());
        };
        Ok(fit(TopicDescription::read(&cluster, &detail)?))
    }
}
