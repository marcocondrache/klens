use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::num::{NonZeroU8, NonZeroU16};

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{tool, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use crate::kafka::KafkaError;

use crate::app::context::Session;
use crate::app::error::ApiError;
use crate::app::topics::{CreateTopic, TopicGroupRow};

use super::super::types::{
    CreatedTopic, Omitted, PartitionRow, Reason, Section, SubjectRow, TopicDescription, TopicList,
    TopicRow, TopicSummary,
};
use super::super::untrusted::Boundary;
use super::super::{CLIENT_VALUES_NOTICE, MAX_ROWS};
use super::ResponseFormat;
use crate::app::mcp::fit::{
    first, fitted_lists, largest_first_unmeasured_last, listed, name_filter, one_cluster,
};
use crate::app::mcp::lanes::{counted, lane_error, lane_error_notice, topology};
use crate::app::mcp::server::KlensMcp;
use crate::app::mcp::view::{omitted, overrides};
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

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TopicsQuery {
    /// A cluster name from klens_clusters. Optional when you see only one.
    cluster: Option<String>,
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

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct TopicName {
    /// A cluster name from klens_clusters. Optional when you see only one.
    cluster: Option<String>,
    /// The topic's exact name.
    topic: String,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TopicToCreate {
    /// A cluster name from klens_clusters. Optional when you see only one.
    cluster: Option<String>,
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

#[tool_router(router = topic_tools, vis = "pub(super)")]
impl KlensMcp {
    /// Lists a cluster's topics with their partition count, records, size, produce rate in records per second, how many groups read them and whether a partition is under-replicated.
    /// `sort` NAME goes from A to Z, and the others put the largest first and unmeasured values last.
    /// `responseFormat` DETAILED adds whether a topic is internal, its replication factor, the records it ever received, its retention and its cleanup policy.
    #[tool(
        title = "List topics",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn klens_topics_list(
        &self,
        session: Session,
        Parameters(query): Parameters<TopicsQuery>,
    ) -> Result<CallToolResult, ApiError> {
        let cluster = one_cluster(&session, query.cluster.as_deref())?;
        let topology = topology(&cluster)?;
        let watermarks = cluster.store.watermarks.load();
        let detailed = query.response_format == ResponseFormat::Detailed;
        let named = name_filter(query.name_contains.as_deref());
        let mut unmeasured = 0;
        let mut topics: Vec<TopicRow> = cluster
            .store
            .topic_rows()
            .into_iter()
            .filter(|row| {
                (query.include_internal || !row.internal)
                    && named(&row.name)
                    && query
                        .under_replicated
                        .is_none_or(|wanted| row.under_replicated == wanted)
            })
            .map(|row| {
                let counted = topology
                    .topics
                    .get(&row.name)
                    .is_some_and(|topic| counted(watermarks.as_deref(), &row.name, topic));
                let rate = cluster.store.rates.get(&row.name);
                TopicRow::new(row, counted, rate, detailed)
            })
            .filter(|row| match (query.empty, row.retained_messages) {
                (None, _) => true,
                (Some(wanted), Some(records)) => (records == 0) == wanted,
                (Some(_), None) => {
                    unmeasured += 1;
                    false
                }
            })
            .collect();
        topics.sort_by(|a, b| {
            match query.sort {
                TopicSort::Name => Ordering::Equal,
                TopicSort::Size => {
                    largest_first_unmeasured_last(a.size_bytes, b.size_bytes, i64::cmp)
                }
                TopicSort::Rate => largest_first_unmeasured_last(a.rate, b.rate, f64::total_cmp),
                TopicSort::Records => largest_first_unmeasured_last(
                    a.retained_messages,
                    b.retained_messages,
                    i64::cmp,
                ),
                TopicSort::Partitions => b.partition_count.cmp(&a.partition_count),
                TopicSort::Groups => b.group_count.cmp(&a.group_count),
            }
            .then_with(|| a.name.cmp(&b.name))
        });
        let unmeasured = (unmeasured > 0).then_some(unmeasured);
        Ok(listed(
            topics,
            query.limit,
            Some("pass `nameContains` or a filter"),
            |topics, showing| {
                json!(TopicList {
                    topics,
                    showing,
                    unmeasured,
                })
            },
        ))
    }

    /// Creates a topic with the partitions, replication factor and configs given, or else the broker's defaults. It changes Kafka, so calls to it are limited per minute.
    /// It needs a cluster that accepts changes, and fails with READ_ONLY_CLUSTER on any other. It fails with REFUSED when Kafka refuses, such as for a topic that exists.
    /// The result gives the topic's name and partition count, which is null when you gave no `partitions` and klens has not seen the topic yet.
    #[tool(
        title = "Create a topic",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = false
        )
    )]
    async fn klens_topic_create(
        &self,
        session: Session,
        Parameters(created): Parameters<TopicToCreate>,
    ) -> Result<CallToolResult, ApiError> {
        let cluster = one_cluster(&session, created.cluster.as_deref())?;
        let topics = cluster.create_topics()?;
        let topic = CreateTopic {
            name: created.topic,
            partitions: created.partitions,
            replication_factor: created.replication_factor,
            configs: created.configs,
        }
        .into_topic()?;
        if !self.state.mcp_live_call(session.guard.subject()) {
            return Err(ApiError::TooManyLiveCalls);
        }
        topics.create_topic(&topic).await?;
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
    #[tool(
        title = "Describe a topic",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn klens_topic_describe(
        &self,
        session: Session,
        Parameters(named): Parameters<TopicName>,
    ) -> Result<CallToolResult, ApiError> {
        let cluster = one_cluster(&session, named.cluster.as_deref())?;
        topology(&cluster)?;
        let Some(detail) = cluster.store.topic_detail(&named.topic) else {
            return Err(KafkaError::UnknownTopic {
                cluster: cluster.name().to_owned(),
                topic: named.topic,
            }
            .into());
        };
        let watermarks = cluster.store.watermarks.load();
        let partitions: Vec<PartitionRow> = detail
            .partitions
            .iter()
            .map(|partition| {
                let counted = watermarks
                    .as_deref()
                    .is_some_and(|table| table.get(&detail.name, partition.id).is_some());
                PartitionRow::new(partition, counted)
            })
            .collect();
        let counted = partitions
            .iter()
            .all(|partition| partition.retained_messages.is_some());
        let topic = TopicSummary::new(&detail, counted, cluster.store.rates.get(&detail.name));
        let mut groups = cluster.store.topic_groups(&detail.name);
        groups.sort_by(|a, b| {
            largest_first_unmeasured_last(a.lag_on_topic, b.lag_on_topic, i64::cmp)
                .then_with(|| a.id.cmp(&b.id))
        });
        let groups: Vec<TopicGroupRow> = groups.into_iter().map(Into::into).collect();
        let boundary = Boundary::new();
        let (configs, omitted) = match cluster.access.topic_configs() {
            Ok(_) => match cluster.store.topic_configs(&detail.name) {
                Some(entries) => (Some(overrides(entries)), None),
                None => (
                    None,
                    Some(Omitted {
                        section: Section::Configs,
                        reason: Reason::NotRead {
                            last_error: lane_error(
                                &boundary,
                                cluster.store.configs.health().last_error,
                            ),
                        },
                    }),
                ),
            },
            Err(error) => (None, Some(omitted(Section::Configs, error)?)),
        };
        let notice = match &omitted {
            Some(Omitted {
                reason: Reason::NotRead {
                    last_error: Some(_),
                },
                ..
            }) => format!("{CLIENT_VALUES_NOTICE} {}", lane_error_notice(&boundary)),
            _ => CLIENT_VALUES_NOTICE.to_owned(),
        };
        let subjects = cluster
            .has_schema_registry()
            .then(|| cluster.store.subjects.load())
            .flatten()
            .map(|table| {
                ["key", "value"]
                    .into_iter()
                    .filter_map(|part| {
                        let subject = format!("{}-{part}", detail.name);
                        let info = table.get(&subject)?;
                        Some(SubjectRow::concise(subject, info))
                    })
                    .collect::<Vec<_>>()
            });
        Ok(fitted_lists(
            &[
                ("configs", configs.as_ref().map_or(0, Vec::len)),
                ("groups", groups.len()),
                ("partitions", partitions.len()),
            ],
            0,
            "the counts above cover every partition",
            |shown, truncated| {
                json!(TopicDescription {
                    topic: &topic,
                    configs: configs.as_deref().map(|configs| first(configs, shown)),
                    omitted: omitted.as_ref(),
                    groups: first(&groups, shown),
                    subjects: subjects.as_deref(),
                    partitions: first(&partitions, shown),
                    notice: &notice,
                    truncated,
                })
            },
        ))
    }
}
