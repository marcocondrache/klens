use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::{tool, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;

use crate::kafka::KafkaError;

use crate::app::context::Session;
use crate::app::error::ApiError;
use crate::app::groups::types::GroupState;

use super::super::findings::Finding;
use super::super::gate::ToolGate;
use super::super::types::{GroupDescription, GroupList, GroupPartitionRow, GroupRow, MemberRow};
use super::super::{CLIENT_VALUES_NOTICE, MAX_ROWS};
use super::ResponseFormat;
use crate::app::mcp::fit::{
    first, fitted_lists, largest_first_unmeasured_last, listed, name_filter, one_cluster,
};
use crate::app::mcp::lanes::topology;
use crate::app::mcp::server::KlensMcp;
use crate::app::mcp::view::shortened;
use crate::kafka::model::GroupMember;
use crate::kafka::store::projections::GroupDetail;
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GroupsQuery {
    /// A cluster name from klens_clusters. Optional when you see only one.
    cluster: Option<String>,
    /// Keeps groups whose id holds this text, in any case.
    name_contains: Option<String>,
    /// Keeps groups in this state.
    state: Option<GroupState>,
    /// Keeps groups whose total lag is at least this many records.
    min_lag: Option<i64>,
    /// Keeps groups that read this exact topic.
    topic: Option<String>,
    /// How many groups to return: 25 unless given, at most 100.
    #[schemars(range(min = 1, max = MAX_ROWS))]
    limit: Option<usize>,
    /// CONCISE unless given.
    #[serde(default)]
    response_format: ResponseFormat,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct GroupId {
    /// A cluster name from klens_clusters. Optional when you see only one.
    cluster: Option<String>,
    /// The consumer group's exact id.
    group: String,
}

pub(super) const GATES: &[ToolGate] = &[
    ToolGate::open("klens_groups_list"),
    ToolGate::open("klens_group_describe"),
];

#[tool_router(router = group_tools, vis = "pub(super)")]
impl KlensMcp {
    /// Lists a cluster's consumer groups, the largest total lag first, with their state, member count and total lag in records.
    /// `lagComplete` is false when the total leaves out partitions whose lag klens has not read.
    /// `responseFormat` DETAILED adds the topics each group reads.
    #[tool(title = "List consumer groups")]
    async fn klens_groups_list(
        &self,
        session: Session,
        Parameters(query): Parameters<GroupsQuery>,
    ) -> Result<CallToolResult, ApiError> {
        let cluster = one_cluster(&session, query.cluster.as_deref())?;
        topology(&cluster)?;
        let detailed = query.response_format == ResponseFormat::Detailed;
        let named = name_filter(query.name_contains.as_deref());
        let mut groups: Vec<GroupRow> = cluster
            .store
            .group_rows()
            .into_iter()
            .filter(|row| {
                named(&row.id)
                    && query
                        .state
                        .is_none_or(|state| GroupState::from(row.state) == state)
                    && query
                        .min_lag
                        .is_none_or(|least| row.total_lag.is_some_and(|lag| lag >= least))
                    && query
                        .topic
                        .as_ref()
                        .is_none_or(|topic| row.topic_names.contains(topic))
            })
            .map(|row| GroupRow::new(row, detailed))
            .collect();
        groups.sort_by(|a, b| {
            largest_first_unmeasured_last(a.total_lag, b.total_lag, i64::cmp)
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(listed(
            groups,
            query.limit,
            Some("pass `nameContains` or a filter"),
            |groups, showing| {
                json!(GroupList {
                    groups,
                    showing,
                    notice: CLIENT_VALUES_NOTICE,
                })
            },
        ))
    }

    /// Describes one consumer group: its state, assignment protocol, total lag, and its members and partitions, the largest lag first.
    /// `findings` names what looks wrong by `kind`: NO_MEMBERS, REBALANCING, MORE_MEMBERS_THAN_PARTITIONS, UNASSIGNED_PARTITIONS, and LAG_ON_ONE_MEMBER when one member holds at least 80% of a complete total lag of 1000 or more.
    /// A call makes klens read the group's offsets more often for a while, so calls to it are limited per minute.
    #[tool(title = "Describe a consumer group")]
    async fn klens_group_describe(
        &self,
        session: Session,
        Parameters(named): Parameters<GroupId>,
    ) -> Result<CallToolResult, ApiError> {
        let cluster = one_cluster(&session, named.cluster.as_deref())?;
        let topology = topology(&cluster)?;
        if !self.state.mcp_live_call(session.guard.subject()) {
            return Err(ApiError::TooManyLiveCalls);
        }
        let Some(group) = cluster.store.group_detail(&named.group) else {
            return Err(KafkaError::UnknownGroup {
                cluster: cluster.name().to_owned(),
                group: named.group,
            }
            .into());
        };
        let findings = super::super::findings::findings(&group, &topology);
        let members = members_by_lag(&group);
        let longest_inner_list = members
            .iter()
            .map(|(member, _)| MemberRow::widest(member))
            .chain(findings.iter().map(Finding::width))
            .max()
            .unwrap_or(0);
        let partitions = partitions_by_lag(&group);
        Ok(fitted_lists(
            &[
                ("findings", findings.len()),
                ("members", members.len()),
                ("partitions", partitions.len()),
            ],
            longest_inner_list,
            "the lag totals and findings above cover every member and partition",
            |shown, truncated| {
                let findings: Vec<Finding> = first(&findings, shown)
                    .iter()
                    .map(|finding| finding.capped(shown))
                    .collect();
                let members: Vec<MemberRow> = first(&members, shown)
                    .iter()
                    .map(|&(member, lag)| MemberRow::new(member, lag, shown))
                    .collect();
                let capped = (shown < longest_inner_list).then(|| {
                    format!(
                        "Each member names at most {shown} topics, and each member and finding \
                         at most {shown} partitions of a topic; `topicsLeftOut` and \
                         `partitionsLeftOut` count the rest"
                    )
                });
                json!(GroupDescription {
                    group: &group.id,
                    state: group.state.into(),
                    protocol: &shortened(&group.protocol),
                    total_lag: group.total_lag,
                    lag_complete: group.lag_complete,
                    findings: &findings,
                    members: &members,
                    partitions: first(&partitions, shown),
                    notice: CLIENT_VALUES_NOTICE,
                    truncated: [truncated, capped]
                        .into_iter()
                        .flatten()
                        .reduce(|note, more| format!("{note}. {more}")),
                })
            },
        ))
    }
}

fn members_by_lag(group: &GroupDetail) -> Vec<(&GroupMember, Option<i64>)> {
    let mut members: Vec<_> = group
        .members
        .iter()
        .zip(super::super::findings::member_lags(group))
        .collect();
    members.sort_by(|(a, a_lag), (b, b_lag)| {
        largest_first_unmeasured_last(*a_lag, *b_lag, i64::cmp).then_with(|| a.id.cmp(&b.id))
    });
    members
}

fn partitions_by_lag(group: &GroupDetail) -> Vec<GroupPartitionRow> {
    let mut partitions: Vec<GroupPartitionRow> = group
        .offsets
        .iter()
        .cloned()
        .map(GroupPartitionRow::from)
        .collect();
    partitions.sort_by(|a, b| {
        largest_first_unmeasured_last(a.lag, b.lag, i64::cmp)
            .then_with(|| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)))
    });
    partitions
}
