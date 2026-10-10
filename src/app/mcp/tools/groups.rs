use std::borrow::Cow;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::{tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::app::context::Session;
use crate::app::groups::types::GroupState;
use crate::app::mcp::args::{NameFilter, ResponseFormat, largest_first};
use crate::app::mcp::ext::{ClusterExt as _, SessionExt as _};
use crate::app::mcp::findings::{Finding, findings, member_lags};
use crate::app::mcp::gate::ToolGate;
use crate::app::mcp::reply::{Cut, Page, Reply, Rows, fit, prefix};
use crate::app::mcp::server::{KlensMcp, ToolResult};
use crate::app::mcp::untrusted::shorten;
use crate::app::mcp::{CLIENT_VALUES_NOTICE, MAX_ROWS};
use crate::kafka::KafkaError;
use crate::kafka::model::{GroupMember, GroupOffset};
use crate::kafka::store::projections::{self, GroupDetail};

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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GroupRow {
    id: String,
    state: GroupState,
    member_count: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic_names: Option<Vec<String>>,
    total_lag: Option<i64>,
    lag_complete: bool,
}

impl GroupRow {
    fn new(row: projections::GroupRow, detailed: bool) -> Self {
        Self {
            id: row.id.to_string(),
            state: row.state.into(),
            member_count: row.member_count,
            topic_names: detailed.then_some(row.topic_names),
            total_lag: row.total_lag,
            lag_complete: row.lag_complete,
        }
    }
}

impl GroupsQuery {
    fn keeps(&self, row: &projections::GroupRow, names: &NameFilter) -> bool {
        names.matches(&row.id)
            && self
                .state
                .is_none_or(|state| GroupState::from(row.state) == state)
            && self
                .min_lag
                .is_none_or(|least| row.total_lag.is_some_and(|lag| lag >= least))
            && self
                .topic
                .as_ref()
                .is_none_or(|topic| row.topic_names.contains(topic))
    }
}

#[derive(Serialize)]
struct GroupList {
    #[serde(flatten)]
    page: Page<GroupRow>,
    notice: &'static str,
}

impl Reply for GroupList {
    fn lists(&mut self) -> Vec<&mut dyn Cut> {
        vec![self.page.rows()]
    }

    fn kept_whole(&self) -> Option<&str> {
        None
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MemberRow<'a> {
    member_id: Cow<'a, str>,
    client_id: Cow<'a, str>,
    host: Cow<'a, str>,
    assignments: Vec<AssignmentRow<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topics_left_out: Option<usize>,
    lag: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AssignmentRow<'a> {
    topic: Cow<'a, str>,
    partitions: &'a [i32],
    #[serde(skip_serializing_if = "Option::is_none")]
    partitions_left_out: Option<usize>,
}

impl<'a> MemberRow<'a> {
    /// The member with at most `most` topics, and at most `most` partitions of each.
    fn new(member: &'a GroupMember, lag: Option<i64>, most: usize) -> Self {
        let (assigned, topics_left_out) = prefix(&member.assignments, most);
        Self {
            member_id: shorten(&member.id),
            client_id: shorten(&member.client_id),
            host: shorten(&member.host),
            assignments: assigned
                .iter()
                .map(|assignment| {
                    let (partitions, partitions_left_out) = prefix(&assignment.partitions, most);
                    AssignmentRow {
                        topic: shorten(&assignment.topic),
                        partitions,
                        partitions_left_out,
                    }
                })
                .collect(),
            topics_left_out,
            lag,
        }
    }

    /// The most topics or partitions of a topic the member holds.
    fn widest(member: &GroupMember) -> usize {
        member
            .assignments
            .iter()
            .map(|assignment| assignment.partitions.len())
            .fold(member.assignments.len(), usize::max)
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GroupPartitionRow {
    topic: String,
    partition: i32,
    committed_offset: Option<i64>,
    end_offset: Option<i64>,
    lag: Option<i64>,
}

impl From<GroupOffset> for GroupPartitionRow {
    fn from(offset: GroupOffset) -> Self {
        Self {
            topic: offset.topic,
            partition: offset.partition,
            committed_offset: offset.current_offset,
            end_offset: offset.end_offset,
            lag: offset.lag,
        }
    }
}

/// A group with its members and partitions, the largest lag first. A member
/// names topics and partitions in lists of their own, which a cut also shortens.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GroupDescription<'a> {
    group: &'a str,
    state: GroupState,
    protocol: Cow<'a, str>,
    total_lag: Option<i64>,
    lag_complete: bool,
    findings: Rows<Finding>,
    members: Rows<MemberRow<'a>>,
    partitions: Rows<GroupPartitionRow>,
    notice: &'static str,
    #[serde(skip)]
    whole: Whole<'a>,
}

/// The findings and members as they stand before a cut shortens their lists.
struct Whole<'a> {
    findings: Vec<Finding>,
    members: Vec<(&'a GroupMember, Option<i64>)>,
    widest: usize,
}

impl<'a> GroupDescription<'a> {
    fn new(group: &'a GroupDetail, findings: Vec<Finding>) -> Self {
        let mut members: Vec<_> = group.members.iter().zip(member_lags(group)).collect();
        members.sort_by(|(a, a_lag), (b, b_lag)| {
            largest_first(*a_lag, *b_lag, i64::cmp).then_with(|| a.id.cmp(&b.id))
        });
        let mut partitions: Vec<GroupPartitionRow> = group
            .offsets
            .iter()
            .cloned()
            .map(GroupPartitionRow::from)
            .collect();
        partitions.sort_by(|a, b| {
            largest_first(a.lag, b.lag, i64::cmp)
                .then_with(|| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)))
        });
        let widest = members
            .iter()
            .map(|(member, _)| MemberRow::widest(member))
            .chain(findings.iter().map(Finding::width))
            .max()
            .unwrap_or(0);
        let mut description = Self {
            group: &group.id,
            state: group.state.into(),
            protocol: shorten(&group.protocol),
            total_lag: group.total_lag,
            lag_complete: group.lag_complete,
            findings: Rows::new("findings", Vec::new()),
            members: Rows::new("members", Vec::new()),
            partitions: Rows::new("partitions", partitions),
            notice: CLIENT_VALUES_NOTICE,
            whole: Whole {
                findings,
                members,
                widest,
            },
        };
        description.cap(widest);
        description
    }
}

impl Reply for GroupDescription<'_> {
    fn lists(&mut self) -> Vec<&mut dyn Cut> {
        vec![&mut self.findings, &mut self.members, &mut self.partitions]
    }

    fn kept_whole(&self) -> Option<&str> {
        Some("the lag totals and findings above cover every member and partition")
    }

    fn inner_rows(&self) -> usize {
        self.whole.widest
    }

    fn cap(&mut self, most: usize) {
        let findings = self.whole.findings.iter();
        self.findings = Rows::new("findings", findings.map(|f| f.capped(most)).collect());
        let members = self.whole.members.iter();
        self.members = Rows::new(
            "members",
            members
                .map(|&(member, lag)| MemberRow::new(member, lag, most))
                .collect(),
        );
    }

    fn capped_note(&self, most: usize) -> Option<String> {
        (most < self.whole.widest).then(|| {
            format!(
                "Each member names at most {most} topics, and each member and finding \
                 at most {most} partitions of a topic; `topicsLeftOut` and \
                 `partitionsLeftOut` count the rest"
            )
        })
    }
}

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
    ) -> ToolResult {
        let cluster = session.cluster_or_only(query.cluster.as_deref())?;
        cluster.topology()?;
        let names = NameFilter::new(query.name_contains.as_deref());
        let mut groups: Vec<GroupRow> = cluster
            .store
            .group_rows()
            .into_iter()
            .filter(|row| query.keeps(row, &names))
            .map(|row| GroupRow::new(row, query.response_format.is_detailed()))
            .collect();
        groups.sort_by(|a, b| {
            largest_first(a.total_lag, b.total_lag, i64::cmp).then_with(|| a.id.cmp(&b.id))
        });
        Ok(fit(GroupList {
            page: Page::new(
                "groups",
                groups,
                query.limit,
                Some("pass `nameContains` or a filter"),
            ),
            notice: CLIENT_VALUES_NOTICE,
        }))
    }

    /// Describes one consumer group: its state, assignment protocol, total lag, and its members and partitions, the largest lag first.
    /// `findings` names what looks wrong by `kind`: NO_MEMBERS, REBALANCING, MORE_MEMBERS_THAN_PARTITIONS, UNASSIGNED_PARTITIONS, and LAG_ON_ONE_MEMBER when one member holds at least 80% of a complete total lag of 1000 or more.
    /// A call makes klens read the group's offsets more often for a while, so calls to it are limited per minute.
    #[tool(title = "Describe a consumer group")]
    async fn klens_group_describe(
        &self,
        session: Session,
        Parameters(query): Parameters<GroupId>,
    ) -> ToolResult {
        let cluster = session.cluster_or_only(query.cluster.as_deref())?;
        let topology = cluster.topology()?;
        self.live_call(&session)?;
        let Some(group) = cluster.store.group_detail(&query.group) else {
            return Err(KafkaError::UnknownGroup {
                cluster: cluster.name().to_owned(),
                group: query.group,
            }
            .into());
        };
        let findings = findings(&group, &topology);
        Ok(fit(GroupDescription::new(&group, findings)))
    }
}
