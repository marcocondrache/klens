use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::Serialize;

use crate::kafka::model::GroupState;
use crate::kafka::store::Topology;
use crate::kafka::store::projections::GroupDetail;

use super::{first, left_out, shortened};

#[cfg(test)]
mod tests;

const LAG_WORTH_NAMING: i64 = 1000;
const ONE_MEMBER_SHARE_PERCENT: i128 = 80;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "SCREAMING_SNAKE_CASE",
    rename_all_fields = "camelCase"
)]
pub enum Finding {
    NoMembers,
    Rebalancing,
    MoreMembersThanPartitions {
        members: usize,
        partitions: usize,
    },
    UnassignedPartitions {
        topic: String,
        partitions: Vec<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        partitions_left_out: Option<usize>,
    },
    LagOnOneMember {
        member_id: String,
        client_id: String,
        host: String,
        lag: i64,
        total_lag: i64,
    },
}

impl Finding {
    pub fn capped(&self, max_partitions: usize) -> Self {
        match self {
            Self::UnassignedPartitions {
                topic, partitions, ..
            } => Self::UnassignedPartitions {
                topic: topic.clone(),
                partitions: first(partitions, max_partitions).to_vec(),
                partitions_left_out: left_out(partitions.len(), max_partitions),
            },
            finding => finding.clone(),
        }
    }

    pub fn width(&self) -> usize {
        match self {
            Self::UnassignedPartitions { partitions, .. } => partitions.len(),
            _ => 0,
        }
    }
}

pub fn findings(group: &GroupDetail, topology: &Topology) -> Vec<Finding> {
    if group.members.is_empty() {
        return vec![Finding::NoMembers];
    }
    match group.state {
        GroupState::Stable => {}
        GroupState::PreparingRebalance | GroupState::CompletingRebalance => {
            return vec![Finding::Rebalancing];
        }
        GroupState::Empty | GroupState::Dead => return Vec::new(),
    }

    let mut assigned: BTreeMap<&str, BTreeSet<i32>> = BTreeMap::new();
    for assignment in group.members.iter().flat_map(|member| &member.assignments) {
        assigned
            .entry(&assignment.topic)
            .or_default()
            .extend(&assignment.partitions);
    }
    let mut unassigned_partitions = Vec::new();
    let mut partitions = 0;
    let mut every_topic_known = true;
    for (topic, held) in &assigned {
        let Some(info) = topology.topics.get(*topic) else {
            every_topic_known = false;
            continue;
        };
        partitions += info.partitions.len();
        let unassigned: Vec<i32> = info
            .partitions
            .iter()
            .map(|partition| partition.id)
            .filter(|id| !held.contains(id))
            .collect();
        if !unassigned.is_empty() {
            unassigned_partitions.push(Finding::UnassignedPartitions {
                topic: (*topic).to_owned(),
                partitions: unassigned,
                partitions_left_out: None,
            });
        }
    }
    let mut findings = Vec::new();
    // klens decodes only consumer assignments, so a Kafka Connect group shows
    // none and says nothing about how many partitions it reads.
    if every_topic_known && !assigned.is_empty() && group.members.len() > partitions {
        findings.push(Finding::MoreMembersThanPartitions {
            members: group.members.len(),
            partitions,
        });
    }
    findings.extend(lag_on_one_member(group));
    // A result that must shrink keeps the first findings, and a group can
    // leave a partition of every topic it reads unassigned.
    findings.extend(unassigned_partitions);
    findings
}

fn lag_on_one_member(group: &GroupDetail) -> Option<Finding> {
    let total = group.total_lag.filter(|_| group.lag_complete)?;
    if group.members.len() < 2 || total < LAG_WORTH_NAMING {
        return None;
    }
    let (member, lag) = group
        .members
        .iter()
        .zip(member_lags(group))
        .filter_map(|(member, lag)| Some((member, lag?)))
        .max_by_key(|&(_, lag)| lag)?;
    (i128::from(lag) * 100 >= i128::from(total) * ONE_MEMBER_SHARE_PERCENT).then(|| {
        Finding::LagOnOneMember {
            member_id: shortened(&member.id).into_owned(),
            client_id: shortened(&member.client_id).into_owned(),
            host: shortened(&member.host).into_owned(),
            lag,
            total_lag: total,
        }
    })
}

/// In `group.members` order, and 0 for a member that holds no partition.
pub fn member_lags(group: &GroupDetail) -> Vec<Option<i64>> {
    let mut held: HashMap<&str, Option<i64>> = HashMap::new();
    for offset in &group.offsets {
        if let Some(member) = offset.member_id.as_deref() {
            let lag = held.entry(member).or_insert(Some(0));
            *lag = lag.zip(offset.lag).map(|(sum, lag)| sum + lag);
        }
    }
    group
        .members
        .iter()
        .map(|member| {
            let lag = held.get(member.id.as_str()).copied().unwrap_or(Some(0));
            group.total_lag.and(lag)
        })
        .collect()
}
