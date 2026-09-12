//! Raw broker group state and the assembled product type.
//!
//! [`GroupSnapshot`] is what the broker reports. [`ConsumerGroup`] joins
//! watermarks so each offset has lag.

use std::collections::HashMap;

/// Raw consumer group state as reported by the broker, before end offsets are
/// joined in to compute lag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSnapshot {
    pub id: String,
    pub state: GroupState,
    pub protocol: String,
    pub coordinator: i32,
    pub members: Vec<GroupMember>,
    pub committed: Vec<CommittedOffset>,
}

impl GroupSnapshot {
    /// Topics the group touches, whether through a live assignment or a
    /// committed offset left behind by a previous member.
    pub fn consumed_topics(&self) -> impl Iterator<Item = &str> {
        self.members
            .iter()
            .flat_map(|member| {
                member
                    .assignments
                    .iter()
                    .map(|assignment| assignment.topic.as_str())
            })
            .chain(self.committed.iter().map(|offset| offset.topic.as_str()))
    }

    pub fn consumes_topic(&self, topic: &str) -> bool {
        self.consumed_topics().any(|name| name == topic)
    }

    pub fn assigned_partition_refs(&self) -> impl Iterator<Item = (&str, i32)> {
        self.members.iter().flat_map(|member| {
            member.assignments.iter().flat_map(|assignment| {
                assignment
                    .partitions
                    .iter()
                    .copied()
                    .map(|partition| (assignment.topic.as_str(), partition))
            })
        })
    }

    pub fn assigned_partitions(&self) -> Vec<(String, i32)> {
        let mut partitions: Vec<(String, i32)> = self
            .assigned_partition_refs()
            .map(|(topic, partition)| (topic.to_owned(), partition))
            .collect();
        partitions.sort();
        partitions.dedup();
        partitions
    }

    pub fn member_for(&self, topic: &str, partition: i32) -> Option<&str> {
        self.members
            .iter()
            .find(|member| member.assigned_to(topic, partition))
            .map(|member| member.id.as_str())
    }

    /// Sorted and deduplicated, so the watermark fetch can be batched.
    pub fn consumed_topic_names(groups: &[Self]) -> Vec<String> {
        let mut names: Vec<String> = groups
            .iter()
            .flat_map(|group| group.consumed_topics())
            .map(str::to_owned)
            .collect();
        names.sort();
        names.dedup();
        names
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Stable,
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Dead,
}

impl GroupState {
    pub fn parse(raw: &str) -> Self {
        let normalized: String = raw
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .map(|ch| ch.to_ascii_lowercase())
            .collect();

        match normalized.as_str() {
            "stable" => Self::Stable,
            "preparingrebalance" => Self::PreparingRebalance,
            "completingrebalance" => Self::CompletingRebalance,
            "dead" => Self::Dead,
            _ => Self::Empty,
        }
    }
}

impl std::fmt::Display for GroupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Stable => "Stable",
            Self::Empty => "Empty",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::CompletingRebalance => "CompletingRebalance",
            Self::Dead => "Dead",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMember {
    pub id: String,
    pub client_id: String,
    pub host: String,
    pub assignments: Vec<MemberAssignment>,
}

impl GroupMember {
    pub fn assigned_to(&self, topic: &str, partition: i32) -> bool {
        self.assignments.iter().any(|assignment| {
            assignment.topic == topic && assignment.partitions.contains(&partition)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupOffset {
    pub topic: String,
    pub partition: i32,
    pub current_offset: i64,
    pub end_offset: i64,
    pub lag: i64,
    pub member_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroup {
    pub id: String,
    pub state: GroupState,
    pub protocol: String,
    pub coordinator: i32,
    pub members: Vec<GroupMember>,
    pub topics: Vec<String>,
    pub lag: i64,
    pub offsets: Vec<GroupOffset>,
}

impl ConsumerGroup {
    /// Joins committed offsets against partition end offsets to produce lag.
    ///
    /// Assigned partitions with no committed offset still appear, lagging by
    /// the full log, which is how a group that has never committed shows up.
    pub fn assemble(group: &GroupSnapshot, ends: &HashMap<(String, i32), i64>) -> Self {
        let mut seen = HashMap::<(String, i32), GroupOffset>::new();

        for committed in &group.committed {
            let key = (committed.topic.clone(), committed.partition);
            let end = ends.get(&key).copied().unwrap_or(committed.offset);
            seen.insert(
                key.clone(),
                GroupOffset {
                    topic: committed.topic.clone(),
                    partition: committed.partition,
                    current_offset: committed.offset,
                    end_offset: end,
                    lag: (end - committed.offset).max(0),
                    member_id: group
                        .member_for(&committed.topic, committed.partition)
                        .map(ToOwned::to_owned),
                },
            );
        }

        for (topic, partition) in group.assigned_partition_refs() {
            seen.entry((topic.to_owned(), partition))
                .or_insert_with(|| {
                    let end = ends
                        .get(&(topic.to_owned(), partition))
                        .copied()
                        .unwrap_or(0);
                    GroupOffset {
                        topic: topic.to_owned(),
                        partition,
                        current_offset: 0,
                        end_offset: end,
                        lag: end,
                        member_id: group.member_for(topic, partition).map(ToOwned::to_owned),
                    }
                });
        }

        let mut offsets: Vec<GroupOffset> = seen.into_values().collect();
        offsets.sort_by(|left, right| {
            left.topic
                .cmp(&right.topic)
                .then(left.partition.cmp(&right.partition))
        });
        let lag = offsets.iter().map(|offset| offset.lag).sum();

        Self {
            id: group.id.clone(),
            state: group.state,
            protocol: group.protocol.clone(),
            coordinator: group.coordinator,
            members: group.members.clone(),
            topics: unique_offset_topics(&offsets),
            lag,
            offsets,
        }
    }
}

/// Topics in `offsets`, which is already sorted by topic then partition.
fn unique_offset_topics(offsets: &[GroupOffset]) -> Vec<String> {
    let mut topics = Vec::new();
    for offset in offsets {
        if topics.last() != Some(&offset.topic) {
            topics.push(offset.topic.clone());
        }
    }
    topics
}

pub fn is_internal_group(id: &str) -> bool {
    id.starts_with(crate::environment::INTERNAL_GROUP_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::watermarks::Watermarks;

    #[test]
    fn assemble_computes_lag_from_end_offsets() {
        let group = GroupSnapshot {
            id: "g".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 1,
            members: vec![GroupMember {
                id: "m1".into(),
                client_id: "c1".into(),
                host: "127.0.0.1".into(),
                assignments: vec![MemberAssignment {
                    topic: "orders".into(),
                    partitions: vec![0],
                }],
            }],
            committed: vec![CommittedOffset {
                topic: "orders".into(),
                partition: 0,
                offset: 4,
            }],
        };
        let mut ends = HashMap::new();
        ends.insert(("orders".into(), 0), 10);

        let view = ConsumerGroup::assemble(&group, &ends);
        assert_eq!(view.lag, 6);
        assert_eq!(view.offsets[0].member_id.as_deref(), Some("m1"));
        assert_eq!(view.topics, vec!["orders"]);
    }

    #[test]
    fn assigned_partition_without_a_commit_lags_by_the_whole_log() {
        let group = GroupSnapshot {
            id: "g".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 1,
            members: vec![GroupMember {
                id: "m1".into(),
                client_id: "c1".into(),
                host: "127.0.0.1".into(),
                assignments: vec![MemberAssignment {
                    topic: "orders".into(),
                    partitions: vec![0],
                }],
            }],
            committed: Vec::new(),
        };
        let ends = HashMap::from([(("orders".to_owned(), 0), 12)]);

        let view = ConsumerGroup::assemble(&group, &ends);
        assert_eq!(view.lag, 12);
        assert_eq!(view.offsets[0].current_offset, 0);
    }

    #[test]
    fn membership_and_consumed_topics() {
        let group = GroupSnapshot {
            id: "g".into(),
            state: GroupState::Stable,
            protocol: "range".into(),
            coordinator: 1,
            members: vec![GroupMember {
                id: "m1".into(),
                client_id: "c1".into(),
                host: "127.0.0.1".into(),
                assignments: vec![MemberAssignment {
                    topic: "orders".into(),
                    partitions: vec![0, 1],
                }],
            }],
            committed: vec![CommittedOffset {
                topic: "payments".into(),
                partition: 0,
                offset: 3,
            }],
        };
        assert!(group.members[0].assigned_to("orders", 1));
        assert!(!group.members[0].assigned_to("orders", 2));
        assert_eq!(group.member_for("orders", 0), Some("m1"));
        assert_eq!(
            GroupSnapshot::consumed_topic_names(&[group]),
            vec!["orders".to_owned(), "payments".to_owned()]
        );
        assert_eq!(Watermarks { low: 2, high: 10 }.messages(), 10);
        assert_eq!(Watermarks { low: 0, high: -1 }.messages(), 0);
    }

    #[test]
    fn state_parses_broker_spellings() {
        assert_eq!(GroupState::parse("Stable"), GroupState::Stable);
        assert_eq!(
            GroupState::parse("PreparingRebalance"),
            GroupState::PreparingRebalance
        );
        assert_eq!(GroupState::parse("Dead"), GroupState::Dead);
        assert_eq!(GroupState::parse("whatever"), GroupState::Empty);
    }
}
