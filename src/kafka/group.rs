//! Raw broker group state.

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

    pub fn member_for(&self, topic: &str, partition: i32) -> Option<&str> {
        self.members
            .iter()
            .find(|member| member.assigned_to(topic, partition))
            .map(|member| member.id.as_str())
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

pub fn is_internal_group(id: &str) -> bool {
    id.starts_with(crate::environment::INTERNAL_GROUP_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            group.consumed_topics().collect::<Vec<_>>(),
            vec!["orders", "payments"],
            "a topic a member left behind a commit on still counts as consumed"
        );
        assert!(group.consumes_topic("payments"));
        assert!(!group.consumes_topic("shipments"));
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
