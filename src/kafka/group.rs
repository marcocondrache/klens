#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSnapshot {
    pub id: String,
    pub state: GroupState,
    pub protocol: String,
    pub members: Vec<GroupMember>,
    pub committed: Vec<CommittedOffset>,
}

impl GroupSnapshot {
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Stable,
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    /// A consumer-protocol (KIP-848) group computing a new assignment.
    Assigning,
    /// A consumer-protocol (KIP-848) group whose members are moving to a new
    /// assignment.
    Reconciling,
    Dead,
    /// A state klens does not know. It counts as held by consumers, so
    /// klens never resets the offsets of a group it cannot read.
    Unknown,
}

impl GroupState {
    /// Whether consumers hold the group, so only they may commit its offsets.
    pub fn has_members(self) -> bool {
        !matches!(self, Self::Empty | Self::Dead)
    }

    pub fn parse(raw: &str) -> Self {
        let normalized: String = raw
            .chars()
            .filter(|ch| ch.is_ascii_alphanumeric())
            .map(|ch| ch.to_ascii_lowercase())
            .collect();

        match normalized.as_str() {
            "stable" => Self::Stable,
            "empty" => Self::Empty,
            "preparingrebalance" => Self::PreparingRebalance,
            "completingrebalance" => Self::CompletingRebalance,
            "assigning" => Self::Assigning,
            "reconciling" => Self::Reconciling,
            "dead" => Self::Dead,
            _ => Self::Unknown,
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
            Self::Assigning => "Assigning",
            Self::Reconciling => "Reconciling",
            Self::Dead => "Dead",
            Self::Unknown => "Unknown",
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupOffset {
    pub topic: String,
    pub partition: i32,
    pub current_offset: Option<i64>,
    pub end_offset: Option<i64>,
    pub lag: Option<i64>,
    pub member_id: Option<String>,
}

const INTERNAL_GROUP_PREFIX: &str = "klens.internal.";

pub fn is_internal_group(id: &str) -> bool {
    id.starts_with(INTERNAL_GROUP_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_parses_broker_spellings() {
        assert_eq!(GroupState::parse("Stable"), GroupState::Stable);
        assert_eq!(
            GroupState::parse("PreparingRebalance"),
            GroupState::PreparingRebalance
        );
        assert_eq!(GroupState::parse("Empty"), GroupState::Empty);
        assert_eq!(GroupState::parse("Assigning"), GroupState::Assigning);
        assert_eq!(GroupState::parse("Reconciling"), GroupState::Reconciling);
        assert_eq!(GroupState::parse("Dead"), GroupState::Dead);
    }

    #[test]
    fn a_state_klens_does_not_know_keeps_the_group_held() {
        let state = GroupState::parse("NotReady");

        assert_eq!(state, GroupState::Unknown);
        assert!(state.has_members());
    }
}
