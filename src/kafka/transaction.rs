#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
    Dead,
    PrepareEpochFence,
    Unknown,
}

impl TransactionState {
    pub const OPEN: [Self; 3] = [Self::Ongoing, Self::PrepareCommit, Self::PrepareAbort];

    pub fn parse(raw: &str) -> Self {
        match raw {
            "Empty" => Self::Empty,
            "Ongoing" => Self::Ongoing,
            "PrepareCommit" => Self::PrepareCommit,
            "PrepareAbort" => Self::PrepareAbort,
            "CompleteCommit" => Self::CompleteCommit,
            "CompleteAbort" => Self::CompleteAbort,
            "Dead" => Self::Dead,
            "PrepareEpochFence" => Self::PrepareEpochFence,
            _ => Self::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "Empty",
            Self::Ongoing => "Ongoing",
            Self::PrepareCommit => "PrepareCommit",
            Self::PrepareAbort => "PrepareAbort",
            Self::CompleteCommit => "CompleteCommit",
            Self::CompleteAbort => "CompleteAbort",
            Self::Dead => "Dead",
            Self::PrepareEpochFence => "PrepareEpochFence",
            Self::Unknown => "Unknown",
        }
    }

    pub fn is_open(self) -> bool {
        Self::OPEN.contains(&self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedTransaction {
    pub transactional_id: String,
    pub producer_id: i64,
    pub state: TransactionState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionDescription {
    pub transactional_id: String,
    pub error: Option<String>,
    pub state: TransactionState,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub timeout_ms: i32,
    pub started_at_ms: Option<i64>,
    pub partitions: Vec<(String, i32)>,
}

impl TransactionDescription {
    pub fn includes(&self, topic: &str, partition: i32) -> bool {
        self.partitions
            .iter()
            .any(|(name, id)| name == topic && *id == partition)
    }

    /// The coordinator aborts a transaction once it outlives its timeout, so
    /// one still open past it is stuck.
    pub fn past_timeout(&self, now_ms: i64) -> bool {
        self.started_at_ms
            .is_some_and(|started| now_ms - started > i64::from(self.timeout_ms))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionProducers {
    pub topic: String,
    pub partition: i32,
    pub error: Option<String>,
    pub producers: Vec<ActiveProducer>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveProducer {
    pub producer_id: i64,
    pub producer_epoch: i32,
    pub last_timestamp_ms: Option<i64>,
    pub open_offset: Option<i64>,
}

/// Kafka reports an unset time or offset as a negative number.
pub fn reported(value: i64) -> Option<i64> {
    (value >= 0).then_some(value)
}

pub fn is_authorization_error(message: &str) -> bool {
    message
        .to_ascii_lowercase()
        .replace([' ', '_'], "")
        .contains("authorizationfailed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn states_round_trip_the_names_kafka_uses() {
        for state in [
            TransactionState::Empty,
            TransactionState::Ongoing,
            TransactionState::PrepareCommit,
            TransactionState::PrepareAbort,
            TransactionState::CompleteCommit,
            TransactionState::CompleteAbort,
            TransactionState::Dead,
            TransactionState::PrepareEpochFence,
        ] {
            assert_eq!(TransactionState::parse(state.as_str()), state);
        }
        assert_eq!(
            TransactionState::parse("Whatever"),
            TransactionState::Unknown
        );
        assert_eq!(TransactionState::Unknown.as_str(), "Unknown");
    }

    #[test]
    fn only_ongoing_and_preparing_transactions_are_open() {
        assert!(TransactionState::Ongoing.is_open());
        assert!(TransactionState::PrepareCommit.is_open());
        assert!(TransactionState::PrepareAbort.is_open());
        assert!(!TransactionState::CompleteCommit.is_open());
        assert!(!TransactionState::CompleteAbort.is_open());
        assert!(!TransactionState::Empty.is_open());
        assert!(!TransactionState::Unknown.is_open());
    }

    #[test]
    fn a_description_includes_only_its_own_partitions() {
        let description = TransactionDescription {
            transactional_id: "payments-1".into(),
            error: None,
            state: TransactionState::Ongoing,
            producer_id: 7,
            producer_epoch: 0,
            timeout_ms: 60_000,
            started_at_ms: Some(1),
            partitions: vec![("orders".into(), 0), ("payments".into(), 2)],
        };

        assert!(description.includes("orders", 0));
        assert!(description.includes("payments", 2));
        assert!(!description.includes("orders", 2));
        assert!(!description.includes("payments", 0));
    }

    #[test]
    fn a_transaction_is_past_its_timeout_only_once_it_outlives_it() {
        let mut description = TransactionDescription {
            transactional_id: "payments-1".into(),
            error: None,
            state: TransactionState::Ongoing,
            producer_id: 7,
            producer_epoch: 0,
            timeout_ms: 60_000,
            started_at_ms: Some(1_000),
            partitions: Vec::new(),
        };

        assert!(!description.past_timeout(61_000));
        assert!(description.past_timeout(61_001));

        description.started_at_ms = None;
        assert!(!description.past_timeout(i64::MAX));
    }

    #[test]
    fn a_negative_time_or_offset_is_unset() {
        assert_eq!(reported(-1), None);
        assert_eq!(reported(0), Some(0));
        assert_eq!(reported(42), Some(42));
    }

    #[test]
    fn authorization_errors_match_both_spellings_kafka_sends() {
        assert!(is_authorization_error("Topic authorization failed."));
        assert!(is_authorization_error("TopicAuthorizationFailed"));
        assert!(is_authorization_error(
            "TRANSACTIONAL_ID_AUTHORIZATION_FAILED"
        ));
        assert!(!is_authorization_error("NotLeaderOrFollower"));
        assert!(!is_authorization_error("authorization is disabled"));
    }
}
