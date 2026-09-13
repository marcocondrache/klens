use std::time::Duration;

use crate::kafka::error::KafkaError;

/// Absolute budget for one adapter operation.
///
/// Lock wait, `run_blocking`, the librdkafka call, and decode all consult
/// [`remaining`](Self::remaining). They do not each get a fresh duration.
#[derive(Clone, Copy, Debug)]
pub(super) struct Deadline {
    instant: tokio::time::Instant,
}

impl From<Duration> for Deadline {
    fn from(budget: Duration) -> Self {
        Self {
            instant: tokio::time::Instant::now() + budget,
        }
    }
}

impl Deadline {
    pub(super) fn remaining(&self) -> Result<Duration, KafkaError> {
        self.instant
            .checked_duration_since(tokio::time::Instant::now())
            .filter(|duration| !duration.is_zero())
            .ok_or(KafkaError::Timeout)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn remaining_is_the_unused_budget() {
        let deadline = Deadline::from(Duration::from_secs(2));
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(deadline.remaining().unwrap(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn expired_deadline_is_timeout() {
        let deadline = Deadline::from(Duration::from_millis(10));
        tokio::time::advance(Duration::from_millis(10)).await;
        assert!(matches!(deadline.remaining(), Err(KafkaError::Timeout)));
    }
}
