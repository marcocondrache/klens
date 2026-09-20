use std::num::NonZeroUsize;

use crate::environment::{MAX_IN_FLIGHT_REQUESTS, MAX_RESPONSE_MB};
use crate::kafka::error::KafkaError;

/// krafka warns when frame × in-flight > this. Equal is silent (32 × 32 MiB).
const MEMORY_CEILING: usize = 1024 * 1024 * 1024;

/// Process-wide shared-pool memory contract. Built once at connect.
///
/// Fetch size is derived from the frame so a later env knob cannot reopen
/// krafka's 50 MiB default fetch on a smaller frame.
#[derive(Clone, Copy, Debug)]
pub(super) struct ConnectionBudget {
    frame_bytes: usize,
    in_flight: NonZeroUsize,
}

impl ConnectionBudget {
    pub(super) fn from_env() -> Result<Self, KafkaError> {
        Self::new(
            (*MAX_RESPONSE_MB).max(1) * 1024 * 1024,
            (*MAX_IN_FLIGHT_REQUESTS).max(1),
        )
    }

    pub(super) fn new(frame_bytes: usize, in_flight: usize) -> Result<Self, KafkaError> {
        let Some(in_flight) = NonZeroUsize::new(in_flight) else {
            return Err(KafkaError::Admin(
                "in-flight requests must be at least 1".into(),
            ));
        };
        let product = frame_bytes.saturating_mul(in_flight.get());
        if product > MEMORY_CEILING {
            return Err(KafkaError::Admin(format!(
                "connection budget {frame_bytes} × {} exceeds the 1 GiB memory ceiling",
                in_flight.get(),
            )));
        }
        Ok(Self {
            frame_bytes,
            in_flight,
        })
    }

    pub(super) fn frame_bytes(self) -> usize {
        self.frame_bytes
    }

    pub(super) fn in_flight(self) -> usize {
        self.in_flight.get()
    }

    pub(super) fn fetch_max_bytes(self) -> i32 {
        i32::try_from(self.frame_bytes / 2).unwrap_or(i32::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_knobs_sit_on_the_memory_ceiling() {
        let budget = ConnectionBudget::new(32 * 1024 * 1024, 32).expect("32 × 32 MiB is 1 GiB");
        assert_eq!(budget.frame_bytes(), 32 * 1024 * 1024);
        assert_eq!(budget.in_flight(), 32);
        assert_eq!(budget.fetch_max_bytes(), 16 * 1024 * 1024);
    }

    #[test]
    fn a_product_past_the_ceiling_is_rejected() {
        let error = ConnectionBudget::new(33 * 1024 * 1024, 32).expect_err("over ceiling");
        assert!(matches!(error, KafkaError::Admin(_)), "{error:?}");
    }

    #[test]
    fn zero_in_flight_is_rejected() {
        let error = ConnectionBudget::new(32 * 1024 * 1024, 0).expect_err("zero in-flight");
        assert!(matches!(error, KafkaError::Admin(_)), "{error:?}");
    }
}
