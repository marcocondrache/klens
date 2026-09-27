use std::time::Duration;

pub use crate::config::RecordLimits;
use crate::kafka::error::QueryError;

impl RecordLimits {
    pub fn clamp_limit(&self, limit: i32) -> Result<usize, QueryError> {
        if limit < 1 {
            return Err(QueryError::LimitTooSmall);
        }

        Ok((limit as usize).min(self.max_limit.get()))
    }

    pub fn window_take(&self, limit: usize, searching: bool) -> i64 {
        let multiplier = if searching {
            self.search_window_multiplier
        } else {
            self.window_multiplier
        };

        (limit.saturating_mul(multiplier)).max(self.min_window) as i64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailLimits {
    pub batch: usize,
    pub interval: Duration,
    pub poll_wait: Duration,
    pub heartbeat: Duration,
    pub records: RecordLimits,
}

impl TailLimits {
    pub fn backlog(&self, searching: bool) -> u64 {
        self.records.window_take(self.batch, searching) as u64
    }
}
