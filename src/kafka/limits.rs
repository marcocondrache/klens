use std::time::Duration;

use crate::environment::{
    MAX_RECORD_LIMIT, RECORD_MIN_WINDOW, RECORD_SEARCH_WINDOW_MULTIPLIER, RECORD_WINDOW_MULTIPLIER,
    SSE_KEEP_ALIVE, TAIL_BATCH_LIMIT, TAIL_INTERVAL,
};
use crate::kafka::error::QueryError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordLimits {
    pub max_limit: usize,
    pub min_window: usize,
    pub window_multiplier: usize,
    pub search_window_multiplier: usize,
}

impl RecordLimits {
    pub fn from_env() -> Self {
        Self {
            max_limit: *MAX_RECORD_LIMIT,
            min_window: *RECORD_MIN_WINDOW,
            window_multiplier: *RECORD_WINDOW_MULTIPLIER,
            search_window_multiplier: *RECORD_SEARCH_WINDOW_MULTIPLIER,
        }
    }

    pub fn clamp_limit(&self, limit: i32) -> Result<usize, QueryError> {
        if limit < 1 {
            return Err(QueryError::LimitTooSmall);
        }

        Ok((limit as usize).min(self.max_limit))
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
    pub heartbeat: Duration,
    pub records: RecordLimits,
}

impl TailLimits {
    pub fn from_env() -> Self {
        Self {
            batch: (*TAIL_BATCH_LIMIT).max(1),
            interval: *TAIL_INTERVAL,
            heartbeat: SSE_KEEP_ALIVE,
            records: RecordLimits::from_env(),
        }
    }

    pub fn backlog(&self, searching: bool) -> u64 {
        self.records.window_take(self.batch, searching) as u64
    }
}
