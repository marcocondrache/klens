use crate::environment::{
    MAX_RECORD_LIMIT, RECORD_MIN_WINDOW, RECORD_SEARCH_WINDOW_MULTIPLIER, RECORD_WINDOW_MULTIPLIER,
};
use crate::kafka::error::QueryError;

/// Bounds on record browse queries and the windows they plan.
///
/// Defaults come from [`crate::environment`]; carrying them in a value keeps
/// the planning code testable at any configuration.
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

    pub fn clamp_page(&self, page: i32) -> Result<usize, QueryError> {
        if page < 0 {
            return Err(QueryError::NegativePage);
        }

        Ok(page as usize)
    }

    /// Searching widens the window because most records read are discarded by
    /// the filter before they reach the page.
    pub fn window_span(
        &self,
        partition_count: usize,
        limit: usize,
        searching: bool,
        page: usize,
    ) -> (i64, i64) {
        let partitions = partition_count.max(1);
        let multiplier = if searching {
            self.search_window_multiplier
        } else {
            self.window_multiplier
        };

        let take = (limit.saturating_mul(multiplier))
            .div_ceil(partitions)
            .max(self.min_window) as i64;
        let skip = (page.saturating_mul(limit)).div_ceil(partitions) as i64;
        (skip, take)
    }
}

impl Default for RecordLimits {
    fn default() -> Self {
        Self::from_env()
    }
}
