use std::ops::{Bound, RangeBounds};

use jiff::Timestamp;

use crate::kafka::error::QueryError;
use crate::kafka::scan::cursor::{CursorDirection, RecordCursor};
use crate::kafka::scan::filter::CompiledFilter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOrder {
    Newest,
    Oldest,
}

impl RecordOrder {
    pub fn flipped(self) -> Self {
        match self {
            Self::Newest => Self::Oldest,
            Self::Oldest => Self::Newest,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordQuery {
    pub topic: String,
    pub partitions: Vec<i32>,
    pub filter: Option<CompiledFilter>,
    pub timestamps: TimestampRange,
    pub limit: i32,
    pub order: RecordOrder,
    pub cursor: Option<RecordCursor>,
    pub schema_id: Option<i32>,
}

impl RecordQuery {
    pub fn walk(&self) -> RecordOrder {
        self.cursor.as_ref().map_or(self.order, RecordCursor::walk)
    }

    pub fn direction(&self) -> CursorDirection {
        self.cursor
            .as_ref()
            .map_or(CursorDirection::Forward, |cursor| cursor.direction)
    }

    pub fn searching(&self) -> bool {
        self.filter.is_some() || self.schema_id.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampRange {
    start: Bound<Timestamp>,
    end: Bound<Timestamp>,
}

impl TimestampRange {
    pub const UNBOUNDED: Self = Self {
        start: Bound::Unbounded,
        end: Bound::Unbounded,
    };

    pub fn new(from: Option<Timestamp>, to: Option<Timestamp>) -> Result<Self, QueryError> {
        match (from, to) {
            (None, None) => Self::UNBOUNDED,
            (Some(from), None) => Self::from_bounds(from..),
            (None, Some(to)) => Self::from_bounds(..=to),
            (Some(from), Some(to)) => Self::from_bounds(from..=to),
        }
        .validate()
    }

    pub fn from_bounds(range: impl RangeBounds<Timestamp>) -> Self {
        Self {
            start: copy_bound(range.start_bound()),
            end: copy_bound(range.end_bound()),
        }
    }

    pub fn validate(self) -> Result<Self, QueryError> {
        match (self.start, self.end) {
            (
                Bound::Included(from) | Bound::Excluded(from),
                Bound::Included(to) | Bound::Excluded(to),
            ) if from > to => Err(QueryError::InvertedTimestampRange),
            _ => Ok(self),
        }
    }

    pub fn start_seek(self) -> Option<i64> {
        timestamp_seek(self.start, false)
    }

    pub fn end_seek(self) -> Option<i64> {
        timestamp_seek(self.end, true)
    }
}

impl Default for TimestampRange {
    fn default() -> Self {
        Self::UNBOUNDED
    }
}

impl RangeBounds<Timestamp> for TimestampRange {
    fn start_bound(&self) -> Bound<&Timestamp> {
        self.start.as_ref()
    }

    fn end_bound(&self) -> Bound<&Timestamp> {
        self.end.as_ref()
    }
}

fn copy_bound(bound: Bound<&Timestamp>) -> Bound<Timestamp> {
    match bound {
        Bound::Included(value) => Bound::Included(*value),
        Bound::Excluded(value) => Bound::Excluded(*value),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Kafka seeks to the first offset at or after a timestamp, so an exclusive
/// bound is expressed by shifting one millisecond.
fn timestamp_seek(bound: Bound<Timestamp>, is_end: bool) -> Option<i64> {
    match (bound, is_end) {
        (Bound::Unbounded, _) => None,
        (Bound::Included(timestamp), false) | (Bound::Excluded(timestamp), true) => {
            Some(timestamp.as_millisecond())
        }
        (Bound::Included(timestamp), true) | (Bound::Excluded(timestamp), false) => {
            Some(timestamp.as_millisecond().saturating_add(1))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::{Bound, RangeBounds};

    use super::*;

    fn unix_datetime(ms: i64) -> Timestamp {
        Timestamp::from_millisecond(ms).unwrap_or(Timestamp::UNIX_EPOCH)
    }

    #[test]
    fn rejects_from_after_to() {
        let early = unix_datetime(1);
        let late = unix_datetime(2);

        assert!(TimestampRange::new(Some(late), Some(early)).is_err());
        assert!(TimestampRange::new(Some(early), Some(early)).is_ok());
        assert!(TimestampRange::new(Some(early), None).is_ok());
        assert!(
            TimestampRange::from_bounds((Bound::Included(late), Bound::Included(early)))
                .validate()
                .is_err()
        );
        assert!(
            TimestampRange::from_bounds(early..=early)
                .validate()
                .is_ok()
        );
        assert!(TimestampRange::from_bounds(early..).validate().is_ok());
    }

    #[test]
    fn rejection_names_the_offending_fields() {
        let early = unix_datetime(1);
        let late = unix_datetime(2);

        assert_eq!(
            TimestampRange::new(Some(late), Some(early))
                .unwrap_err()
                .to_string(),
            "timestampFrom must not be after timestampTo"
        );
    }

    #[test]
    fn maps_std_ranges_to_kafka_seek_times() {
        let start = unix_datetime(10);
        let end = unix_datetime(20);
        let inside = unix_datetime(19);
        let after = unix_datetime(21);

        let inclusive = TimestampRange::from_bounds(start..=end);
        assert_eq!(inclusive.start_bound(), Bound::Included(&start));
        assert_eq!(inclusive.end_bound(), Bound::Included(&end));
        assert_eq!(inclusive.start_seek(), Some(10));
        assert_eq!(inclusive.end_seek(), Some(21));
        assert!(inclusive.contains(&start));
        assert!(inclusive.contains(&end));
        assert!(!inclusive.contains(&after));

        let exclusive_end = TimestampRange::from_bounds(start..end);
        assert_eq!(exclusive_end.end_seek(), Some(20));
        assert!(exclusive_end.contains(&inside));
        assert!(!exclusive_end.contains(&end));

        let from = TimestampRange::from_bounds(start..);
        assert_eq!(from.start_seek(), Some(10));
        assert_eq!(from.end_seek(), None);

        let to = TimestampRange::from_bounds(..=end);
        assert_eq!(to.start_seek(), None);
        assert_eq!(to.end_seek(), Some(21));
    }

    #[test]
    fn unbounded_seeks_nowhere() {
        assert_eq!(TimestampRange::UNBOUNDED.start_seek(), None);
        assert_eq!(TimestampRange::UNBOUNDED.end_seek(), None);
        assert_eq!(TimestampRange::default(), TimestampRange::UNBOUNDED);
    }
}
