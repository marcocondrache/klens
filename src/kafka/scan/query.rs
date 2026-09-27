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
            .map_or(CursorDirection::Forward, RecordCursor::direction)
    }

    pub fn searching(&self) -> bool {
        self.filter.is_some() || self.schema_id.is_some()
    }
}

/// Both ends are inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampRange {
    from: Option<Timestamp>,
    to: Option<Timestamp>,
}

impl TimestampRange {
    pub const UNBOUNDED: Self = Self {
        from: None,
        to: None,
    };

    pub fn new(from: Option<Timestamp>, to: Option<Timestamp>) -> Result<Self, QueryError> {
        if let (Some(from), Some(to)) = (from, to)
            && from > to
        {
            return Err(QueryError::InvertedTimestampRange);
        }
        Ok(Self { from, to })
    }

    pub fn start_seek(self) -> Option<i64> {
        self.from.map(Timestamp::as_millisecond)
    }

    /// Kafka seeks to the first offset at or after a timestamp, so the end
    /// seeks one millisecond past it to keep records stamped `to`.
    pub fn end_seek(self) -> Option<i64> {
        self.to.map(|to| to.as_millisecond().saturating_add(1))
    }
}

#[cfg(test)]
mod tests {
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
        assert!(TimestampRange::new(None, Some(early)).is_ok());
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
    fn maps_inclusive_bounds_to_kafka_seek_times() {
        let start = Some(unix_datetime(10));
        let end = Some(unix_datetime(20));

        let both = TimestampRange::new(start, end).unwrap();
        assert_eq!(both.start_seek(), Some(10));
        assert_eq!(both.end_seek(), Some(21));

        let from = TimestampRange::new(start, None).unwrap();
        assert_eq!(from.start_seek(), Some(10));
        assert_eq!(from.end_seek(), None);

        let to = TimestampRange::new(None, end).unwrap();
        assert_eq!(to.start_seek(), None);
        assert_eq!(to.end_seek(), Some(21));
    }

    #[test]
    fn unbounded_seeks_nowhere() {
        assert_eq!(TimestampRange::UNBOUNDED.start_seek(), None);
        assert_eq!(TimestampRange::UNBOUNDED.end_seek(), None);
        assert_eq!(
            TimestampRange::new(None, None).unwrap(),
            TimestampRange::UNBOUNDED
        );
    }
}
