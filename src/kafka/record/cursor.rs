use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use crate::kafka::error::QueryError;

/// Per-partition resume offsets for a record browse.
///
/// Oldest pages treat each value as the next start offset. Newest pages treat
/// it as the exclusive end of the next window.
///
/// A missing cursor is the first page (start at the watermark). Once a cursor
/// exists, an omitted partition is exhausted and must not restart.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RecordCursor {
    pub offsets: BTreeMap<i32, i64>,
}

impl RecordCursor {
    pub fn parse(value: &str) -> Result<Self, QueryError> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Ok(Self::default());
        }

        let mut offsets = BTreeMap::new();
        for part in trimmed.split(',') {
            let (partition, offset) = part.split_once(':').ok_or(QueryError::InvalidCursor)?;
            let partition: i32 = partition.parse().map_err(|_| QueryError::InvalidCursor)?;
            let offset: i64 = offset.parse().map_err(|_| QueryError::InvalidCursor)?;
            if offset < 0 || offsets.contains_key(&partition) {
                return Err(QueryError::InvalidCursor);
            }
            offsets.insert(partition, offset);
        }

        Ok(Self { offsets })
    }

    pub fn encode(&self) -> String {
        self.to_string()
    }
}

impl Display for RecordCursor {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for (partition, offset) in &self.offsets {
            if !first {
                formatter.write_str(",")?;
            }
            first = false;
            write!(formatter, "{partition}:{offset}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_partition_offsets() {
        let cursor = RecordCursor::parse("1:8,0:15").unwrap();
        assert_eq!(cursor.offsets, BTreeMap::from([(0, 15), (1, 8)]));
        assert_eq!(cursor.encode(), "0:15,1:8");
    }

    #[test]
    fn empty_string_is_an_empty_cursor() {
        assert_eq!(RecordCursor::parse("").unwrap(), RecordCursor::default());
        assert_eq!(RecordCursor::default().encode(), "");
    }

    #[test]
    fn rejects_malformed_cursors() {
        assert_eq!(
            RecordCursor::parse("0-15").unwrap_err(),
            QueryError::InvalidCursor
        );
        assert_eq!(
            RecordCursor::parse("0:15,0:16").unwrap_err(),
            QueryError::InvalidCursor
        );
        assert_eq!(
            RecordCursor::parse("0:-1").unwrap_err(),
            QueryError::InvalidCursor
        );
    }
}
