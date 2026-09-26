use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

use crate::kafka::error::QueryError;

use super::query::RecordOrder;

const VERSION: &str = "v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorDirection {
    Forward,
    Backward,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordCursor {
    pub order: RecordOrder,
    pub remaining: Remaining,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Remaining {
    From(BTreeMap<i32, i64>),
    Before(BTreeMap<i32, i64>),
}

impl Remaining {
    pub fn walking(walk: RecordOrder, offsets: BTreeMap<i32, i64>) -> Self {
        match walk {
            RecordOrder::Oldest => Self::From(offsets),
            RecordOrder::Newest => Self::Before(offsets),
        }
    }

    pub fn walk(&self) -> RecordOrder {
        match self {
            Self::From(_) => RecordOrder::Oldest,
            Self::Before(_) => RecordOrder::Newest,
        }
    }

    fn offsets(&self) -> &BTreeMap<i32, i64> {
        match self {
            Self::From(offsets) | Self::Before(offsets) => offsets,
        }
    }
}

impl RecordCursor {
    pub fn walk(&self) -> RecordOrder {
        self.remaining.walk()
    }

    pub fn direction(&self) -> CursorDirection {
        if self.walk() == self.order {
            CursorDirection::Forward
        } else {
            CursorDirection::Backward
        }
    }

    pub fn parse(value: &str) -> Result<Self, QueryError> {
        let trimmed = value.trim();
        let mut parts = trimmed.splitn(4, ':');
        let (Some(VERSION), Some(order), Some(direction)) =
            (parts.next(), parts.next(), parts.next())
        else {
            return Err(QueryError::InvalidCursor);
        };

        let order = match order {
            "n" => RecordOrder::Newest,
            "o" => RecordOrder::Oldest,
            _ => return Err(QueryError::InvalidCursor),
        };
        let walk = match direction {
            "f" => order,
            "b" => order.flipped(),
            _ => return Err(QueryError::InvalidCursor),
        };

        let mut offsets = BTreeMap::new();
        for part in parts.next().unwrap_or_default().split(',') {
            if part.is_empty() {
                continue;
            }
            let (partition, offset) = part.split_once(':').ok_or(QueryError::InvalidCursor)?;
            let partition: i32 = partition.parse().map_err(|_| QueryError::InvalidCursor)?;
            let offset: i64 = offset.parse().map_err(|_| QueryError::InvalidCursor)?;
            if offset < 0 || offsets.insert(partition, offset).is_some() {
                return Err(QueryError::InvalidCursor);
            }
        }

        Ok(Self {
            order,
            remaining: Remaining::walking(walk, offsets),
        })
    }

    pub fn validate_for(&self, order: RecordOrder) -> Result<(), QueryError> {
        if self.order == order {
            Ok(())
        } else {
            Err(QueryError::InvalidCursor)
        }
    }

    pub fn encode(&self) -> String {
        self.to_string()
    }
}

impl Display for RecordCursor {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        let order = match self.order {
            RecordOrder::Newest => "n",
            RecordOrder::Oldest => "o",
        };
        let direction = match self.direction() {
            CursorDirection::Forward => "f",
            CursorDirection::Backward => "b",
        };
        write!(formatter, "{VERSION}:{order}:{direction}:")?;
        for (index, (partition, offset)) in self.remaining.offsets().iter().enumerate() {
            let separator = if index == 0 { "" } else { "," };
            write!(formatter, "{separator}{partition}:{offset}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_order_direction_and_offsets() {
        let cursor = RecordCursor::parse("v2:n:f:1:8,0:15").unwrap();

        assert_eq!(cursor.order, RecordOrder::Newest);
        assert_eq!(cursor.direction(), CursorDirection::Forward);
        assert_eq!(
            cursor.remaining,
            Remaining::Before(BTreeMap::from([(0, 15), (1, 8)]))
        );
        assert_eq!(cursor.encode(), "v2:n:f:0:15,1:8");
        assert_eq!(RecordCursor::parse(&cursor.encode()).unwrap(), cursor);
    }

    #[test]
    fn every_order_and_direction_round_trips_byte_for_byte() {
        for (token, walk) in [
            ("v2:n:f:0:40,3:7", RecordOrder::Newest),
            ("v2:n:b:0:40,3:7", RecordOrder::Oldest),
            ("v2:o:f:0:40,3:7", RecordOrder::Oldest),
            ("v2:o:b:0:40,3:7", RecordOrder::Newest),
        ] {
            let cursor = RecordCursor::parse(token).unwrap();

            assert_eq!(cursor.walk(), walk, "{token}");
            assert_eq!(cursor.encode(), token);
        }
    }

    #[test]
    fn an_empty_boundary_list_round_trips() {
        let cursor = RecordCursor::parse("v2:o:b:").unwrap();

        assert_eq!(cursor.remaining, Remaining::Before(BTreeMap::new()));
        assert_eq!(cursor.encode(), "v2:o:b:");
        assert_eq!(RecordCursor::parse("v2:o:b").unwrap(), cursor);
    }

    #[test]
    fn rejects_malformed_cursors() {
        for source in [
            "",
            "0:15",
            "v1:n:f:0:15",
            "v2:x:f:0:15",
            "v2:n:x:0:15",
            "v2:n:f:0-15",
            "v2:n:f:0:15,0:16",
            "v2:n:f:0:-1",
        ] {
            assert_eq!(
                RecordCursor::parse(source).unwrap_err(),
                QueryError::InvalidCursor,
                "{source} should not parse"
            );
        }
    }

    #[test]
    fn a_cursor_is_bound_to_the_order_that_minted_it() {
        let cursor = RecordCursor::parse("v2:n:f:0:15").unwrap();

        assert!(cursor.validate_for(RecordOrder::Newest).is_ok());
        assert_eq!(
            cursor.validate_for(RecordOrder::Oldest).unwrap_err(),
            QueryError::InvalidCursor
        );
    }

    #[test]
    fn a_backward_page_walks_the_log_the_other_way() {
        let forward = RecordCursor::parse("v2:n:f:0:15").unwrap();
        let backward = RecordCursor::parse("v2:n:b:0:15").unwrap();

        assert_eq!(forward.walk(), RecordOrder::Newest);
        assert_eq!(backward.walk(), RecordOrder::Oldest);
        assert_eq!(
            RecordCursor::parse("v2:o:b:0:15").unwrap().walk(),
            RecordOrder::Newest
        );
    }
}
