pub mod batch;
pub mod cursor;
pub mod filter;
pub mod obfuscate;
pub mod payload;
pub mod plan;
pub mod query;
pub mod read;
pub mod session;

use std::cmp::Ordering;

use batch::SortKey;
use query::RecordOrder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

/// The name CEL filters and the API use for a compression codec.
pub fn compression_name(compression: Compression) -> &'static str {
    match compression {
        Compression::None => "none",
        Compression::Gzip => "gzip",
        Compression::Snappy => "snappy",
        Compression::Lz4 => "lz4",
        Compression::Zstd => "zstd",
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<String>,
    pub value: Option<String>,
    pub schema_id: Option<i32>,
    pub headers: Vec<RecordHeader>,
    pub size_bytes: u64,
    pub compression: Compression,
}

impl Record {
    pub fn sort_key(&self) -> SortKey {
        SortKey {
            timestamp: self.timestamp,
            partition: self.partition,
            offset: self.offset,
        }
    }

    pub fn cmp_for_order(&self, other: &Self, order: RecordOrder) -> Ordering {
        self.sort_key().cmp_for_order(&other.sort_key(), order)
    }
}

/// One page of a browse.
///
/// `complete` is false when the scan hit its deadline with windows still
/// unread: the records are real, but the page is not everything the query
/// would have matched. `next_cursor` then resumes where the scan stopped
/// rather than where the page ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordPage {
    pub records: Vec<Record>,
    pub complete: bool,
    pub next_cursor: Option<String>,
    pub prev_cursor: Option<String>,
}

impl RecordPage {
    pub fn empty() -> Self {
        Self {
            records: Vec::new(),
            complete: true,
            next_cursor: None,
            prev_cursor: None,
        }
    }

    pub fn has_more(&self) -> bool {
        self.next_cursor.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(timestamp: i64, partition: i32, offset: i64) -> Record {
        Record {
            topic: "orders".to_owned(),
            partition,
            offset,
            timestamp,
            key: None,
            value: None,
            schema_id: None,
            headers: Vec::new(),
            size_bytes: 0,
            compression: Compression::None,
        }
    }

    #[test]
    fn newest_orders_by_timestamp_then_partition_then_offset() {
        assert_eq!(
            record(200, 0, 1).cmp_for_order(&record(100, 0, 1), RecordOrder::Newest),
            Ordering::Less
        );
        assert_eq!(
            record(100, 0, 9).cmp_for_order(&record(100, 1, 0), RecordOrder::Newest),
            Ordering::Less
        );
        assert_eq!(
            record(100, 0, 9).cmp_for_order(&record(100, 0, 8), RecordOrder::Newest),
            Ordering::Less
        );
    }

    #[test]
    fn oldest_is_the_mirror_of_newest() {
        assert_eq!(
            record(100, 0, 1).cmp_for_order(&record(200, 0, 1), RecordOrder::Oldest),
            Ordering::Less
        );
        assert_eq!(
            record(100, 0, 8).cmp_for_order(&record(100, 0, 9), RecordOrder::Oldest),
            Ordering::Less
        );
    }

    #[test]
    fn an_empty_page_is_complete_and_has_no_edges() {
        let page = RecordPage::empty();

        assert!(page.complete);
        assert!(!page.has_more());
        assert!(page.prev_cursor.is_none());
    }

    #[test]
    fn compression_names_are_stable() {
        assert_eq!(compression_name(Compression::None), "none");
        assert_eq!(compression_name(Compression::Zstd), "zstd");
    }
}
