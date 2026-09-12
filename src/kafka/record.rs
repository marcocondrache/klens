pub mod cursor;
pub mod filter;
pub mod page;
pub mod plan;
pub mod query;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
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
    pub fn cmp_for_order(&self, other: &Self, order: query::RecordOrder) -> std::cmp::Ordering {
        match order {
            query::RecordOrder::Newest => self
                .timestamp
                .cmp(&other.timestamp)
                .reverse()
                .then(self.partition.cmp(&other.partition))
                .then(self.offset.cmp(&other.offset).reverse()),
            query::RecordOrder::Oldest => self
                .timestamp
                .cmp(&other.timestamp)
                .then(self.partition.cmp(&other.partition))
                .then(self.offset.cmp(&other.offset)),
        }
    }

    pub fn matches(&self, filter: Option<&filter::RecordFilter>) -> bool {
        filter.is_none_or(|filter| filter.matches(self))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordPage {
    pub records: Vec<Record>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
}

/// Best-effort text for bytes that carry no schema.
pub fn decode_bytes(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}
