pub mod cursor;
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
    pub headers: Vec<RecordHeader>,
    pub size_bytes: u64,
    pub compression: Compression,
}

impl Record {
    /// `term` is expected to be lowercase already; the fetch plan normalises it
    /// once rather than per record.
    pub fn matches(&self, term: &str) -> bool {
        if term.is_empty() {
            return true;
        }

        self.key
            .as_deref()
            .is_some_and(|key| key.to_ascii_lowercase().contains(term))
            || self
                .value
                .as_deref()
                .is_some_and(|value| value.to_ascii_lowercase().contains(term))
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
