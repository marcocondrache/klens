pub mod batch;
pub mod cursor;
pub mod export;
pub mod filter;
pub mod obfuscate;
pub mod payload;
pub mod pipeline;
pub mod plan;
pub mod query;
pub mod read;
pub mod session;
pub mod tail;

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordPage {
    pub records: Vec<Record>,
    pub complete: bool,
    pub obfuscated: bool,
    pub next_cursor: Option<String>,
    pub prev_cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundRecord {
    pub record: Record,
    pub obfuscated: bool,
}

impl RecordPage {
    pub fn empty() -> Self {
        Self {
            records: Vec::new(),
            complete: true,
            obfuscated: false,
            next_cursor: None,
            prev_cursor: None,
        }
    }

    pub fn has_more(&self) -> bool {
        self.next_cursor.is_some()
    }
}
