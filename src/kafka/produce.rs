use bytes::Bytes;

use crate::kafka::scan::RecordHeader;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewRecord {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<RecordHeader>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducedRecord {
    pub partition: i32,
    pub offset: i64,
}
