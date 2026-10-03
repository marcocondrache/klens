use bytes::Bytes;

use crate::kafka::model::RawRecord;
use crate::kafka::scan::RecordHeader;

pub const ORDERS: &str = "orders.created";

#[derive(Debug, Clone)]
pub struct FixtureRecord {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp: i64,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<RecordHeader>,
}

impl FixtureRecord {
    pub fn new(topic: &str, partition: i32, offset: i64) -> Self {
        Self {
            topic: topic.to_owned(),
            partition,
            offset,
            timestamp: offset,
            key: None,
            value: None,
            headers: Vec::new(),
        }
    }

    pub fn order(partition: i32, offset: i64) -> Self {
        Self::new(ORDERS, partition, offset)
    }

    pub fn at(mut self, timestamp: i64) -> Self {
        self.timestamp = timestamp;
        self
    }

    pub fn key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn value(mut self, value: impl Into<Bytes>) -> Self {
        self.value = Some(value.into());
        self
    }

    pub fn header(mut self, key: &str, value: &str) -> Self {
        self.headers.push(RecordHeader {
            key: key.to_owned(),
            value: value.to_owned(),
        });
        self
    }

    pub(super) fn topic(&self) -> &str {
        &self.topic
    }

    pub(super) fn partition(&self) -> i32 {
        self.partition
    }

    pub(super) fn offset(&self) -> i64 {
        self.offset
    }

    pub(super) fn timestamp(&self) -> i64 {
        self.timestamp
    }

    pub(super) fn raw(&self) -> RawRecord {
        RawRecord {
            partition: self.partition,
            offset: self.offset,
            timestamp: self.timestamp,
            key: self.key.clone(),
            value: self.value.clone(),
            headers: self
                .headers
                .iter()
                .map(|header| {
                    (
                        Bytes::from(header.key.clone()),
                        Some(Bytes::from(header.value.clone())),
                    )
                })
                .collect(),
        }
    }
}

pub fn framed(schema_id: u32, body: &str) -> Bytes {
    schemreg::encode_wire_format(schema_id, body.as_bytes())
}

pub fn card_record(offset: i64, pan: &str) -> FixtureRecord {
    FixtureRecord::order(0, offset)
        .at(1_700_000_000_000 + offset)
        .key(format!("ord_{offset}"))
        .value(framed(
            7,
            &format!(r#"{{"orderId":"ord_{offset}","card":{{"number":"{pan}","cvv":"123"}}}}"#),
        ))
        .header("x-user-id", "ada")
}
