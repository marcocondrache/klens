use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::app::error::ApiError;
use crate::kafka::model as domain;
use crate::kafka::{QueryError, RecordCursor, Tail, TailBatch, TailPosition, TailQuery};
use crate::r#macro::from_same_variants;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RecordOrder {
    Newest,
    Oldest,
}

from_same_variants!(RecordOrder => domain::RecordOrder { Newest, Oldest });

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecordHeader {
    pub key: String,
    pub value: String,
}

impl From<domain::RecordHeader> for RecordHeader {
    fn from(header: domain::RecordHeader) -> Self {
        Self {
            key: header.key,
            value: header.value,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Timestamp,
    pub key: Option<String>,
    pub value: Option<String>,
    pub schema_id: Option<i32>,
    pub headers: Vec<RecordHeader>,
    pub size_bytes: u64,
    /// True when the key, value and headers show the record's exact bytes,
    /// so producing them again writes the same record.
    pub verbatim: bool,
}

impl From<domain::Record> for Record {
    fn from(record: domain::Record) -> Self {
        Self {
            topic: record.topic,
            partition: record.partition,
            offset: record.offset,
            timestamp: Timestamp::from_millisecond(record.timestamp)
                .unwrap_or(Timestamp::UNIX_EPOCH),
            key: record.key,
            value: record.value,
            schema_id: record.schema_id,
            headers: record.headers.into_iter().map(Into::into).collect(),
            size_bytes: record.size_bytes,
            verbatim: record.verbatim,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RecordPage {
    pub records: Vec<Record>,
    /// False when the scan hit its deadline with windows still unread: the
    /// records are real, but the page is not everything the query matched.
    /// `nextCursor` resumes where the scan stopped.
    pub complete: bool,
    /// True when an obfuscation rule covers this topic. Keys, values, and
    /// headers are then a view of the records: protected fields render as
    /// `***` or as `kx:` tokens, a value that never decoded may be masked
    /// whole, and filters match that view rather than the wire record.
    pub obfuscated: bool,
    pub next_cursor: Option<String>,
    pub prev_cursor: Option<String>,
}

impl From<domain::RecordPage> for RecordPage {
    fn from(page: domain::RecordPage) -> Self {
        Self {
            records: page.records.into_iter().map(Into::into).collect(),
            complete: page.complete,
            obfuscated: page.obfuscated,
            next_cursor: page.next_cursor,
            prev_cursor: page.prev_cursor,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PayloadEncoding {
    Text,
    Base64,
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecordPayload {
    pub encoding: PayloadEncoding,
    pub data: String,
}

impl RecordPayload {
    fn into_bytes(self, part: &str) -> Result<Bytes, ApiError> {
        match self.encoding {
            PayloadEncoding::Text => Ok(Bytes::from(self.data)),
            PayloadEncoding::Base64 => {
                let mut data = self.data.into_bytes();
                // Pasted base64 is often wrapped at 76 columns.
                data.retain(|byte| !byte.is_ascii_whitespace());
                STANDARD
                    .decode(data)
                    .map(Bytes::from)
                    .map_err(|_| ApiError::unprocessable(format!("the {part} is not valid base64")))
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProduceRecord {
    /// When omitted, the producer picks one, by the key's hash when there is a key.
    #[ts(optional)]
    pub partition: Option<i32>,
    /// `null` sends a record without a key.
    pub key: Option<RecordPayload>,
    /// `null` sends a tombstone.
    pub value: Option<RecordPayload>,
    #[serde(default)]
    pub headers: Vec<RecordHeader>,
}

impl ProduceRecord {
    pub(crate) fn into_record(self, topic: String) -> Result<domain::NewRecord, ApiError> {
        Ok(domain::NewRecord {
            topic,
            partition: self.partition,
            key: self.key.map(|key| key.into_bytes("key")).transpose()?,
            value: self
                .value
                .map(|value| value.into_bytes("value"))
                .transpose()?,
            headers: self
                .headers
                .into_iter()
                .map(|header| domain::RecordHeader {
                    key: header.key,
                    value: header.value,
                })
                .collect(),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeleteParams {
    #[serde(default)]
    pub partition: Vec<i32>,
    pub before: Option<i64>,
}

impl DeleteParams {
    pub(crate) fn before(&self) -> Result<Option<i64>, ApiError> {
        match self.before {
            Some(offset) if offset < 0 => Err(ApiError::unprocessable(
                "before must be an offset of zero or more",
            )),
            before => Ok(before),
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ProducedRecord {
    pub partition: i32,
    pub offset: i64,
}

impl From<domain::ProducedRecord> for ProducedRecord {
    fn from(produced: domain::ProducedRecord) -> Self {
        Self {
            partition: produced.partition,
            offset: produced.offset,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RecordLookup {
    pub record: Record,
    pub obfuscated: bool,
}

impl From<domain::FoundRecord> for RecordLookup {
    fn from(found: domain::FoundRecord) -> Self {
        Self {
            record: found.record.into(),
            obfuscated: found.obfuscated,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LookupParams {
    pub schema_id: Option<i32>,
}

pub(crate) fn record_at(
    topic: String,
    partition: i32,
    offset: i64,
    params: LookupParams,
) -> domain::RecordAt {
    domain::RecordAt {
        topic,
        partition,
        offset,
        schema_id: params.schema_id,
    }
}

pub(crate) fn record_query(
    topic: String,
    params: RecordParams,
) -> Result<domain::RecordQuery, QueryError> {
    let order = params.order.unwrap_or(RecordOrder::Newest).into();
    Ok(domain::RecordQuery {
        timestamps: domain::TimestampRange::new(params.from, params.to)?,
        filter: params
            .contains
            .as_deref()
            .and_then(crate::kafka::compile_contains_filter),
        cursor: match params.cursor.as_deref().map(str::trim) {
            None | Some("") => None,
            Some(cursor) => {
                let cursor = RecordCursor::parse(cursor)?;
                cursor.validate_for(order)?;
                Some(cursor)
            }
        },
        topic,
        partitions: params.partition,
        limit: params.limit,
        order,
        schema_id: params.schema_id,
    })
}

/// Asks for the largest page the limits allow.
pub(crate) fn export_query(
    topic: String,
    params: ExportParams,
) -> Result<domain::RecordQuery, QueryError> {
    Ok(domain::RecordQuery {
        timestamps: domain::TimestampRange::new(params.from, params.to)?,
        filter: params
            .contains
            .as_deref()
            .and_then(crate::kafka::compile_contains_filter),
        cursor: None,
        topic,
        partitions: params.partition,
        limit: i32::MAX,
        order: params.order.unwrap_or(RecordOrder::Newest).into(),
        schema_id: params.schema_id,
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordParams {
    #[serde(default)]
    pub partition: Vec<i32>,
    pub order: Option<RecordOrder>,
    pub from: Option<Timestamp>,
    pub to: Option<Timestamp>,
    #[serde(default = "default_record_limit")]
    pub limit: i32,
    pub contains: Option<String>,
    pub schema_id: Option<i32>,
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportParams {
    #[serde(default)]
    pub partition: Vec<i32>,
    pub order: Option<RecordOrder>,
    pub from: Option<Timestamp>,
    pub to: Option<Timestamp>,
    pub contains: Option<String>,
    pub schema_id: Option<i32>,
}

fn default_record_limit() -> i32 {
    50
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TailStart {
    pub partition: i32,
    pub offset: i64,
}

impl From<&TailPosition> for TailStart {
    fn from(position: &TailPosition) -> Self {
        Self {
            partition: position.partition,
            offset: position.offset,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum TailEvent {
    Ready {
        start: Vec<TailStart>,
        obfuscated: bool,
    },
    Records {
        records: Vec<Record>,
        skipped: u64,
    },
}

impl TailEvent {
    pub(crate) fn ready(tail: &Tail) -> Self {
        Self::Ready {
            start: tail.start().iter().map(TailStart::from).collect(),
            obfuscated: tail.obfuscated(),
        }
    }

    pub(crate) fn event(&self) -> &'static str {
        match self {
            Self::Ready { .. } => "ready",
            Self::Records { .. } => "records",
        }
    }
}

impl From<TailBatch> for TailEvent {
    fn from(batch: TailBatch) -> Self {
        Self::Records {
            records: batch.records.into_iter().map(Into::into).collect(),
            skipped: batch.skipped,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TailParams {
    #[serde(default)]
    pub partition: Vec<i32>,
    pub contains: Option<String>,
    pub schema_id: Option<i32>,
}

pub(crate) fn tail_query(topic: String, params: TailParams) -> TailQuery {
    TailQuery {
        filter: params
            .contains
            .as_deref()
            .and_then(crate::kafka::compile_contains_filter),
        topic,
        partitions: params.partition,
        schema_id: params.schema_id,
    }
}
