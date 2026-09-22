use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::kafka::model as domain;
use crate::kafka::{QueryError, RecordCursor, Tail, TailBatch, TailPosition, TailQuery};
use crate::r#macro::from_same_variants;

use super::super::int64::Int64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Compression {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

from_same_variants!(domain::Compression => Compression { None, Gzip, Snappy, Lz4, Zstd });

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RecordOrder {
    Newest,
    Oldest,
}

from_same_variants!(RecordOrder => domain::RecordOrder { Newest, Oldest });

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
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
    pub offset: Int64,
    pub timestamp: Timestamp,
    pub key: Option<String>,
    pub value: Option<String>,
    pub schema_id: Option<i32>,
    pub headers: Vec<RecordHeader>,
    pub size_bytes: Int64,
    pub compression: Compression,
}

impl From<domain::Record> for Record {
    fn from(record: domain::Record) -> Self {
        Self {
            topic: record.topic,
            partition: record.partition,
            offset: record.offset.into(),
            timestamp: Timestamp::from_millisecond(record.timestamp)
                .unwrap_or(Timestamp::UNIX_EPOCH),
            key: record.key,
            value: record.value,
            schema_id: record.schema_id,
            headers: record.headers.into_iter().map(Into::into).collect(),
            size_bytes: record.size_bytes.into(),
            compression: record.compression.into(),
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

pub(crate) fn record_query(
    topic: String,
    params: RecordParams,
) -> Result<domain::RecordQuery, QueryError> {
    Ok(domain::RecordQuery {
        timestamps: domain::TimestampRange::new(params.from, params.to)?,
        filter: params
            .contains
            .as_deref()
            .and_then(crate::kafka::compile_contains_filter),
        cursor: match params.cursor.as_deref().map(str::trim) {
            None | Some("") => None,
            Some(cursor) => Some(RecordCursor::parse(cursor)?),
        },
        topic,
        partitions: parse_partitions(params.partition.as_deref())?,
        limit: params.limit,
        order: params.order.unwrap_or(RecordOrder::Newest).into(),
        schema_id: params.schema_id,
    })
}

fn parse_partitions(raw: Option<&str>) -> Result<Vec<i32>, QueryError> {
    let Some(raw) = raw.map(str::trim).filter(|raw| !raw.is_empty()) else {
        return Ok(Vec::new());
    };

    raw.split(',')
        .map(|part| {
            part.trim()
                .parse::<i32>()
                .ok()
                .filter(|id| *id >= 0)
                .ok_or(QueryError::InvalidPartition)
        })
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecordParams {
    /// Comma-separated partition ids, such as `0,2`; absent reads them all.
    pub partition: Option<String>,
    pub order: Option<RecordOrder>,
    pub from: Option<Timestamp>,
    pub to: Option<Timestamp>,
    #[serde(default = "default_record_limit")]
    pub limit: i32,
    pub contains: Option<String>,
    pub schema_id: Option<i32>,
    pub cursor: Option<String>,
}

fn default_record_limit() -> i32 {
    50
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TailStart {
    pub partition: i32,
    pub offset: Int64,
}

impl From<&TailPosition> for TailStart {
    fn from(position: &TailPosition) -> Self {
        Self {
            partition: position.partition,
            offset: position.offset.into(),
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
        skipped: Int64,
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
            skipped: batch.skipped.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TailParams {
    /// Comma-separated partition ids, such as `0,2`; absent follows them all.
    pub partition: Option<String>,
    pub contains: Option<String>,
    pub schema_id: Option<i32>,
}

pub(crate) fn tail_query(topic: String, params: TailParams) -> Result<TailQuery, QueryError> {
    Ok(TailQuery {
        filter: params
            .contains
            .as_deref()
            .and_then(crate::kafka::compile_contains_filter),
        topic,
        partitions: parse_partitions(params.partition.as_deref())?,
        schema_id: params.schema_id,
    })
}
