use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use cel::extractors::This;
use cel::{Context, Program, Timestamp, Value};
use chrono::{DateTime, Utc};

use crate::kafka::error::QueryError;
use crate::kafka::record::{Compression, Record};

/// Reject expressions larger than this so a browse request cannot carry an
/// arbitrarily large program.
const MAX_FILTER_BYTES: usize = 4096;

/// A compiled CEL predicate evaluated against each decoded record.
///
/// Compile once per query. Evaluation errors (missing fields, type mismatches)
/// are treated as a non-match so one bad payload cannot fail the page.
#[derive(Clone)]
pub struct RecordFilter {
    source: String,
    program: Arc<Program>,
}

impl PartialEq for RecordFilter {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
    }
}

impl Eq for RecordFilter {}

impl Debug for RecordFilter {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RecordFilter")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl RecordFilter {
    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn matches(&self, record: &Record) -> bool {
        let Ok(context) = record_context(record) else {
            return false;
        };
        matches!(self.program.execute(&context), Ok(Value::Bool(true)))
    }
}

/// Compile a CEL filter. Whitespace-only input means no predicate.
pub fn compile(source: &str) -> Result<Option<RecordFilter>, QueryError> {
    let trimmed = source.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > MAX_FILTER_BYTES {
        return Err(QueryError::InvalidFilter(format!(
            "filter must be at most {MAX_FILTER_BYTES} bytes"
        )));
    }

    let program =
        Program::compile(trimmed).map_err(|error| QueryError::InvalidFilter(error.to_string()))?;

    Ok(Some(RecordFilter {
        source: trimmed.to_owned(),
        program: Arc::new(program),
    }))
}

fn record_context(record: &Record) -> Result<Context<'static>, cel::SerializationError> {
    let mut context = Context::default();
    // cel-rust's stdlib does not yet include string.lowerAscii from the CEL spec.
    context.add_function("lowerAscii", lower_ascii);
    context.add_variable("key", payload(record.key.as_deref()))?;
    context.add_variable("value", payload(record.value.as_deref()))?;
    context.add_variable("keyText", record.key.as_deref().unwrap_or(""))?;
    context.add_variable("valueText", record.value.as_deref().unwrap_or(""))?;
    context.add_variable("headers", header_map(record))?;
    context.add_variable("partition", record.partition)?;
    context.add_variable("offset", record.offset)?;
    context.add_variable("timestamp", Timestamp(record_timestamp(record.timestamp)))?;
    context.add_variable("size", i64::try_from(record.size_bytes).unwrap_or(i64::MAX))?;
    context.add_variable("schemaId", record.schema_id)?;
    context.add_variable("compression", compression_name(record.compression))?;
    context.add_variable("topic", record.topic.as_str())?;
    Ok(context)
}

fn lower_ascii(This(this): This<Arc<String>>) -> String {
    this.to_ascii_lowercase()
}

fn payload(text: Option<&str>) -> serde_json::Value {
    match text {
        None => serde_json::Value::Null,
        Some(text) => serde_json::from_str(text)
            .unwrap_or_else(|_| serde_json::Value::String(text.to_owned())),
    }
}

fn header_map(record: &Record) -> BTreeMap<&str, &str> {
    record
        .headers
        .iter()
        .map(|header| (header.key.as_str(), header.value.as_str()))
        .collect()
}

fn record_timestamp(millis: i64) -> chrono::DateTime<chrono::FixedOffset> {
    DateTime::<Utc>::from_timestamp_millis(millis)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
        .fixed_offset()
}

fn compression_name(compression: Compression) -> &'static str {
    match compression {
        Compression::None => "none",
        Compression::Gzip => "gzip",
        Compression::Snappy => "snappy",
        Compression::Lz4 => "lz4",
        Compression::Zstd => "zstd",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::record::RecordHeader;

    fn record() -> Record {
        Record {
            topic: "orders.created".into(),
            partition: 1,
            offset: 42,
            timestamp: 1_700_000_000_000,
            key: Some("ord_1".into()),
            value: Some(r#"{"status":"FAILED","user":{"id":42},"amount":9.5}"#.into()),
            schema_id: Some(7),
            headers: vec![RecordHeader {
                key: "x-trace-id".into(),
                value: "abc".into(),
            }],
            size_bytes: 128,
            compression: Compression::Gzip,
        }
    }

    fn filter(source: &str) -> RecordFilter {
        compile(source).unwrap().expect("filter should compile")
    }

    #[test]
    fn empty_source_is_no_filter() {
        assert_eq!(compile("").unwrap(), None);
        assert_eq!(compile("   \n").unwrap(), None);
    }

    #[test]
    fn rejects_invalid_cel() {
        let error = compile("value.status ==").unwrap_err();
        assert!(error.to_string().contains("invalid filter"), "{error}");
    }

    #[test]
    fn rejects_oversized_source() {
        let source = "true && ".repeat(MAX_FILTER_BYTES);
        assert!(matches!(
            compile(&source),
            Err(QueryError::InvalidFilter(_))
        ));
    }

    #[test]
    fn matches_json_field_path() {
        assert!(filter(r#"value.status == "FAILED""#).matches(&record()));
        assert!(!filter(r#"value.status == "OK""#).matches(&record()));
        assert!(filter("value.user.id == 42").matches(&record()));
    }

    #[test]
    fn matches_headers_and_metadata() {
        let sample = record();
        assert!(filter(r#"headers["x-trace-id"] == "abc""#).matches(&sample));
        assert!(filter("partition == 1 && offset == 42 && size == 128").matches(&sample));
        assert!(filter(r#"compression == "gzip" && schemaId == 7"#).matches(&sample));
        assert!(filter(r#"topic == "orders.created""#).matches(&sample));
    }

    #[test]
    fn contains_fold_uses_raw_text() {
        assert!(
            filter(r#"keyText.lowerAscii().contains("ord_") || valueText.lowerAscii().contains("ord_")"#)
                .matches(&record())
        );
        assert!(filter(r#"valueText.lowerAscii().contains("failed")"#).matches(&record()));
        assert!(!filter(r#"valueText.lowerAscii().contains("missing")"#).matches(&record()));
    }

    #[test]
    fn eval_errors_are_non_matches() {
        assert!(!filter("value.missing.nested == 1").matches(&record()));
        assert!(!filter("key").matches(&record()));
    }

    #[test]
    fn unparsed_payloads_bind_as_strings() {
        let mut sample = record();
        sample.key = Some("plain-key".into());
        sample.value = Some("not-json".into());
        assert!(filter(r#"key == "plain-key" && value == "not-json""#).matches(&sample));
    }

    #[test]
    fn null_payloads_are_null() {
        let mut sample = record();
        sample.key = None;
        sample.value = None;
        assert!(filter("key == null && value == null && keyText == \"\"").matches(&sample));
    }
}
