use std::collections::BTreeMap;
use std::sync::Arc;

use cel::extractors::This;
use cel::{Context, Program, Timestamp, Value};

use crate::kafka::record::filter::RecordFilter;
use crate::kafka::record::{Compression, RecordHeader};
use crate::kafka::scan::payload::DecodedPayload;
use crate::utils::datetime_from_unix_millis;

const PAYLOAD_VARS: &[&str] = &["key", "value", "keyText", "valueText"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetaVerdict {
    Pass,
    Fail,
    NeedsPayload,
}

pub(crate) struct RecordMeta<'a> {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub size_bytes: u64,
    pub topic: &'a str,
    pub schema_id: Option<i32>,
    pub compression: Compression,
    pub headers: &'a [RecordHeader],
}

pub(crate) enum CompiledFilter {
    Contains(ContainsFilter),
    Cel(CelFilter),
}

pub(crate) struct ContainsFilter {
    needle: Vec<u8>,
}

pub(crate) struct CelFilter {
    program: Arc<Program>,
    payload: bool,
}

impl CompiledFilter {
    pub(crate) fn compile(filter: &RecordFilter) -> Self {
        if let Some(needle) = contains_needle(filter.source()) {
            return Self::Contains(ContainsFilter {
                needle: needle.into_bytes(),
            });
        }
        Self::Cel(CelFilter::new(filter.program()))
    }

    pub(crate) fn on_meta(&self, meta: &RecordMeta<'_>) -> MetaVerdict {
        match self {
            Self::Contains(_) => MetaVerdict::NeedsPayload,
            Self::Cel(filter) if filter.payload => MetaVerdict::NeedsPayload,
            Self::Cel(filter) => {
                if filter.eval(meta, None, None) {
                    MetaVerdict::Pass
                } else {
                    MetaVerdict::Fail
                }
            }
        }
    }

    pub(crate) fn on_payload(
        &self,
        meta: &RecordMeta<'_>,
        key: &DecodedPayload,
        value: &DecodedPayload,
    ) -> bool {
        match self {
            Self::Contains(filter) => filter.matches(key, value),
            Self::Cel(filter) => filter.eval(meta, Some(key), Some(value)),
        }
    }
}

impl ContainsFilter {
    fn matches(&self, key: &DecodedPayload, value: &DecodedPayload) -> bool {
        payload_contains(key, &self.needle) || payload_contains(value, &self.needle)
    }
}

fn payload_contains(payload: &DecodedPayload, needle: &[u8]) -> bool {
    if payload.is_framed() {
        payload.contains_ignore_ascii(needle)
    } else {
        crate::kafka::scan::payload::contains_ignore_ascii(payload.raw(), needle)
    }
}

impl CelFilter {
    fn new(program: Arc<Program>) -> Self {
        let payload = program
            .references()
            .variables()
            .into_iter()
            .any(|name| PAYLOAD_VARS.contains(&name));
        Self { program, payload }
    }

    fn eval(
        &self,
        meta: &RecordMeta<'_>,
        key: Option<&DecodedPayload>,
        value: Option<&DecodedPayload>,
    ) -> bool {
        let mut context = Context::default();
        context.add_function("lowerAscii", lower_ascii);
        if bind_meta(&mut context, meta).is_err() {
            return false;
        }
        if let (Some(key), Some(value)) = (key, value)
            && bind_payload(&mut context, key, value).is_err()
        {
            return false;
        }
        matches!(self.program.execute(&context), Ok(Value::Bool(true)))
    }
}

fn bind_meta(
    context: &mut Context<'_>,
    meta: &RecordMeta<'_>,
) -> Result<(), cel::SerializationError> {
    let headers: BTreeMap<&str, &str> = meta
        .headers
        .iter()
        .map(|header| (header.key.as_str(), header.value.as_str()))
        .collect();
    context.add_variable("headers", headers)?;
    context.add_variable("partition", meta.partition)?;
    context.add_variable("offset", meta.offset)?;
    context.add_variable(
        "timestamp",
        Timestamp(datetime_from_unix_millis(meta.timestamp).fixed_offset()),
    )?;
    context.add_variable("size", i64::try_from(meta.size_bytes).unwrap_or(i64::MAX))?;
    context.add_variable("schemaId", meta.schema_id)?;
    context.add_variable("compression", compression_name(meta.compression))?;
    context.add_variable("topic", meta.topic)?;
    Ok(())
}

fn bind_payload(
    context: &mut Context<'_>,
    key: &DecodedPayload,
    value: &DecodedPayload,
) -> Result<(), cel::SerializationError> {
    context.add_variable("key", key.json_or_text())?;
    context.add_variable("value", value.json_or_text())?;
    context.add_variable("keyText", key.text())?;
    context.add_variable("valueText", value.text())?;
    Ok(())
}

fn lower_ascii(This(this): This<Arc<String>>) -> String {
    this.to_ascii_lowercase()
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

fn contains_needle(source: &str) -> Option<String> {
    let source = source.trim();
    let rest = source.strip_prefix("keyText.lowerAscii().contains(")?;
    let (first, rest) = parse_json_string_prefix(rest)?;
    let rest = rest.strip_prefix(") || valueText.lowerAscii().contains(")?;
    let (second, rest) = parse_json_string_prefix(rest)?;
    let rest = rest.strip_prefix(')')?;
    if rest.trim().is_empty() && first == second {
        Some(first)
    } else {
        None
    }
}

fn parse_json_string_prefix(input: &str) -> Option<(String, &str)> {
    if !input.starts_with('"') {
        return None;
    }
    let bytes = input.as_bytes();
    let mut index = 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index += 2,
            b'"' => {
                let literal = &input[..=index];
                let value: String = serde_json::from_str(literal).ok()?;
                return Some((value, &input[index + 1..]));
            }
            _ => index += 1,
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::compile_record_filter;
    use crate::kafka::record::{Compression, Record, RecordHeader};

    fn meta<'a>(headers: &'a [RecordHeader]) -> RecordMeta<'a> {
        RecordMeta {
            partition: 1,
            offset: 42,
            timestamp: 1_700_000_000_000,
            size_bytes: 128,
            topic: "orders.created",
            schema_id: Some(7),
            compression: Compression::Gzip,
            headers,
        }
    }

    fn filter(source: &str) -> CompiledFilter {
        CompiledFilter::compile(compile_record_filter(source).unwrap().as_ref().unwrap())
    }

    #[test]
    fn detects_ui_contains_filter() {
        let compiled = filter(
            r#"keyText.lowerAscii().contains("ord_") || valueText.lowerAscii().contains("ord_")"#,
        );
        assert!(matches!(compiled, CompiledFilter::Contains(_)));
    }

    #[test]
    fn metadata_only_cel_does_not_need_payload() {
        let compiled = filter("partition == 1 && offset == 42 && size == 128");
        let headers = [RecordHeader {
            key: "x-trace-id".into(),
            value: "abc".into(),
        }];
        assert_eq!(compiled.on_meta(&meta(&headers)), MetaVerdict::Pass);
        assert_eq!(
            filter("partition == 0").on_meta(&meta(&headers)),
            MetaVerdict::Fail
        );
    }

    #[test]
    fn payload_cel_needs_decode() {
        let compiled = filter(r#"value.status == "FAILED""#);
        assert_eq!(compiled.on_meta(&meta(&[])), MetaVerdict::NeedsPayload);
    }

    #[test]
    fn contains_matches_unframed_bytes() {
        let compiled = filter(
            r#"keyText.lowerAscii().contains("ord_") || valueText.lowerAscii().contains("ord_")"#,
        );
        let key = DecodedPayload::from_raw(b"ORD_1".to_vec(), None, false);
        let value = DecodedPayload::from_raw(b"miss".to_vec(), None, false);
        assert!(compiled.on_payload(&meta(&[]), &key, &value));
    }

    #[test]
    fn record_filter_still_matches_json_paths() {
        let record = Record {
            topic: "orders.created".into(),
            partition: 1,
            offset: 42,
            timestamp: 1_700_000_000_000,
            key: Some("ord_1".into()),
            value: Some(r#"{"status":"FAILED"}"#.into()),
            schema_id: Some(7),
            headers: Vec::new(),
            size_bytes: 128,
            compression: Compression::Gzip,
        };
        assert!(
            compile_record_filter(r#"value.status == "FAILED""#)
                .unwrap()
                .unwrap()
                .matches(&record)
        );
    }
}
