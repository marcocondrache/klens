use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use cel::extractors::This;
use cel::{Context, Env, Program, Timestamp, Value};

use crate::kafka::error::QueryError;
use crate::kafka::scan::payload::DecodedPayload;
use crate::kafka::scan::{Compression, compression_name};
use crate::utils::datetime_from_unix_millis;

const MAX_FILTER_BYTES: usize = 4096;

const PAYLOAD_VARIABLES: [&str; 4] = ["key", "value", "keyText", "valueText"];

/// Built once: `Context::default` rebuilds the whole standard library on
/// every call.
static STDLIB: LazyLock<Arc<Env>> = LazyLock::new(|| Arc::new(Env::stdlib()));

#[derive(Debug, Clone, Copy)]
pub struct RecordMeta<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub size_bytes: u64,
    pub compression: Compression,
    /// Schema id read off the value's Confluent frame, if any.
    pub schema_id: Option<i32>,
    pub headers: &'a [(Bytes, Option<Bytes>)],
}

#[derive(Debug, Clone, Copy)]
pub struct RawField<'a> {
    pub bytes: &'a [u8],
    /// Whether a registry decode would change what these bytes say.
    pub framed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail,
    NeedsPayload,
}

#[derive(Clone)]
pub enum CompiledFilter {
    Contains(ContainsFilter),
    Cel(CelFilter),
}

impl PartialEq for CompiledFilter {
    fn eq(&self, other: &Self) -> bool {
        self.source() == other.source()
    }
}

impl Eq for CompiledFilter {}

impl Debug for CompiledFilter {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CompiledFilter")
            .field("source", &self.source())
            .finish_non_exhaustive()
    }
}

impl CompiledFilter {
    pub fn source(&self) -> &str {
        match self {
            Self::Contains(filter) => &filter.needle,
            Self::Cel(filter) => &filter.source,
        }
    }

    /// `NeedsPayload` means the record survives on metadata and has to be
    /// looked at more closely.
    pub fn on_meta(&self, meta: &RecordMeta<'_>) -> Verdict {
        match self {
            Self::Contains(_) => Verdict::NeedsPayload,
            Self::Cel(filter) => filter.on_meta(meta),
        }
    }

    pub fn on_raw(&self, key: Option<RawField<'_>>, value: Option<RawField<'_>>) -> Verdict {
        match self {
            Self::Contains(filter) => filter.on_raw(key, value),
            Self::Cel(_) => Verdict::NeedsPayload,
        }
    }

    pub fn on_payload(
        &self,
        meta: &RecordMeta<'_>,
        key: Option<&DecodedPayload>,
        value: Option<&DecodedPayload>,
    ) -> bool {
        match self {
            Self::Contains(filter) => filter.on_payload(key, value),
            Self::Cel(filter) => filter.on_payload(meta, key, value),
        }
    }
}

/// Case-insensitive substring over key and value text.
#[derive(Clone)]
pub struct ContainsFilter {
    needle: String,
}

impl ContainsFilter {
    fn matches_bytes(&self, bytes: &[u8]) -> bool {
        contains_ascii_ci(bytes, self.needle.as_bytes())
    }

    fn on_raw(&self, key: Option<RawField<'_>>, value: Option<RawField<'_>>) -> Verdict {
        let mut pending = false;
        for field in [key, value].into_iter().flatten() {
            if field.framed {
                pending = true;
            } else if self.matches_bytes(field.bytes) {
                return Verdict::Pass;
            }
        }

        if pending {
            Verdict::NeedsPayload
        } else {
            Verdict::Fail
        }
    }

    fn on_payload(&self, key: Option<&DecodedPayload>, value: Option<&DecodedPayload>) -> bool {
        [key, value]
            .into_iter()
            .flatten()
            .any(|payload| self.matches_bytes(payload.text().as_bytes()))
    }
}

/// A compiled CEL predicate.
///
/// Evaluation errors (missing fields, type mismatches) are a non-match so one
/// bad payload cannot fail the page.
#[derive(Clone)]
pub struct CelFilter {
    source: String,
    program: Arc<Program>,
    /// Whether the expression reads `key` / `value` / `keyText` / `valueText`.
    /// A metadata-only expression is answered before anything is decoded.
    needs_payload: bool,
}

impl CelFilter {
    pub fn needs_payload(&self) -> bool {
        self.needs_payload
    }

    fn on_meta(&self, meta: &RecordMeta<'_>) -> Verdict {
        if self.needs_payload {
            return Verdict::NeedsPayload;
        }
        match self.evaluate(meta, None, None) {
            true => Verdict::Pass,
            false => Verdict::Fail,
        }
    }

    fn on_payload(
        &self,
        meta: &RecordMeta<'_>,
        key: Option<&DecodedPayload>,
        value: Option<&DecodedPayload>,
    ) -> bool {
        self.evaluate(meta, key, value)
    }

    fn evaluate(
        &self,
        meta: &RecordMeta<'_>,
        key: Option<&DecodedPayload>,
        value: Option<&DecodedPayload>,
    ) -> bool {
        let Ok(context) = record_context(meta, key, value) else {
            return false;
        };
        matches!(self.program.execute(&context), Ok(Value::Bool(true)))
    }
}

/// Compile a CEL filter. Whitespace-only input means no predicate.
pub fn cel(source: &str) -> Result<Option<CompiledFilter>, QueryError> {
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
    let references = program.references();
    let needs_payload = PAYLOAD_VARIABLES
        .iter()
        .any(|name| references.has_variable(name));

    Ok(Some(CompiledFilter::Cel(CelFilter {
        source: trimmed.to_owned(),
        program: Arc::new(program),
        needs_payload,
    })))
}

/// Compile the substring filter. Whitespace-only input means no predicate.
pub fn contains(needle: &str) -> Option<CompiledFilter> {
    let trimmed = needle.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(CompiledFilter::Contains(ContainsFilter {
        needle: trimmed.to_owned(),
    }))
}

fn contains_ascii_ci(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if haystack.len() < needle.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

fn record_context<'a>(
    meta: &RecordMeta<'a>,
    key: Option<&'a DecodedPayload>,
    value: Option<&'a DecodedPayload>,
) -> Result<Context<'a>, cel::SerializationError> {
    let mut context = Context::with_env(Arc::clone(&STDLIB));
    // cel-rust's stdlib does not yet include string.lowerAscii from the CEL spec.
    context.add_function("lowerAscii", lower_ascii);
    context.add_variable("partition", meta.partition)?;
    context.add_variable("offset", meta.offset)?;
    context.add_variable("timestamp", Timestamp(record_timestamp(meta.timestamp)))?;
    context.add_variable("size", i64::try_from(meta.size_bytes).unwrap_or(i64::MAX))?;
    context.add_variable("schemaId", meta.schema_id)?;
    context.add_variable("compression", compression_name(meta.compression))?;
    context.add_variable("topic", meta.topic)?;
    context.add_variable("headers", header_map(meta.headers))?;
    context.add_variable("key", structured(key))?;
    context.add_variable("value", structured(value))?;
    context.add_variable("keyText", key.map(DecodedPayload::text).unwrap_or(""))?;
    context.add_variable("valueText", value.map(DecodedPayload::text).unwrap_or(""))?;
    Ok(context)
}

fn lower_ascii(This(this): This<Arc<String>>) -> String {
    this.to_ascii_lowercase()
}

fn structured(payload: Option<&DecodedPayload>) -> serde_json::Value {
    let Some(payload) = payload else {
        return serde_json::Value::Null;
    };
    if let Some(json) = payload.json() {
        return json.clone();
    }
    let text = payload.text();
    serde_json::from_str(text).unwrap_or_else(|_| serde_json::Value::String(text.to_owned()))
}

fn header_map(headers: &[(Bytes, Option<Bytes>)]) -> std::collections::BTreeMap<String, String> {
    headers
        .iter()
        .map(|(key, value)| {
            (
                String::from_utf8_lossy(key).into_owned(),
                value
                    .as_deref()
                    .map(|value| String::from_utf8_lossy(value).into_owned())
                    .unwrap_or_default(),
            )
        })
        .collect()
}

fn record_timestamp(millis: i64) -> chrono::DateTime<chrono::FixedOffset> {
    datetime_from_unix_millis(millis).fixed_offset()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers() -> Vec<(Bytes, Option<Bytes>)> {
        vec![(
            Bytes::from_static(b"x-trace-id"),
            Some(Bytes::from_static(b"abc")),
        )]
    }

    fn meta<'a>(headers: &'a [(Bytes, Option<Bytes>)]) -> RecordMeta<'a> {
        RecordMeta {
            topic: "orders.created",
            partition: 1,
            offset: 42,
            timestamp: 1_700_000_000_000,
            size_bytes: 128,
            compression: Compression::Gzip,
            schema_id: Some(7),
            headers,
        }
    }

    fn key() -> DecodedPayload {
        DecodedPayload::raw(Bytes::from_static(b"ord_1"))
    }

    fn value() -> DecodedPayload {
        DecodedPayload::raw(Bytes::from_static(
            br#"{"status":"FAILED","user":{"id":42},"amount":9.5}"#,
        ))
    }

    fn filter(source: &str) -> CompiledFilter {
        cel(source).unwrap().expect("filter should compile")
    }

    fn matches(source: &str) -> bool {
        let headers = headers();
        let meta = meta(&headers);
        filter(source).on_payload(&meta, Some(&key()), Some(&value()))
    }

    #[test]
    fn empty_source_is_no_filter() {
        assert_eq!(cel("").unwrap(), None);
        assert_eq!(cel("   \n").unwrap(), None);
        assert_eq!(contains("  "), None);
    }

    #[test]
    fn rejects_invalid_cel() {
        let error = cel("value.status ==").unwrap_err();
        assert!(error.to_string().contains("invalid filter"), "{error}");
    }

    #[test]
    fn rejects_oversized_source() {
        let source = "true && ".repeat(MAX_FILTER_BYTES);
        assert!(matches!(cel(&source), Err(QueryError::InvalidFilter(_))));
    }

    #[test]
    fn matches_json_field_path() {
        assert!(matches(r#"value.status == "FAILED""#));
        assert!(!matches(r#"value.status == "OK""#));
        assert!(matches("value.user.id == 42"));
    }

    #[test]
    fn matches_headers_and_metadata() {
        assert!(matches(r#"headers["x-trace-id"] == "abc""#));
        assert!(matches("partition == 1 && offset == 42 && size == 128"));
        assert!(matches(r#"compression == "gzip" && schemaId == 7"#));
        assert!(matches(r#"topic == "orders.created""#));
    }

    #[test]
    fn contains_fold_uses_raw_text() {
        assert!(matches(
            r#"keyText.lowerAscii().contains("ord_") || valueText.lowerAscii().contains("ord_")"#
        ));
        assert!(matches(r#"valueText.lowerAscii().contains("failed")"#));
        assert!(!matches(r#"valueText.lowerAscii().contains("missing")"#));
    }

    #[test]
    fn eval_errors_are_non_matches() {
        assert!(!matches("value.missing.nested == 1"));
        assert!(!matches("key"));
    }

    #[test]
    fn unparsed_payloads_bind_as_strings() {
        let headers = headers();
        let meta = meta(&headers);
        let key = DecodedPayload::raw(Bytes::from_static(b"plain-key"));
        let value = DecodedPayload::raw(Bytes::from_static(b"not-json"));

        assert!(
            filter(r#"key == "plain-key" && value == "not-json""#).on_payload(
                &meta,
                Some(&key),
                Some(&value)
            )
        );
    }

    #[test]
    fn null_payloads_are_null() {
        let headers = headers();
        let meta = meta(&headers);

        assert!(
            filter("key == null && value == null && keyText == \"\"").on_payload(&meta, None, None)
        );
    }

    #[test]
    fn decoded_payloads_bind_their_structured_value_without_reparsing() {
        let headers = headers();
        let meta = meta(&headers);
        let value = DecodedPayload::decoded(
            Bytes::from_static(b"\0\0\0\0\x07"),
            Some(7),
            serde_json::json!({"status": "FAILED"}),
        );

        assert!(filter(r#"value.status == "FAILED""#).on_payload(&meta, None, Some(&value)));
    }

    #[test]
    fn a_metadata_only_expression_never_asks_for_a_payload() {
        let headers = headers();
        let meta = meta(&headers);

        assert_eq!(filter("partition == 1").on_meta(&meta), Verdict::Pass);
        assert_eq!(filter("partition == 9").on_meta(&meta), Verdict::Fail);
        assert_eq!(
            filter(r#"headers["x-trace-id"] == "abc" && size < 200"#).on_meta(&meta),
            Verdict::Pass
        );
    }

    #[test]
    fn an_expression_touching_the_payload_defers() {
        let headers = headers();
        let meta = meta(&headers);

        assert_eq!(
            filter(r#"partition == 1 && value.status == "FAILED""#).on_meta(&meta),
            Verdict::NeedsPayload
        );
        assert_eq!(
            filter(r#"keyText.contains("ord")"#).on_meta(&meta),
            Verdict::NeedsPayload
        );
    }

    #[test]
    fn a_substring_filter_answers_from_unframed_bytes() {
        let filter = contains("FAILED").expect("needle");
        let unframed = |bytes: &'static [u8]| RawField {
            bytes,
            framed: false,
        };

        assert_eq!(
            filter.on_raw(
                Some(unframed(b"ord_1")),
                Some(unframed(br#"{"s":"failed"}"#))
            ),
            Verdict::Pass,
            "case folds on both sides"
        );
        assert_eq!(
            filter.on_raw(Some(unframed(b"ord_1")), Some(unframed(b"{}"))),
            Verdict::Fail
        );
        assert_eq!(filter.on_raw(None, None), Verdict::Fail);
    }

    #[test]
    fn a_framed_payload_defers_the_substring_match_to_the_decode() {
        let filter = contains("failed").expect("needle");
        let framed = RawField {
            bytes: b"\0\0\0\0\x07binary",
            framed: true,
        };

        assert_eq!(
            filter.on_raw(
                Some(RawField {
                    bytes: b"ord_1",
                    framed: false
                }),
                Some(framed)
            ),
            Verdict::NeedsPayload
        );
        assert_eq!(
            filter.on_raw(
                Some(RawField {
                    bytes: b"failed-key",
                    framed: false
                }),
                Some(framed)
            ),
            Verdict::Pass,
            "an unframed key that already matches short-circuits the decode"
        );
    }

    #[test]
    fn a_substring_filter_matches_decoded_text() {
        let filter = contains("FAILED").expect("needle");
        let decoded = DecodedPayload::decoded(
            Bytes::from_static(b"\0\0\0\0\x07"),
            Some(7),
            serde_json::json!({"status": "failed"}),
        );

        assert!(filter.on_payload(&meta(&[]), None, Some(&decoded)));
        assert!(!filter.on_payload(&meta(&[]), None, None));
    }

    #[test]
    fn ascii_case_folding_handles_boundaries() {
        assert!(contains_ascii_ci(b"ORDER", b"order"));
        assert!(contains_ascii_ci(b"xxorderxx", b"ORDER"));
        assert!(!contains_ascii_ci(b"ord", b"order"));
        assert!(contains_ascii_ci(b"", b""));
    }
}
