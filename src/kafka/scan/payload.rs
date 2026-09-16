use std::sync::OnceLock;

use crate::kafka::record::decode_bytes;
use crate::kafka::registry::decode::DecodeValue;

#[derive(Debug, Clone)]
pub(crate) struct DecodedPayload {
    raw: Vec<u8>,
    schema_id: Option<i32>,
    json: Option<serde_json::Value>,
    framed: bool,
    present: bool,
    text: OnceLock<String>,
}

impl DecodedPayload {
    pub(crate) fn absent() -> Self {
        Self {
            raw: Vec::new(),
            schema_id: None,
            json: None,
            framed: false,
            present: false,
            text: OnceLock::new(),
        }
    }

    pub(crate) fn from_raw(bytes: Vec<u8>, schema_id: Option<i32>, framed: bool) -> Self {
        Self {
            raw: bytes,
            schema_id,
            json: None,
            framed,
            present: true,
            text: OnceLock::new(),
        }
    }

    pub(crate) fn from_decode(bytes: Vec<u8>, decoded: DecodeValue) -> Self {
        Self {
            raw: bytes,
            schema_id: decoded.schema_id,
            json: decoded.json,
            framed: decoded.framed,
            present: true,
            text: OnceLock::new(),
        }
    }

    pub(crate) fn is_framed(&self) -> bool {
        self.framed
    }

    pub(crate) fn schema_id(&self) -> Option<i32> {
        self.schema_id
    }

    pub(crate) fn raw(&self) -> &[u8] {
        &self.raw
    }

    pub(crate) fn is_absent(&self) -> bool {
        !self.present
    }

    pub(crate) fn text(&self) -> &str {
        self.text.get_or_init(|| match &self.json {
            Some(json) => serde_json::to_string(json).unwrap_or_else(|_| decode_bytes(&self.raw)),
            None if self.raw.is_empty() && !self.framed => String::new(),
            None => decode_bytes(&self.raw),
        })
    }

    pub(crate) fn json_or_text(&self) -> serde_json::Value {
        if let Some(json) = &self.json {
            return json.clone();
        }
        let text = self.text();
        if text.is_empty() && self.is_absent() {
            return serde_json::Value::Null;
        }
        serde_json::from_str(text).unwrap_or_else(|_| serde_json::Value::String(text.to_owned()))
    }

    pub(crate) fn contains_ignore_ascii(&self, needle: &[u8]) -> bool {
        if needle.is_empty() {
            return true;
        }
        if let Some(json) = &self.json {
            return json_contains_ignore_ascii(json, needle);
        }
        contains_ignore_ascii(&self.raw, needle)
    }
}

pub(crate) fn contains_ignore_ascii(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

fn json_contains_ignore_ascii(value: &serde_json::Value, needle: &[u8]) -> bool {
    match value {
        serde_json::Value::String(text) => contains_ignore_ascii(text.as_bytes(), needle),
        serde_json::Value::Array(values) => values
            .iter()
            .any(|value| json_contains_ignore_ascii(value, needle)),
        serde_json::Value::Object(map) => map.iter().any(|(key, value)| {
            contains_ignore_ascii(key.as_bytes(), needle)
                || json_contains_ignore_ascii(value, needle)
        }),
        serde_json::Value::Number(number) => {
            contains_ignore_ascii(number.to_string().as_bytes(), needle)
        }
        serde_json::Value::Bool(flag) => {
            contains_ignore_ascii(if *flag { b"true" } else { b"false" }, needle)
        }
        serde_json::Value::Null => contains_ignore_ascii(b"null", needle),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_contains_is_case_insensitive() {
        assert!(contains_ignore_ascii(b"Hello ORD_1", b"ord_"));
        assert!(!contains_ignore_ascii(b"Hello", b"ord_"));
        assert!(contains_ignore_ascii(b"", b""));
    }

    #[test]
    fn unframed_payload_renders_lossy_utf8() {
        let payload = DecodedPayload::from_raw(b"plain".to_vec(), None, false);
        assert_eq!(payload.text(), "plain");
        assert!(!payload.is_framed());
    }
}
