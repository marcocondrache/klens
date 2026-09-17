//! Payloads decoded once, rendered late.
//!
//! The v1 pipeline decoded every in-window record to a `String`, then the CEL
//! filter parsed that string back into JSON. v2 decodes to a
//! [`serde_json::Value`] once, filters against the structured value, and only
//! renders text for the records that survive the limit heap.

use std::cell::OnceCell;

use async_trait::async_trait;
use bytes::Bytes;

/// Confluent wire framing: magic byte 0, then a big-endian schema id.
const CONFLUENT_MAGIC: u8 = 0;
const FRAME_LEN: usize = 5;

/// One key or value, decoded at most once.
///
/// `json` is `Some` only for payloads a registry codec understood. `text` is
/// rendered on first use, which for a filtered page is only the records that
/// reach the page.
#[derive(Debug)]
pub struct DecodedPayload {
    raw: Bytes,
    schema_id: Option<i32>,
    json: Option<serde_json::Value>,
    text: OnceCell<String>,
}

impl DecodedPayload {
    /// A payload no registry codec claimed: the text is the raw bytes.
    pub fn raw(raw: Bytes) -> Self {
        let schema_id = framed_schema_id(&raw);
        Self {
            raw,
            schema_id,
            json: None,
            text: OnceCell::new(),
        }
    }

    /// A payload a registry codec decoded into structured JSON.
    pub fn decoded(raw: Bytes, schema_id: Option<i32>, json: serde_json::Value) -> Self {
        Self {
            raw,
            schema_id,
            json: Some(json),
            text: OnceCell::new(),
        }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.raw
    }

    /// Schema id read off the Confluent frame, not from an override: an
    /// override says how to read bytes that carry no id of their own.
    pub fn schema_id(&self) -> Option<i32> {
        self.schema_id
    }

    pub fn json(&self) -> Option<&serde_json::Value> {
        self.json.as_ref()
    }

    /// Rendered text, built on first use.
    pub fn text(&self) -> &str {
        self.text.get_or_init(|| match &self.json {
            Some(json) => serde_json::to_string(json).unwrap_or_else(|_| render_raw(&self.raw)),
            None => render_raw(&self.raw),
        })
    }

    pub fn into_text(self) -> String {
        match self.text.into_inner() {
            Some(text) => text,
            None => match &self.json {
                Some(json) => serde_json::to_string(json).unwrap_or_else(|_| render_raw(&self.raw)),
                None => render_raw(&self.raw),
            },
        }
    }
}

/// One payload handed to a codec, and the slot its decode lands in.
pub struct PayloadSlot {
    pub raw: Bytes,
    /// Explicit schema id for bytes that carry no Confluent frame. Wire ids
    /// always win.
    pub override_id: Option<i32>,
    pub decoded: Option<DecodedPayload>,
}

impl PayloadSlot {
    pub fn new(raw: Bytes, override_id: Option<i32>) -> Self {
        Self {
            raw,
            override_id,
            decoded: None,
        }
    }

    /// The decode, or the raw bytes when the codec declined or was absent.
    pub fn take(self) -> DecodedPayload {
        match self.decoded {
            Some(decoded) => decoded,
            None => DecodedPayload::raw(self.raw),
        }
    }
}

/// Registry-aware decoding, batched.
///
/// A batch is decoded in one call so per-schema decoder state — an Avro
/// reader with its resolved schemata — is built once per batch instead of
/// once per record.
#[async_trait]
pub trait PayloadCodec: Send + Sync {
    async fn decode_batch(&self, slots: &mut [PayloadSlot]);
}

/// Schema id from a Confluent frame, if the bytes carry one.
pub fn framed_schema_id(bytes: &[u8]) -> Option<i32> {
    if bytes.len() < FRAME_LEN || bytes[0] != CONFLUENT_MAGIC {
        return None;
    }
    Some(i32::from_be_bytes(bytes[1..FRAME_LEN].try_into().ok()?))
}

/// Whether these bytes need a registry round trip before they mean anything.
///
/// Unframed payloads without an override are plain text, so a substring
/// filter can scan them without decoding at all.
pub fn needs_decode(bytes: &[u8], override_id: Option<i32>) -> bool {
    framed_schema_id(bytes).is_some() || override_id.is_some()
}

fn render_raw(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn framed(id: i32, body: &[u8]) -> Bytes {
        let mut bytes = vec![CONFLUENT_MAGIC];
        bytes.extend_from_slice(&id.to_be_bytes());
        bytes.extend_from_slice(body);
        Bytes::from(bytes)
    }

    #[test]
    fn raw_payloads_render_their_bytes_lossily() {
        let payload = DecodedPayload::raw(Bytes::from_static(b"ord_1"));

        assert_eq!(payload.text(), "ord_1");
        assert_eq!(payload.schema_id(), None);
        assert!(payload.json().is_none());
    }

    #[test]
    fn a_framed_payload_reports_its_wire_schema_id_even_undecoded() {
        let payload = DecodedPayload::raw(framed(7, b"junk"));

        assert_eq!(payload.schema_id(), Some(7));
    }

    #[test]
    fn decoded_payloads_render_from_json_once() {
        let payload = DecodedPayload::decoded(
            framed(7, b"..."),
            Some(7),
            serde_json::json!({"status": "FAILED"}),
        );

        assert_eq!(payload.text(), r#"{"status":"FAILED"}"#);
        assert_eq!(payload.text(), r#"{"status":"FAILED"}"#);
        assert_eq!(payload.into_text(), r#"{"status":"FAILED"}"#);
    }

    #[test]
    fn only_framed_or_overridden_bytes_need_a_registry() {
        assert!(!needs_decode(b"plain", None));
        assert!(needs_decode(b"plain", Some(7)));
        assert!(needs_decode(&framed(7, b"body"), None));
        assert!(!needs_decode(&[0, 1, 2], None), "too short to be a frame");
    }

    #[test]
    fn an_undecoded_slot_falls_back_to_raw_text() {
        let slot = PayloadSlot::new(Bytes::from_static(b"plain"), None);

        assert_eq!(slot.take().text(), "plain");
    }
}
