use std::cell::OnceCell;

use async_trait::async_trait;
use bytes::Bytes;
use schemreg::{SchemaId, decode_wire_prefix};

/// One key or value, decoded at most once.
///
/// `json` is `Some` only for payloads a registry codec understood.
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

    /// The decoded tree, for in-place rewriting.
    ///
    /// Any text rendered so far is dropped, so the next read renders the tree
    /// as it now stands.
    pub fn json_mut(&mut self) -> Option<&mut serde_json::Value> {
        if self.json.is_some() {
            self.text.take();
        }
        self.json.as_mut()
    }

    /// Replace the whole payload with `text`, forgetting the decode.
    ///
    /// The raw bytes go with it: nothing downstream may reach the original
    /// value once it has been replaced.
    pub fn replace(&mut self, text: String) {
        self.raw = Bytes::new();
        self.json = None;
        self.text = OnceCell::from(text);
    }

    pub fn text(&self) -> &str {
        self.text.get_or_init(|| match &self.json {
            Some(json) => serde_json::to_string(json).unwrap_or_else(|_| render_raw(&self.raw)),
            None => render_raw(&self.raw),
        })
    }

    pub fn drop_tree_if_rendered(&mut self) {
        if self.text.get().is_some() {
            self.json = None;
        }
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
#[async_trait]
pub trait PayloadCodec: Send + Sync {
    async fn decode_batch(&self, slots: &mut [PayloadSlot]);
}

/// Schema id from a Confluent wire-format prefix, if the bytes carry one.
///
/// A v1 prefix names a 16-byte GUID rather than a numeric id, so a payload
/// can be framed — and decodable — while reporting `None` here.
pub fn framed_schema_id(bytes: &[u8]) -> Option<i32> {
    let (key, _) = decode_wire_prefix(bytes).ok()?;
    i32::try_from(SchemaId::as_u32(key.as_id()?)).ok()
}

/// Whether these bytes need a registry round trip before they mean anything.
pub fn needs_decode(bytes: &[u8], override_id: Option<i32>) -> bool {
    decode_wire_prefix(bytes).is_ok() || override_id.is_some()
}

fn render_raw(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use schemreg::encode_wire_format;

    fn framed(id: u32, body: &[u8]) -> Bytes {
        encode_wire_format(id, body)
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
    fn a_guid_framed_payload_is_decodable_without_a_numeric_id() {
        let guid: schemreg::SchemaGuid = "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
        let raw = encode_wire_format(guid, b"body");

        assert!(needs_decode(&raw, None));
        assert_eq!(framed_schema_id(&raw), None);
    }

    #[test]
    fn an_undecoded_slot_falls_back_to_raw_text() {
        let slot = PayloadSlot::new(Bytes::from_static(b"plain"), None);

        assert_eq!(slot.take().text(), "plain");
    }

    #[test]
    fn a_tree_is_dropped_only_once_its_text_is_rendered() {
        let mut payload = DecodedPayload::decoded(
            framed(7, b"..."),
            Some(7),
            serde_json::json!({"status": "FAILED"}),
        );

        payload.drop_tree_if_rendered();
        assert!(payload.json().is_some(), "nothing rendered yet");

        assert_eq!(payload.text(), r#"{"status":"FAILED"}"#);
        payload.drop_tree_if_rendered();
        assert!(payload.json().is_none());
        assert_eq!(payload.into_text(), r#"{"status":"FAILED"}"#);
    }

    #[test]
    fn replacing_a_payload_drops_its_bytes_and_its_tree() {
        let mut payload = DecodedPayload::decoded(
            framed(7, b"..."),
            Some(7),
            serde_json::json!({"pan": "4111"}),
        );
        assert_eq!(payload.text(), r#"{"pan":"4111"}"#);

        payload.replace("***".to_owned());

        assert_eq!(payload.text(), "***");
        assert!(payload.json().is_none());
        assert!(payload.bytes().is_empty());
        assert_eq!(
            payload.schema_id(),
            Some(7),
            "metadata still describes the wire record"
        );
    }

    #[test]
    fn mutating_the_tree_invalidates_an_already_rendered_text() {
        let mut payload = DecodedPayload::decoded(
            framed(7, b"..."),
            Some(7),
            serde_json::json!({"pan": "4111"}),
        );
        assert_eq!(payload.text(), r#"{"pan":"4111"}"#);

        payload.json_mut().expect("tree")["pan"] = serde_json::json!("***");

        assert_eq!(payload.text(), r#"{"pan":"***"}"#);
    }
}
