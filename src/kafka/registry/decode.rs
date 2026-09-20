use std::sync::Arc;

use foldhash::{HashMap, HashMapExt, HashSet, HashSetExt};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use moka::future::Cache;
use schemreg::{
    AvroSchemaDecoder, Schema, SchemaId, SchemaKey, SchemaRegistryClient as _, SchemaVersion,
    decode_wire_prefix, encode_wire_format,
};
use serde_json::Value;
use thiserror::Error;

use super::client::{Registry, SchemaRegistryClient, references};
use super::protobuf::{ProtobufCodec, ProtobufError};
use crate::environment::{MISSING_SCHEMA_TTL, SUBJECT_FETCH_CONCURRENCY};
use crate::kafka::model::{SchemaReference, SchemaType};
use crate::kafka::scan::payload::{DecodedPayload, PayloadCodec, PayloadSlot, framed_schema_id};

const MAX_CACHED_SCHEMAS: u64 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Framing {
    key: SchemaKey,
    payload_start: usize,
    has_wire_prefix: bool,
}

impl Framing {
    fn of(slot: &PayloadSlot) -> Option<Self> {
        if let Ok((key, payload_start)) = decode_wire_prefix(&slot.raw) {
            return Some(Self {
                key,
                payload_start,
                has_wire_prefix: true,
            });
        }

        let id = u32::try_from(slot.override_id?).ok()?;
        Some(Self {
            key: SchemaKey::Id(SchemaId::new(id)),
            payload_start: 0,
            has_wire_prefix: false,
        })
    }
}

#[derive(Clone)]
pub(crate) struct PayloadDecoder {
    client: SchemaRegistryClient,
    avro: Arc<AvroSchemaDecoder<Arc<Registry>>>,
    pools: Cache<SchemaKey, Arc<ProtobufCodec>>,
    missing: Cache<SchemaKey, ()>,
}

#[derive(Clone)]
enum Resolved {
    Avro,
    Json,
    Protobuf(Arc<ProtobufCodec>),
    Missing,
}

impl PayloadDecoder {
    pub(crate) fn new(client: SchemaRegistryClient) -> Self {
        let avro = AvroSchemaDecoder::new(client.registry().clone());

        Self {
            client,
            avro: Arc::new(avro),
            pools: Cache::builder().max_capacity(MAX_CACHED_SCHEMAS).build(),
            missing: Cache::builder()
                .max_capacity(MAX_CACHED_SCHEMAS)
                .time_to_live(*MISSING_SCHEMA_TTL)
                .build(),
        }
    }

    pub(crate) fn client(&self) -> &SchemaRegistryClient {
        &self.client
    }

    fn registry(&self) -> &Arc<Registry> {
        self.client.registry()
    }

    async fn resolved(&self, key: SchemaKey) -> Result<Resolved, DecodeError> {
        if self.missing.get(&key).await.is_some() {
            return Ok(Resolved::Missing);
        }

        let schema = match self.registry().get_schema_by_key(key).await {
            Ok(schema) => schema,
            Err(error) if error.is_not_found() => {
                self.missing.insert(key, ()).await;
                return Ok(Resolved::Missing);
            }
            Err(error) => return Err(DecodeError::failed(error)),
        };

        match SchemaType::from(schema.schema_type) {
            SchemaType::Json => Ok(Resolved::Json),
            SchemaType::Avro => Ok(Resolved::Avro),
            SchemaType::Protobuf => self.pool(key, &schema).await.map(Resolved::Protobuf),
        }
    }

    async fn pool(
        &self,
        key: SchemaKey,
        schema: &Schema,
    ) -> Result<Arc<ProtobufCodec>, DecodeError> {
        self.pools
            .try_get_with(key, async {
                let dependencies = self.collect_named_references(schema).await?;
                let codec = ProtobufCodec::compile(&schema.schema, &dependencies)?;
                Ok::<_, DecodeError>(Arc::new(codec))
            })
            .await
            .map_err(|error| (*error).clone())
    }

    async fn collect_named_references(
        &self,
        schema: &Schema,
    ) -> Result<Vec<(String, String)>, DecodeError> {
        let mut bodies = Vec::new();
        let mut pending = references(&schema.references);
        let mut seen = HashSet::new();

        while !pending.is_empty() {
            let wave: Vec<SchemaReference> = pending
                .drain(..)
                .filter(|reference| seen.insert((reference.subject.clone(), reference.version)))
                .collect();

            let mut fetches = futures::stream::iter(wave.into_iter().map(|reference| async move {
                let fetched = self
                    .registry()
                    .get_schema_by_version(
                        &reference.subject,
                        SchemaVersion::new(reference.version),
                    )
                    .await
                    .map_err(DecodeError::failed)?;
                Ok::<_, DecodeError>((reference.name, fetched))
            }))
            .buffer_unordered(*SUBJECT_FETCH_CONCURRENCY);

            while let Some(result) = fetches.next().await {
                let (name, fetched) = result?;
                pending.extend(references(&fetched.references));
                bodies.push((name, fetched.schema.to_string()));
            }
        }

        Ok(bodies)
    }
}

#[async_trait]
impl PayloadCodec for PayloadDecoder {
    async fn decode_batch(&self, slots: &mut [PayloadSlot]) {
        for failure in self.decode_slots(slots).await.into_iter().flatten() {
            report(failure.0, &failure.1);
        }
    }
}

impl PayloadDecoder {
    async fn decode_slots(
        &self,
        slots: &mut [PayloadSlot],
    ) -> Vec<Option<(SchemaKey, DecodeError)>> {
        let mut failures = vec![None; slots.len()];
        let framings: Vec<Option<Framing>> = slots.iter().map(Framing::of).collect();
        let keys: HashSet<SchemaKey> = framings
            .iter()
            .flatten()
            .map(|framing| framing.key)
            .collect();
        if keys.is_empty() {
            return failures;
        }

        let mut schemas = HashMap::with_capacity(keys.len());
        let mut loads: FuturesUnordered<_> = keys
            .into_iter()
            .map(|key| async move { (key, self.resolved(key).await) })
            .collect();
        while let Some((key, resolved)) = loads.next().await {
            schemas.insert(key, resolved);
        }

        for ((slot, framing), failure) in slots.iter_mut().zip(framings).zip(failures.iter_mut()) {
            let Some(framing) = framing else {
                continue;
            };
            let resolved = match schemas.get(&framing.key) {
                Some(Ok(resolved)) => resolved,
                Some(Err(error)) => {
                    *failure = Some((framing.key, error.clone()));
                    continue;
                }
                None => continue,
            };

            let raw = slot.raw.clone();
            match self.decode_body(resolved, framing, &raw).await {
                Ok(json) => {
                    slot.decoded = Some(DecodedPayload::decoded(
                        raw.clone(),
                        framed_schema_id(&raw),
                        json,
                    ));
                }
                Err(error) => *failure = Some((framing.key, error)),
            }
        }

        failures
    }

    async fn decode_body(
        &self,
        resolved: &Resolved,
        framing: Framing,
        raw: &Bytes,
    ) -> Result<Value, DecodeError> {
        let body = raw.slice(framing.payload_start.min(raw.len())..);

        match resolved {
            Resolved::Missing => Err(DecodeError::missing("schema id not found in registry")),
            Resolved::Json => serde_json::from_slice(&body).map_err(DecodeError::failed),
            Resolved::Avro => {
                // The decoder reads the identifier off the wire prefix, so an
                // override has to be handed bytes that carry one.
                let framed = if framing.has_wire_prefix {
                    raw.clone()
                } else {
                    encode_wire_format(framing.key, &body)
                };
                let value = self
                    .avro
                    .decode(framed)
                    .await
                    .map_err(DecodeError::failed)?;
                Value::try_from(value).map_err(DecodeError::failed)
            }
            Resolved::Protobuf(codec) => if framing.has_wire_prefix {
                codec.decode_framed(&body)
            } else {
                codec.decode_raw(&body)
            }
            .map_err(DecodeError::from),
        }
    }
}

fn report(key: SchemaKey, error: &DecodeError) {
    match error {
        DecodeError::Missing(message) => tracing::debug!(
            schema_id = %key,
            error = %message,
            "skipping schema registry payload decode"
        ),
        DecodeError::Failed(message) => tracing::warn!(
            schema_id = %key,
            error = %message,
            "failed to decode schema registry payload"
        ),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
enum DecodeError {
    #[error("{0}")]
    Missing(String),
    #[error("{0}")]
    Failed(String),
}

impl DecodeError {
    fn missing(message: impl Into<String>) -> Self {
        Self::Missing(message.into())
    }

    fn failed(error: impl std::fmt::Display) -> Self {
        Self::Failed(error.to_string())
    }
}

impl From<ProtobufError> for DecodeError {
    fn from(error: ProtobufError) -> Self {
        Self::Failed(error.to_string())
    }
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedField {
    pub text: String,
    pub schema_id: Option<i32>,
}

#[cfg(test)]
impl PayloadDecoder {
    pub(crate) async fn decode(&self, bytes: &[u8]) -> String {
        self.decode_with(bytes, None).await.text
    }

    pub(crate) async fn decode_with(&self, bytes: &[u8], override_id: Option<i32>) -> DecodedField {
        let mut slots = [PayloadSlot::new(Bytes::copy_from_slice(bytes), override_id)];
        self.decode_batch(&mut slots).await;
        let [slot] = slots;
        let decoded = slot.take();

        DecodedField {
            schema_id: decoded.schema_id(),
            text: decoded.into_text(),
        }
    }

    async fn decode_failure(&self, bytes: &[u8], override_id: Option<i32>) -> Option<DecodeError> {
        let mut slots = [PayloadSlot::new(Bytes::copy_from_slice(bytes), override_id)];
        let failures = self.decode_slots(&mut slots).await;
        failures
            .into_iter()
            .flatten()
            .next()
            .map(|(_, error)| error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SchemaRegistryConfig;
    use crate::kafka::scan::filter::{RecordMeta, cel};

    fn decode_bytes(bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).into_owned()
    }
    use apache_avro::Schema as AvroSchema;
    use apache_avro::types::{Record, Value as AvroValue};
    use schemreg::encode_protobuf_wire_format;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ORDER_SCHEMA: &str = r#"{
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "orderId", "type": "string"},
            {"name": "amount", "type": "long"}
        ]
    }"#;

    const STATUS_SCHEMA: &str = r#"{
        "type": "enum",
        "name": "Status",
        "symbols": ["OPEN", "CLOSED"]
    }"#;

    const ORDER_WITH_STATUS: &str = r#"{
        "type": "record",
        "name": "TaggedOrder",
        "fields": [
            {"name": "status", "type": "Status"}
        ]
    }"#;

    const ORDER_PROTO: &str = r#"
        syntax = "proto3";
        message Order {
            string order_id = 1;
            int64 amount = 2;
        }
        message Wrapper {
            message Inner {
                string name = 1;
            }
        }
        message Count {
            int32 n = 1;
        }
    "#;

    const STATUS_PROTO: &str = r#"
        syntax = "proto3";
        package common;
        message Status {
            string code = 1;
        }
    "#;

    const TAGGED_ORDER_PROTO: &str = r#"
        syntax = "proto3";
        import "common.proto";
        message TaggedOrder {
            common.Status status = 1;
        }
    "#;

    fn config(url: &str) -> SchemaRegistryConfig {
        SchemaRegistryConfig {
            url: url.to_owned(),
            username: None,
            password: None,
        }
    }

    fn decoder(url: &str) -> PayloadDecoder {
        PayloadDecoder::new(SchemaRegistryClient::new("local", &config(url)).unwrap())
    }

    fn frame(schema_id: u32, payload: &[u8]) -> Vec<u8> {
        encode_wire_format(schema_id, payload).to_vec()
    }

    fn proto_frame(schema_id: u32, indexes: &[u32], payload: &[u8]) -> Vec<u8> {
        encode_protobuf_wire_format(schema_id, indexes, payload).to_vec()
    }

    fn encode_avro(schema: &str, build: impl FnOnce(&AvroSchema, &mut Record<'_>)) -> Vec<u8> {
        let parsed = AvroSchema::parse_str(schema).unwrap();
        let mut record = Record::new(&parsed).unwrap();
        build(&parsed, &mut record);
        apache_avro::to_avro_datum(&parsed, AvroValue::Record(record.fields)).unwrap()
    }

    async fn mock_schema(server: &MockServer, id: u32, schema_type: &str, schema: &str) {
        Mock::given(method("GET"))
            .and(path(format!("/schemas/ids/{id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schemaType": schema_type,
                "schema": schema,
            })))
            .mount(server)
            .await;
    }

    fn matches_orderid(value: &DecodedPayload) -> bool {
        cel(r#"valueText.lowerAscii().contains("orderid")"#)
            .unwrap()
            .unwrap()
            .on_payload(&sample_meta(), None, Some(value))
    }

    fn sample_meta() -> RecordMeta<'static> {
        RecordMeta {
            topic: "orders",
            partition: 0,
            offset: 1,
            timestamp: 0,
            size_bytes: 0,
            compression: crate::kafka::model::Compression::None,
            schema_id: None,
            headers: &[],
        }
    }

    async fn decode_payload(decoder: &PayloadDecoder, bytes: &[u8]) -> DecodedPayload {
        let mut slots = [PayloadSlot::new(Bytes::copy_from_slice(bytes), None)];
        decoder.decode_batch(&mut slots).await;
        let [slot] = slots;
        slot.take()
    }

    #[test]
    fn decode_error_variants_display_their_message() {
        assert_eq!(
            DecodeError::missing("schema id not found in registry").to_string(),
            "schema id not found in registry"
        );
        assert_eq!(
            DecodeError::from(ProtobufError::EmptyIndexPath),
            DecodeError::failed("protobuf message index path is empty")
        );
    }

    #[test]
    fn parses_confluent_frame() {
        let bytes = frame(12, b"datum");
        let slot = PayloadSlot::new(Bytes::from(bytes), None);
        let parsed = Framing::of(&slot).unwrap();
        assert_eq!(parsed.key, 12u32);
        assert!(parsed.has_wire_prefix);
        assert_eq!(&slot.raw[parsed.payload_start..], b"datum");
    }

    #[test]
    fn parses_a_schema_guid_frame() {
        let guid: schemreg::SchemaGuid = "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
        let slot = PayloadSlot::new(encode_wire_format(guid, b"datum"), None);
        let parsed = Framing::of(&slot).unwrap();

        assert_eq!(parsed.key, SchemaKey::Guid(guid));
        assert!(parsed.has_wire_prefix);
        assert_eq!(&slot.raw[parsed.payload_start..], b"datum");
    }

    #[test]
    fn rejects_short_or_non_magic_frames() {
        for bytes in [vec![0, 0, 0, 1], b"hello".to_vec(), Vec::new()] {
            let slot = PayloadSlot::new(Bytes::from(bytes), None);
            assert!(Framing::of(&slot).is_none());
        }
    }

    #[test]
    fn an_override_frames_unframed_bytes_only() {
        let framed = PayloadSlot::new(Bytes::from(frame(12, b"datum")), Some(99));
        assert_eq!(Framing::of(&framed).unwrap().key, 12u32);

        let bare = PayloadSlot::new(Bytes::from_static(b"datum"), Some(99));
        let framing = Framing::of(&bare).unwrap();
        assert_eq!(framing.key, 99u32);
        assert_eq!(framing.payload_start, 0);
        assert!(!framing.has_wire_prefix);
    }

    #[tokio::test]
    async fn decodes_avro_record_to_json() {
        let server = MockServer::start().await;
        mock_schema(&server, 12, "AVRO", ORDER_SCHEMA).await;

        let payload = encode_avro(ORDER_SCHEMA, |_, record| {
            record.put("orderId", "abc".to_owned());
            record.put("amount", 42i64);
        });
        let framed = frame(12, &payload);
        let decoded = decode_payload(&decoder(&server.uri()), &framed).await;
        let value = decoded.json().expect("decoded to structured json");

        assert_eq!(value["orderId"], "abc");
        assert_eq!(value["amount"], 42);
        assert!(
            !String::from_utf8_lossy(&framed)
                .to_ascii_lowercase()
                .contains("orderid")
        );
        assert!(matches_orderid(&decoded));
    }

    #[tokio::test]
    async fn decodes_unnamed_avro_schemas_without_references() {
        let server = MockServer::start().await;
        for (id, schema, value, expected) in [
            (1u32, r#""long""#, AvroValue::Long(42), "42"),
            (2, r#""null""#, AvroValue::Null, "null"),
            (
                3,
                r#"{"type":"array","items":"long"}"#,
                AvroValue::Array(vec![AvroValue::Long(1), AvroValue::Long(2)]),
                "[1,2]",
            ),
        ] {
            mock_schema(&server, id, "AVRO", schema).await;
            let parsed = AvroSchema::parse_str(schema).unwrap();
            let payload = apache_avro::to_avro_datum(&parsed, value).unwrap();
            assert_eq!(
                decoder(&server.uri()).decode(&frame(id, &payload)).await,
                expected
            );
        }
    }

    #[tokio::test]
    async fn decodes_json_schema_payload() {
        let server = MockServer::start().await;
        mock_schema(&server, 7, "JSON", r#"{"type":"object"}"#).await;

        let framed = frame(7, br#"{"ok": true}"#);
        let json = decoder(&server.uri()).decode(&framed).await;
        assert_eq!(json, r#"{"ok":true}"#);
    }

    #[tokio::test]
    async fn decodes_key_and_value_independently() {
        let server = MockServer::start().await;
        mock_schema(&server, 1, "JSON", "{}").await;
        mock_schema(&server, 2, "JSON", "{}").await;
        let decoder = decoder(&server.uri());

        let key = decoder.decode(&frame(1, br#""user-1""#)).await;
        let value = decoder.decode(&frame(2, br#"{"n":1}"#)).await;
        assert_eq!(key, r#""user-1""#);
        assert_eq!(value, r#"{"n":1}"#);
    }

    #[tokio::test]
    async fn leaves_unframed_payloads_unchanged() {
        let server = MockServer::start().await;
        let raw = br#"{"plain":true}"#;
        assert_eq!(
            decoder(&server.uri()).decode(raw).await,
            String::from_utf8_lossy(raw)
        );
    }

    #[tokio::test]
    async fn falls_back_when_schema_is_missing() {
        let server = MockServer::start().await;
        mock_missing_schema(&server, 99).await;

        let framed = frame(99, b"not-json");
        assert_eq!(
            decoder(&server.uri()).decode(&framed).await,
            decode_bytes(&framed)
        );
        assert_eq!(
            decoder(&server.uri())
                .decode_failure(&framed, None)
                .await
                .unwrap(),
            DecodeError::missing("schema id not found in registry")
        );
    }

    #[tokio::test]
    async fn a_missing_schema_is_not_re_queried_within_its_ttl() {
        let server = MockServer::start().await;
        mock_missing_schema(&server, 99).await;

        let decoder = decoder(&server.uri());
        let framed = frame(99, b"not-json");
        decoder.decode(&framed).await;
        decoder.decode(&framed).await;

        assert_eq!(fetches(&server, "/schemas/ids/99").await, 1);
    }

    #[tokio::test]
    async fn falls_back_when_avro_payload_does_not_match_schema() {
        let server = MockServer::start().await;
        mock_schema(&server, 12, "AVRO", ORDER_SCHEMA).await;
        let framed = frame(12, b"????");
        assert_eq!(
            decoder(&server.uri()).decode(&framed).await,
            decode_bytes(&framed)
        );
    }

    #[tokio::test]
    async fn decodes_protobuf_record_to_json() {
        let server = MockServer::start().await;
        mock_schema(&server, 3, "PROTOBUF", ORDER_PROTO).await;

        let framed = proto_frame(3, &[0], b"\x0a\x03abc\x10\x2a");
        let decoded = decode_payload(&decoder(&server.uri()), &framed).await;
        let value = decoded.json().expect("decoded to structured json");

        assert_eq!(value["orderId"], "abc");
        assert_eq!(value["amount"], "42");
        assert!(matches_orderid(&decoded));
    }

    #[tokio::test]
    async fn decodes_protobuf_nested_message_index() {
        let server = MockServer::start().await;
        mock_schema(&server, 3, "PROTOBUF", ORDER_PROTO).await;

        let framed = proto_frame(3, &[2], b"\x08\x07");
        let json = decoder(&server.uri()).decode(&framed).await;
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["n"], 7);
    }

    #[tokio::test]
    async fn falls_back_when_protobuf_schema_has_no_messages() {
        let server = MockServer::start().await;
        mock_schema(&server, 3, "PROTOBUF", "syntax = \"proto3\";").await;
        let framed = proto_frame(3, &[0], b"\x08\x01");
        assert_eq!(
            decoder(&server.uri()).decode(&framed).await,
            decode_bytes(&framed)
        );
    }

    #[tokio::test]
    async fn a_truncated_protobuf_index_is_a_typed_decode_error() {
        let server = MockServer::start().await;
        mock_schema(&server, 3, "PROTOBUF", ORDER_PROTO).await;
        let framed = frame(3, &[]);
        assert!(matches!(
            decoder(&server.uri())
                .decode_failure(&framed, None)
                .await
                .unwrap(),
            DecodeError::Failed(_)
        ));
    }

    #[tokio::test]
    async fn decodes_unframed_protobuf_with_override_schema_id() {
        let server = MockServer::start().await;
        mock_schema(&server, 3, "PROTOBUF", ORDER_PROTO).await;

        let payload = b"\x0a\x03abc\x10\x2a";
        let decoded = decoder(&server.uri()).decode_with(payload, Some(3)).await;
        let value: serde_json::Value = serde_json::from_str(&decoded.text).unwrap();
        assert_eq!(value["orderId"], "abc");
        assert_eq!(value["amount"], "42");
        assert_eq!(decoded.schema_id, None);
    }

    #[tokio::test]
    async fn decodes_protobuf_schema_references() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/20"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schemaType": "PROTOBUF",
                "schema": TAGGED_ORDER_PROTO,
                "references": [{
                    "name": "common.proto",
                    "subject": "common.proto",
                    "version": 1
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/subjects/common.proto/versions/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 4,
                "version": 1,
                "subject": "common.proto",
                "schemaType": "PROTOBUF",
                "schema": STATUS_PROTO,
            })))
            .mount(&server)
            .await;

        let framed = proto_frame(20, &[0], b"\x0a\x06\x0a\x04OPEN");
        let json = decoder(&server.uri()).decode(&framed).await;
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["status"]["code"], "OPEN");
    }

    #[tokio::test]
    async fn decodes_unframed_avro_with_override_schema_id() {
        let server = MockServer::start().await;
        mock_schema(&server, 12, "AVRO", ORDER_SCHEMA).await;

        let payload = encode_avro(ORDER_SCHEMA, |_, record| {
            record.put("orderId", "abc".to_owned());
            record.put("amount", 42i64);
        });
        let decoded = decoder(&server.uri()).decode_with(&payload, Some(12)).await;
        let value: serde_json::Value = serde_json::from_str(&decoded.text).unwrap();

        assert_eq!(value["orderId"], "abc");
        assert_eq!(value["amount"], 42);
        assert_eq!(decoded.schema_id, None);
        assert!(
            !decode_bytes(&payload)
                .to_ascii_lowercase()
                .contains("orderid")
        );
    }

    #[tokio::test]
    async fn framed_payload_ignores_override_schema_id() {
        let server = MockServer::start().await;
        mock_schema(&server, 12, "AVRO", ORDER_SCHEMA).await;
        mock_schema(&server, 99, "JSON", r#"{"type":"object"}"#).await;

        let payload = encode_avro(ORDER_SCHEMA, |_, record| {
            record.put("orderId", "abc".to_owned());
            record.put("amount", 42i64);
        });
        let framed = frame(12, &payload);
        let decoded = decoder(&server.uri()).decode_with(&framed, Some(99)).await;
        let value: serde_json::Value = serde_json::from_str(&decoded.text).unwrap();

        assert_eq!(value["orderId"], "abc");
        assert_eq!(decoded.schema_id, Some(12));
    }

    #[tokio::test]
    async fn unframed_override_falls_back_when_payload_does_not_match() {
        let server = MockServer::start().await;
        mock_schema(&server, 12, "AVRO", ORDER_SCHEMA).await;
        let raw = b"????";
        let decoded = decoder(&server.uri()).decode_with(raw, Some(12)).await;
        assert_eq!(decoded.text, decode_bytes(raw));
        assert_eq!(decoded.schema_id, None);
    }

    #[tokio::test]
    async fn unframed_override_falls_back_when_schema_is_missing() {
        let server = MockServer::start().await;
        mock_missing_schema(&server, 99).await;

        let raw = b"not-json";
        let decoded = decoder(&server.uri()).decode_with(raw, Some(99)).await;
        assert_eq!(decoded.text, decode_bytes(raw));
        assert_eq!(decoded.schema_id, None);
    }

    #[tokio::test]
    async fn decodes_avro_schema_references() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/20"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schemaType": "AVRO",
                "schema": ORDER_WITH_STATUS,
                "references": [{
                    "name": "Status",
                    "subject": "Status",
                    "version": 1
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/subjects/Status/versions/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": 4,
                "version": 1,
                "subject": "Status",
                "schemaType": "AVRO",
                "schema": STATUS_SCHEMA,
            })))
            .mount(&server)
            .await;

        let (writer, dependencies) =
            AvroSchema::parse_str_with_list(ORDER_WITH_STATUS, [STATUS_SCHEMA]).unwrap();
        let mut record = Record::new(&writer).unwrap();
        record.put("status", "OPEN");
        let mut schemata: Vec<&AvroSchema> = dependencies.iter().collect();
        schemata.push(&writer);
        let payload = apache_avro::to_avro_datum_schemata(
            &writer,
            schemata,
            AvroValue::Record(record.fields),
        )
        .unwrap();

        let json = decoder(&server.uri()).decode(&frame(20, &payload)).await;
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["status"], "OPEN");
    }

    #[tokio::test]
    async fn a_batch_resolves_each_schema_id_once() {
        let server = MockServer::start().await;
        mock_schema(&server, 12, "AVRO", ORDER_SCHEMA).await;

        let decoder = decoder(&server.uri());
        let mut slots: Vec<PayloadSlot> = (0..5)
            .map(|index| {
                let payload = encode_avro(ORDER_SCHEMA, |_, record| {
                    record.put("orderId", format!("id-{index}"));
                    record.put("amount", index as i64);
                });
                PayloadSlot::new(Bytes::from(frame(12, &payload)), None)
            })
            .collect();

        decoder.decode_batch(&mut slots).await;

        for (index, slot) in slots.into_iter().enumerate() {
            let value = slot.take().json().expect("decoded").clone();
            assert_eq!(value["orderId"], format!("id-{index}"));
        }
        assert_eq!(
            fetches(&server, "/schemas/ids/12").await,
            1,
            "every record in the batch shares one resolution"
        );
    }

    #[tokio::test]
    async fn a_resolved_schema_is_not_fetched_again() {
        let server = MockServer::start().await;
        mock_schema(&server, 12, "AVRO", ORDER_SCHEMA).await;

        let decoder = decoder(&server.uri());
        let payload = encode_avro(ORDER_SCHEMA, |_, record| {
            record.put("orderId", "abc".to_owned());
            record.put("amount", 1i64);
        });
        let framed = frame(12, &payload);

        decode_payload(&decoder, &framed).await;
        decode_payload(&decoder, &framed).await;

        assert_eq!(fetches(&server, "/schemas/ids/12").await, 1);
    }

    async fn mock_missing_schema(server: &MockServer, id: u32) {
        Mock::given(method("GET"))
            .and(path(format!("/schemas/ids/{id}")))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": 40403,
                "message": "Schema not found.",
            })))
            .mount(server)
            .await;
    }

    async fn fetches(server: &MockServer, path: &str) -> usize {
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|request| request.url.path() == path)
            .count()
    }
}
