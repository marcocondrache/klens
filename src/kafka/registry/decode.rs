use std::sync::Arc;
use std::time::Duration;

use foldhash::{HashMap, HashMapExt, HashSet};

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use moka::future::Cache;
use schemreg::{Schema, SchemaId, SchemaKey, SchemaRegistryClient as _, decode_wire_prefix};
use serde_json::Value;
use thiserror::Error;

use super::avro::AvroCodec;
use super::client::{Registry, SchemaRegistryClient};
use super::protobuf::{ProtobufCodec, ProtobufError};
use crate::kafka::model::SchemaType;
use crate::kafka::scan::payload::{PayloadCodec, PayloadSlot, ResolvedCodec};

#[cfg(test)]
use bytes::Bytes;

const MAX_CACHED_SCHEMAS: u64 = 10_000;
const FAILED_LOOKUP_TTL: Duration = Duration::from_secs(5);

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

        let id = u32::try_from(slot.fallback_schema_id?).ok()?;
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
    codecs: Cache<SchemaKey, Resolved>,
    missing: Cache<SchemaKey, ()>,
    failed: Cache<SchemaKey, DecodeError>,
}

#[derive(Clone)]
enum Resolved {
    Avro(Arc<AvroCodec>),
    Json,
    Protobuf(Arc<ProtobufCodec>),
    Missing,
}

impl Resolved {
    fn compile(
        schema_type: SchemaType,
        source: &str,
        dependencies: &[(String, String)],
    ) -> Result<Self, DecodeError> {
        match schema_type {
            SchemaType::Avro => AvroCodec::compile(source, dependencies)
                .map(|codec| Self::Avro(Arc::new(codec)))
                .map_err(DecodeError::failed),
            SchemaType::Json => Ok(Self::Json),
            SchemaType::Protobuf => ProtobufCodec::compile(source, dependencies)
                .map(|codec| Self::Protobuf(Arc::new(codec)))
                .map_err(DecodeError::from),
        }
    }

    fn decode(&self, framing: Framing, raw: &[u8]) -> Result<Value, DecodeError> {
        let body = &raw[framing.payload_start.min(raw.len())..];

        match self {
            Self::Missing => Err(DecodeError::missing("schema id not found in registry")),
            Self::Json => serde_json::from_slice(body).map_err(DecodeError::failed),
            Self::Avro(codec) => codec.decode(body).map_err(DecodeError::failed),
            Self::Protobuf(codec) => if framing.has_wire_prefix {
                codec.decode_framed(body)
            } else {
                codec.decode_raw(body)
            }
            .map_err(DecodeError::from),
        }
    }
}

impl PayloadDecoder {
    pub(crate) fn new(client: SchemaRegistryClient, missing_ttl: Duration) -> Self {
        Self::with_ttls(client, missing_ttl, FAILED_LOOKUP_TTL)
    }

    fn with_ttls(
        client: SchemaRegistryClient,
        missing_ttl: Duration,
        failed_ttl: Duration,
    ) -> Self {
        Self {
            client,
            codecs: Cache::builder().max_capacity(MAX_CACHED_SCHEMAS).build(),
            missing: Cache::builder()
                .max_capacity(MAX_CACHED_SCHEMAS)
                .time_to_live(missing_ttl)
                .build(),
            failed: Cache::builder()
                .max_capacity(MAX_CACHED_SCHEMAS)
                .time_to_live(failed_ttl)
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
        if let Some(error) = self.failed.get(&key).await {
            return Err(error);
        }

        let resolved = self.lookup(key).await;
        if let Err(error) = &resolved {
            self.failed.insert(key, error.clone()).await;
        }
        resolved
    }

    async fn lookup(&self, key: SchemaKey) -> Result<Resolved, DecodeError> {
        let schema = match self.registry().get_schema_by_key(key).await {
            Ok(schema) => schema,
            Err(error) if error.is_not_found() => {
                self.missing.insert(key, ()).await;
                return Ok(Resolved::Missing);
            }
            Err(error) => return Err(DecodeError::failed(error)),
        };

        match SchemaType::try_from(schema.schema_type).map_err(DecodeError::failed)? {
            SchemaType::Json => Ok(Resolved::Json),
            schema_type => self.compiled(key, schema_type, &schema).await,
        }
    }

    async fn compiled(
        &self,
        key: SchemaKey,
        schema_type: SchemaType,
        schema: &Schema,
    ) -> Result<Resolved, DecodeError> {
        self.codecs
            .try_get_with(key, async {
                let dependencies = self
                    .client
                    .dependencies(&schema.references)
                    .await
                    .map_err(DecodeError::failed)?;
                let source = schema.schema.to_string();
                tokio::task::spawn_blocking(move || {
                    Resolved::compile(schema_type, &source, &dependencies)
                })
                .await
                .map_err(DecodeError::failed)?
            })
            .await
            .map_err(|error| (*error).clone())
    }
}

#[async_trait]
impl PayloadCodec for PayloadDecoder {
    async fn resolve(&self, slots: &[PayloadSlot]) -> Box<dyn ResolvedCodec> {
        let (schemas, failures) = self.resolve_slots(slots).await;
        for (key, error) in failures {
            error.report(key);
        }
        Box::new(schemas)
    }
}

impl PayloadDecoder {
    async fn resolve_slots(
        &self,
        slots: &[PayloadSlot],
    ) -> (ResolvedSchemas, Vec<(SchemaKey, DecodeError)>) {
        let keys: HashSet<SchemaKey> = slots
            .iter()
            .filter_map(Framing::of)
            .map(|framing| framing.key)
            .collect();

        let mut schemas = HashMap::with_capacity(keys.len());
        let mut failures = Vec::new();
        let mut loads: FuturesUnordered<_> = keys
            .into_iter()
            .map(|key| async move { (key, self.resolved(key).await) })
            .collect();
        while let Some((key, resolved)) = loads.next().await {
            match resolved {
                Ok(resolved) => {
                    schemas.insert(key, resolved);
                }
                Err(error) => failures.push((key, error)),
            }
        }

        (ResolvedSchemas(schemas), failures)
    }
}

struct ResolvedSchemas(HashMap<SchemaKey, Resolved>);

impl ResolvedCodec for ResolvedSchemas {
    fn decode(&self, slots: &mut [PayloadSlot]) {
        for (key, error) in self.decode_slots(slots) {
            error.report(key);
        }
    }
}

impl ResolvedSchemas {
    fn decode_slots(&self, slots: &mut [PayloadSlot]) -> Vec<(SchemaKey, DecodeError)> {
        let mut failures = Vec::new();

        for slot in slots {
            let Some(framing) = Framing::of(slot) else {
                continue;
            };
            let Some(resolved) = self.0.get(&framing.key) else {
                continue;
            };

            match resolved.decode(framing, &slot.raw) {
                Ok(json) => {
                    slot.json = Some(json);
                }
                Err(error) => failures.push((framing.key, error)),
            }
        }

        failures
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Error)]
enum DecodeError {
    #[error("{0}")]
    Missing(&'static str),
    #[error("{0}")]
    Failed(String),
}

impl DecodeError {
    fn missing(message: &'static str) -> Self {
        Self::Missing(message)
    }

    fn failed(error: impl std::fmt::Display) -> Self {
        Self::Failed(error.to_string())
    }

    fn report(&self, key: SchemaKey) {
        match self {
            Self::Missing(message) => tracing::debug!(
                schema_id = %key,
                error = *message,
                "skipping schema registry payload decode"
            ),
            Self::Failed(message) => tracing::warn!(
                schema_id = %key,
                error = message.as_str(),
                "failed to decode schema registry payload"
            ),
        }
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
    pub wire_schema_id: Option<i32>,
}

#[cfg(test)]
impl PayloadDecoder {
    pub(crate) async fn decode(&self, bytes: &[u8]) -> String {
        self.decode_with(bytes, None).await.text
    }

    pub(crate) async fn decode_with(
        &self,
        bytes: &[u8],
        fallback_schema_id: Option<i32>,
    ) -> DecodedField {
        let mut slots = [PayloadSlot::new(
            Bytes::copy_from_slice(bytes),
            fallback_schema_id,
        )];
        self.resolve(&slots).await.decode(&mut slots);
        let [slot] = slots;
        let decoded = slot.take();

        DecodedField {
            wire_schema_id: decoded.wire_schema_id(),
            text: decoded.into_text(),
        }
    }

    async fn decode_failure(
        &self,
        bytes: &[u8],
        fallback_schema_id: Option<i32>,
    ) -> Option<DecodeError> {
        let mut slots = [PayloadSlot::new(
            Bytes::copy_from_slice(bytes),
            fallback_schema_id,
        )];
        let (schemas, mut failures) = self.resolve_slots(&slots).await;
        failures.extend(schemas.decode_slots(&mut slots));
        failures.into_iter().next().map(|(_, error)| error)
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::Schema as AvroSchema;
    use apache_avro::types::{Record, Value as AvroValue};
    use apache_avro::writer::datum::GenericDatumWriter;
    use schemreg::{encode_protobuf_wire_format, encode_wire_format};

    use super::*;
    use crate::config::SchemaRegistryTuning;
    use crate::kafka::registry::testing::{FakeRegistry, Schema};
    use crate::kafka::scan::filter::CompiledFilter;
    use crate::kafka::scan::payload::DecodedPayload;
    use crate::testing::LogCapture;

    fn decode_bytes(bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).into_owned()
    }

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
        write_avro(&parsed, record)
    }

    fn write_avro(schema: &AvroSchema, value: impl Into<AvroValue>) -> Vec<u8> {
        GenericDatumWriter::builder(schema)
            .build()
            .unwrap()
            .write_value_to_vec(value)
            .unwrap()
    }

    fn matches_orderid(value: &DecodedPayload) -> bool {
        CompiledFilter::contains("orderid")
            .unwrap()
            .unwrap()
            .on_payload(None, Some(value))
    }

    async fn decode_batch(decoder: &PayloadDecoder, slots: &mut [PayloadSlot]) {
        decoder.resolve(slots).await.decode(slots);
    }

    async fn decode_payload(decoder: &PayloadDecoder, bytes: &[u8]) -> DecodedPayload {
        let mut slots = [PayloadSlot::new(Bytes::copy_from_slice(bytes), None)];
        decode_batch(decoder, &mut slots).await;
        let [slot] = slots;
        slot.take()
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
        let registry = FakeRegistry::start().await;
        registry.put(12, Schema::avro(ORDER_SCHEMA));

        let payload = encode_avro(ORDER_SCHEMA, |_, record| {
            record.put("orderId", "abc".to_owned());
            record.put("amount", 42i64);
        });
        let framed = frame(12, &payload);
        let decoded = decode_payload(&registry.decoder(), &framed).await;
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
        let registry = FakeRegistry::start().await;
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
            registry.put(id, Schema::avro(schema));
            let parsed = AvroSchema::parse_str(schema).unwrap();
            let payload = write_avro(&parsed, value);
            assert_eq!(
                registry.decoder().decode(&frame(id, &payload)).await,
                expected
            );
        }
    }

    #[tokio::test]
    async fn decodes_json_schema_payload() {
        let registry = FakeRegistry::start().await;
        registry.put(7, Schema::json(r#"{"type":"object"}"#));

        let framed = frame(7, br#"{"ok": true}"#);
        let json = registry.decoder().decode(&framed).await;
        assert_eq!(json, r#"{"ok":true}"#);
    }

    #[tokio::test]
    async fn leaves_unframed_payloads_unchanged() {
        let registry = FakeRegistry::start().await;
        let raw = br#"{"plain":true}"#;
        assert_eq!(
            registry.decoder().decode(raw).await,
            String::from_utf8_lossy(raw)
        );
    }

    #[tokio::test]
    async fn falls_back_when_schema_is_missing() {
        let registry = FakeRegistry::start().await;

        let framed = frame(99, b"not-json");
        assert_eq!(
            registry.decoder().decode(&framed).await,
            decode_bytes(&framed)
        );
        assert_eq!(
            registry
                .decoder()
                .decode_failure(&framed, None)
                .await
                .unwrap(),
            DecodeError::missing("schema id not found in registry")
        );
    }

    #[tokio::test]
    async fn a_missing_schema_is_not_re_queried_within_its_ttl() {
        let registry = FakeRegistry::start().await;

        let decoder = registry.decoder();
        let framed = frame(99, b"not-json");
        decoder.decode(&framed).await;
        decoder.decode(&framed).await;

        assert_eq!(registry.hits("/schemas/ids/99").await, 1);
    }

    #[tokio::test]
    async fn falls_back_when_avro_payload_does_not_match_schema() {
        let registry = FakeRegistry::start().await;
        registry.put(12, Schema::avro(ORDER_SCHEMA));
        let framed = frame(12, b"????");
        assert_eq!(
            registry.decoder().decode(&framed).await,
            decode_bytes(&framed)
        );
    }

    #[tokio::test]
    async fn decodes_protobuf_record_to_json() {
        let registry = FakeRegistry::start().await;
        registry.put(3, Schema::protobuf(ORDER_PROTO));

        let framed = proto_frame(3, &[0], b"\x0a\x03abc\x10\x2a");
        let decoded = decode_payload(&registry.decoder(), &framed).await;
        let value = decoded.json().expect("decoded to structured json");

        assert_eq!(value["orderId"], "abc");
        assert_eq!(value["amount"], "42");
        assert!(matches_orderid(&decoded));
    }

    #[tokio::test]
    async fn falls_back_when_protobuf_schema_has_no_messages() {
        let registry = FakeRegistry::start().await;
        registry.put(3, Schema::protobuf("syntax = \"proto3\";"));
        let framed = proto_frame(3, &[0], b"\x08\x01");
        assert_eq!(
            registry.decoder().decode(&framed).await,
            decode_bytes(&framed)
        );
    }

    #[tokio::test]
    async fn an_any_type_url_with_a_newline_stays_on_its_decode_warning() {
        let registry = FakeRegistry::start().await;
        registry.put(
            3,
            Schema::protobuf(
                r#"
                syntax = "proto3";
                import "google/protobuf/any.proto";
                message Envelope {
                    google.protobuf.Any body = 1;
                }
                "#,
            ),
        );
        let framed = proto_frame(3, &[0], b"\x0a\x11\x0a\x0fx\nuser=\"mallory");
        let logs = LogCapture::at(tracing::Level::WARN);

        registry.decoder().decode(&framed).await;

        logs.assert_contains(r#"error="unsupported type url 'x\nuser=\"mallory'"#);
        logs.assert_lacks("\nuser=");
    }

    #[tokio::test]
    async fn a_truncated_protobuf_index_is_a_typed_decode_error() {
        let registry = FakeRegistry::start().await;
        registry.put(3, Schema::protobuf(ORDER_PROTO));
        let framed = frame(3, &[]);
        assert!(matches!(
            registry
                .decoder()
                .decode_failure(&framed, None)
                .await
                .unwrap(),
            DecodeError::Failed(_)
        ));
    }

    #[tokio::test]
    async fn decodes_unframed_protobuf_with_override_schema_id() {
        let registry = FakeRegistry::start().await;
        registry.put(3, Schema::protobuf(ORDER_PROTO));

        let payload = b"\x0a\x03abc\x10\x2a";
        let decoded = registry.decoder().decode_with(payload, Some(3)).await;
        let value: serde_json::Value = serde_json::from_str(&decoded.text).unwrap();
        assert_eq!(value["orderId"], "abc");
        assert_eq!(value["amount"], "42");
        assert_eq!(decoded.wire_schema_id, None);
    }

    #[tokio::test]
    async fn decodes_protobuf_schema_references() {
        let registry = FakeRegistry::start().await;
        registry.put(
            20,
            Schema::protobuf(TAGGED_ORDER_PROTO).referencing("common.proto", "common.proto", 1),
        );
        registry.register("common.proto", 1, 4, Schema::protobuf(STATUS_PROTO));

        let framed = proto_frame(20, &[0], b"\x0a\x06\x0a\x04OPEN");
        let json = registry.decoder().decode(&framed).await;
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["status"]["code"], "OPEN");
    }

    #[tokio::test]
    async fn decodes_unframed_avro_with_override_schema_id() {
        let registry = FakeRegistry::start().await;
        registry.put(12, Schema::avro(ORDER_SCHEMA));

        let payload = encode_avro(ORDER_SCHEMA, |_, record| {
            record.put("orderId", "abc".to_owned());
            record.put("amount", 42i64);
        });
        let decoded = registry.decoder().decode_with(&payload, Some(12)).await;
        let value: serde_json::Value = serde_json::from_str(&decoded.text).unwrap();

        assert_eq!(value["orderId"], "abc");
        assert_eq!(value["amount"], 42);
        assert_eq!(decoded.wire_schema_id, None);
        assert!(
            !decode_bytes(&payload)
                .to_ascii_lowercase()
                .contains("orderid")
        );
    }

    #[tokio::test]
    async fn framed_payload_ignores_override_schema_id() {
        let registry = FakeRegistry::start().await;
        registry.put(12, Schema::avro(ORDER_SCHEMA));
        registry.put(99, Schema::json(r#"{"type":"object"}"#));

        let payload = encode_avro(ORDER_SCHEMA, |_, record| {
            record.put("orderId", "abc".to_owned());
            record.put("amount", 42i64);
        });
        let framed = frame(12, &payload);
        let decoded = registry.decoder().decode_with(&framed, Some(99)).await;
        let value: serde_json::Value = serde_json::from_str(&decoded.text).unwrap();

        assert_eq!(value["orderId"], "abc");
        assert_eq!(decoded.wire_schema_id, Some(12));
    }

    #[tokio::test]
    async fn unframed_override_falls_back_when_payload_does_not_match() {
        let registry = FakeRegistry::start().await;
        registry.put(12, Schema::avro(ORDER_SCHEMA));
        let raw = b"????";
        let decoded = registry.decoder().decode_with(raw, Some(12)).await;
        assert_eq!(decoded.text, decode_bytes(raw));
        assert_eq!(decoded.wire_schema_id, None);
    }

    #[tokio::test]
    async fn unframed_override_falls_back_when_schema_is_missing() {
        let registry = FakeRegistry::start().await;

        let raw = b"not-json";
        let decoded = registry.decoder().decode_with(raw, Some(99)).await;
        assert_eq!(decoded.text, decode_bytes(raw));
        assert_eq!(decoded.wire_schema_id, None);
    }

    #[tokio::test]
    async fn decodes_avro_schema_references() {
        let registry = FakeRegistry::start().await;
        registry.put(
            20,
            Schema::avro(ORDER_WITH_STATUS).referencing("Status", "Status", 1),
        );
        registry.register("Status", 1, 4, Schema::avro(STATUS_SCHEMA));

        let (writer, dependencies) =
            AvroSchema::parse_str_with_list(ORDER_WITH_STATUS, [STATUS_SCHEMA]).unwrap();
        let mut record = Record::new(&writer).unwrap();
        record.put("status", "OPEN");
        let mut schemata: Vec<&AvroSchema> = dependencies.iter().collect();
        schemata.push(&writer);
        let payload = GenericDatumWriter::builder(&writer)
            .schemata(schemata)
            .unwrap()
            .build()
            .unwrap()
            .write_value_to_vec(record)
            .unwrap();

        let json = registry.decoder().decode(&frame(20, &payload)).await;
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["status"], "OPEN");
    }

    #[tokio::test]
    async fn a_batch_resolves_each_schema_id_once() {
        let registry = FakeRegistry::start().await;
        registry.put(12, Schema::avro(ORDER_SCHEMA));

        let decoder = registry.decoder();
        let mut slots: Vec<PayloadSlot> = (0..5)
            .map(|index| {
                let payload = encode_avro(ORDER_SCHEMA, |_, record| {
                    record.put("orderId", format!("id-{index}"));
                    record.put("amount", index as i64);
                });
                PayloadSlot::new(Bytes::from(frame(12, &payload)), None)
            })
            .collect();

        decode_batch(&decoder, &mut slots).await;

        for (index, slot) in slots.into_iter().enumerate() {
            let value = slot.take().json().expect("decoded").clone();
            assert_eq!(value["orderId"], format!("id-{index}"));
        }
        assert_eq!(
            registry.hits("/schemas/ids/12").await,
            1,
            "every record in the batch shares one resolution"
        );
    }

    #[tokio::test]
    async fn a_resolved_schema_is_not_fetched_again() {
        let registry = FakeRegistry::start().await;
        registry.put(12, Schema::avro(ORDER_SCHEMA));

        let decoder = registry.decoder();
        let payload = encode_avro(ORDER_SCHEMA, |_, record| {
            record.put("orderId", "abc".to_owned());
            record.put("amount", 1i64);
        });
        let framed = frame(12, &payload);

        decode_payload(&decoder, &framed).await;
        decode_payload(&decoder, &framed).await;

        assert_eq!(registry.hits("/schemas/ids/12").await, 1);
    }

    #[tokio::test]
    async fn a_failed_lookup_is_reported_once_per_batch_and_not_retried_within_its_ttl() {
        let registry = FakeRegistry::start().await;
        registry.fail("/schemas/ids/12");
        let logs = LogCapture::at(tracing::Level::WARN);

        let decoder = registry.decoder();
        let framed = Bytes::from(frame(12, b"datum"));
        for batch in 1..=2 {
            let mut slots: Vec<PayloadSlot> = (0..20)
                .map(|_| PayloadSlot::new(framed.clone(), None))
                .collect();
            decode_batch(&decoder, &mut slots).await;

            assert!(slots.iter().all(|slot| slot.json.is_none()));
            assert_eq!(
                logs.count("failed to decode schema registry payload"),
                batch
            );
        }
        assert_eq!(registry.hits("/schemas/ids/12").await, 1);
    }

    #[tokio::test]
    async fn a_failed_lookup_is_retried_after_its_ttl() {
        let registry = FakeRegistry::start().await;
        registry.fail_once("/schemas/ids/7");
        registry.put(7, Schema::json("{}"));
        let decoder = PayloadDecoder::with_ttls(
            registry.client(),
            SchemaRegistryTuning::default().missing_schema_ttl,
            Duration::from_millis(50),
        );
        let framed = frame(7, br#"{"ok":true}"#);

        assert!(matches!(
            decoder.decode_failure(&framed, None).await,
            Some(DecodeError::Failed(_))
        ));
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(decoder.decode(&framed).await, r#"{"ok":true}"#);
        assert_eq!(registry.hits("/schemas/ids/7").await, 2);
    }
}
