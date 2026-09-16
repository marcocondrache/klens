use std::collections::HashSet;
use std::sync::Arc;

use bytes::Bytes;
use moka::future::Cache;
use schemreg::{
    AvroSchemaDecoder, JsonSchemaDecoder, SchemaId, SchemaKey, SchemaRegError,
    decode_schema_id_header, decode_wire_format, encode_wire_format,
};

use super::client::{CachedRegistry, SchemaRegistryClient};
use super::protobuf::{ProtobufCodec, ProtobufError};
use crate::kafka::model::decode_bytes;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedField {
    pub text: String,
    pub schema_id: Option<i32>,
}

struct Resolved<'a> {
    key: SchemaKey,
    framed: Bytes,
    proto: ProtoInput<'a>,
    schema_id: Option<i32>,
}

enum ProtoInput<'a> {
    Framed(&'a [u8]),
    Raw(&'a [u8]),
    Path {
        indexes: Vec<i32>,
        payload: &'a [u8],
    },
}

pub(crate) struct PayloadDecoder {
    client: SchemaRegistryClient,
    avro: AvroSchemaDecoder<Arc<CachedRegistry>>,
    json: JsonSchemaDecoder<Arc<CachedRegistry>>,
    proto: Cache<SchemaKey, Arc<CachedSchema>>,
}

enum CachedSchema {
    Protobuf(ProtobufCodec),
    Missing,
}

impl PayloadDecoder {
    pub(crate) fn new(client: SchemaRegistryClient) -> Self {
        let cached = client.cached();
        Self {
            avro: AvroSchemaDecoder::new(Arc::clone(&cached)),
            json: JsonSchemaDecoder::new(cached),
            proto: Cache::builder().max_capacity(10_000).build(),
            client,
        }
    }

    pub(crate) fn client(&self) -> &SchemaRegistryClient {
        &self.client
    }

    #[cfg(test)]
    pub(crate) async fn decode(&self, bytes: &[u8]) -> String {
        self.decode_with(bytes, None, None).await.text
    }

    /// Decode a payload. Wire framing wins, then a Kafka schema-id header, then
    /// `override_id` for unframed bytes. Wire and header schema ids are the
    /// ones exposed on the record; an override is only a decode hint.
    pub(crate) async fn decode_with(
        &self,
        bytes: &[u8],
        override_id: Option<i32>,
        header: Option<&[u8]>,
    ) -> DecodedField {
        match resolve_frame(bytes, override_id, header) {
            Some(frame) => DecodedField {
                text: self.decode_or_raw(&frame, bytes).await,
                schema_id: frame.schema_id,
            },
            None => DecodedField {
                text: decode_bytes(bytes),
                schema_id: None,
            },
        }
    }

    async fn decode_or_raw(&self, frame: &Resolved<'_>, original: &[u8]) -> String {
        match self.decode_frame(frame).await {
            Ok(json) => json,
            Err(error) => {
                match &error {
                    DecodeError::Missing(message) => {
                        tracing::debug!(
                            schema_id = frame.schema_id,
                            error = %message,
                            "skipping schema registry payload decode"
                        );
                    }
                    DecodeError::Failed(message) => {
                        tracing::warn!(
                            schema_id = frame.schema_id,
                            error = %message,
                            "failed to decode schema registry payload"
                        );
                    }
                }
                decode_bytes(original)
            }
        }
    }
}

pub(crate) async fn decode_field(
    decoder: Option<&PayloadDecoder>,
    bytes: Option<&[u8]>,
    override_id: Option<i32>,
    header: Option<&[u8]>,
) -> Option<DecodedField> {
    match (bytes, decoder) {
        (None, _) => None,
        (Some(bytes), Some(decoder)) => Some(decoder.decode_with(bytes, override_id, header).await),
        (Some(bytes), None) => Some(DecodedField {
            text: decode_bytes(bytes),
            schema_id: resolve_frame(bytes, None, header).and_then(|frame| frame.schema_id),
        }),
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

impl PayloadDecoder {
    async fn decode_frame(&self, frame: &Resolved<'_>) -> Result<String, DecodeError> {
        let schema = match self.client.schema_by_key(frame.key).await {
            Ok(Some(schema)) => schema,
            Ok(None) => return Err(DecodeError::missing("schema id not found in registry")),
            Err(error) => return Err(DecodeError::failed(error)),
        };
        match schema.schema_type {
            schemreg::SchemaType::Json => json_payload(&self.json, frame.framed.clone()).await,
            schemreg::SchemaType::Avro => avro_payload(&self.avro, frame.framed.clone()).await,
            schemreg::SchemaType::Protobuf => self.decode_protobuf(&frame.proto, frame.key).await,
            _ => Err(DecodeError::failed("unsupported schema type")),
        }
    }

    async fn decode_protobuf(
        &self,
        input: &ProtoInput<'_>,
        key: SchemaKey,
    ) -> Result<String, DecodeError> {
        let cached = self.resolved_proto(key).await?;
        match cached.as_ref() {
            CachedSchema::Missing => Err(DecodeError::missing("schema id not found in registry")),
            CachedSchema::Protobuf(codec) => match input {
                ProtoInput::Framed(payload) => codec.decode_framed(payload),
                ProtoInput::Raw(payload) => codec.decode_raw(payload),
                ProtoInput::Path { indexes, payload } => codec.decode_message(indexes, payload),
            }
            .map_err(DecodeError::from),
        }
    }

    async fn resolved_proto(&self, key: SchemaKey) -> Result<Arc<CachedSchema>, DecodeError> {
        self.proto
            .try_get_with(key, self.load_proto(key))
            .await
            .map_err(|error| (*error).clone())
    }

    async fn load_proto(&self, key: SchemaKey) -> Result<Arc<CachedSchema>, DecodeError> {
        let registered = match self.client.schema_by_key(key).await {
            Ok(Some(schema)) => schema,
            Ok(None) => return Ok(Arc::new(CachedSchema::Missing)),
            Err(error) => return Err(DecodeError::failed(error)),
        };
        let dependencies = self.collect_named_references(&registered).await?;
        let codec = ProtobufCodec::compile(&registered.schema, &dependencies)?;
        Ok(Arc::new(CachedSchema::Protobuf(codec)))
    }

    async fn collect_named_references(
        &self,
        registered: &schemreg::Schema,
    ) -> Result<Vec<(String, String)>, DecodeError> {
        let mut bodies = Vec::new();
        let mut pending = registered.references.clone();
        let mut seen = HashSet::new();

        while let Some(reference) = pending.pop() {
            if !seen.insert((reference.subject.clone(), reference.version)) {
                continue;
            }
            let fetched = self
                .client
                .schema_version(&reference.subject, reference.version)
                .await
                .map_err(DecodeError::failed)?;
            pending.extend(fetched.references.iter().cloned());
            bodies.push((reference.name, fetched.schema.to_string()));
        }

        Ok(bodies)
    }
}

fn resolve_frame<'a>(
    bytes: &'a [u8],
    override_id: Option<i32>,
    header: Option<&[u8]>,
) -> Option<Resolved<'a>> {
    if let Ok((key, after_prefix)) = decode_wire_format(bytes) {
        return Some(Resolved {
            key,
            framed: Bytes::copy_from_slice(bytes),
            proto: ProtoInput::Framed(after_prefix),
            schema_id: schema_id_i32(key),
        });
    }
    if let Some(header) = header
        && let Ok((key, indexes)) = decode_schema_id_header(header)
    {
        return Some(Resolved {
            key,
            framed: encode_wire_format(key, bytes),
            proto: match indexes {
                Some(indexes) => ProtoInput::Path {
                    indexes: indexes.into_iter().map(|index| index as i32).collect(),
                    payload: bytes,
                },
                None => ProtoInput::Raw(bytes),
            },
            schema_id: schema_id_i32(key),
        });
    }
    let id = override_id.and_then(|id| u32::try_from(id).ok())?;
    let key = SchemaKey::from(SchemaId::new(id));
    Some(Resolved {
        key,
        framed: encode_wire_format(key, bytes),
        proto: ProtoInput::Raw(bytes),
        schema_id: None,
    })
}

fn schema_id_i32(key: SchemaKey) -> Option<i32> {
    key.as_id().and_then(|id| i32::try_from(id.as_u32()).ok())
}

async fn avro_payload(
    decoder: &AvroSchemaDecoder<Arc<CachedRegistry>>,
    framed: Bytes,
) -> Result<String, DecodeError> {
    let value: serde_json::Value = decoder.decode_de(framed).await.map_err(registry_decode)?;
    serde_json::to_string(&value).map_err(DecodeError::failed)
}

async fn json_payload(
    decoder: &JsonSchemaDecoder<Arc<CachedRegistry>>,
    framed: Bytes,
) -> Result<String, DecodeError> {
    let value = decoder.decode(framed).await.map_err(registry_decode)?;
    serde_json::to_string(&value).map_err(DecodeError::failed)
}

fn registry_decode(error: SchemaRegError) -> DecodeError {
    if error.is_not_found() {
        DecodeError::missing("schema id not found in registry")
    } else {
        DecodeError::failed(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SchemaRegistryConfig;
    use crate::kafka::model::{Compression, Record as KafkaRecord};
    use apache_avro::Schema;
    use apache_avro::types::{Record, Value};
    use apache_avro::writer::datum::GenericDatumWriter;
    use schemreg::{VALUE_SCHEMA_ID_HEADER, encode_schema_id_header};
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

    fn frame(schema_id: i32, payload: &[u8]) -> Vec<u8> {
        encode_wire_format(u32::try_from(schema_id).unwrap(), payload).to_vec()
    }

    fn encode_avro(schema: &str, build: impl FnOnce(&Schema, &mut Record<'_>)) -> Vec<u8> {
        let parsed = Schema::parse_str(schema).unwrap();
        let mut record = Record::new(&parsed).unwrap();
        build(&parsed, &mut record);
        GenericDatumWriter::builder(&parsed)
            .build()
            .unwrap()
            .write_value_to_vec(Value::Record(record.fields))
            .unwrap()
    }

    async fn mock_schema(server: &MockServer, id: i32, schema_type: &str, schema: &str) {
        Mock::given(method("GET"))
            .and(path(format!("/schemas/ids/{id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schemaType": schema_type,
                "schema": schema,
            })))
            .mount(server)
            .await;
    }

    fn matches_orderid(record: &KafkaRecord) -> bool {
        crate::kafka::record::filter::compile(r#"valueText.lowerAscii().contains("orderid")"#)
            .unwrap()
            .unwrap()
            .matches(record)
    }

    fn sample_record(key: Option<String>, value: Option<String>) -> KafkaRecord {
        KafkaRecord {
            topic: "orders".into(),
            partition: 0,
            offset: 1,
            timestamp: 0,
            key,
            value,
            schema_id: None,
            headers: Vec::new(),
            size_bytes: 0,
            compression: Compression::None,
        }
    }

    #[test]
    fn decode_error_variants_display_their_message() {
        assert_eq!(
            DecodeError::missing("schema id not found in registry").to_string(),
            "schema id not found in registry"
        );
        assert_eq!(
            DecodeError::from(ProtobufError::TruncatedIndex).to_string(),
            "truncated protobuf message index"
        );
        assert_eq!(
            DecodeError::from(ProtobufError::EmptyIndexPath),
            DecodeError::failed("protobuf message index path is empty")
        );
    }

    #[test]
    fn parses_confluent_frame() {
        let bytes = frame(12, b"datum");
        let parsed = resolve_frame(&bytes, None, None).unwrap();
        assert_eq!(parsed.schema_id, Some(12));
        assert!(matches!(parsed.proto, ProtoInput::Framed(payload) if payload == b"datum"));
    }

    #[test]
    fn rejects_short_or_non_magic_frames() {
        assert!(resolve_frame(&[0, 0, 0, 1], None, None).is_none());
        assert!(resolve_frame(b"hello", None, None).is_none());
        assert!(resolve_frame(&[], None, None).is_none());
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
        let json = decoder(&server.uri()).decode(&framed).await;
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["orderId"], "abc");
        assert_eq!(value["amount"], 42);
        assert!(
            !decode_bytes(&framed)
                .to_ascii_lowercase()
                .contains("orderid")
        );
        assert!(matches_orderid(&sample_record(None, Some(json))));
    }

    #[tokio::test]
    async fn decodes_unnamed_avro_schemas_without_references() {
        let server = MockServer::start().await;
        for (id, schema, value, expected) in [
            (1, r#""long""#, Value::Long(42), "42"),
            (2, r#""null""#, Value::Null, "null"),
            (
                3,
                r#"{"type":"array","items":"long"}"#,
                Value::Array(vec![Value::Long(1), Value::Long(2)]),
                "[1,2]",
            ),
        ] {
            mock_schema(&server, id, "AVRO", schema).await;
            let parsed = Schema::parse_str(schema).unwrap();
            let payload = GenericDatumWriter::builder(&parsed)
                .build()
                .unwrap()
                .write_value_to_vec(value)
                .unwrap();
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
        Mock::given(method("GET"))
            .and(path("/schemas/ids/99"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": 40403,
                "message": "Schema not found.",
            })))
            .mount(&server)
            .await;

        let framed = frame(99, b"not-json");
        assert_eq!(
            decoder(&server.uri()).decode(&framed).await,
            decode_bytes(&framed)
        );
        let parsed = resolve_frame(&framed, None, None).unwrap();
        assert_eq!(
            decoder(&server.uri())
                .decode_frame(&parsed)
                .await
                .unwrap_err(),
            DecodeError::missing("schema id not found in registry")
        );
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

        let mut payload = crate::kafka::registry::protobuf::encode_indexes(&[0]);
        payload.extend_from_slice(b"\x0a\x03abc\x10\x2a");
        let framed = frame(3, &payload);
        let json = decoder(&server.uri()).decode(&framed).await;
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(value["orderId"], "abc");
        assert_eq!(value["amount"], "42");
        assert!(matches_orderid(&sample_record(None, Some(json))));
    }

    #[tokio::test]
    async fn decodes_protobuf_nested_message_index() {
        let server = MockServer::start().await;
        mock_schema(&server, 3, "PROTOBUF", ORDER_PROTO).await;

        let mut payload = crate::kafka::registry::protobuf::encode_indexes(&[2]);
        payload.extend_from_slice(b"\x08\x07");
        let json = decoder(&server.uri()).decode(&frame(3, &payload)).await;
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["n"], 7);
    }

    #[tokio::test]
    async fn falls_back_when_protobuf_schema_has_no_messages() {
        let server = MockServer::start().await;
        mock_schema(&server, 3, "PROTOBUF", "syntax = \"proto3\";").await;
        let framed = frame(3, b"\x00\x08\x01");
        assert_eq!(
            decoder(&server.uri()).decode(&framed).await,
            decode_bytes(&framed)
        );
    }

    #[tokio::test]
    async fn truncated_protobuf_index_is_a_typed_decode_error() {
        let server = MockServer::start().await;
        mock_schema(&server, 3, "PROTOBUF", ORDER_PROTO).await;
        let framed = frame(3, &[]);
        let parsed = resolve_frame(&framed, None, None).unwrap();
        assert_eq!(
            decoder(&server.uri())
                .decode_frame(&parsed)
                .await
                .unwrap_err(),
            DecodeError::failed("truncated protobuf message index")
        );
    }

    #[tokio::test]
    async fn decodes_unframed_protobuf_with_override_schema_id() {
        let server = MockServer::start().await;
        mock_schema(&server, 3, "PROTOBUF", ORDER_PROTO).await;

        let payload = b"\x0a\x03abc\x10\x2a";
        let decoded = decoder(&server.uri())
            .decode_with(payload, Some(3), None)
            .await;
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
                "subject": "common.proto",
                "id": 4,
                "version": 1,
                "schemaType": "PROTOBUF",
                "schema": STATUS_PROTO,
            })))
            .mount(&server)
            .await;

        let mut payload = crate::kafka::registry::protobuf::encode_indexes(&[0]);
        payload.extend_from_slice(b"\x0a\x06\x0a\x04OPEN");
        let json = decoder(&server.uri()).decode(&frame(20, &payload)).await;
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["status"]["code"], "OPEN");
    }

    #[tokio::test]
    async fn decode_field_without_decoder_is_lossy_utf8() {
        let decoded = decode_field(None, Some(b"hello"), None, None)
            .await
            .unwrap();
        assert_eq!(decoded.text, "hello");
        assert_eq!(decoded.schema_id, None);
        assert!(decode_field(None, None, None, None).await.is_none());
    }

    #[tokio::test]
    async fn decode_field_without_decoder_exposes_wire_schema_id() {
        let framed = frame(12, b"datum");
        let decoded = decode_field(None, Some(&framed), None, None).await.unwrap();
        assert_eq!(decoded.schema_id, Some(12));
        assert_eq!(decoded.text, decode_bytes(&framed));
    }

    #[tokio::test]
    async fn decodes_unframed_avro_with_override_schema_id() {
        let server = MockServer::start().await;
        mock_schema(&server, 12, "AVRO", ORDER_SCHEMA).await;

        let payload = encode_avro(ORDER_SCHEMA, |_, record| {
            record.put("orderId", "abc".to_owned());
            record.put("amount", 42i64);
        });
        let decoded = decoder(&server.uri())
            .decode_with(&payload, Some(12), None)
            .await;
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
        let decoded = decoder(&server.uri())
            .decode_with(&framed, Some(99), None)
            .await;
        let value: serde_json::Value = serde_json::from_str(&decoded.text).unwrap();

        assert_eq!(value["orderId"], "abc");
        assert_eq!(decoded.schema_id, Some(12));
    }

    #[tokio::test]
    async fn unframed_override_falls_back_when_payload_does_not_match() {
        let server = MockServer::start().await;
        mock_schema(&server, 12, "AVRO", ORDER_SCHEMA).await;
        let raw = b"????";
        let decoded = decoder(&server.uri())
            .decode_with(raw, Some(12), None)
            .await;
        assert_eq!(decoded.text, decode_bytes(raw));
        assert_eq!(decoded.schema_id, None);
    }

    #[tokio::test]
    async fn unframed_override_falls_back_when_schema_is_missing() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/99"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": 40403,
                "message": "Schema not found.",
            })))
            .mount(&server)
            .await;

        let raw = b"not-json";
        let decoded = decoder(&server.uri())
            .decode_with(raw, Some(99), None)
            .await;
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
                "subject": "Status",
                "id": 4,
                "version": 1,
                "schemaType": "AVRO",
                "schema": STATUS_SCHEMA,
            })))
            .mount(&server)
            .await;

        let (writer, dependencies) =
            Schema::parse_str_with_list(ORDER_WITH_STATUS, [STATUS_SCHEMA]).unwrap();
        let mut record = Record::new(&writer).unwrap();
        record.put("status", "OPEN");
        let mut schemata: Vec<&Schema> = dependencies.iter().collect();
        schemata.push(&writer);
        let payload = GenericDatumWriter::builder(&writer)
            .schemata(schemata)
            .unwrap()
            .build()
            .unwrap()
            .write_value_to_vec(Value::Record(record.fields))
            .unwrap();

        let json = decoder(&server.uri()).decode(&frame(20, &payload)).await;
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["status"], "OPEN");
    }

    #[tokio::test]
    async fn decodes_header_framed_json() {
        let server = MockServer::start().await;
        mock_schema(&server, 7, "JSON", r#"{"type":"object"}"#).await;

        let header = encode_schema_id_header(7u32, None);
        let decoded = decoder(&server.uri())
            .decode_with(br#"{"ok": true}"#, None, Some(&header))
            .await;
        assert_eq!(decoded.text, r#"{"ok":true}"#);
        assert_eq!(decoded.schema_id, Some(7));
        assert_eq!(VALUE_SCHEMA_ID_HEADER, "__value_schema_id");
    }

    #[tokio::test]
    async fn decode_field_without_decoder_exposes_header_schema_id() {
        let header = encode_schema_id_header(12u32, None);
        let decoded = decode_field(None, Some(b"datum"), None, Some(&header))
            .await
            .unwrap();
        assert_eq!(decoded.schema_id, Some(12));
        assert_eq!(decoded.text, "datum");
    }
}
