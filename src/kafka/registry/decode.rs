use std::collections::HashSet;
use std::sync::Arc;

use apache_avro::Schema;
use apache_avro::reader::datum::GenericDatumReader;
use moka::future::Cache;

use super::client::SchemaRegistryClient;
use crate::kafka::error::KafkaError;
use crate::kafka::model::{RegisteredSchema, SchemaType, decode_bytes};

const CONFLUENT_MAGIC: u8 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConfluentFrame<'a> {
    schema_id: i32,
    payload: &'a [u8],
}

impl<'a> ConfluentFrame<'a> {
    fn parse(bytes: &'a [u8]) -> Option<Self> {
        if bytes.len() < 5 || bytes[0] != CONFLUENT_MAGIC {
            return None;
        }
        Some(Self {
            schema_id: i32::from_be_bytes(bytes[1..5].try_into().ok()?),
            payload: &bytes[5..],
        })
    }
}

#[derive(Clone)]
pub(crate) struct PayloadDecoder {
    client: SchemaRegistryClient,
    cache: Cache<i32, Arc<CachedSchema>>,
}

#[derive(Clone)]
enum CachedSchema {
    Avro(AvroCodec),
    Json,
    Unsupported,
    Missing,
}

#[derive(Clone)]
struct AvroCodec {
    writer: Schema,
    dependencies: Vec<Schema>,
}

impl PayloadDecoder {
    pub(crate) fn new(client: SchemaRegistryClient) -> Self {
        Self {
            client,
            cache: schema_id_cache(),
        }
    }

    pub(crate) fn client(&self) -> &SchemaRegistryClient {
        &self.client
    }

    pub(crate) async fn decode(&self, bytes: &[u8]) -> String {
        let Some(frame) = ConfluentFrame::parse(bytes) else {
            return decode_bytes(bytes);
        };
        match self.decode_frame(frame).await {
            Ok(json) => json,
            Err(error) => {
                match error.kind {
                    DecodeKind::Unsupported | DecodeKind::Missing => {
                        tracing::debug!(
                            schema_id = frame.schema_id,
                            error = %error.message,
                            "skipping schema registry payload decode"
                        );
                    }
                    DecodeKind::Failed => {
                        tracing::warn!(
                            schema_id = frame.schema_id,
                            error = %error.message,
                            "failed to decode schema registry payload"
                        );
                    }
                }
                decode_bytes(bytes)
            }
        }
    }
}

pub(crate) async fn decode_field(
    decoder: Option<&PayloadDecoder>,
    bytes: Option<&[u8]>,
) -> Option<String> {
    match (bytes, decoder) {
        (None, _) => None,
        (Some(bytes), Some(decoder)) => Some(decoder.decode(bytes).await),
        (Some(bytes), None) => Some(decode_bytes(bytes)),
    }
}

fn schema_id_cache() -> Cache<i32, Arc<CachedSchema>> {
    Cache::builder().max_capacity(10_000).build()
}

#[derive(Clone)]
struct DecodeError {
    kind: DecodeKind,
    message: String,
}

#[derive(Clone, Copy)]
enum DecodeKind {
    Missing,
    Unsupported,
    Failed,
}

impl DecodeError {
    fn missing(message: impl Into<String>) -> Self {
        Self {
            kind: DecodeKind::Missing,
            message: message.into(),
        }
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self {
            kind: DecodeKind::Unsupported,
            message: message.into(),
        }
    }

    fn failed(message: impl Into<String>) -> Self {
        Self {
            kind: DecodeKind::Failed,
            message: message.into(),
        }
    }
}

impl PayloadDecoder {
    async fn decode_frame(&self, frame: ConfluentFrame<'_>) -> Result<String, DecodeError> {
        let cached = self.resolved(frame.schema_id).await?;
        match cached.as_ref() {
            CachedSchema::Missing => Err(DecodeError::missing("schema id not found in registry")),
            CachedSchema::Unsupported => Err(DecodeError::unsupported("unsupported schema type")),
            CachedSchema::Json => json_payload(frame.payload),
            CachedSchema::Avro(codec) => avro_payload(codec, frame.payload),
        }
    }

    async fn resolved(&self, id: i32) -> Result<Arc<CachedSchema>, DecodeError> {
        self.cache
            .try_get_with(id, self.load(id))
            .await
            .map_err(|error| (*error).clone())
    }

    async fn load(&self, id: i32) -> Result<Arc<CachedSchema>, DecodeError> {
        let registered = match self.client.schema_by_id(id).await {
            Ok(Some(schema)) => schema,
            Ok(None) => return Ok(Arc::new(CachedSchema::Missing)),
            Err(error) => return Err(registry_error(error)),
        };
        Ok(Arc::new(self.parse_registered(registered).await?))
    }

    async fn parse_registered(
        &self,
        registered: RegisteredSchema,
    ) -> Result<CachedSchema, DecodeError> {
        match registered.schema_type {
            SchemaType::Protobuf => Ok(CachedSchema::Unsupported),
            SchemaType::Json => Ok(CachedSchema::Json),
            SchemaType::Avro => {
                let dependencies = self.collect_references(&registered).await?;
                let codec = parse_avro(&registered.schema, &dependencies)?;
                Ok(CachedSchema::Avro(codec))
            }
        }
    }

    async fn collect_references(
        &self,
        registered: &RegisteredSchema,
    ) -> Result<Vec<String>, DecodeError> {
        let mut bodies = Vec::new();
        let mut pending = registered.references.clone();
        let mut seen = HashSet::new();

        while let Some(reference) = pending.pop() {
            if !seen.insert((reference.subject.clone(), reference.version)) {
                continue;
            }
            let fetched = self
                .client
                .schema_by_subject_version(&reference.subject, reference.version)
                .await
                .map_err(registry_error)?;
            pending.extend(fetched.references);
            bodies.push(fetched.schema);
        }

        Ok(bodies)
    }
}

fn parse_avro(schema: &str, dependencies: &[String]) -> Result<AvroCodec, DecodeError> {
    if dependencies.is_empty() {
        let writer =
            Schema::parse_str(schema).map_err(|error| DecodeError::failed(error.to_string()))?;
        return Ok(AvroCodec {
            writer,
            dependencies: Vec::new(),
        });
    }

    let (writer, dependencies) = Schema::parse_str_with_list(schema, dependencies)
        .map_err(|error| DecodeError::failed(error.to_string()))?;
    Ok(AvroCodec {
        writer,
        dependencies,
    })
}

fn avro_payload(codec: &AvroCodec, payload: &[u8]) -> Result<String, DecodeError> {
    let value = if codec.dependencies.is_empty() {
        GenericDatumReader::builder(&codec.writer)
            .build()
            .map_err(|error| DecodeError::failed(error.to_string()))?
            .read_value(&mut &*payload)
            .map_err(|error| DecodeError::failed(error.to_string()))?
    } else {
        let mut schemata: Vec<&Schema> = codec.dependencies.iter().collect();
        schemata.push(&codec.writer);
        GenericDatumReader::builder(&codec.writer)
            .writer_schemata(schemata)
            .map_err(|error| DecodeError::failed(error.to_string()))?
            .build()
            .map_err(|error| DecodeError::failed(error.to_string()))?
            .read_value(&mut &*payload)
            .map_err(|error| DecodeError::failed(error.to_string()))?
    };
    let json = serde_json::Value::try_from(value)
        .map_err(|error| DecodeError::failed(error.to_string()))?;
    serde_json::to_string(&json).map_err(|error| DecodeError::failed(error.to_string()))
}

fn json_payload(payload: &[u8]) -> Result<String, DecodeError> {
    let json: serde_json::Value =
        serde_json::from_slice(payload).map_err(|error| DecodeError::failed(error.to_string()))?;
    serde_json::to_string(&json).map_err(|error| DecodeError::failed(error.to_string()))
}

fn registry_error(error: KafkaError) -> DecodeError {
    DecodeError::failed(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SchemaRegistryConfig;
    use crate::kafka::model::{Compression, Record as KafkaRecord};
    use apache_avro::types::{Record, Value};
    use apache_avro::writer::datum::GenericDatumWriter;
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
        let mut out = Vec::with_capacity(5 + payload.len());
        out.push(CONFLUENT_MAGIC);
        out.extend_from_slice(&schema_id.to_be_bytes());
        out.extend_from_slice(payload);
        out
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

    fn sample_record(key: Option<String>, value: Option<String>) -> KafkaRecord {
        KafkaRecord {
            topic: "orders".into(),
            partition: 0,
            offset: 1,
            timestamp: 0,
            key,
            value,
            headers: Vec::new(),
            size_bytes: 0,
            compression: Compression::None,
        }
    }

    #[test]
    fn parses_confluent_frame() {
        let bytes = frame(12, b"datum");
        let parsed = ConfluentFrame::parse(&bytes).unwrap();
        assert_eq!(parsed.schema_id, 12);
        assert_eq!(parsed.payload, b"datum");
    }

    #[test]
    fn rejects_short_or_non_magic_frames() {
        assert!(ConfluentFrame::parse(&[0, 0, 0, 1]).is_none());
        assert!(ConfluentFrame::parse(b"hello").is_none());
        assert!(ConfluentFrame::parse(&[]).is_none());
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
        assert!(sample_record(None, Some(json)).matches("orderid"));
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
    async fn falls_back_for_protobuf() {
        let server = MockServer::start().await;
        mock_schema(&server, 3, "PROTOBUF", "syntax = \"proto3\";").await;
        let framed = frame(3, b"\x08\x01");
        assert_eq!(
            decoder(&server.uri()).decode(&framed).await,
            decode_bytes(&framed)
        );
    }

    #[tokio::test]
    async fn decode_field_without_decoder_is_lossy_utf8() {
        assert_eq!(
            decode_field(None, Some(b"hello")).await.as_deref(),
            Some("hello")
        );
        assert!(decode_field(None, None).await.is_none());
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
}
