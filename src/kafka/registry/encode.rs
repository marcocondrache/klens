use apache_avro::Schema as AvroSchema;
use apache_avro::types::Value as AvroValue;
use bytes::Bytes;
use schemreg::{encode_protobuf_wire_format, encode_wire_format};
use serde::de::IgnoredAny;
use thiserror::Error;

use super::protobuf::{ProtobufCodec, ProtobufError};
use super::{RegisteredSchema, SchemaType};
use crate::kafka::error::KafkaError;

#[derive(Debug, Error)]
enum EncodeError {
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Avro(#[from] apache_avro::Error),
    #[error(transparent)]
    Protobuf(#[from] ProtobufError),
}

impl RegisteredSchema {
    /// Writes `json` the way the registry's serializers do, as the first
    /// message of a protobuf schema. `dependencies` holds the schemas this one
    /// references, each after the schemas it references in turn.
    pub(crate) fn encode(
        &self,
        dependencies: &[(String, String)],
        json: &str,
    ) -> Result<Bytes, KafkaError> {
        let id = self.id.cast_unsigned();
        let framed = match self.schema_type {
            SchemaType::Json => serde_json::from_str::<IgnoredAny>(json)
                .map(|_| encode_wire_format(id, json.as_bytes()))
                .map_err(EncodeError::from),
            SchemaType::Avro => {
                avro(&self.schema, dependencies, json).map(|datum| encode_wire_format(id, &datum))
            }
            SchemaType::Protobuf => protobuf(&self.schema, dependencies, json)
                .map(|message| encode_protobuf_wire_format(id, &[0], &message)),
        };
        framed.map_err(|error| KafkaError::Unencodable {
            id: self.id,
            message: error.to_string(),
        })
    }
}

fn avro(
    schema: &str,
    dependencies: &[(String, String)],
    json: &str,
) -> Result<Vec<u8>, EncodeError> {
    let value = AvroValue::from(serde_json::from_str::<serde_json::Value>(json)?);
    let (root, named) =
        AvroSchema::parse_str_with_list(schema, dependencies.iter().map(|(_, body)| body))?;
    // The writer learns names front to back, so a schema follows those it references.
    let schemata: Vec<&AvroSchema> = named.iter().chain([&root]).collect();
    let datum = value.resolve_schemata(&root, schemata.clone())?;
    Ok(apache_avro::to_avro_datum_schemata(&root, schemata, datum)?)
}

fn protobuf(
    schema: &str,
    dependencies: &[(String, String)],
    json: &str,
) -> Result<Vec<u8>, EncodeError> {
    Ok(ProtobufCodec::compile(schema, dependencies)?.encode_first(json)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORDER: &str = r#"{
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "orderId", "type": "string"},
            {"name": "amount", "type": "long"},
            {"name": "note", "type": ["null", "string"], "default": null}
        ]
    }"#;

    const STATUS: &str = r#"{"type": "enum", "name": "Status", "symbols": ["OPEN", "CLOSED"]}"#;

    const LINE: &str = r#"{
        "type": "record",
        "name": "Line",
        "fields": [{"name": "status", "type": "Status"}]
    }"#;

    const SHIPMENT: &str = r#"{
        "type": "record",
        "name": "Shipment",
        "fields": [
            {"name": "line", "type": "Line"},
            {"name": "status", "type": "Status"}
        ]
    }"#;

    const ORDER_PROTO: &str = r#"
        syntax = "proto3";
        message Order {
            string order_id = 1;
            int64 amount = 2;
        }
        message Count {
            int32 n = 1;
        }
    "#;

    fn schema(schema_type: SchemaType, text: &str) -> RegisteredSchema {
        RegisteredSchema {
            id: 7,
            schema_type,
            schema: text.to_owned(),
            references: Vec::new(),
        }
    }

    fn framed(body: &[u8]) -> Vec<u8> {
        [b"\0\0\0\0\x07".as_slice(), body].concat()
    }

    #[track_caller]
    fn unencodable(error: KafkaError) -> String {
        match error {
            KafkaError::Unencodable { id: 7, message } => message,
            error => panic!("expected an unencodable payload, got {error:?}"),
        }
    }

    #[test]
    fn a_json_schema_frames_the_document_as_written() {
        let json = r#"{"b": 1, "a": [true]}"#;

        let encoded = schema(SchemaType::Json, "{}").encode(&[], json).unwrap();

        assert_eq!(encoded, framed(json.as_bytes()));
    }

    #[test]
    fn a_json_schema_takes_only_json() {
        let error = schema(SchemaType::Json, "{}")
            .encode(&[], "{oops")
            .unwrap_err();

        assert!(unencodable(error).contains("line 1 column 2"));
    }

    #[test]
    fn an_avro_record_takes_its_fields_from_json() {
        let encoded = schema(SchemaType::Avro, ORDER)
            .encode(&[], r#"{"orderId": "o-1", "amount": 42, "note": "rush"}"#)
            .unwrap();

        assert_eq!(encoded, framed(b"\x06o-1\x54\x02\x08rush"));
    }

    #[test]
    fn an_avro_record_fills_a_missing_field_from_its_default() {
        let encoded = schema(SchemaType::Avro, ORDER)
            .encode(&[], r#"{"orderId": "o-1", "amount": 42}"#)
            .unwrap();

        assert_eq!(encoded, framed(b"\x06o-1\x54\x00"));
    }

    #[test]
    fn an_avro_primitive_schema_takes_a_bare_value() {
        let encoded = schema(SchemaType::Avro, r#""string""#)
            .encode(&[], r#""abc""#)
            .unwrap();

        assert_eq!(encoded, framed(b"\x06abc"));
    }

    #[test]
    fn an_avro_record_refuses_a_field_of_the_wrong_type() {
        let error = schema(SchemaType::Avro, ORDER)
            .encode(&[], r#"{"orderId": 1, "amount": 42}"#)
            .unwrap_err();

        assert!(unencodable(error).contains("got: Int(1)"));
    }

    #[test]
    fn an_avro_schema_resolves_names_its_dependencies_define() {
        let dependencies = [
            ("Status".to_owned(), STATUS.to_owned()),
            ("Line".to_owned(), LINE.to_owned()),
        ];

        let encoded = schema(SchemaType::Avro, SHIPMENT)
            .encode(
                &dependencies,
                r#"{"line": {"status": "CLOSED"}, "status": "OPEN"}"#,
            )
            .unwrap();

        assert_eq!(encoded, framed(b"\x02\x00"));
    }

    #[test]
    fn a_protobuf_schema_writes_its_first_message() {
        let encoded = schema(SchemaType::Protobuf, ORDER_PROTO)
            .encode(&[], r#"{"orderId": "abc", "amount": "42"}"#)
            .unwrap();

        assert_eq!(encoded, framed(b"\x00\x0a\x03abc\x10\x2a"));
    }

    #[test]
    fn a_protobuf_message_refuses_a_field_it_does_not_have() {
        let error = schema(SchemaType::Protobuf, ORDER_PROTO)
            .encode(&[], r#"{"n": 1}"#)
            .unwrap_err();

        assert!(unencodable(error).contains("unrecognized field name 'n'"));
    }

    #[test]
    fn a_protobuf_schema_that_does_not_compile_is_unencodable() {
        let error = schema(SchemaType::Protobuf, "message {")
            .encode(&[], "{}")
            .unwrap_err();

        unencodable(error);
    }
}
