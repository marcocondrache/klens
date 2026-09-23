use std::collections::HashMap;

use prost_reflect::{DynamicMessage, MessageDescriptor};
use protox::Compiler;
use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};
use schemreg::decode_protobuf_message_indexes;
use serde_json::Value;
use thiserror::Error;

const ROOT_FILE: &str = "schema.proto";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum ProtobufError {
    #[error("{0}")]
    Compile(String),
    #[error("compiled protobuf is missing {ROOT_FILE}")]
    MissingRoot,
    #[error("protobuf message index path is empty")]
    EmptyIndexPath,
    #[error("protobuf message index {index} is out of range in {scope}")]
    IndexOutOfRange { index: u32, scope: &'static str },
    #[error("{0}")]
    Index(String),
    #[error("{0}")]
    Decode(String),
    #[error("{0}")]
    Json(String),
}

#[derive(Clone)]
pub(crate) struct ProtobufCodec {
    messages: Option<Vec<MessageDescriptor>>,
}

struct MemoryResolver {
    files: HashMap<String, String>,
}

impl FileResolver for MemoryResolver {
    fn open_file(&self, name: &str) -> Result<File, protox::Error> {
        match self.files.get(name) {
            Some(source) => File::from_source(name, source),
            None => Err(protox::Error::file_not_found(name)),
        }
    }
}

impl ProtobufCodec {
    pub(crate) fn compile(
        schema: &str,
        references: &[(String, String)],
    ) -> Result<Self, ProtobufError> {
        let mut files = HashMap::new();
        files.insert(ROOT_FILE.to_owned(), schema.to_owned());
        for (name, source) in references {
            files.insert(name.clone(), source.clone());
        }

        let mut resolver = ChainFileResolver::new();
        resolver.add(MemoryResolver { files });
        resolver.add(GoogleFileResolver::new());

        let mut compiler = Compiler::with_file_resolver(resolver);
        compiler
            .include_imports(true)
            .open_file(ROOT_FILE)
            .map_err(|error| ProtobufError::Compile(error.to_string()))?;

        Ok(Self {
            messages: compiler
                .descriptor_pool()
                .get_file_by_name(ROOT_FILE)
                .map(|root| root.messages().collect()),
        })
    }

    pub(crate) fn decode_framed(&self, payload: &[u8]) -> Result<Value, ProtobufError> {
        let (indexes, consumed) = decode_protobuf_message_indexes(payload)
            .map_err(|error| ProtobufError::Index(error.to_string()))?;
        self.decode_message(&indexes, &payload[consumed..])
    }

    pub(crate) fn decode_raw(&self, payload: &[u8]) -> Result<Value, ProtobufError> {
        self.decode_message(&[0], payload)
    }

    fn decode_message(&self, indexes: &[u32], payload: &[u8]) -> Result<Value, ProtobufError> {
        let descriptor = self.message_at(indexes)?;
        let message = DynamicMessage::decode(descriptor, payload)
            .map_err(|error| ProtobufError::Decode(error.to_string()))?;
        serde_json::to_value(&message).map_err(|error| ProtobufError::Json(error.to_string()))
    }

    fn message_at(&self, indexes: &[u32]) -> Result<MessageDescriptor, ProtobufError> {
        let mut indexes = indexes.iter().copied();
        let first = indexes.next().ok_or(ProtobufError::EmptyIndexPath)?;
        let roots = self.messages.as_deref().ok_or(ProtobufError::MissingRoot)?;
        let mut current = nth_message(roots.iter().cloned(), first, "file")?;
        for index in indexes {
            current = nth_message(current.child_messages(), index, "nested message")?;
        }
        Ok(current)
    }
}

fn nth_message(
    mut messages: impl Iterator<Item = MessageDescriptor>,
    index: u32,
    scope: &'static str,
) -> Result<MessageDescriptor, ProtobufError> {
    messages
        .nth(index as usize)
        .ok_or(ProtobufError::IndexOutOfRange { index, scope })
}

#[cfg(test)]
mod tests {
    use super::*;
    use schemreg::encode_protobuf_wire_format;

    /// The message-index prefix on its own: the wire helper always emits a
    /// schema prefix, and `decode_framed` is handed the bytes after it.
    fn indexed(indexes: &[u32], payload: &[u8]) -> Vec<u8> {
        encode_protobuf_wire_format(0u32, indexes, payload)[schemreg::PREFIX_LEN_V0..].to_vec()
    }

    const ORDER: &str = r#"
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

    #[test]
    fn decodes_first_message_to_json() {
        let codec = ProtobufCodec::compile(ORDER, &[]).unwrap();
        let payload = indexed(&[0], b"\x0a\x03abc\x10\x2a");
        let json: serde_json::Value = codec.decode_framed(&payload).unwrap();
        assert_eq!(json["orderId"], "abc");
        assert_eq!(json["amount"], "42");
    }

    #[test]
    fn decodes_later_top_level_message() {
        let codec = ProtobufCodec::compile(ORDER, &[]).unwrap();
        let payload = indexed(&[2], b"\x08\x07");
        let json: serde_json::Value = codec.decode_framed(&payload).unwrap();
        assert_eq!(json["n"], 7);
    }

    #[test]
    fn decodes_nested_message() {
        let codec = ProtobufCodec::compile(ORDER, &[]).unwrap();
        let payload = indexed(&[1, 0], b"\x0a\x03xyz");
        let json: serde_json::Value = codec.decode_framed(&payload).unwrap();
        assert_eq!(json["name"], "xyz");
    }

    #[test]
    fn decodes_raw_payload_as_first_message() {
        let codec = ProtobufCodec::compile(ORDER, &[]).unwrap();
        let json: serde_json::Value = codec.decode_raw(b"\x0a\x03abc\x10\x2a").unwrap();
        assert_eq!(json["orderId"], "abc");
        assert_eq!(json["amount"], "42");
    }

    #[test]
    fn compiles_schema_references() {
        let common = r#"
            syntax = "proto3";
            package common;
            message Status {
                string code = 1;
            }
        "#;
        let root = r#"
            syntax = "proto3";
            import "common.proto";
            message TaggedOrder {
                common.Status status = 1;
            }
        "#;
        let codec =
            ProtobufCodec::compile(root, &[("common.proto".into(), common.into())]).unwrap();
        let payload = indexed(&[0], b"\x0a\x06\x0a\x04OPEN");
        let json: serde_json::Value = codec.decode_framed(&payload).unwrap();
        assert_eq!(json["status"]["code"], "OPEN");
    }

    #[test]
    fn protobuf_errors_display_their_message() {
        assert_eq!(
            ProtobufError::MissingRoot.to_string(),
            "compiled protobuf is missing schema.proto"
        );
        assert_eq!(
            ProtobufError::EmptyIndexPath.to_string(),
            "protobuf message index path is empty"
        );
        assert_eq!(
            ProtobufError::IndexOutOfRange {
                index: 9,
                scope: "nested message"
            }
            .to_string(),
            "protobuf message index 9 is out of range in nested message"
        );
    }

    #[test]
    fn a_truncated_index_path_is_a_typed_error() {
        let codec = ProtobufCodec::compile(ORDER, &[]).unwrap();
        assert!(matches!(
            codec.decode_framed(&[]).unwrap_err(),
            ProtobufError::Index(_)
        ));
    }

    #[test]
    fn empty_index_path_is_a_typed_error() {
        let codec = ProtobufCodec::compile(ORDER, &[]).unwrap();
        assert_eq!(
            codec.decode_message(&[], b"").unwrap_err(),
            ProtobufError::EmptyIndexPath
        );
    }

    #[test]
    fn out_of_range_index_is_a_typed_error() {
        let codec = ProtobufCodec::compile(ORDER, &[]).unwrap();
        assert_eq!(
            codec.decode_message(&[9], b"").unwrap_err(),
            ProtobufError::IndexOutOfRange {
                index: 9,
                scope: "file"
            }
        );
    }
}
