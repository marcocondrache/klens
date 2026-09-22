use std::collections::HashMap;
use std::sync::OnceLock;

use prost_reflect::{DescriptorPool, DynamicMessage, FileDescriptor, MessageDescriptor};
use protox::Compiler;
use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};
use schemreg::decode_protobuf_message_indexes;
use serde_json::Value;
use thiserror::Error;

const ROOT_FILE: &str = "schema.proto";

/// Import path of Buf's protovalidate schema. Some registries store a copy
/// whose CEL regexes contain `\.`, which protox reports as `invalid string
/// escape`. [`BUNDLED_VALIDATE_SOURCE`] is protovalidate 1.2.2, with those
/// regexes written as `\\.`.
const BUNDLED_VALIDATE: &str = "buf/validate/validate.proto";

const BUNDLED_VALIDATE_SOURCE: &str = include_str!("validate.proto");

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
    pool: DescriptorPool,
}

struct MemoryResolver {
    files: HashMap<String, String>,
}

impl FileResolver for MemoryResolver {
    fn open_file(&self, name: &str) -> Result<File, protox::Error> {
        let Some(source) = self.files.get(name) else {
            return Err(protox::Error::file_not_found(name));
        };
        match File::from_source(name, source) {
            Ok(file) => Ok(file),
            Err(error) if use_bundled_validate(name, &error) => {
                tracing::debug!(
                    file = name,
                    %error,
                    "registry protobuf import has an unknown string escape; compiling bundled protovalidate"
                );
                Ok(bundled_validate())
            }
            Err(error) => Err(error),
        }
    }
}

fn use_bundled_validate(name: &str, error: &protox::Error) -> bool {
    name == BUNDLED_VALIDATE && error.to_string() == "invalid string escape"
}

fn bundled_validate() -> File {
    static PARSED: OnceLock<File> = OnceLock::new();
    PARSED
        .get_or_init(|| {
            File::from_source(BUNDLED_VALIDATE, BUNDLED_VALIDATE_SOURCE).unwrap_or_else(|error| {
                panic!("bundled protovalidate 1.2.2 failed to parse: {error}")
            })
        })
        .clone()
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
            pool: compiler.descriptor_pool(),
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

    fn root_file(&self) -> Result<FileDescriptor, ProtobufError> {
        self.pool
            .get_file_by_name(ROOT_FILE)
            .ok_or(ProtobufError::MissingRoot)
    }

    fn message_at(&self, indexes: &[u32]) -> Result<MessageDescriptor, ProtobufError> {
        let mut indexes = indexes.iter().copied();
        let first = indexes.next().ok_or(ProtobufError::EmptyIndexPath)?;
        let mut current = nth_message(self.root_file()?.messages(), first, "file")?;
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
    fn unknown_string_escape_in_a_user_schema_stays_an_error() {
        let source = r#"
            syntax = "proto2";
            message Order {
              optional string order_id = 1 [default = "(\.[A-Za-z_]"];
            }
        "#;
        let Err(error) = ProtobufCodec::compile(source, &[]) else {
            panic!("schema with an unknown string escape compiled");
        };
        assert_eq!(error.to_string(), "invalid string escape");
    }

    #[test]
    fn a_parseable_registry_copy_of_protovalidate_is_kept() {
        let validate = r#"
            syntax = "proto3";
            package buf.validate;
            message OnlyInRegistry {
              string code = 1;
            }
        "#;
        let root = r#"
            syntax = "proto3";
            import "buf/validate/validate.proto";
            message Order {
              buf.validate.OnlyInRegistry status = 1;
            }
        "#;
        let codec = ProtobufCodec::compile(
            root,
            &[("buf/validate/validate.proto".into(), validate.into())],
        )
        .unwrap();
        let json = codec.decode_raw(b"\x0a\x06\x0a\x04OPEN").unwrap();
        assert_eq!(json["status"]["code"], "OPEN");
    }

    #[test]
    fn unreadable_protovalidate_import_uses_the_bundled_schema() {
        let validate = r#"
            syntax = "proto2";
            package buf.validate;
            message Marker {
              optional string name = 1 [default = "(\.[A-Za-z_]"];
            }
        "#;
        let root = r#"
            syntax = "proto3";
            import "buf/validate/validate.proto";
            message Order {
              string order_id = 1 [(buf.validate.field).string.min_len = 1];
              int64 amount = 2;
            }
        "#;
        let codec = ProtobufCodec::compile(
            root,
            &[("buf/validate/validate.proto".into(), validate.into())],
        )
        .unwrap();
        let json = codec.decode_raw(b"\x0a\x03abc\x10\x2a").unwrap();
        assert_eq!(json["orderId"], "abc");
        assert_eq!(json["amount"], "42");
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
