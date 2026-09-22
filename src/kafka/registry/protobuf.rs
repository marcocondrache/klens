use std::borrow::Cow;
use std::collections::HashMap;

use prost_reflect::{DescriptorPool, DynamicMessage, FileDescriptor, MessageDescriptor};
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
    pool: DescriptorPool,
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
        files.insert(
            ROOT_FILE.to_owned(),
            source_for_compiler(schema).into_owned(),
        );
        for (name, source) in references {
            files.insert(name.clone(), source_for_compiler(source).into_owned());
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

/// protox rejects unknown string escapes such as `\.`. Wire, which AKHQ uses
/// through Confluent's protobuf parser, keeps the character after the
/// backslash and continues. Drop that backslash so the same schema text
/// still compiles. Valid escapes are copied through unchanged.
fn source_for_compiler(source: &str) -> Cow<'_, str> {
    let bytes = source.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    let mut changed = false;

    while index < bytes.len() {
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'/') {
            let end = bytes[index..]
                .iter()
                .position(|&byte| byte == b'\n')
                .map_or(bytes.len(), |offset| index + offset + 1);
            out.extend_from_slice(&bytes[index..end]);
            index = end;
            continue;
        }
        if bytes[index] == b'/' && bytes.get(index + 1) == Some(&b'*') {
            let end = bytes[index + 2..]
                .windows(2)
                .position(|pair| pair == b"*/")
                .map_or(bytes.len(), |offset| index + 2 + offset + 2);
            out.extend_from_slice(&bytes[index..end]);
            index = end;
            continue;
        }
        if bytes[index] == b'"' || bytes[index] == b'\'' {
            let quote = bytes[index];
            out.push(quote);
            index += 1;
            while index < bytes.len() {
                if bytes[index] == quote {
                    out.push(quote);
                    index += 1;
                    break;
                }
                if bytes[index] == b'\\' {
                    if let Some(length) = valid_escape_len(&bytes[index..]) {
                        out.extend_from_slice(&bytes[index..index + length]);
                        index += length;
                    } else {
                        changed = true;
                        index += 1;
                    }
                    continue;
                }
                out.push(bytes[index]);
                index += 1;
            }
            continue;
        }
        out.push(bytes[index]);
        index += 1;
    }

    if changed {
        Cow::Owned(String::from_utf8(out).expect("dropping an ascii backslash keeps utf-8"))
    } else {
        Cow::Borrowed(source)
    }
}

/// Length of a protobuf string escape starting at `bytes[0] == b'\\'`, when
/// protox accepts it.
fn valid_escape_len(bytes: &[u8]) -> Option<usize> {
    if bytes.first() != Some(&b'\\') || bytes.len() < 2 {
        return None;
    }
    match bytes[1] {
        b'a' | b'b' | b'f' | b'n' | b'r' | b't' | b'v' | b'?' | b'\\' | b'\'' | b'"' => Some(2),
        b'x' | b'X' => {
            let digits = bytes[2..]
                .iter()
                .take(2)
                .take_while(|byte| byte.is_ascii_hexdigit())
                .count();
            (digits > 0).then_some(2 + digits)
        }
        b'0'..=b'7' => {
            let mut digits = 1;
            while digits < 3
                && bytes
                    .get(1 + digits)
                    .is_some_and(|byte| (b'0'..=b'7').contains(byte))
            {
                digits += 1;
            }
            let value =
                u32::from_str_radix(std::str::from_utf8(&bytes[1..1 + digits]).ok()?, 8).ok()?;
            (value <= 0xff).then_some(1 + digits)
        }
        b'u' => unicode_escape_len(bytes, 4),
        b'U' => unicode_escape_len(bytes, 8),
        _ => None,
    }
}

fn unicode_escape_len(bytes: &[u8], digits: usize) -> Option<usize> {
    let hex = bytes.get(2..2 + digits)?;
    if !hex.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    let value = u32::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?;
    char::from_u32(value).map(|_| 2 + digits)
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
    fn unknown_dot_escape_is_dropped_and_valid_escapes_stay() {
        assert_eq!(
            source_for_compiler(r#""com\.example""#).as_ref(),
            r#""com.example""#
        );
        assert_eq!(
            source_for_compiler(r#"'^[A-Za-z_]*(\.[A-Za-z_]*)*$'"#).as_ref(),
            r#"'^[A-Za-z_]*(.[A-Za-z_]*)*$'"#
        );
        let valid = r#""a\\b\n\t\u0041\x2E\123""#;
        assert_eq!(source_for_compiler(valid).as_ref(), valid);
        assert_eq!(
            source_for_compiler("// \\d in a comment\n").as_ref(),
            "// \\d in a comment\n"
        );
    }

    #[test]
    fn compiles_an_imported_schema_with_an_unknown_dot_escape() {
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
              string order_id = 1;
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
