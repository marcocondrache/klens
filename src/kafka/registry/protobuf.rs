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
            normalize_proto_escapes(schema).into_owned(),
        );
        for (name, source) in references {
            files.insert(name.clone(), normalize_proto_escapes(source).into_owned());
        }

        let mut resolver = ChainFileResolver::new();
        resolver.add(MemoryResolver { files });
        resolver.add(GoogleFileResolver::new());

        let mut compiler = Compiler::with_file_resolver(resolver);
        compiler
            .include_imports(true)
            .open_file(ROOT_FILE)
            .map_err(|error| ProtobufError::Compile(format!("{error:?}")))?;

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

/// `protoc` rejects unknown string escapes. Schema registries still store
/// schemas that contain them, usually a validation pattern written with a
/// single backslash (`\s`, `\d`, `\.`). Doubling that backslash keeps the
/// pattern the author typed and lets the file compile.
///
/// UTF-16 surrogate pairs (`\uD83D\uDE00`) are rewritten to a single `\U`
/// escape, which is how `protoc` decodes them.
fn normalize_proto_escapes(source: &str) -> Cow<'_, str> {
    let bytes = source.as_bytes();
    let mut rewriter = ProtoRewriter::new(source);
    let mut index = 0;
    while index < bytes.len() {
        if let Some(end) = comment_end(bytes, index) {
            index = end;
            continue;
        }
        if bytes[index] == b'"' || bytes[index] == b'\'' {
            index = rewriter.normalize_string(bytes, index);
            continue;
        }
        index += 1;
    }
    rewriter.finish()
}

struct ProtoRewriter<'a> {
    source: &'a str,
    out: Option<String>,
    copied: usize,
}

impl<'a> ProtoRewriter<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            source,
            out: None,
            copied: 0,
        }
    }

    fn replace(&mut self, start: usize, end: usize, replacement: &str) {
        let out = self
            .out
            .get_or_insert_with(|| String::with_capacity(self.source.len() + replacement.len()));
        out.push_str(&self.source[self.copied..start]);
        out.push_str(replacement);
        self.copied = end;
    }

    fn normalize_string(&mut self, bytes: &[u8], start: usize) -> usize {
        let delimiter = bytes[start];
        let mut index = start + 1;
        while index < bytes.len() {
            match bytes[index] {
                b'\n' => return index + 1,
                ch if ch == delimiter => return index + 1,
                b'\\' => match classify_escape(bytes, index) {
                    Escape::Keep(end) => index = end,
                    Escape::Surrogate { end, code } => {
                        self.replace(index, end, &format!("\\U{code:08X}"));
                        index = end;
                    }
                    Escape::Invalid => {
                        self.replace(index, index + 1, "\\\\");
                        index += 1;
                    }
                },
                _ => index += 1,
            }
        }
        index
    }

    fn finish(self) -> Cow<'a, str> {
        match self.out {
            None => Cow::Borrowed(self.source),
            Some(mut out) => {
                out.push_str(&self.source[self.copied..]);
                Cow::Owned(out)
            }
        }
    }
}

enum Escape {
    Keep(usize),
    Surrogate { end: usize, code: u32 },
    Invalid,
}

fn classify_escape(bytes: &[u8], index: usize) -> Escape {
    let Some(next) = bytes.get(index + 1).copied() else {
        return Escape::Invalid;
    };
    if is_simple_escape(next) {
        return Escape::Keep(index + 2);
    }
    if next.is_ascii_digit() && next < b'8' {
        return classify_octal(bytes, index);
    }
    match next {
        b'x' | b'X' => classify_hex(bytes, index),
        b'u' | b'U' => classify_unicode(bytes, index),
        _ => Escape::Invalid,
    }
}

fn is_simple_escape(byte: u8) -> bool {
    matches!(
        byte,
        b'a' | b'b' | b'f' | b'n' | b'r' | b't' | b'v' | b'\\' | b'?' | b'\'' | b'"'
    )
}

fn classify_octal(bytes: &[u8], index: usize) -> Escape {
    let mut end = index + 1;
    let mut value: u32 = 0;
    let mut digits = 0;
    while digits < 3 && end < bytes.len() && bytes[end].is_ascii_digit() && bytes[end] < b'8' {
        value = value * 8 + u32::from(bytes[end] - b'0');
        end += 1;
        digits += 1;
    }
    if value > u32::from(u8::MAX) {
        Escape::Invalid
    } else {
        Escape::Keep(end)
    }
}

fn classify_hex(bytes: &[u8], index: usize) -> Escape {
    let mut end = index + 2;
    let mut digits = 0;
    while digits < 2 && end < bytes.len() && bytes[end].is_ascii_hexdigit() {
        end += 1;
        digits += 1;
    }
    if digits == 0 {
        Escape::Invalid
    } else {
        Escape::Keep(end)
    }
}

fn classify_unicode(bytes: &[u8], index: usize) -> Escape {
    let marker = bytes[index + 1];
    let width = if marker == b'u' { 4 } else { 8 };
    let Some(value) = hex_value(bytes, index + 2, width) else {
        return Escape::Invalid;
    };
    let end = index + 2 + width;
    if marker == b'u'
        && is_head_surrogate(value)
        && let Some(trail) = hex_value(bytes, end + 2, 4)
        && bytes.get(end) == Some(&b'\\')
        && bytes.get(end + 1) == Some(&b'u')
        && is_trail_surrogate(trail)
    {
        return Escape::Surrogate {
            end: end + 6,
            code: 0x1_0000 + (((value - 0xD800) << 10) | (trail - 0xDC00)),
        };
    }
    if char::from_u32(value).is_some() {
        Escape::Keep(end)
    } else {
        Escape::Invalid
    }
}

fn hex_value(bytes: &[u8], start: usize, width: usize) -> Option<u32> {
    let end = start.checked_add(width)?;
    let digits = bytes.get(start..end)?;
    if !digits.iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    u32::from_str_radix(std::str::from_utf8(digits).ok()?, 16).ok()
}

fn is_head_surrogate(value: u32) -> bool {
    (0xD800..0xDC00).contains(&value)
}

fn is_trail_surrogate(value: u32) -> bool {
    (0xDC00..0xE000).contains(&value)
}

fn comment_end(bytes: &[u8], index: usize) -> Option<usize> {
    match bytes.get(index..index + 2)? {
        b"//" => Some(
            bytes
                .iter()
                .skip(index + 2)
                .position(|byte| *byte == b'\n')
                .map_or(bytes.len(), |offset| index + 2 + offset + 1),
        ),
        b"/*" => Some(
            bytes
                .windows(2)
                .skip(index + 2)
                .position(|pair| pair == b"*/")
                .map_or(bytes.len(), |offset| index + 2 + offset + 2),
        ),
        _ => None,
    }
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
    fn leaves_doubled_backslashes_in_patterns_unchanged() {
        let source = r#""'^:?[0-9a-zA-Z!#$%&\\'*+.^_|~\\x60-]+$' :""#;
        assert!(matches!(
            normalize_proto_escapes(source),
            std::borrow::Cow::Borrowed(_)
        ));
        let source = r#"'^[^\\u0000-\\u0008\\u000A-\\u001F\\u007F]*$'"#;
        assert!(matches!(
            normalize_proto_escapes(source),
            std::borrow::Cow::Borrowed(_)
        ));
    }

    #[test]
    fn leaves_valid_proto_escapes_unchanged() {
        let source = "syntax = \"proto2\";\n// pattern \\s stays a comment\n/* block \\d and \"\\w\" */\nmessage Item {\n  optional string name = 1 [default = \"a\\nb\\t\\\\\\'\\\"\\x41\\u0042\\123\\a\\b\\f\\r\\v\\?\"];\n}\n";
        assert!(matches!(
            normalize_proto_escapes(source),
            std::borrow::Cow::Borrowed(_)
        ));
        let codec = ProtobufCodec::compile(source, &[]).unwrap();
        let field = codec
            .pool
            .get_message_by_name("Item")
            .unwrap()
            .get_field_by_name("name")
            .unwrap();
        assert_eq!(
            field.default_value(),
            prost_reflect::Value::String("a\nb\t\\'\"A\u{42}S\u{7}\u{8}\u{c}\r\u{b}?".to_owned())
        );
    }

    #[test]
    fn repairs_octal_overflow_and_short_hex_escapes() {
        let source = r#"
            syntax = "proto2";
            message Item {
                optional string overflow = 1 [default = "\400"];
                optional string short_hex = 2 [default = "\x"];
                optional string one_hex = 3 [default = "\x4"];
                optional string lone_surrogate = 4 [default = "\uD800"];
            }
        "#;
        let codec = ProtobufCodec::compile(source, &[]).unwrap();
        let message = codec.pool.get_message_by_name("Item").unwrap();
        let value = |name: &str| message.get_field_by_name(name).unwrap().default_value();
        assert_eq!(
            value("overflow"),
            prost_reflect::Value::String(r"\400".into())
        );
        assert_eq!(
            value("short_hex"),
            prost_reflect::Value::String(r"\x".into())
        );
        assert_eq!(
            value("one_hex"),
            prost_reflect::Value::String("\u{4}".into())
        );
        assert_eq!(
            value("lone_surrogate"),
            prost_reflect::Value::String(r"\uD800".into())
        );
    }

    #[test]
    fn compiles_unknown_string_escapes_as_literal_backslashes() {
        let source = r#"
            syntax = "proto2";
            message Item {
                optional string pattern = 1 [default = "\s+\d+"];
            }
        "#;
        let codec = ProtobufCodec::compile(source, &[]).unwrap();
        let field = codec
            .pool
            .get_message_by_name("Item")
            .unwrap()
            .get_field_by_name("pattern")
            .unwrap();
        assert_eq!(
            field.default_value(),
            prost_reflect::Value::String(r"\s+\d+".to_owned())
        );
    }

    #[test]
    fn compiles_utf16_surrogate_pair_escapes() {
        let source = r#"
            syntax = "proto2";
            message Item {
                optional string name = 1 [default = "\uD83D\uDE00"];
            }
        "#;
        let codec = ProtobufCodec::compile(source, &[]).unwrap();
        let field = codec
            .pool
            .get_message_by_name("Item")
            .unwrap()
            .get_field_by_name("name")
            .unwrap();
        assert_eq!(
            field.default_value(),
            prost_reflect::Value::String("😀".to_owned())
        );
    }

    #[test]
    fn decodes_buf_validate_schema_with_pattern_escapes() {
        let validate = r#"
            syntax = "proto2";
            package buf.validate;
            import "google/protobuf/descriptor.proto";
            extend google.protobuf.FieldOptions {
                optional FieldRules field = 1159;
            }
            message FieldRules {
                optional StringRules string = 1;
                optional RepeatedRules repeated = 2;
            }
            message StringRules {
                optional bool uuid = 1;
                optional string pattern = 2;
            }
            message RepeatedRules {
                optional uint64 min_items = 1;
            }
        "#;
        let lifecycle = r#"
            syntax = "proto3";
            package pha.messages;
            import "google/protobuf/struct.proto";
            import "buf/validate/validate.proto";
            message DataItem {
                google.protobuf.Struct payload = 1;
                string note = 2 [(buf.validate.field).string.pattern = "\s+"];
            }
        "#;
        let root = r#"
            syntax = "proto3";
            package pha.messages;
            import "google/protobuf/struct.proto";
            import "buf/validate/validate.proto";
            import "lifecycle.1.proto";
            message DataMessage {
                repeated DataMetric metrics = 1 [(buf.validate.field).repeated.min_items = 1];
            }
            message DataMetric {
                string id = 1 [(buf.validate.field).string.uuid = true];
                string name = 2;
                optional string norm_name = 3;
                optional string uom = 4;
                pha.messages.DataItem data = 5;
            }
        "#;
        let codec = ProtobufCodec::compile(
            root,
            &[
                ("buf/validate/validate.proto".into(), validate.into()),
                ("lifecycle.1.proto".into(), lifecycle.into()),
            ],
        )
        .unwrap();
        let payload = indexed(&[0], b"\x0a\x05\x0a\x03abc");
        let json = codec.decode_framed(&payload).unwrap();
        assert_eq!(json["metrics"][0]["id"], "abc");
    }

    #[test]
    fn compile_errors_include_the_source_location() {
        let Err(error) = ProtobufCodec::compile("syntax = \"proto3\";\nmessage {", &[]) else {
            panic!("expected a compile error");
        };
        assert!(error.to_string().contains("schema.proto:"), "{error}");
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
