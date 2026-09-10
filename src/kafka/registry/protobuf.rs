use std::collections::HashMap;

use prost_reflect::{DescriptorPool, DynamicMessage, FileDescriptor, MessageDescriptor};
use protox::Compiler;
use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};

const ROOT_FILE: &str = "schema.proto";

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
    pub(crate) fn compile(schema: &str, references: &[(String, String)]) -> Result<Self, String> {
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
            .map_err(|error| error.to_string())?;

        Ok(Self {
            pool: compiler.descriptor_pool(),
        })
    }

    pub(crate) fn decode_framed(&self, payload: &[u8]) -> Result<String, String> {
        let (indexes, rest) = parse_indexes(payload)?;
        self.decode_message(&indexes, rest)
    }

    pub(crate) fn decode_raw(&self, payload: &[u8]) -> Result<String, String> {
        self.decode_message(&[0], payload)
    }

    fn decode_message(&self, indexes: &[i32], payload: &[u8]) -> Result<String, String> {
        let descriptor = self.message_at(indexes)?;
        let message =
            DynamicMessage::decode(descriptor, payload).map_err(|error| error.to_string())?;
        serde_json::to_string(&message).map_err(|error| error.to_string())
    }

    fn root_file(&self) -> Result<FileDescriptor, String> {
        self.pool
            .get_file_by_name(ROOT_FILE)
            .ok_or_else(|| format!("compiled protobuf is missing {ROOT_FILE}"))
    }

    fn message_at(&self, indexes: &[i32]) -> Result<MessageDescriptor, String> {
        let mut indexes = indexes.iter().copied();
        let first = indexes
            .next()
            .ok_or_else(|| "protobuf message index path is empty".to_owned())?;
        let mut current = nth_message(self.root_file()?.messages(), first, "file")?;
        for index in indexes {
            current = nth_message(current.child_messages(), index, "nested message")?;
        }
        Ok(current)
    }
}

fn nth_message(
    mut messages: impl Iterator<Item = MessageDescriptor>,
    index: i32,
    scope: &str,
) -> Result<MessageDescriptor, String> {
    let index = usize::try_from(index)
        .map_err(|_| format!("protobuf message index {index} is invalid in {scope}"))?;
    messages
        .nth(index)
        .ok_or_else(|| format!("protobuf message index {index} is out of range in {scope}"))
}

fn parse_indexes(payload: &[u8]) -> Result<(Vec<i32>, &[u8]), String> {
    let mut rest = payload;
    let count = read_zigzag_varint(&mut rest)?;
    if count == 0 {
        return Ok((vec![0], rest));
    }
    let count = usize::try_from(count)
        .map_err(|_| format!("protobuf message index count {count} is invalid"))?;
    let mut indexes = Vec::with_capacity(count);
    for _ in 0..count {
        indexes.push(read_zigzag_varint(&mut rest)?);
    }
    Ok((indexes, rest))
}

fn read_zigzag_varint(buf: &mut &[u8]) -> Result<i32, String> {
    let mut value: u32 = 0;
    let mut shift = 0;
    loop {
        let Some((&byte, rest)) = buf.split_first() else {
            return Err("truncated protobuf message index".into());
        };
        *buf = rest;
        if shift >= 32 {
            return Err("protobuf message index varint is too long".into());
        }
        value |= u32::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(((value >> 1) as i32) ^ -((value & 1) as i32));
        }
        shift += 7;
    }
}

#[cfg(test)]
pub(crate) fn encode_indexes(indexes: &[i32]) -> Vec<u8> {
    if indexes == [0] {
        return vec![0];
    }
    let mut out = Vec::new();
    write_zigzag_varint(&mut out, i32::try_from(indexes.len()).expect("index count"));
    for index in indexes {
        write_zigzag_varint(&mut out, *index);
    }
    out
}

#[cfg(test)]
fn write_zigzag_varint(out: &mut Vec<u8>, value: i32) {
    let mut encoded = ((value as u32) << 1) ^ ((value >> 31) as u32);
    while encoded > 0x7f {
        out.push((encoded as u8 & 0x7f) | 0x80);
        encoded >>= 7;
    }
    out.push(encoded as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn parse_indexes_optimizes_first_message() {
        let payload = [0x00, 0x08, 0x01];
        let (indexes, rest) = parse_indexes(&payload).unwrap();
        assert_eq!(indexes, vec![0]);
        assert_eq!(rest, [0x08, 0x01]);
    }

    #[test]
    fn parse_indexes_reads_nested_path() {
        let mut payload = encode_indexes(&[1, 0]);
        payload.extend_from_slice(&[0x0a, 0x01, b'x']);
        let (indexes, rest) = parse_indexes(&payload).unwrap();
        assert_eq!(indexes, vec![1, 0]);
        assert_eq!(rest, [0x0a, 0x01, b'x']);
    }

    #[test]
    fn decodes_first_message_to_json() {
        let codec = ProtobufCodec::compile(ORDER, &[]).unwrap();
        let mut payload = encode_indexes(&[0]);
        payload.extend_from_slice(b"\x0a\x03abc\x10\x2a");
        let json: serde_json::Value =
            serde_json::from_str(&codec.decode_framed(&payload).unwrap()).unwrap();
        assert_eq!(json["orderId"], "abc");
        assert_eq!(json["amount"], "42");
    }

    #[test]
    fn decodes_later_top_level_message() {
        let codec = ProtobufCodec::compile(ORDER, &[]).unwrap();
        let mut payload = encode_indexes(&[2]);
        payload.extend_from_slice(b"\x08\x07");
        let json: serde_json::Value =
            serde_json::from_str(&codec.decode_framed(&payload).unwrap()).unwrap();
        assert_eq!(json["n"], 7);
    }

    #[test]
    fn decodes_nested_message() {
        let codec = ProtobufCodec::compile(ORDER, &[]).unwrap();
        let mut payload = encode_indexes(&[1, 0]);
        payload.extend_from_slice(b"\x0a\x03xyz");
        let json: serde_json::Value =
            serde_json::from_str(&codec.decode_framed(&payload).unwrap()).unwrap();
        assert_eq!(json["name"], "xyz");
    }

    #[test]
    fn decodes_raw_payload_as_first_message() {
        let codec = ProtobufCodec::compile(ORDER, &[]).unwrap();
        let json: serde_json::Value =
            serde_json::from_str(&codec.decode_raw(b"\x0a\x03abc\x10\x2a").unwrap()).unwrap();
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
        let mut payload = encode_indexes(&[0]);
        payload.extend_from_slice(b"\x0a\x06\x0a\x04OPEN");
        let json: serde_json::Value =
            serde_json::from_str(&codec.decode_framed(&payload).unwrap()).unwrap();
        assert_eq!(json["status"]["code"], "OPEN");
    }
}
