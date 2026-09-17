//! Schema Registry types, catalog port, and payload decode.
//!
//! [`SchemaSubject`] and [`RegisteredSchema`] are the domain types. `client`
//! is the registry port ([`client::SchemaRegistryClient`]), a thin adapter
//! over [`schemreg`]. `decode` turns a Confluent-framed payload into text.
//! `protobuf` is the protobuf path inside decode.

pub mod client;
pub mod decode;
pub mod protobuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType {
    Avro,
    Json,
    Protobuf,
}

impl From<schemreg::SchemaType> for SchemaType {
    /// Schema Registry omits `schemaType` for Avro, so that is the fallback.
    fn from(value: schemreg::SchemaType) -> Self {
        match value {
            schemreg::SchemaType::Json => Self::Json,
            schemreg::SchemaType::Protobuf => Self::Protobuf,
            _ => Self::Avro,
        }
    }
}

impl std::fmt::Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Avro => "AVRO",
            Self::Json => "JSON",
            Self::Protobuf => "PROTOBUF",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaCompatibility {
    Backward,
    Forward,
    Full,
    None,
}

impl From<schemreg::CompatibilityLevel> for SchemaCompatibility {
    /// Transitive variants collapse onto their base mode.
    fn from(value: schemreg::CompatibilityLevel) -> Self {
        use schemreg::CompatibilityLevel as Level;

        match value {
            Level::Forward | Level::ForwardTransitive => Self::Forward,
            Level::Full | Level::FullTransitive => Self::Full,
            Level::None => Self::None,
            _ => Self::Backward,
        }
    }
}

impl std::fmt::Display for SchemaCompatibility {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Backward => "BACKWARD",
            Self::Forward => "FORWARD",
            Self::Full => "FULL",
            Self::None => "NONE",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaSubject {
    pub subject: String,
    pub id: i32,
    pub schema_type: SchemaType,
    pub latest_version: i32,
    pub versions: Vec<i32>,
    pub compatibility: SchemaCompatibility,
    pub schema: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredSchema {
    pub id: i32,
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<SchemaReference>,
}
