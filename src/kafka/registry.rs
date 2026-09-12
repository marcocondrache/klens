//! Schema Registry types, HTTP client, and payload decode.
//!
//! [`SchemaSubject`] and [`RegisteredSchema`] are the domain types. `client`
//! is the HTTP port ([`client::SchemaRegistryClient`]). `decode` turns a
//! Confluent-framed payload into text. `protobuf` is the protobuf path
//! inside decode.

pub mod client;
pub mod decode;
pub mod protobuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType {
    Avro,
    Json,
    Protobuf,
}

impl SchemaType {
    /// Schema Registry omits `schemaType` for Avro, so that is the fallback.
    pub fn from_registry(value: Option<&str>) -> Self {
        match value
            .map(|value| value.trim().to_ascii_uppercase())
            .as_deref()
        {
            Some("JSON") | Some("JSONSCHEMA") => Self::Json,
            Some("PROTOBUF") => Self::Protobuf,
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

impl SchemaCompatibility {
    /// Transitive variants collapse onto their base mode.
    pub fn from_registry(value: Option<&str>) -> Self {
        match value
            .map(|value| value.trim().to_ascii_uppercase().replace('-', "_"))
            .as_deref()
        {
            Some("FORWARD") | Some("FORWARD_TRANSITIVE") => Self::Forward,
            Some("FULL") | Some("FULL_TRANSITIVE") => Self::Full,
            Some("NONE") => Self::None,
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
