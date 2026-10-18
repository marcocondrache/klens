mod avro;
pub mod client;
pub mod decode;
mod encode;
pub mod protobuf;
#[cfg(test)]
pub(crate) mod testing;

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum SchemaType {
    Avro,
    Json,
    Protobuf,
}

#[derive(Debug, Error)]
#[error("klens does not know the schema type {0}")]
pub struct UnknownSchemaType(schemreg::SchemaType);

impl TryFrom<schemreg::SchemaType> for SchemaType {
    type Error = UnknownSchemaType;

    fn try_from(value: schemreg::SchemaType) -> Result<Self, Self::Error> {
        match value {
            schemreg::SchemaType::Avro => Ok(Self::Avro),
            schemreg::SchemaType::Json => Ok(Self::Json),
            schemreg::SchemaType::Protobuf => Ok(Self::Protobuf),
            unknown => Err(UnknownSchemaType(unknown)),
        }
    }
}

impl From<SchemaType> for schemreg::SchemaType {
    fn from(value: SchemaType) -> Self {
        match value {
            SchemaType::Avro => Self::Avro,
            SchemaType::Json => Self::Json,
            SchemaType::Protobuf => Self::Protobuf,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaCompatibility {
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
    None,
}

#[derive(Debug, Error)]
#[error("klens does not know the compatibility level {0}")]
pub struct UnknownCompatibility(schemreg::CompatibilityLevel);

impl TryFrom<schemreg::CompatibilityLevel> for SchemaCompatibility {
    type Error = UnknownCompatibility;

    fn try_from(value: schemreg::CompatibilityLevel) -> Result<Self, Self::Error> {
        use schemreg::CompatibilityLevel as Level;

        match value {
            Level::Backward => Ok(Self::Backward),
            Level::BackwardTransitive => Ok(Self::BackwardTransitive),
            Level::Forward => Ok(Self::Forward),
            Level::ForwardTransitive => Ok(Self::ForwardTransitive),
            Level::Full => Ok(Self::Full),
            Level::FullTransitive => Ok(Self::FullTransitive),
            Level::None => Ok(Self::None),
            unknown => Err(UnknownCompatibility(unknown)),
        }
    }
}

impl From<SchemaCompatibility> for schemreg::CompatibilityLevel {
    fn from(value: SchemaCompatibility) -> Self {
        match value {
            SchemaCompatibility::Backward => Self::Backward,
            SchemaCompatibility::BackwardTransitive => Self::BackwardTransitive,
            SchemaCompatibility::Forward => Self::Forward,
            SchemaCompatibility::ForwardTransitive => Self::ForwardTransitive,
            SchemaCompatibility::Full => Self::Full,
            SchemaCompatibility::FullTransitive => Self::FullTransitive,
            SchemaCompatibility::None => Self::None,
        }
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

/// A schema for `subject`. The registry stores it as the subject's next
/// version unless the subject already holds the same schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSchema {
    pub subject: String,
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<SchemaReference>,
}

/// Deletes one version of `subject`, or every version without one. A soft
/// delete hides it from reads and keeps its ids resolvable for records that
/// carry them. A permanent delete soft-deletes first, as the registry asks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDeletion {
    pub subject: String,
    pub version: Option<i32>,
    pub permanent: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegisteredVersion {
    pub id: i32,
    pub version: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredSchema {
    pub id: i32,
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<SchemaReference>,
}

#[cfg(test)]
mod tests {
    use schemreg::CompatibilityLevel as Level;

    use super::*;

    #[test]
    fn every_compatibility_level_keeps_its_name_through_the_trip_to_the_registry() {
        for level in [
            Level::Backward,
            Level::BackwardTransitive,
            Level::Forward,
            Level::ForwardTransitive,
            Level::Full,
            Level::FullTransitive,
            Level::None,
        ] {
            let ours = SchemaCompatibility::try_from(level).unwrap();

            assert_eq!(Level::from(ours), level);
            assert_eq!(format!("{ours:?}"), format!("{level:?}"));
        }
    }
}
