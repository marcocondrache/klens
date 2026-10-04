pub mod client;
pub mod decode;
pub mod protobuf;
#[cfg(test)]
pub(crate) mod testing;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaType {
    Avro,
    Json,
    Protobuf,
}

impl From<schemreg::SchemaType> for SchemaType {
    fn from(value: schemreg::SchemaType) -> Self {
        match value {
            schemreg::SchemaType::Avro => Self::Avro,
            schemreg::SchemaType::Json => Self::Json,
            schemreg::SchemaType::Protobuf => Self::Protobuf,
            _ => Self::Avro,
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
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
    None,
}

impl From<schemreg::CompatibilityLevel> for SchemaCompatibility {
    fn from(value: schemreg::CompatibilityLevel) -> Self {
        use schemreg::CompatibilityLevel as Level;

        match value {
            Level::Backward => Self::Backward,
            Level::BackwardTransitive => Self::BackwardTransitive,
            Level::Forward => Self::Forward,
            Level::ForwardTransitive => Self::ForwardTransitive,
            Level::Full => Self::Full,
            Level::FullTransitive => Self::FullTransitive,
            _ => Self::None,
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
    fn every_compatibility_level_survives_the_trip_to_the_registry() {
        for level in [
            Level::Backward,
            Level::BackwardTransitive,
            Level::Forward,
            Level::ForwardTransitive,
            Level::Full,
            Level::FullTransitive,
            Level::None,
        ] {
            assert_eq!(Level::from(SchemaCompatibility::from(level)), level);
        }
    }

    #[test]
    fn every_registry_compatibility_level_keeps_its_name() {
        for level in [
            Level::Backward,
            Level::BackwardTransitive,
            Level::Forward,
            Level::ForwardTransitive,
            Level::Full,
            Level::FullTransitive,
            Level::None,
        ] {
            assert_eq!(
                format!("{:?}", SchemaCompatibility::from(level)),
                format!("{level:?}")
            );
        }
    }
}
