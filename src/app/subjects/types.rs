use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::kafka::model as domain;
use crate::kafka::store::projections;
use crate::r#macro::from_same_variants;

use super::super::clusters::LaneHealth;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SchemaType {
    Avro,
    Json,
    Protobuf,
}

from_same_variants!(domain::SchemaType => SchemaType { Avro, Json, Protobuf });
from_same_variants!(SchemaType => domain::SchemaType { Avro, Json, Protobuf });

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SchemaCompatibility {
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
    None,
}

from_same_variants!(domain::SchemaCompatibility => SchemaCompatibility {
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
    None,
});

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SubjectRow {
    pub subject: String,
    pub id: i32,
    #[serde(rename = "type")]
    pub schema_type: SchemaType,
    pub latest_version: i32,
    pub versions: Vec<i32>,
    pub compatibility: SchemaCompatibility,
}

impl From<projections::SubjectRow> for SubjectRow {
    fn from(row: projections::SubjectRow) -> Self {
        Self {
            subject: row.subject.to_string(),
            id: row.info.id,
            schema_type: row.info.schema_type.into(),
            latest_version: row.info.latest_version,
            versions: row.info.versions,
            compatibility: row.info.compatibility.into(),
        }
    }
}

/// An empty `rows` with an unhealthy `sourceHealth` is a registry outage, not
/// a registry with no subjects.
#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SubjectRowsResult {
    pub rows: Vec<SubjectRow>,
    pub source_health: LaneHealth,
    pub has_registry: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

impl From<domain::SchemaReference> for SchemaReference {
    fn from(reference: domain::SchemaReference) -> Self {
        Self {
            name: reference.name,
            subject: reference.subject,
            version: reference.version,
        }
    }
}

impl From<SchemaReference> for domain::SchemaReference {
    fn from(reference: SchemaReference) -> Self {
        Self {
            name: reference.name,
            subject: reference.subject,
            version: reference.version,
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SubjectDetail {
    pub subject: String,
    pub version: i32,
    pub id: i32,
    #[serde(rename = "type")]
    pub schema_type: SchemaType,
    pub schema: String,
    pub references: Vec<SchemaReference>,
}

impl SubjectDetail {
    pub(crate) fn new(subject: String, version: i32, schema: domain::RegisteredSchema) -> Self {
        Self {
            subject,
            version,
            id: schema.id,
            schema_type: schema.schema_type.into(),
            schema: schema.schema,
            references: schema.references.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RegisterSchema {
    #[serde(rename = "type")]
    pub schema_type: SchemaType,
    pub schema: String,
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

impl RegisterSchema {
    pub(crate) fn into_schema(self, subject: String) -> domain::NewSchema {
        domain::NewSchema {
            subject,
            schema_type: self.schema_type.into(),
            schema: self.schema,
            references: self.references.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RegisteredVersion {
    pub id: i32,
    pub version: i32,
}

impl From<domain::RegisteredVersion> for RegisteredVersion {
    fn from(registered: domain::RegisteredVersion) -> Self {
        Self {
            id: registered.id,
            version: registered.version,
        }
    }
}
