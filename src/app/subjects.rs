use axum::Json;
use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use serde::Deserialize;

use crate::AppState;
use crate::kafka::KafkaError;
use crate::kafka::model::SchemaDeletion;

use super::context::Session;
use super::error::ApiError;
use super::extract::{self, Path, Query};

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::{
    EditSubject, RegisterSchema, RegisteredVersion, SubjectDetail, SubjectRow, SubjectRowsResult,
};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(subjects))
        // Subject names may contain `/`.
        .route(
            "/{*subject}",
            get(subject)
                .post(register_schema)
                .patch(edit_subject)
                .delete(delete_schema),
        )
}

async fn subjects(
    session: Session,
    Path(name): Path<String>,
) -> Result<Json<SubjectRowsResult>, ApiError> {
    let cluster = session.cluster(&name)?;
    Ok(Json(SubjectRowsResult {
        rows: cluster
            .store
            .subject_rows()
            .into_iter()
            .map(SubjectRow::from)
            .collect(),
        source_health: cluster.store.subjects.health().into(),
        has_registry: cluster.has_schema_registry(),
    }))
}

#[derive(Debug, Default, Deserialize)]
struct VersionQuery {
    version: Option<i32>,
}

async fn subject(
    session: Session,
    Path((name, subject)): Path<(String, String)>,
    Query(query): Query<VersionQuery>,
) -> Result<Json<SubjectDetail>, ApiError> {
    let cluster = session.cluster(&name)?;
    let schema_text = cluster.schema_text()?;
    let version = match query.version {
        Some(version) => version,
        None => latest_version(&cluster, &subject)?,
    };

    let schema = schema_text.subject_schema(&subject, version).await?;

    Ok(Json(SubjectDetail::new(subject, version, schema)))
}

async fn register_schema(
    session: Session,
    Path((name, subject)): Path<(String, String)>,
    extract::Json(request): extract::Json<RegisterSchema>,
) -> Result<Json<RegisteredVersion>, ApiError> {
    let cluster = session.cluster(&name)?;
    let schemas = cluster.manage_schemas()?;
    let registered = schemas
        .register_schema(&request.into_schema(subject))
        .await?;
    Ok(Json(registered.into()))
}

async fn edit_subject(
    session: Session,
    Path((name, subject)): Path<(String, String)>,
    extract::Json(request): extract::Json<EditSubject>,
) -> Result<StatusCode, ApiError> {
    let cluster = session.cluster(&name)?;
    let schemas = cluster.manage_schemas()?;
    schemas
        .set_compatibility(&subject, request.compatibility.into())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, Default, Deserialize)]
struct DeleteQuery {
    version: Option<i32>,
    #[serde(default)]
    permanent: bool,
}

async fn delete_schema(
    session: Session,
    Path((name, subject)): Path<(String, String)>,
    Query(query): Query<DeleteQuery>,
) -> Result<StatusCode, ApiError> {
    let cluster = session.cluster(&name)?;
    let schemas = cluster.manage_schemas()?;
    schemas
        .delete_schema(&SchemaDeletion {
            subject,
            version: query.version,
            permanent: query.permanent,
        })
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

fn latest_version(
    cluster: &super::context::ClusterHandle<'_>,
    subject: &str,
) -> Result<i32, ApiError> {
    cluster
        .store
        .subjects
        .load()
        .and_then(|subjects| subjects.get(subject).map(|info| info.latest_version))
        .ok_or_else(|| {
            KafkaError::UnknownSubject {
                cluster: cluster.name().to_owned(),
                subject: subject.to_owned(),
                version: 0,
            }
            .into()
        })
}
