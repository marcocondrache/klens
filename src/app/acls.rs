use axum::Json;
use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;

use crate::AppState;

use super::context::Session;
use super::error::ApiError;
use super::extract::{self, Path, Query};

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::{Acl, AclListing, CreateAcls};

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/", get(acls).post(create_acls).delete(delete_acl))
}

async fn acls(session: Session, Path(name): Path<String>) -> Result<Json<AclListing>, ApiError> {
    let cluster = session.cluster(&name)?;
    cluster.access.acls()?;
    Ok(Json(AclListing::new(
        cluster.store.acls.load().as_deref(),
        cluster.store.acls.health().into(),
    )))
}

async fn create_acls(
    session: Session,
    Path(name): Path<String>,
    extract::Json(request): extract::Json<CreateAcls>,
) -> Result<StatusCode, ApiError> {
    let cluster = session.cluster(&name)?;
    let acls = cluster.create_acls()?;
    acls.create_acls(&request.into_acls()?).await?;
    Ok(StatusCode::CREATED)
}

async fn delete_acl(
    session: Session,
    Path(name): Path<String>,
    Query(acl): Query<Acl>,
) -> Result<StatusCode, ApiError> {
    let cluster = session.cluster(&name)?;
    let acls = cluster.delete_acls()?;
    acls.delete_acl(&acl.into()).await?;
    Ok(StatusCode::NO_CONTENT)
}
