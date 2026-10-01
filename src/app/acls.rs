use axum::Json;
use axum::Router;
use axum::routing::get;

use crate::AppState;

use super::context::Session;
use super::error::ApiError;
use super::extract::Path;

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::AclListing;

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/", get(acls))
}

async fn acls(session: Session, Path(name): Path<String>) -> Result<Json<AclListing>, ApiError> {
    let cluster = session.cluster(&name)?;
    cluster.access.acls()?;
    Ok(Json(AclListing::new(
        cluster.store.acls.load().as_deref(),
        cluster.store.acls.health().into(),
    )))
}
