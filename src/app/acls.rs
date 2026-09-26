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
    let acls = session.cluster(&name)?.acls()?;
    Ok(Json(AclListing::from(acls.list().await?)))
}
