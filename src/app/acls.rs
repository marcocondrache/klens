use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::routing::get;

use crate::AppState;

use super::context::Session;
use super::error::ApiError;

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::AclListing;

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/", get(acls))
}

async fn acls(session: Session, Path(name): Path<String>) -> Result<Json<AclListing>, ApiError> {
    let capability = session.cluster(&name)?.access.acls()?;
    Ok(Json(AclListing::from(
        session.state.live_acls(capability.cluster()).await?,
    )))
}
