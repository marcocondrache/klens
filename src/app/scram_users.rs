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

pub(crate) use types::ScramListing;

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/", get(scram_users))
}

async fn scram_users(
    session: Session,
    Path(name): Path<String>,
) -> Result<Json<ScramListing>, ApiError> {
    let cluster = session.cluster(&name)?;
    cluster.access.acls()?;
    Ok(Json(ScramListing::new(
        cluster.store.scram_users.load().as_deref(),
        cluster.store.scram_users.health().into(),
    )))
}
