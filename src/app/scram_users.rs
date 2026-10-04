use axum::Json;
use axum::Router;
use axum::http::StatusCode;
use axum::routing::{get, put};

use crate::AppState;

use super::context::Session;
use super::error::ApiError;
use super::extract::{self, Path, Query};

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::{DeleteScramCredential, ScramListing, SetScramCredential};

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/", get(scram_users)).route(
        "/{*user}",
        put(set_scram_credential).delete(delete_scram_credential),
    )
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

async fn set_scram_credential(
    session: Session,
    Path((name, user)): Path<(String, String)>,
    extract::Json(request): extract::Json<SetScramCredential>,
) -> Result<StatusCode, ApiError> {
    let cluster = session.cluster(&name)?;
    let users = cluster.manage_acls()?;
    users
        .set_scram_credential(&request.into_credential(user)?)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_scram_credential(
    session: Session,
    Path((name, user)): Path<(String, String)>,
    Query(params): Query<DeleteScramCredential>,
) -> Result<StatusCode, ApiError> {
    let cluster = session.cluster(&name)?;
    cluster
        .manage_acls()?
        .delete_scram_credential(&user, params.mechanism.into())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
