use axum::Json;
use axum::Router;
use axum::routing::get;
use jiff::Timestamp;

use crate::AppState;

use super::context::Session;
use super::error::ApiError;
use super::extract::Path;

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::Transactions;

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/", get(transactions))
}

async fn transactions(
    session: Session,
    Path(name): Path<String>,
) -> Result<Json<Transactions>, ApiError> {
    let cluster = session.cluster(&name)?;
    Ok(Json(Transactions::assemble(
        cluster.store.transactions.load().as_deref(),
        cluster.store.topology.load().as_deref(),
        Timestamp::now(),
    )))
}
