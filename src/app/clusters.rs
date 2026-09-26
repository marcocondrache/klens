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

pub(crate) use types::{ClusterHealth, LaneHealth};

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(clusters))
        .nest("/{cluster}", cluster_routes())
}

fn cluster_routes() -> Router<AppState> {
    Router::new()
        .route("/", get(cluster))
        .nest("/topics", super::topics::router())
        .nest("/groups", super::groups::router())
        .nest("/group-offsets", super::group_offsets::router())
        .nest("/brokers", super::brokers::router())
        .nest("/subjects", super::subjects::router())
        .nest("/acls", super::acls::router())
        .nest("/search", super::search::router())
        .nest("/updates", super::updates::router())
}

async fn clusters(session: Session) -> Json<Vec<ClusterHealth>> {
    Json(
        session
            .state
            .clusters
            .iter()
            .filter(|cluster| session.access.can_see_cluster(cluster.name()))
            .map(|cluster| ClusterHealth::from(cluster.store.health()))
            .collect(),
    )
}

async fn cluster(
    session: Session,
    Path(name): Path<String>,
) -> Result<Json<ClusterHealth>, ApiError> {
    let cluster = session.cluster(&name)?;
    Ok(Json(ClusterHealth::from(cluster.store.health())))
}
