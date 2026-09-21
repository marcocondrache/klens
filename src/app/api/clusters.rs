use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::routing::get;

use crate::AppState;

use super::context::Session;
use super::error::ApiError;

mod types;

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
            .stores
            .iter()
            .filter(|store| session.access.can_see_cluster(store.name()))
            .map(|store| ClusterHealth::from(store.health()))
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
