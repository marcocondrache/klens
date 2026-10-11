use axum::Json;
use axum::Router;
use axum::routing::get;

use crate::AppState;

use crate::app::context::Session;

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::{ClusterGrant, Identity};

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/whoami", get(whoami))
}

async fn whoami(session: Session) -> Json<Identity> {
    let clusters = session
        .clusters()
        .map(|cluster| ClusterGrant {
            cluster: cluster.name().to_owned(),
            roles: cluster
                .access
                .role_names()
                .into_iter()
                .map(ToOwned::to_owned)
                .collect(),
            privileges: cluster
                .access
                .privileges()
                .into_iter()
                .map(Into::into)
                .collect(),
            writable: cluster.is_writable(),
        })
        .collect();

    Json(Identity {
        subject: session.guard.subject().map(ToOwned::to_owned),
        clusters,
    })
}
