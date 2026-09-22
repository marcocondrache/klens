use axum::Json;
use axum::Router;
use axum::routing::get;

use crate::AppState;

use super::context::Session;

pub mod types;

#[cfg(test)]
mod tests;

pub(crate) use types::{ClusterGrant, Identity};

pub(crate) fn router() -> Router<AppState> {
    Router::new().route("/whoami", get(whoami))
}

async fn whoami(session: Session) -> Json<Identity> {
    let clusters = session
        .access
        .visible_clusters(session.state.stores.names())
        .into_iter()
        .filter_map(|name| {
            let access = session.access.cluster(name).ok()?;
            Some(ClusterGrant {
                cluster: name.to_owned(),
                roles: access
                    .role_names()
                    .into_iter()
                    .map(ToOwned::to_owned)
                    .collect(),
                privileges: access.privileges().into_iter().map(Into::into).collect(),
            })
        })
        .collect();

    Json(Identity {
        subject: session.guard.subject().map(ToOwned::to_owned),
        clusters,
    })
}
