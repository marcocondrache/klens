use axum::Router;

use crate::AppState;

pub(super) mod acls;
pub(super) mod brokers;
pub(super) mod clusters;
pub(super) mod configs;
pub(super) mod groups;
pub(super) mod quotas;
pub(super) mod records;
pub(super) mod scram_users;
pub(super) mod search;
pub(super) mod subjects;
pub(super) mod topics;
pub(super) mod updates;
pub(super) mod whoami;

pub fn router() -> Router<AppState> {
    Router::new()
        .merge(whoami::router())
        .nest("/clusters", clusters::router())
}
