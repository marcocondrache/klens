mod acls;
mod brokers;
mod clusters;
mod configs;
mod context;
mod error;
mod groups;
mod int64;
mod paging;
mod records;
mod search;
mod subjects;
mod topics;
mod typescript;
mod updates;
mod whoami;

pub use typescript::typescript;

pub(crate) fn router() -> axum::Router<crate::AppState> {
    axum::Router::new().nest(
        "/api",
        axum::Router::new()
            .merge(whoami::router())
            .nest("/clusters", clusters::router()),
    )
}

#[cfg(test)]
mod harness;
