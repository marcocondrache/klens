mod context;
mod error;
mod handlers;
mod types;
mod updates;

pub use types::typescript;

pub(crate) fn router() -> axum::Router<crate::AppState> {
    handlers::router()
}

#[cfg(test)]
mod tests;
