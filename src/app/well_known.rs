use axum::Router;
use axum::routing::any;

use super::AppState;
use super::error::ApiError;

#[cfg(test)]
mod tests;

/// OAuth and MCP clients probe discovery documents here and take a 200 for
/// the document, so the UI's HTML fallback must never answer under this
/// prefix.
pub fn router() -> Router<AppState> {
    Router::new().nest_service("/.well-known", any(not_found))
}

async fn not_found() -> ApiError {
    ApiError::NotFound
}
