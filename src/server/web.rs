use axum::Router;

#[cfg(feature = "ui")]
pub fn router() -> Router {
    use axum::http::StatusCode;
    use memory_serve::CacheControl;

    memory_serve::load!()
        .fallback(Some("/index.html"))
        .fallback_status(StatusCode::OK)
        .html_cache_control(CacheControl::NoCache)
        .cache_control(CacheControl::Long)
        .into_router()
}

#[cfg(not(feature = "ui"))]
pub fn router() -> Router {
    Router::new().fallback(|| async { axum::response::Html("") })
}
