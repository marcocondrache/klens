use axum::Router;

mod health;

pub fn router() -> Router {
    Router::new()
        .merge(health::router())
        .fallback(klens_server::web::serve)
}
