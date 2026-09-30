use axum::Router;
use axum::http::{Request, Response, header};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::sensitive_headers::{
    SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer,
};
use tower_http::trace::TraceLayer;
use tracing::Span;

use crate::telemetry::log_http_completed;

pub mod web;

pub async fn serve(router: Router, bind: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(bind).await?;
    let sensitive_headers: Arc<[_]> = Arc::new([
        header::AUTHORIZATION,
        header::COOKIE,
        header::PROXY_AUTHORIZATION,
        header::SET_COOKIE,
    ]);

    let layers = ServiceBuilder::new()
        .layer(CatchPanicLayer::new())
        .layer(SetSensitiveRequestHeadersLayer::from_shared(Arc::clone(
            &sensitive_headers,
        )))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<_>| {
                    tracing::info_span!(
                        "http.request",
                        method = %request.method(),
                        path = %request.uri().path(),
                    )
                })
                .on_response(|response: &Response<_>, latency: Duration, _span: &Span| {
                    log_http_completed(response.status().as_u16(), latency);
                })
                .on_failure(()),
        )
        .layer(SetSensitiveResponseHeadersLayer::from_shared(
            sensitive_headers,
        ));

    let app = router.layer(layers);

    tracing::info!(bind = %bind, "listening");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    tracing::info!("server stopped");

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received Ctrl+C"),
        _ = terminate => tracing::info!("received SIGTERM"),
    }

    tracing::info!("starting graceful shutdown");
}

#[cfg(test)]
mod tests {
    use axum::routing::get;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use super::*;
    use crate::telemetry::capture::subscriber as capture;

    #[tokio::test]
    async fn the_request_log_has_the_path_but_not_the_query() {
        let (logs, _guard) = capture(tracing::Level::INFO);
        let router = Router::new().route(
            "/api/auth/callback",
            get(|| async { tracing::info!("handled") }),
        );
        let bind = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .expect("free port");
        let server = tokio::spawn(serve(router, bind));

        let mut stream = loop {
            assert!(!server.is_finished(), "serve returned before accepting");
            match TcpStream::connect(bind).await {
                Ok(stream) => break stream,
                Err(_) => tokio::task::yield_now().await,
            }
        };
        stream
            .write_all(
                b"GET /api/auth/callback?code=secret-code&state=secret-state HTTP/1.1\r\n\
                  Host: localhost\r\nConnection: close\r\n\r\n",
            )
            .await
            .expect("write request");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .expect("read response");
        server.abort();

        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        let text = logs.as_string();
        assert!(text.contains("path=/api/auth/callback"), "{text}");
        assert!(!text.contains("secret-code"), "{text}");
        assert!(!text.contains("secret-state"), "{text}");
    }
}
