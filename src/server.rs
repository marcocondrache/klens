use axum::Router;
use axum::http::{Request, Response, header};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::FutureExt;
use futures::future::try_join_all;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::sensitive_headers::{
    SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer,
};
use tower_http::trace::TraceLayer;
use tracing::Span;

use crate::telemetry::log_http_completed;

pub mod hosts;
#[cfg(test)]
pub(crate) mod testing;
pub mod web;

/// A router and the address it answers on.
pub struct Listener {
    pub router: Router,
    pub bind: SocketAddr,
}

/// Binds every listener before it serves any, so a taken port stops startup
/// instead of leaving klens half up.
pub async fn serve(listeners: impl IntoIterator<Item = Listener>) -> Result<()> {
    let mut bound = Vec::new();
    for Listener { router, bind } in listeners {
        let listener = TcpListener::bind(bind)
            .await
            .with_context(|| format!("failed to bind {bind}"))?;
        bound.push((listener, router, bind));
    }

    let shutdown = shutdown_signal().shared();
    let servers = bound.into_iter().map(|(listener, router, bind)| {
        tracing::info!(bind = %bind, "listening");
        axum::serve(listener, with_layers(router))
            .with_graceful_shutdown(shutdown.clone())
            .into_future()
    });
    try_join_all(servers).await?;

    tracing::info!("server stopped");

    Ok(())
}

fn with_layers(router: Router) -> Router {
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
                        path = request.uri().path(),
                        user = tracing::field::Empty,
                    )
                })
                .on_response(|response: &Response<_>, latency: Duration, _span: &Span| {
                    log_http_completed(response.status().as_u16(), latency);
                })
                .on_failure(()),
        )
        .layer(SetSensitiveResponseHeadersLayer::from_shared(
            sensitive_headers,
        ))
        .layer(CompressionLayer::new());

    router.layer(layers)
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
    use std::convert::Infallible;

    use axum::http::StatusCode;
    use axum::response::sse::{Event, Sse};
    use axum::routing::get;
    use futures::stream;

    use super::*;
    use crate::testing::{LogCapture, exchange};

    #[tokio::test]
    async fn the_request_log_has_the_path_but_not_the_query() {
        let logs = LogCapture::at(tracing::Level::INFO);
        let router = Router::new().route(
            "/api/auth/callback",
            get(|| async { tracing::info!("handled") }),
        );

        let response = exchange(
            router,
            "GET /api/auth/callback?code=secret-code&state=secret-state HTTP/1.1\r\n\
             Host: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;

        let response = String::from_utf8(response).expect("utf-8 response");
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        logs.assert_contains("path=\"/api/auth/callback\"");
        logs.assert_lacks("secret-code");
        logs.assert_lacks("secret-state");
    }

    #[tokio::test]
    async fn only_a_server_error_logs_its_completion_at_warn() {
        let logs = LogCapture::at(tracing::Level::WARN);
        let router = Router::new()
            .route("/done", get(|| async { StatusCode::NO_CONTENT }))
            .route(
                "/broken",
                get(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
            )
            .route("/unreachable", get(|| async { StatusCode::BAD_GATEWAY }));
        let request = |path: &str| {
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        };

        exchange(router.clone(), &request("/done")).await;
        logs.assert_lacks("request completed");

        exchange(router.clone(), &request("/broken")).await;
        exchange(router, &request("/unreachable")).await;
        logs.assert_contains("status=500");
        logs.assert_contains("status=502");
    }

    #[tokio::test]
    async fn a_quote_in_the_path_cannot_forge_a_span_field() {
        let logs = LogCapture::at(tracing::Level::INFO);
        let router = Router::new().route(
            "/api/groups/{group}",
            get(|| async { tracing::info!("handled") }),
        );

        let response = exchange(
            router,
            "GET /api/groups/x\"user=\"root\" HTTP/1.1\r\n\
             Host: localhost\r\nConnection: close\r\n\r\n",
        )
        .await;

        let response = String::from_utf8(response).expect("utf-8 response");
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        logs.assert_contains(r#"path="/api/groups/x\"user=\"root\"""#);
        logs.assert_lacks(r#"x"user="root""#);
    }

    #[tokio::test]
    async fn a_taken_port_stops_startup() {
        let taken = std::net::TcpListener::bind("127.0.0.1:0").expect("free port");
        let listeners = [Listener {
            router: Router::new(),
            bind: taken.local_addr().expect("address"),
        }];

        let error = serve(listeners).await.expect_err("port is taken");

        assert!(error.to_string().starts_with("failed to bind"), "{error}");
    }

    const PAGE: &str = "{\"records\":[{\"offset\":0},{\"offset\":1},{\"offset\":2}]}";

    async fn fetch(path: &str, accept_encoding: Option<&str>) -> (Option<String>, Vec<u8>) {
        let _logs = LogCapture::at(tracing::Level::INFO);
        let router = Router::new().route("/page", get(|| async { PAGE })).route(
            "/stream",
            get(|| async {
                Sse::new(stream::iter([Ok::<_, Infallible>(
                    Event::default().data(PAGE),
                )]))
            }),
        );
        let accept_encoding = accept_encoding
            .map(|value| format!("Accept-Encoding: {value}\r\n"))
            .unwrap_or_default();

        let response = exchange(
            router,
            &format!(
                "GET {path} HTTP/1.1\r\nHost: localhost\r\n{accept_encoding}\
                 Connection: close\r\n\r\n"
            ),
        )
        .await;

        let split = response
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("end of head");
        let head = std::str::from_utf8(&response[..split]).expect("ascii head");
        assert!(head.starts_with("HTTP/1.1 200"), "{head}");
        let encoding = head
            .lines()
            .find_map(|line| line.strip_prefix("content-encoding: "))
            .map(str::to_owned);
        (encoding, response[split + 4..].to_vec())
    }

    fn contains(body: &[u8], text: &str) -> bool {
        body.windows(text.len())
            .any(|window| window == text.as_bytes())
    }

    #[tokio::test]
    async fn a_response_is_compressed_in_each_encoding_the_client_accepts() {
        for encoding in ["gzip", "br", "zstd"] {
            let (served, body) = fetch("/page", Some(encoding)).await;

            assert_eq!(served.as_deref(), Some(encoding));
            assert!(!contains(&body, PAGE), "{encoding}");
        }
    }

    #[tokio::test]
    async fn an_event_stream_is_never_compressed() {
        let (served, body) = fetch("/stream", Some("gzip, br, zstd")).await;

        assert_eq!(served, None);
        assert!(contains(&body, &format!("data: {PAGE}\n\n")));
    }
}
