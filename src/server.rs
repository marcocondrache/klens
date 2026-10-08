use axum::Router;
use axum::http::{Request, Response, header};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
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
    use std::convert::Infallible;

    use axum::response::sse::{Event, Sse};
    use axum::routing::get;
    use futures::stream;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    use super::*;
    use crate::testing::LogCapture;

    async fn exchange(router: Router, request: &str) -> Vec<u8> {
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
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("read response");
        server.abort();
        response
    }

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
