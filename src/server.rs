use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, Response, header};
use axum::middleware;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::future::{BoxFuture, Shared, try_join_all};
use futures::{FutureExt, StreamExt};
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

/// Resolves once klens starts to shut down.
type Shutdown = Shared<BoxFuture<'static, ()>>;

/// Binds every listener before it serves any, so a taken port stops startup
/// instead of leaving klens half up.
pub async fn serve(listeners: impl IntoIterator<Item = Listener>) -> Result<()> {
    serve_until(listeners, shutdown_signal()).await
}

async fn serve_until(
    listeners: impl IntoIterator<Item = Listener>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let mut bound = Vec::new();
    for Listener { router, bind } in listeners {
        let listener = TcpListener::bind(bind)
            .await
            .with_context(|| format!("failed to bind {bind}"))?;
        bound.push((listener, router, bind));
    }

    let shutdown = shutdown.boxed().shared();
    let servers = bound.into_iter().map(|(listener, router, bind)| {
        tracing::info!(bind = %bind, "listening");
        axum::serve(listener, with_layers(router, shutdown.clone()))
            .with_graceful_shutdown(shutdown.clone())
            .into_future()
    });
    try_join_all(servers).await?;

    tracing::info!("server stopped");

    Ok(())
}

fn with_layers(router: Router, shutdown: Shutdown) -> Router {
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
        .layer(CompressionLayer::new())
        .layer(middleware::map_response_with_state(
            shutdown,
            end_on_shutdown,
        ));

    router.layer(layers)
}

/// Graceful shutdown waits for every open response, and an event stream never
/// ends on its own. Ending it lets shutdown finish, and the client reconnects.
async fn end_on_shutdown(
    State(shutdown): State<Shutdown>,
    response: Response<Body>,
) -> Response<Body> {
    let is_event_stream = response
        .headers()
        .get(header::CONTENT_TYPE)
        .is_some_and(|value| value.as_bytes().starts_with(b"text/event-stream"));
    if !is_event_stream {
        return response;
    }

    let (parts, body) = response.into_parts();
    let body = body.into_data_stream().take_until(shutdown);
    Response::from_parts(parts, Body::from_stream(body))
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
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::*;
    use crate::testing::{LogCapture, connect, exchange, free_address};

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
            );
        let request = |path: &str| {
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        };

        exchange(router.clone(), &request("/done")).await;
        logs.assert_lacks("request completed");

        exchange(router, &request("/broken")).await;
        logs.assert_contains("status=500");
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

    async fn open(stream: &mut TcpStream, path: &str, first: &str) {
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .expect("write request");

        let mut received = Vec::new();
        while !contains(&received, first) {
            let mut chunk = [0; 1024];
            let read = stream.read(&mut chunk).await.expect("read response");
            assert_ne!(read, 0, "{path} closed before it sent {first}");
            received.extend_from_slice(&chunk[..read]);
        }
    }

    async fn rest(mut stream: TcpStream) -> Vec<u8> {
        let mut rest = Vec::new();
        timeout(Duration::from_secs(5), stream.read_to_end(&mut rest))
            .await
            .expect("the response ends")
            .expect("read response");
        rest
    }

    #[tokio::test]
    async fn shutdown_ends_event_streams_and_lets_other_responses_finish() {
        let (release, released) = oneshot::channel::<()>();
        let released = released.shared();
        let router = Router::new()
            .route(
                "/events",
                get(|| async {
                    let first = Event::default().data("open");
                    Sse::new(stream::iter([Ok::<_, Infallible>(first)]).chain(stream::pending()))
                }),
            )
            .route(
                "/export",
                get(move || {
                    let released = released.clone();
                    async move {
                        let tail = async move {
                            let _ = released.await;
                            Ok::<_, Infallible>("tail")
                        };
                        Body::from_stream(stream::iter([Ok("head")]).chain(stream::once(tail)))
                    }
                }),
            );
        let (stop, stopped) = oneshot::channel::<()>();
        let bind = free_address();
        let server = tokio::spawn(serve_until([Listener { router, bind }], async {
            let _ = stopped.await;
        }));

        let mut events = connect(bind, &server).await;
        open(&mut events, "/events", "data: open").await;
        let mut export = connect(bind, &server).await;
        open(&mut export, "/export", "head").await;
        stop.send(()).expect("server is running");

        rest(events).await;
        release.send(()).expect("export is waiting");
        assert!(contains(&rest(export).await, "tail"));
        server.await.expect("server task").expect("server stops");
    }
}
