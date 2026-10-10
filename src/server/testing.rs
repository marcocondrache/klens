use axum::Router;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::serve;

pub async fn exchange(router: Router, request: &str) -> Vec<u8> {
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
