use std::net::SocketAddr;

use axum::Router;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use super::{Listener, serve};

pub async fn exchange(router: Router, request: &str) -> Vec<u8> {
    let bind = free_address();
    let server = tokio::spawn(serve([Listener { router, bind }]));

    let mut stream = connect(bind, &server).await;
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

pub fn free_address() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("free port")
}

pub async fn connect<T>(bind: SocketAddr, server: &JoinHandle<T>) -> TcpStream {
    loop {
        assert!(!server.is_finished(), "serve returned before accepting");
        match TcpStream::connect(bind).await {
            Ok(stream) => break stream,
            Err(_) => tokio::task::yield_now().await,
        }
    }
}
