use std::time::Duration;

use tokio::task::yield_now;
use tokio::time::{Instant, sleep};

pub async fn settle<T>(what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
    for _ in 0..2_000 {
        if let Some(value) = probe() {
            return value;
        }
        yield_now().await;
    }
    panic!("{what} never happened");
}

pub async fn until(what: &str, mut ready: impl FnMut() -> bool) {
    settle(what, || ready().then_some(())).await;
}

pub async fn quiesce() {
    for _ in 0..100 {
        yield_now().await;
    }
}

pub async fn eventually(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready() {
        assert!(Instant::now() < deadline, "{what} never happened");
        sleep(Duration::from_millis(1)).await;
    }
}
