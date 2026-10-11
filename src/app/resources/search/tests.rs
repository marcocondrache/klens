use std::collections::BTreeSet;

use axum::http::StatusCode;

use crate::kafka::store::search::MAX_QUERY_CHARS;
use crate::testing::TestApp;

#[tokio::test]
async fn search_is_answered_from_the_committed_tables() {
    let hits = TestApp::local()
        .await
        .get("/clusters/local/search?q=orders")
        .await
        .ok();
    let kinds: BTreeSet<&str> = hits
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| hit["kind"].as_str().expect("kind"))
        .collect();

    assert!(kinds.contains("TOPIC"), "{kinds:?}");
    assert!(kinds.contains("SUBJECT"), "{kinds:?}");
}

#[tokio::test]
async fn a_query_past_the_cap_is_refused_before_it_is_matched() {
    let app = TestApp::local().await;
    let longest = "o+".repeat(MAX_QUERY_CHARS / 2);
    let past = "o+".repeat(MAX_QUERY_CHARS / 2 + 1);

    app.get(&format!("/clusters/local/search?q={longest}"))
        .await
        .ok();
    app.get(&format!("/clusters/local/search?q={past}"))
        .await
        .assert_error(StatusCode::UNPROCESSABLE_ENTITY, "INVALID_REQUEST");
}
