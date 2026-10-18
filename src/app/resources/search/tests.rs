use std::collections::BTreeSet;

use axum::http::StatusCode;

use super::MAX_QUERY_CHARS;
use crate::testing::{Api, TestApp};

#[tokio::test]
async fn search_is_answered_from_the_committed_tables() {
    let app = TestApp::local().await;
    let hits = app.get("/clusters/local/search?q=orders").await.ok();
    let kinds: BTreeSet<&str> = hits
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| hit["kind"].as_str().expect("kind"))
        .collect();

    assert!(kinds.contains("TOPIC"), "{kinds:?}");
    assert!(kinds.contains("SUBJECT"), "{kinds:?}");
    assert_eq!(app.cluster().calls(Api::Metadata), 0);
    assert_eq!(app.cluster().calls(Api::SchemaSubjects), 0);
}

#[tokio::test]
async fn a_query_past_the_cap_is_refused() {
    let app = TestApp::local().await;
    let longest = "%C3%B6".repeat(MAX_QUERY_CHARS);
    let longer = "o".repeat(MAX_QUERY_CHARS + 1);

    app.get(&format!("/clusters/local/search?q={longest}"))
        .await
        .ok();
    let refused = app.get(&format!("/clusters/local/search?q={longer}")).await;

    refused.assert_error(StatusCode::UNPROCESSABLE_ENTITY, "INVALID_REQUEST");
    assert_eq!(
        refused.body["error"],
        "a query holds at most 256 characters"
    );
}
