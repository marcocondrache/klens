use std::collections::BTreeSet;

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
