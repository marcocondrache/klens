use std::collections::BTreeSet;

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
