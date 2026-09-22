use std::collections::BTreeSet;

use super::super::harness::{ok, seeded};

#[tokio::test]
async fn search_is_answered_from_the_prebuilt_index() {
    let state = seeded();
    let data = ok(&state, "/clusters/local/search?q=orders").await;
    let kinds: BTreeSet<&str> = data
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| hit["kind"].as_str().expect("kind"))
        .collect();

    assert!(kinds.contains("TOPIC"), "{kinds:?}");
    assert!(kinds.contains("SUBJECT"), "{kinds:?}");
}
