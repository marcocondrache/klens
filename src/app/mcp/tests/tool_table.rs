use super::*;

#[test]
fn the_tool_table_names_every_tool_the_router_serves() {
    let served: Vec<String> = KlensMcp::tools()
        .list_all()
        .into_iter()
        .map(|tool| tool.name.into_owned())
        .collect();

    assert_eq!(served, tool_names());
}

/// A client copies an argument marked `x-mcp-header` into an HTTP header,
/// where proxies log it.
#[test]
fn no_tool_copies_an_argument_into_a_header() {
    assert!(!tool_list().contains("x-mcp-header"));
}

#[test]
fn every_tool_is_titled_and_says_whether_it_changes_kafka() {
    for tool in KlensMcp::tools().list_all() {
        let name = &*tool.name;
        let annotations = tool.annotations.as_ref().expect("annotations");
        let description = tool.description.as_deref().expect("a description");
        let changes = gates()
            .find(|gate| gate.name == name)
            .expect("a gate")
            .changes();

        assert!(name.starts_with("klens_") && name.len() <= 64, "{name}");
        assert!(
            name.bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
            "{name}"
        );
        assert!(tool.title.is_some(), "{name}");
        assert_eq!(
            (
                annotations.read_only_hint,
                annotations.destructive_hint,
                annotations.open_world_hint,
            ),
            (Some(!changes), Some(false), Some(false)),
            "{name}"
        );
        assert!(
            annotations
                .idempotent_hint
                .is_some_and(|idempotent| idempotent || changes),
            "{name}"
        );
        assert!(description.len() < 2048, "{name}");
        assert!(tool.output_schema.is_none(), "{name}");
    }
}

#[tokio::test]
async fn tools_list_serves_the_checked_in_snapshot() {
    let listed = writable().await.mcp("tools/list", json!({})).await.ok();
    let snapshot: Value = serde_json::from_str(&tool_list()).expect("json");

    assert_eq!(listed["result"]["tools"], snapshot["tools"]);
}
