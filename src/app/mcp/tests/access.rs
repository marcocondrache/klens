use strum::VariantArray as _;

use super::*;

#[tokio::test]
async fn every_tool_opens_to_exactly_the_privilege_it_names() {
    let app = TestApp::of([FakeCluster::local()])
        .limits(Limits {
            mcp_live_calls_per_minute: NonZeroU32::MAX,
            ..Limits::new(&Tuning::default())
        })
        .writable(&["local"])
        .ingested()
        .await
        .serving_mcp(every_tool());
    let _lanes = lanes_writes_wait_on(&app);
    let tools = KlensMcp::tools().list_all();
    let mut wrong = Vec::new();

    for held in std::iter::once(None).chain(Privilege::VARIANTS.iter().copied().map(Some)) {
        let session = app.serving_mcp(Mcp::holding(held));
        for (name, section, needs) in gate_rows() {
            let tool = tools.iter().find(|tool| tool.name == name).expect("a tool");
            let expected = match (section, needs) {
                (_, None) => Outcome::Answered,
                (None, needs) if needs == held => Outcome::Answered,
                (None, _) => Outcome::Refused(json!("FORBIDDEN")),
                (Some(_), needs) if needs == held => Outcome::Shown,
                (Some(_), Some(needs)) => Outcome::Omitted(json!(needs)),
            };
            let result = call(&session, name, naming(tool, "local")).await;
            let got = outcome(&result, section);
            if got != expected {
                wrong.push(format!(
                    "{name} {section:?} holding {held:?} answered {got:?}, not {expected:?}"
                ));
            }
        }
    }

    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

#[tokio::test]
async fn a_ceiling_without_a_privilege_closes_each_tool_and_section_that_needs_it() {
    let app = writable().await;
    let tools = KlensMcp::tools().list_all();

    for (name, section, needs) in gate_rows() {
        let Some(needs) = needs else { continue };
        let capped = app.serving_mcp(Mcp::holding(
            every_tool()
                .privileges
                .into_iter()
                .filter(|&privilege| privilege != needs),
        ));
        let tool = tools.iter().find(|tool| tool.name == name).expect("a tool");

        let result = call_through_router(&capped, name, naming(tool, "local")).await;

        let expected = match section {
            None => Outcome::Refused(json!("FORBIDDEN")),
            Some(_) => Outcome::Omitted(json!(needs)),
        };
        assert_eq!(outcome(&result, section), expected, "{name} {section:?}");
    }
}

#[test]
fn mcp_privileges_take_exactly_the_privileges_the_tools_need() {
    let needed = every_tool().privileges;

    for &privilege in Privilege::VARIANTS {
        let named = json!(privilege);
        let name = named.as_str().expect("a name").to_lowercase();
        let parsed =
            crate::config::parse::<Mcp>(&format!("{{bind: 127.0.0.1:0, privileges: [{name}]}}"));
        assert_eq!(parsed.is_ok(), needed.contains(&privilege), "{name}");
    }
}

#[tokio::test]
async fn every_tool_that_changes_kafka_is_refused_on_a_read_only_cluster() {
    let app = TestApp::local().await.serving_mcp(every_tool());
    let tools = KlensMcp::tools().list_all();

    for name in tool_names_that_change_kafka() {
        let tool = tools.iter().find(|tool| tool.name == name).expect("a tool");

        let refused = refusal(&call(&app, name, naming(tool, "local")).await);

        assert_eq!(refused["code"], "READ_ONLY_CLUSTER", "{name}");
        assert_eq!(refused["error"], "cluster 'local' is read-only");
    }
    assert_eq!(app.cluster().calls(Api::CreateTopic), 0);
    assert_eq!(app.cluster().calls(Api::Produce), 0);
    assert_eq!(app.cluster().calls(Api::RegisterSchema), 0);
}

#[tokio::test]
async fn tools_list_leaves_out_a_tool_the_caller_may_use_on_no_cluster() {
    let app = TestApp::of([FakeCluster::local(), FakeCluster::named("prod")])
        .ingested()
        .await;
    let listed = async |mcp: Mcp| {
        let listed = app.serving_mcp(mcp).mcp("tools/list", json!({})).await.ok();
        names(&listed["result"], "tools", "name")
    };

    let viewing = listed(Mcp::holding([])).await;
    let reading = listed(Mcp::default()).await;
    let reading_nowhere = listed(yaml("{bind: 127.0.0.1:0, clusters: []}")).await;

    let gated: Vec<&str> = gates()
        .filter(|gate| gate.needs.is_some())
        .map(|gate| gate.name)
        .collect();
    assert!(gated.contains(&"klens_records_read"), "{gated:?}");
    for listed in [&viewing, &reading_nowhere] {
        assert!(listed.contains(&"klens_clusters".to_owned()), "{listed:?}");
        assert!(
            !gated.iter().any(|&tool| listed.contains(&tool.to_owned())),
            "{listed:?}"
        );
    }
    assert_eq!(reading, read_tool_names());
}

#[tokio::test]
async fn a_tool_that_changes_kafka_is_offered_only_where_the_cluster_accepts_changes() {
    let app = TestApp::of([FakeCluster::local(), FakeCluster::named("prod")])
        .writable(&["local"])
        .ingested()
        .await;
    let offered = async |app: TestApp| {
        let listed = app.mcp("tools/list", json!({})).await.ok();
        names(&listed["result"], "tools", "name").contains(&"klens_topic_create".to_owned())
    };
    let everywhere = app.serving_mcp(Mcp::holding([Privilege::CreateTopics]));
    let only_prod = app.serving_mcp(yaml(
        "{bind: 127.0.0.1:0, privileges: [create_topics], clusters: [prod]}",
    ));

    let explained = structured(&call(&everywhere, "klens_access_explain", json!({})).await);

    let rights = |cluster: usize| {
        explained["clusters"][cluster]["tools"]
            .as_array()
            .expect("tools")
            .iter()
            .find(|tool| tool["name"] == "klens_topic_create")
            .cloned()
            .expect("the create tool")
    };
    assert_eq!(explained["clusters"][0]["writable"], true);
    assert_eq!(
        rights(0),
        json!({ "name": "klens_topic_create", "available": true })
    );
    assert_eq!(
        rights(1),
        json!({ "name": "klens_topic_create", "available": false })
    );
    assert!(offered(everywhere).await);
    assert!(!offered(only_prod).await);
}

#[tokio::test]
async fn a_ceiling_without_records_hides_both_record_tools_in_either_protocol() {
    let app = TestApp::local()
        .await
        .serving_mcp(yaml("{bind: 127.0.0.1:0, privileges: [acls]}"));

    let listed = app
        .reply_to_mcp(request_2026(
            "tools/list",
            None,
            json!({ "_meta": meta_2026() }),
        ))
        .await
        .ok();
    let legacy = app.mcp("tools/list", json!({})).await.ok();

    let result = &listed["result"];
    let tools = names(result, "tools", "name");
    assert_eq!(result["cacheScope"], "private", "{result}");
    assert!(legacy["result"].get("cacheScope").is_none(), "{legacy}");
    assert_eq!(names(&legacy["result"], "tools", "name"), tools);
    let open: Vec<&str> = gates()
        .filter(|gate| gate.needs.is_none_or(|needs| needs == Privilege::Acls))
        .map(|gate| gate.name)
        .collect();
    assert_eq!(tools, open);
    assert!(
        !tools
            .iter()
            .any(|tool| tool == "klens_record_get" || tool == "klens_records_read"),
        "{tools:?}"
    );
}

#[tokio::test]
async fn the_ceiling_holds_through_the_router() {
    let app = TestApp::of([FakeCluster::local(), FakeCluster::named("other")])
        .ingested()
        .await
        .serving_mcp(yaml(
            "{bind: 127.0.0.1:0, privileges: [acls], clusters: [other]}",
        ));

    let explained = structured(&call_through_router(&app, "klens_access_explain", json!({})).await);

    let clusters = explained["clusters"].as_array().expect("clusters");
    assert_eq!(clusters.len(), 1, "{explained}");
    assert_eq!(clusters[0]["cluster"], "other");
    assert_eq!(clusters[0]["privileges"], json!(["ACLS"]));
    for tool in KlensMcp::tools().list_all() {
        let refused = refusal(&call_through_router(&app, &tool.name, naming(&tool, "local")).await);
        assert_eq!(refused["code"], "UNKNOWN_CLUSTER", "{}", tool.name);
    }
}

#[tokio::test]
async fn with_auth_on_a_call_from_the_gateway_holds_the_ceiling_whatever_its_groups() {
    let mut mcp = behind_gateway(Mcp::holding([Privilege::Acls, Privilege::BrokerConfigs]));
    mcp.clusters = Some(vec!["other".to_owned()]);
    let app = TestApp::of([FakeCluster::local(), FakeCluster::named("other")])
        .auth(auth_granting_nothing())
        .ingested()
        .await
        .serving_mcp(mcp);
    let call = |tool: &str, arguments: Value| {
        let request = from_gateway(mcp_request(&call_body(tool, arguments)));
        async { app.reply_to_mcp(request).await.ok()["result"].take() }
    };

    let explained = structured(&call("klens_access_explain", json!({})).await);

    let clusters = explained["clusters"].as_array().expect("clusters");
    assert_eq!(clusters.len(), 1, "{explained}");
    assert_eq!(clusters[0]["cluster"], "other");
    assert_eq!(clusters[0]["privileges"], json!(["BROKER_CONFIGS", "ACLS"]));
    for tool in KlensMcp::tools().list_all() {
        let refused = refusal(&call(&tool.name, naming(&tool, "local")).await);
        assert_eq!(refused["code"], "UNKNOWN_CLUSTER", "{}", tool.name);
    }
}

#[tokio::test]
async fn every_tool_hides_a_cluster_the_caller_cannot_see() {
    let app = TestApp::of([FakeCluster::local(), FakeCluster::named("prod")])
        .ingested()
        .await
        .serving_mcp(yaml("{bind: 127.0.0.1:0, clusters: [local]}"));

    for tool in KlensMcp::tools().list_all() {
        let refused = refusal(&call(&app, &tool.name, naming(&tool, "prod")).await);
        assert_eq!(refused["code"], "UNKNOWN_CLUSTER", "{}", tool.name);
    }
}

#[tokio::test]
async fn a_cluster_tool_needs_no_cluster_when_the_caller_sees_only_one() {
    let app = TestApp::of([FakeCluster::local(), FakeCluster::named("prod")])
        .ingested()
        .await;
    let local_only = app.serving_mcp(yaml("{bind: 127.0.0.1:0, clusters: [local]}"));
    let blind = app.serving_mcp(yaml("{bind: 127.0.0.1:0, clusters: []}"));

    let served = structured(&call(&local_only, "klens_brokers_list", json!({})).await);
    let ambiguous = refusal(&call(&app, "klens_brokers_list", json!({})).await);
    let nothing = refusal(&call(&blind, "klens_brokers_list", json!({})).await);

    assert_eq!(served["brokers"][0]["id"], 1);
    assert_eq!(ambiguous["code"], "INVALID_REQUEST");
    assert_eq!(ambiguous["error"], "pass `cluster` as one of local, prod");
    assert_eq!(nothing["code"], "INVALID_REQUEST");
    assert_eq!(nothing["error"], "you can see no cluster");
}

#[test]
fn mcp_reaches_clusters_only_through_the_session() {
    let app = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/app");
    let sources = WalkDir::new(app.join("mcp"))
        .into_iter()
        .map(|entry| entry.expect("a source entry").into_path())
        .chain([app.join("mcp.rs")])
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"));

    for path in sources {
        let source = std::fs::read_to_string(&path).expect("a source file");
        for (unchecked, skips) in [
            (concat!("state", ".clusters"), "access checks"),
            (concat!("Cluster", "Session"), "obfuscation"),
        ] {
            assert!(
                !source.contains(unchecked),
                "{} names {unchecked}, which skips {skips}",
                path.display()
            );
        }
    }
}
