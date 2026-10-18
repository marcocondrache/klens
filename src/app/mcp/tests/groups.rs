use super::*;

fn consumers() -> FakeCluster {
    FakeCluster::local()
        .with_topic("payments", 1, 4)
        .with_groups([
            group("billing", "orders.created", vec![0, 1])
                .with_committed(&[("orders.created", 0, 0), ("orders.created", 1, 0)]),
            group("ledger", "payments", vec![0]).with_committed(&[("payments", 0, 4)]),
            group("archive", "payments", vec![0])
                .with_committed(&[("payments", 0, 1)])
                .stopped(),
        ])
}

fn pair(id: &str, topic: &str) -> GroupSnapshot {
    let mut group = group(id, topic, vec![0]);
    let mut second = group.members[0].clone();
    second.id = format!("{id}-m2");
    second.client_id = "c2".into();
    second.host = "10.0.0.2".into();
    second.assignments[0].partitions = vec![1];
    group.members.push(second);
    group
}

#[tokio::test]
async fn groups_list_reads_each_group_from_the_snapshot() {
    let app = TestApp::local().await;

    let listed = structured(&call(&app, "klens_groups_list", json!({})).await);

    assert_eq!(
        listed,
        json!({
            "groups": [{
                "id": "order-processor",
                "state": "STABLE",
                "memberCount": 1,
                "totalLag": 5,
                "lagComplete": true,
            }],
            "showing": "1 of 1",
            "notice": CLIENT_VALUES_NOTICE,
        })
    );
}

#[tokio::test]
async fn detailed_group_rows_match_the_http_rows() {
    let app = TestApp::over(consumers()).await;

    let listed = structured(
        &call(
            &app,
            "klens_groups_list",
            json!({ "responseFormat": "DETAILED" }),
        )
        .await,
    );
    let mut http = app.get("/clusters/local/groups").await.ok();
    http.as_array_mut()
        .expect("rows")
        .sort_by_key(|row| -row["totalLag"].as_i64().expect("a lag"));

    assert_eq!(listed["groups"], http);
}

#[tokio::test]
async fn groups_list_filters_by_name_state_lag_and_topic() {
    let app = TestApp::over(consumers()).await;
    let groups = async |arguments: Value| {
        names(
            &structured(&call(&app, "klens_groups_list", arguments).await),
            "groups",
            "id",
        )
    };

    assert_eq!(
        [
            groups(json!({})).await,
            groups(json!({ "nameContains": "LED" })).await,
            groups(json!({ "state": "EMPTY" })).await,
            groups(json!({ "minLag": 5 })).await,
            groups(json!({ "topic": "payments" })).await,
            groups(json!({ "topic": "payment" })).await,
        ],
        [
            vec!["billing", "order-processor", "archive", "ledger"],
            vec!["ledger"],
            vec!["archive"],
            vec!["billing", "order-processor"],
            vec!["archive", "ledger"],
            vec![],
        ]
        .map(|names| names.into_iter().map(str::to_owned).collect::<Vec<_>>())
    );
}

#[tokio::test]
async fn groups_list_puts_lag_klens_has_not_read_last() {
    let cluster = consumers();
    let app = TestApp::over(cluster.clone()).await;
    cluster.put_group(group("fresh", "orders.created", vec![0]));
    let rig = app.rig();
    rig.poll(&mut rig.topology()).await;

    let listed = structured(&call(&app, "klens_groups_list", json!({ "minLag": 0 })).await);
    let all = structured(&call(&app, "klens_groups_list", json!({ "limit": 5 })).await);

    assert!(!names(&listed, "groups", "id").contains(&"fresh".to_owned()));
    assert_eq!(all["groups"][4]["id"], "fresh");
    assert_eq!(all["groups"][4]["totalLag"], Value::Null);
    assert_eq!(all["groups"][3]["totalLag"], 0);
}

#[tokio::test]
async fn group_describe_joins_members_offsets_and_findings() {
    let app = TestApp::local().await;

    let described = structured(
        &call(
            &app,
            "klens_group_describe",
            json!({ "group": "order-processor" }),
        )
        .await,
    );

    let offset = |partition: i32, committed: i64| {
        json!({
            "topic": "orders.created",
            "partition": partition,
            "committedOffset": committed,
            "endOffset": 8,
            "lag": 8 - committed,
        })
    };
    assert_eq!(
        described,
        json!({
            "group": "order-processor",
            "state": "STABLE",
            "protocol": "range",
            "totalLag": 5,
            "lagComplete": true,
            "findings": [],
            "members": [{
                "memberId": "member-1",
                "clientId": "orders",
                "host": "127.0.0.1",
                "assignments": [{ "topic": "orders.created", "partitions": [0, 1] }],
                "lag": 5,
            }],
            "partitions": [offset(1, 5), offset(0, 6)],
            "notice": CLIENT_VALUES_NOTICE,
        })
    );
    assert!(app.store().interest.is_hot("order-processor"));
}

#[tokio::test]
async fn group_describe_names_the_member_that_holds_the_lag() {
    let cluster = FakeCluster::local()
        .with_topic("payments", 3, 5000)
        .with_groups([pair("billing", "payments")
            .with_committed(&[("payments", 0, 5000), ("payments", 1, 0)])]);
    let app = TestApp::over(cluster).await;

    let described =
        structured(&call(&app, "klens_group_describe", json!({ "group": "billing" })).await);

    assert_eq!(
        described["findings"],
        json!([
            {
                "kind": "LAG_ON_ONE_MEMBER",
                "memberId": "billing-m2",
                "clientId": "c2",
                "host": "10.0.0.2",
                "lag": 5000,
                "totalLag": 5000,
            },
            { "kind": "UNASSIGNED_PARTITIONS", "topic": "payments", "partitions": [2] },
        ])
    );
    assert_eq!(
        names(&described, "members", "memberId"),
        ["billing-m2", "billing-m1"]
    );
    assert_eq!(described["members"][0]["lag"], 5000);
    assert_eq!(described["members"][1]["lag"], 0);
}

#[tokio::test]
async fn group_describe_leaves_lag_klens_has_not_read_null() {
    let cluster = FakeCluster::local();
    let app = TestApp::over(cluster.clone()).await;
    cluster.put_group(pair("fresh", "orders.created").stopped());
    let rig = app.rig();
    rig.poll(&mut rig.topology()).await;

    let described =
        structured(&call(&app, "klens_group_describe", json!({ "group": "fresh" })).await);

    assert_eq!(described["state"], "EMPTY");
    assert_eq!(described["totalLag"], Value::Null);
    assert_eq!(described["findings"], json!([{ "kind": "NO_MEMBERS" }]));
    assert_eq!(described["members"], json!([]));
}

#[tokio::test]
async fn a_member_lag_is_null_until_klens_reads_the_offsets() {
    let cluster = FakeCluster::local();
    let app = TestApp::over(cluster.clone()).await;
    cluster.put_group(pair("fresh", "orders.created"));
    let rig = app.rig();
    rig.poll(&mut rig.topology()).await;

    let described =
        structured(&call(&app, "klens_group_describe", json!({ "group": "fresh" })).await);

    assert_eq!(described["members"][0]["lag"], Value::Null);
    assert_eq!(described["partitions"][0]["committedOffset"], Value::Null);
    assert_eq!(described["partitions"][0]["lag"], Value::Null);
}

#[tokio::test]
async fn a_member_lag_is_null_while_one_of_its_partitions_has_no_end_offset() {
    let cluster = FakeCluster::local();
    let app = TestApp::over(cluster.clone()).await;
    let mut billing = pair("billing", "orders.created")
        .with_committed(&[("orders.created", 0, 6), ("payments", 0, 1)]);
    billing.members[1].assignments[0].topic = "payments".into();
    billing.members[1].assignments[0].partitions = vec![0];
    cluster.add_topic("payments", 1, 4);
    cluster.put_group(billing);
    let rig = app.rig();
    rig.poll(&mut rig.topology()).await;
    rig.sweep(&mut rig.offsets()).await;

    let described =
        structured(&call(&app, "klens_group_describe", json!({ "group": "billing" })).await);

    assert_eq!(described["totalLag"], 2);
    assert_eq!(described["lagComplete"], false);
    assert_eq!(
        described["members"],
        json!([
            {
                "memberId": "billing-m1",
                "clientId": "c1",
                "host": "127.0.0.1",
                "assignments": [{ "topic": "orders.created", "partitions": [0] }],
                "lag": 2,
            },
            {
                "memberId": "billing-m2",
                "clientId": "c2",
                "host": "10.0.0.2",
                "assignments": [{ "topic": "payments", "partitions": [0] }],
                "lag": null,
            },
        ])
    );
}

#[tokio::test]
async fn group_describe_keeps_the_most_lagging_members_that_fit() {
    let mut billing = group("billing", "orders.created", vec![0]);
    let template = billing.members[0].clone();
    billing.members = (0..400)
        .map(|id| {
            let mut member = template.clone();
            member.id = format!("billing-consumer-{id:03}-5f0c1d2e-8a4b-4c3d-9e2f-0a1b2c3d4e5f");
            match id {
                0 | 1 => member.assignments[0].partitions = vec![id],
                _ => member.assignments.clear(),
            }
            member
        })
        .collect();
    let app = TestApp::over(FakeCluster::local().with_groups([billing])).await;

    let result = call(&app, "klens_group_describe", json!({ "group": "billing" })).await;

    let described = structured(&result);
    let shown = described["members"].as_array().expect("members").len();
    assert!(2 < shown && shown < 400, "{shown}");
    assert_eq!(described["members"][1]["lag"], 8);
    assert_eq!(described["members"][2]["lag"], 0);
    assert_eq!(described["partitions"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        described["findings"],
        json!([{ "kind": "MORE_MEMBERS_THAN_PARTITIONS", "members": 400, "partitions": 2 }])
    );
    assert_eq!(
        described["truncated"],
        format!(
            "{} of 400 members left out to fit the result; the lag totals and findings above \
             cover every member and partition",
            400 - shown
        )
    );
    assert!(serde_json::to_vec(&result).expect("json").len() <= RESULT_BYTES);
}

#[tokio::test]
async fn group_describe_caps_each_assignment_and_finding_to_fit() {
    let mut wide = group("wide", "wide", (0..1500).collect());
    let mut second = wide.members[0].clone();
    second.id = "wide-m2".into();
    second.assignments[0].partitions = (1500..3000).collect();
    wide.members.push(second);
    let app = TestApp::over(
        FakeCluster::local()
            .with_topic("wide", 5000, 1)
            .with_groups([wide]),
    )
    .await;

    let result = call(&app, "klens_group_describe", json!({ "group": "wide" })).await;

    let described = structured(&result);
    let held = |row: &Value| {
        row["partitions"].as_array().expect("partitions").len()
            + row["partitionsLeftOut"]
                .as_u64()
                .map_or(0, |rest| rest as usize)
    };
    let members = described["members"].as_array().expect("members");
    let shown = members[0]["assignments"][0]["partitions"]
        .as_array()
        .expect("partitions")
        .len();
    assert!(0 < shown && shown < 1500, "{shown}");
    assert_eq!(members.len(), 2);
    for member in members {
        assert_eq!(held(&member["assignments"][0]), 1500, "{member}");
        assert!(member.get("topicsLeftOut").is_none(), "{member}");
    }
    let finding = &described["findings"][0];
    assert_eq!(finding["kind"], "UNASSIGNED_PARTITIONS");
    assert_eq!(finding["partitions"][0], 3000);
    assert_eq!(held(finding), 2000);
    let rows = described["partitions"]
        .as_array()
        .expect("partitions")
        .len();
    assert_eq!(rows, shown);
    assert_eq!(
        described["truncated"],
        format!(
            "{} of 3000 partitions left out to fit the result; the lag totals and findings above \
             cover every member and partition. Each member names at most {shown} topics, and each \
             member and finding at most {shown} partitions of a topic; `topicsLeftOut` and \
             `partitionsLeftOut` count the rest",
            3000 - shown
        )
    );
    assert!(serde_json::to_vec(&result).expect("json").len() <= RESULT_BYTES);
}

#[tokio::test]
async fn group_describe_sizes_the_result_by_a_finding_wider_than_the_partition_rows() {
    let app = TestApp::over(
        FakeCluster::local()
            .with_topic("wide", 60, 1)
            .with_groups([group("sparse", "wide", vec![0])]),
    )
    .await;

    let result = call(&app, "klens_group_describe", json!({ "group": "sparse" })).await;

    let described = structured(&result);
    assert_eq!(
        described["findings"],
        json!([{
            "kind": "UNASSIGNED_PARTITIONS",
            "topic": "wide",
            "partitions": (1..60).collect::<Vec<_>>(),
        }])
    );
    assert!(described.get("truncated").is_none(), "{described}");
}

#[tokio::test]
async fn group_describe_sizes_the_result_by_a_member_with_more_topics_than_partition_rows() {
    let mut idle = group("idle", "events.00", Vec::new());
    let member = &mut idle.members[0];
    member.assignments = (0..60)
        .map(|topic| {
            let mut assignment = member.assignments[0].clone();
            assignment.topic = format!("events.{topic:02}");
            assignment
        })
        .collect();
    let app = TestApp::over(FakeCluster::local().with_groups([idle])).await;

    let result = call(&app, "klens_group_describe", json!({ "group": "idle" })).await;

    let described = structured(&result);
    let member = &described["members"][0];
    assert_eq!(member["assignments"].as_array().map(Vec::len), Some(60));
    assert!(member.get("topicsLeftOut").is_none(), "{member}");
    assert!(described.get("truncated").is_none(), "{described}");
}

#[tokio::test]
async fn group_describe_keeps_the_findings_that_fit_first() {
    let cluster = FakeCluster::local();
    for topic in 0..400 {
        cluster.add_topic(&format!("events.{topic:03}"), 2, 1);
    }
    let mut spread = group("spread", "events.000", vec![0]);
    let member = &mut spread.members[0];
    member.assignments = (0..400)
        .map(|topic| {
            let mut assignment = member.assignments[0].clone();
            assignment.topic = format!("events.{topic:03}");
            assignment
        })
        .collect();
    let app = TestApp::over(cluster.with_groups([spread])).await;

    let result = call(&app, "klens_group_describe", json!({ "group": "spread" })).await;

    let described = structured(&result);
    let findings = described["findings"].as_array().expect("findings");
    let shown = findings.len();
    assert!(0 < shown && shown < 400, "{shown}");
    assert_eq!(findings[0]["kind"], "UNASSIGNED_PARTITIONS");
    assert_eq!(findings[0]["topic"], "events.000");
    assert!(
        described["truncated"]
            .as_str()
            .is_some_and(|note| note.starts_with(&format!("{} of 400 findings", 400 - shown))),
        "{described}"
    );
    assert!(serde_json::to_vec(&result).expect("json").len() <= RESULT_BYTES);
}

#[tokio::test]
async fn group_describe_shortens_the_names_a_client_chose() {
    let mut billing = pair("billing", "payments");
    let long = "c".repeat(8_000);
    billing.protocol = long.clone();
    for member in &mut billing.members {
        member.id = format!("{long}-{}", member.id);
        member.client_id = long.clone();
        member.host = long.clone();
    }
    let app = TestApp::over(
        FakeCluster::local()
            .with_topic("payments", 2, 5000)
            .with_groups([billing.with_committed(&[("payments", 0, 5000), ("payments", 1, 0)])]),
    )
    .await;

    let result = call(&app, "klens_group_describe", json!({ "group": "billing" })).await;

    let described = structured(&result);
    let shortened = format!("{}…", "c".repeat(256));
    assert_eq!(described["protocol"], shortened);
    let lagging = &described["findings"][0];
    assert_eq!(lagging["kind"], "LAG_ON_ONE_MEMBER");
    for row in [lagging, &described["members"][0], &described["members"][1]] {
        for key in ["memberId", "clientId", "host"] {
            assert_eq!(row[key], shortened, "{key}");
        }
    }
    assert!(described.get("truncated").is_none(), "{described}");
    assert!(serde_json::to_vec(&result).expect("json").len() <= RESULT_BYTES);
}

#[tokio::test]
async fn group_describe_names_an_unknown_group() {
    let app = TestApp::local().await;

    let refused = refusal(&call(&app, "klens_group_describe", json!({ "group": "ghost" })).await);

    assert_eq!(refused["code"], "UNKNOWN_GROUP");
    assert_eq!(refused["hint"], "Call klens_search to find the exact name.");
}
