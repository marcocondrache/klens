use super::*;

#[tokio::test]
async fn brokers_list_reads_hosts_and_log_dirs_from_the_snapshot() {
    let app = TestApp::local().await;

    let listed = structured(&call(&app, "klens_brokers_list", json!({})).await);

    assert_eq!(
        listed,
        json!({
            "brokers": [{
                "id": 1,
                "host": "localhost",
                "port": 9092,
                "rack": null,
                "controller": null,
                "partitionCount": 2,
                "leaderCount": 2,
                "sizeBytes": 6144,
                "logDirs": [{
                    "path": "/var/lib/kafka/data",
                    "error": null,
                    "totalBytes": 1_000_000,
                    "usableBytes": 750_000,
                    "cordoned": false,
                    "sizeBytes": 6144,
                    "replicaCount": 2,
                }],
            }],
            "showing": "1 of 1",
        })
    );
    assert_eq!(app.cluster().calls(Api::LogDirs), 0);
}

#[tokio::test]
async fn brokers_list_leaves_unread_log_dirs_null_and_stops_at_the_limit() {
    let cluster = FakeCluster::local();
    cluster.add_broker(2);
    cluster.add_broker(3);
    let app = TestApp::of([cluster]).build();
    let rig = app.rig();
    rig.poll(&rig.topology()).await;

    let listed = structured(&call(&app, "klens_brokers_list", json!({ "limit": 2 })).await);

    let brokers = listed["brokers"].as_array().expect("brokers");
    assert_eq!(brokers.len(), 2);
    assert_eq!(brokers[0]["sizeBytes"], Value::Null);
    assert_eq!(brokers[0]["logDirs"], Value::Null);
    assert_eq!(
        listed["showing"],
        "2 of 3; pass the last id shown as `after`, or raise `limit`, to see others"
    );
}

#[tokio::test]
async fn brokers_list_reaches_the_brokers_past_the_budget_with_after() {
    let cluster = FakeCluster::local();
    for id in 2..=150 {
        cluster.add_broker(id);
    }
    let app = TestApp::over(cluster).await;

    let first = call(&app, "klens_brokers_list", json!({ "limit": 100 })).await;
    let page = structured(&first);
    let ids: Vec<i64> = page["brokers"]
        .as_array()
        .expect("brokers")
        .iter()
        .map(|broker| broker["id"].as_i64().expect("an id"))
        .collect();
    let last = *ids.last().expect("a broker");
    let rest = structured(
        &call(
            &app,
            "klens_brokers_list",
            json!({ "after": last, "limit": 100 }),
        )
        .await,
    );

    assert!(25 < ids.len() && ids.len() < 100, "{}", ids.len());
    assert_eq!(ids, (1..=last).collect::<Vec<_>>());
    assert_eq!(
        page["showing"],
        format!(
            "{} of 150, as no more fit the result; pass the last id shown as `after` to see \
             others",
            ids.len()
        )
    );
    assert!(serde_json::to_vec(&first).expect("json").len() <= RESULT_BYTES);
    assert_eq!(rest["brokers"][0]["id"], last + 1);
    assert_eq!(rest["showing"], format!("{} of {}", 150 - last, 150 - last));
}

#[tokio::test]
async fn brokers_list_reads_one_brokers_overrides_from_the_http_configs() {
    let cluster = FakeCluster::local();
    cluster.set_broker_configs(
        1,
        vec![
            config("log.retention.hours", "168", ConfigSource::Default),
            config("log.retention.hours", "72", ConfigSource::DynamicBroker),
            config("num.io.threads", "16", ConfigSource::StaticBroker),
        ],
    );
    let app = TestApp::over(cluster).await;

    let described = structured(&call(&app, "klens_brokers_list", json!({ "broker": 1 })).await);
    let http = app.get("/clusters/local/brokers/1/configs").await.ok();
    let listed = structured(&call(&app, "klens_brokers_list", json!({})).await);

    let overrides: Vec<&Value> = http
        .as_array()
        .expect("configs")
        .iter()
        .filter(|entry| entry["source"] != "DEFAULT_CONFIG")
        .collect();
    assert_eq!(described["configs"], json!(overrides));
    assert_eq!(overrides.len(), 2);
    let mut row = described.clone();
    row.as_object_mut().expect("a broker").remove("configs");
    assert_eq!(listed["brokers"], json!([row]));
    assert_eq!(app.cluster().calls(Api::BrokerConfigs), 2);
}

#[tokio::test]
async fn broker_configs_draw_on_the_live_budget_only_when_read() {
    let app = TestApp::of([FakeCluster::local()])
        .limits(Limits {
            mcp_live_calls_per_minute: NonZeroU32::MIN,
            ..Limits::new(&Tuning::default())
        })
        .ingested()
        .await;
    let viewer = app.serving_mcp(Mcp::holding([]));
    let describe = async |app: &TestApp, broker: i32| {
        call(app, "klens_brokers_list", json!({ "broker": broker })).await
    };

    let unknown = refusal(&describe(&app, 7).await);
    let paged = refusal(
        &call(
            &app,
            "klens_brokers_list",
            json!({ "broker": 1, "limit": 5 }),
        )
        .await,
    );
    let withheld = structured(&describe(&viewer, 1).await);
    let served = structured(&describe(&app, 1).await);
    let spent = refusal(&describe(&app, 1).await);

    assert_eq!(unknown["code"], "UNKNOWN_BROKER");
    assert_eq!(unknown["error"], "unknown broker 7 in cluster 'local'");
    assert_eq!(paged["error"], "pass `broker` without `after` or `limit`");
    assert_eq!(withheld["configs"], Value::Null);
    assert_eq!(
        withheld["omitted"],
        json!({ "section": "configs", "needs": "BROKER_CONFIGS" })
    );
    assert_eq!(withheld["id"], 1);
    assert_eq!(served["configs"], json!([]));
    assert_eq!(spent["code"], "RATE_LIMITED");
    assert_eq!(app.cluster().calls(Api::BrokerConfigs), 1);
}
