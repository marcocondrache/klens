use std::time::{Duration, SystemTime, UNIX_EPOCH};

use e2e::{Kafka, Klens};
use reqwest::StatusCode;
use serde_json::json;
use tokio::time::sleep;

use crate::row;

const GROUPS: &str = "/api/clusters/local/groups";
const BILLING_OFFSETS: &str = "/api/clusters/local/group-offsets/billing";

async fn billing_behind_by_six() -> (Kafka, Klens) {
    let kafka = Kafka::start().await;
    kafka.topic("orders", 1).await;
    kafka.fill("orders", 0, &["a"; 10]).await;
    kafka.commit("billing", "orders", &[(0, 4)]).await;
    let klens = Klens::over(&kafka).await;
    klens
        .eventually(GROUPS, |groups| {
            row(groups, "id", "billing")["totalLag"] == 6
        })
        .await;
    (kafka, klens)
}

#[tokio::test]
async fn a_group_shows_how_far_its_commits_trail_the_log_end() {
    let (_kafka, klens) = billing_behind_by_six().await;

    let billing = klens.get(&format!("{GROUPS}/billing")).await;

    assert_eq!(billing["state"], "EMPTY");
    assert_eq!(billing["lagComplete"], true);
    assert_eq!(
        billing["offsets"],
        json!([{
            "topic": "orders",
            "partition": 0,
            "currentOffset": 4,
            "endOffset": 10,
            "lag": 6,
            "memberId": null
        }])
    );
}

#[tokio::test]
async fn a_commit_moves_the_lag_live_on_the_updates_stream() {
    let (kafka, klens) = billing_behind_by_six().await;
    let mut updates = klens
        .open("/api/clusters/local/updates?group=billing")
        .await;

    kafka.commit("billing", "orders", &[(0, 10)]).await;
    let caught_up = updates
        .find(|event| event.name == "groupLag" && event.data["lag"] == 0)
        .await;

    assert_eq!(caught_up.data["group"], "billing");
    assert_eq!(caught_up.data["offsets"][0]["currentOffset"], 10);
}

#[tokio::test]
async fn a_deleted_group_is_gone_once_the_delete_answers() {
    let (_kafka, klens) = billing_behind_by_six().await;
    let billing = format!("{GROUPS}/billing");

    let (status, body) = klens.delete(&billing).await;

    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    assert_eq!(klens.status(&billing).await, StatusCode::NOT_FOUND);
    let groups = klens.get(GROUPS).await;
    assert!(
        groups
            .as_array()
            .expect("groups")
            .iter()
            .all(|group| group["id"] != "billing"),
        "{groups}"
    );
}

#[tokio::test]
async fn deleted_offsets_leave_the_group_once_the_delete_answers() {
    let (kafka, klens) = billing_behind_by_six().await;
    kafka.topic("refunds", 1).await;
    kafka.commit("billing", "refunds", &[(0, 0)]).await;
    let billing = format!("{GROUPS}/billing");
    klens
        .eventually(&billing, |group| {
            group["offsets"]
                .as_array()
                .is_some_and(|offsets| offsets.len() == 2)
        })
        .await;

    let (status, body) = klens
        .delete(&format!("{BILLING_OFFSETS}?topic=refunds"))
        .await;

    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");
    let offsets = &klens.get(&billing).await["offsets"];
    assert_eq!(offsets.as_array().map(Vec::len), Some(1), "{offsets}");
    assert_eq!(offsets[0]["topic"], "orders");
}

#[tokio::test]
async fn a_reset_previews_its_plan_and_shows_once_it_answers() {
    let (_kafka, klens) = billing_behind_by_six().await;
    let shift = json!({ "kind": "SHIFT", "by": -3 });

    let (status, plan) = klens
        .patch(BILLING_OFFSETS, &json!({ "to": shift, "dryRun": true }))
        .await;
    assert_eq!(status, StatusCode::OK, "{plan}");
    assert_eq!(
        plan,
        json!([{
            "topic": "orders",
            "partition": 0,
            "currentOffset": 4,
            "newOffset": 1,
            "endOffset": 10
        }])
    );
    assert_eq!(klens.get(&format!("{GROUPS}/billing")).await["totalLag"], 6);

    let (status, applied) = klens.patch(BILLING_OFFSETS, &json!({ "to": shift })).await;

    assert_eq!(status, StatusCode::OK, "{applied}");
    assert_eq!(applied, plan);
    let billing = klens.get(&format!("{GROUPS}/billing")).await;
    assert_eq!(billing["offsets"][0]["currentOffset"], 1);
    assert_eq!(billing["totalLag"], 9);
}

#[tokio::test]
async fn a_reset_to_a_time_lands_on_the_first_record_at_or_after_it() {
    let kafka = Kafka::start().await;
    kafka.topic("orders", 1).await;
    kafka.fill("orders", 0, &["early"]).await;
    sleep(Duration::from_millis(50)).await;
    let cut = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock after the epoch")
        .as_millis();
    sleep(Duration::from_millis(50)).await;
    kafka.fill("orders", 0, &["late", "later"]).await;
    kafka.commit("billing", "orders", &[(0, 0)]).await;
    let klens = Klens::over(&kafka).await;
    klens
        .eventually(GROUPS, |groups| {
            row(groups, "id", "billing")["totalLag"] == 3
        })
        .await;

    let (status, plan) = klens
        .patch(
            BILLING_OFFSETS,
            &json!({
                "topic": "orders",
                "to": { "kind": "TIMESTAMP", "timestamp": cut },
                "dryRun": true
            }),
        )
        .await;

    assert_eq!(status, StatusCode::OK, "{plan}");
    assert_eq!(plan[0]["newOffset"], 1);
}
