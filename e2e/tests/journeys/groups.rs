use e2e::{Kafka, Klens};
use serde_json::json;

use crate::row;

const GROUPS: &str = "/api/clusters/local/groups";

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
