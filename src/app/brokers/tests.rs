use axum::http::StatusCode;

use super::super::harness::{failure, ok, ok_as, seeded, viewer_everywhere};

#[tokio::test]
async fn broker_rows_count_the_partitions_each_node_carries() {
    let state = seeded();
    let data = ok(&state, "/clusters/local/brokers").await;

    assert_eq!(data[0]["host"], "localhost");
    assert_eq!(data[0]["partitionCount"], 3);
    assert_eq!(data[0]["leaderCount"], 3);
}

#[tokio::test]
async fn broker_configs_stay_live_because_no_lane_sweeps_them() {
    let state = seeded();
    let configs = ok(&state, "/clusters/local/brokers/1/configs").await;

    assert_eq!(configs[0]["name"], "log.retention.hours");
}

#[tokio::test]
async fn broker_configs_are_forbidden_without_the_configs_privilege() {
    let (status, code) = failure(
        &seeded(),
        "/clusters/local/brokers/1/configs",
        viewer_everywhere(),
    )
    .await;

    assert_eq!(
        (status, code.as_str()),
        (StatusCode::FORBIDDEN, "FORBIDDEN")
    );
}

#[tokio::test]
async fn broker_rows_stay_open_to_a_viewer() {
    let brokers = ok_as(&seeded(), "/clusters/local/brokers", viewer_everywhere()).await;

    assert_eq!(brokers[0]["id"], 1);
}
