use std::sync::Arc;

use crate::kafka::store::{Change, TopologyDelta};
use crate::testing::{Api, FakeCluster, Rig};

#[tokio::test]
async fn the_topology_lane_assembles_brokers_topics_and_groups() {
    let rig = Rig::local();

    rig.poll(&rig.topology()).await;

    let topology = rig.store.topology.load().expect("topology");
    assert_eq!(topology.cluster_id.as_deref(), Some("test-cluster"));
    assert_eq!(topology.brokers.len(), 1);
    assert_eq!(topology.topics["orders.created"].partitions.len(), 2);
    assert_eq!(
        topology.groups_for_topic("orders.created"),
        [Arc::from("order-processor")],
        "the reverse index is built at commit time"
    );
    assert_eq!(rig.store.topic_rows().len(), 1);
    assert_eq!(rig.store.broker_rows()[0].host, "localhost");
}

#[tokio::test]
async fn a_topology_poll_that_changes_nothing_commits_and_publishes_nothing() {
    let rig = Rig::local();
    let lane = rig.topology();
    let mut bus = rig.store.bus.probe();

    rig.poll(&lane).await;
    rig.poll(&lane).await;

    assert_eq!(rig.cluster.calls(Api::Metadata), 2);
    assert_eq!(
        rig.store.topology.version(),
        1,
        "an unchanged cluster costs a hash-equal check and nothing else"
    );
    bus.next(Change::topology);
    bus.assert_quiet();
}

#[tokio::test]
async fn a_new_partition_is_published_as_a_changed_topic() {
    let rig = Rig::local();
    let lane = rig.topology();
    rig.poll(&lane).await;
    let mut bus = rig.store.bus.probe();

    rig.cluster.add_partition("orders.created", 2);
    rig.poll(&lane).await;

    assert_eq!(
        *bus.next(Change::topology),
        TopologyDelta {
            changed_topics: vec![Arc::from("orders.created")],
            ..TopologyDelta::default()
        }
    );
}

#[tokio::test]
async fn the_topology_lane_keeps_search_current() {
    let rig = Rig::local();

    rig.poll(&rig.topology()).await;
    rig.poll(&rig.subjects()).await;

    let hits = rig.store.search("order");
    let ids: Vec<&str> = hits.iter().map(|hit| hit.id.as_str()).collect();
    assert_eq!(
        ids,
        ["orders.created", "order-processor", "orders.created-value"],
        "topics, groups and subjects are all searchable"
    );
}

#[tokio::test(start_paused = true)]
async fn a_deleted_topic_loses_its_rate() {
    let rig = Rig::new(FakeCluster::local().with_topic("payments", 1, 4));
    let lane = rig.topology();
    rig.poll(&lane).await;
    rig.poll(&rig.watermarks()).await;
    assert_eq!(rig.store.rates.get("payments"), Some(0.0));

    rig.cluster.remove_topic("payments");
    rig.poll(&lane).await;

    assert_eq!(rig.store.rates.get("payments"), None);
    assert_eq!(rig.store.rates.get("orders.created"), Some(0.0));
}
