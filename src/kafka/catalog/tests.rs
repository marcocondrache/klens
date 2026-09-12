use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};

use super::*;
use crate::kafka::QueryEngine;
use crate::kafka::broker::Broker;
use crate::kafka::cluster::{ClusterIdentity, ClusterOverview};
use crate::kafka::error::KafkaError;
use crate::kafka::topic::Topic;

use super::snapshot::empty_identity;
use crate::kafka::cluster::ClusterHealth;
use crate::kafka::group::{ConsumerGroup, GroupState};
use crate::kafka::rates::RateStore;
use crate::kafka::registry::{SchemaCompatibility, SchemaSubject, SchemaType};
use crate::kafka::session::ClusterSession;
use crate::kafka::testing::FakeCluster;
use crate::kafka::topic_config::CleanupPolicy;

fn start_from_engine(
    catalog: CatalogCache,
    subjects: SubjectCache,
    engine: Arc<QueryEngine<dyn ClusterSession>>,
    rates: RateStore,
    catalog_interval: Duration,
    subject_interval: Duration,
    config_interval: Duration,
) -> CatalogPoller {
    let clusters: Vec<String> = engine.names().into_iter().map(str::to_owned).collect();
    let catalog_engine = Arc::clone(&engine);
    CatalogPoller::start(
        catalog,
        subjects,
        clusters,
        CatalogPollerIntervals {
            catalog: catalog_interval,
            subjects: subject_interval,
            configs: config_interval,
        },
        CatalogPollerIo {
            fetch_catalog: move |cluster: String, reuse, fetch_configs| {
                let engine = Arc::clone(&catalog_engine);
                async move {
                    engine
                        .assemble_catalog(&cluster, Some(&reuse), fetch_configs)
                        .await
                }
            },
            observe: {
                let rates = rates.clone();
                move |cluster: &str, counts| rates.observe(cluster, counts)
            },
            fetch_subjects: move |cluster: String| {
                let engine = Arc::clone(&engine);
                async move { engine.schema_subjects(&cluster).await }
            },
        },
    )
}

fn test_group(id: &str) -> ConsumerGroup {
    ConsumerGroup {
        id: id.to_owned(),
        state: GroupState::Stable,
        protocol: "range".into(),
        coordinator: 1,
        members: Vec::new(),
        topics: vec!["orders".into()],
        lag: 4,
        offsets: Vec::new(),
    }
}

fn test_subject(name: &str) -> SchemaSubject {
    SchemaSubject {
        subject: name.to_owned(),
        id: 1,
        schema_type: SchemaType::Avro,
        latest_version: 1,
        versions: vec![1],
        compatibility: SchemaCompatibility::Backward,
        schema: "{}".into(),
    }
}

fn test_topic(name: &str) -> Topic {
    Topic {
        name: name.to_owned(),
        internal: false,
        partitions: Vec::new(),
        replication_factor: 1,
        message_count: 0,
        cleanup_policy: CleanupPolicy::Delete,
        retention_ms: 0,
        consumer_groups: Vec::new(),
        under_replicated: false,
    }
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..200 {
        if predicate() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition not met");
}

#[test]
fn store_overwrites_and_seed_does_not() {
    let cache = CatalogCache::new();
    cache.store(
        "local",
        ClusterSnapshot::from_topics(vec![test_topic("first")]),
    );
    cache.store(
        "local",
        ClusterSnapshot::from_topics(vec![test_topic("second")]),
    );
    assert_eq!(cache.topic("local", "second").unwrap().name, "second");
    assert!(cache.topic("local", "first").is_none());

    assert!(!cache.seed(
        "local",
        ClusterSnapshot::from_topics(vec![test_topic("seeded")])
    ));
    assert_eq!(cache.topic("local", "second").unwrap().name, "second");

    let other = CatalogCache::new();
    assert!(other.seed(
        "staging",
        ClusterSnapshot::from_topics(vec![test_topic("seeded")])
    ));
    assert_eq!(other.topic("staging", "seeded").unwrap().name, "seeded");
    assert!(other.updated_at("staging").is_some());
    assert!(other.snapshot("missing").is_none());

    cache.invalidate("local");
    assert!(cache.snapshot("local").is_none());
    cache.invalidate("missing");
}

#[test]
fn record_poll_keeps_last_error_until_a_success() {
    let cache = CatalogCache::new();
    cache.record_poll("local", Duration::from_millis(9), Some("down".into()));
    assert_eq!(cache.poll_lane("local").last_error.as_deref(), Some("down"));
    assert_eq!(cache.poll_lane("local").last_poll_duration_ms, Some(9));
    cache.record_poll("local", Duration::from_millis(4), None);
    assert_eq!(cache.poll_lane("local").last_error, None);
    assert_eq!(cache.poll_lane("local").last_poll_duration_ms, Some(4));
}

#[test]
fn store_notifies_only_when_the_roster_changes() {
    let cache = CatalogCache::new();
    let mut updates = cache.subscribe_updates("local");
    let _ = updates.borrow_and_update();

    cache.store(
        "local",
        ClusterSnapshot::from_topics(vec![test_topic("orders")]),
    );
    assert_eq!(cache.generation("local"), Some(1));
    assert!(updates.has_changed().unwrap());
    assert_eq!(updates.borrow_and_update().as_ref().unwrap().generation, 1);

    let mut louder = test_topic("orders");
    louder.message_count = 40;
    cache.store("local", ClusterSnapshot::from_topics(vec![louder]));
    assert_eq!(cache.generation("local"), Some(1));
    assert!(!updates.has_changed().unwrap());

    cache.store(
        "local",
        ClusterSnapshot::from_topics(vec![test_topic("orders"), test_topic("payments")]),
    );
    assert_eq!(cache.generation("local"), Some(2));
    assert!(updates.has_changed().unwrap());
    let revision = updates.borrow_and_update().clone().unwrap();
    assert_eq!(revision.cluster, "local");
    assert_eq!(revision.generation, 2);
}

#[test]
fn store_notifies_each_cluster_on_its_own_watch() {
    let cache = CatalogCache::new();
    let mut local = cache.subscribe_updates("local");
    let mut other = cache.subscribe_updates("other");
    let _ = local.borrow_and_update();
    let _ = other.borrow_and_update();

    cache.store(
        "local",
        ClusterSnapshot::from_topics(vec![test_topic("orders")]),
    );
    cache.store(
        "other",
        ClusterSnapshot::from_topics(vec![test_topic("payments")]),
    );

    assert_eq!(local.borrow_and_update().as_ref().unwrap().cluster, "local");
    assert_eq!(other.borrow_and_update().as_ref().unwrap().cluster, "other");
}

#[test]
fn snapshot_reads_share_one_arc() {
    let cache = CatalogCache::new();
    cache.store(
        "local",
        ClusterSnapshot::from_topics(vec![test_topic("orders")]),
    );
    let first = cache.snapshot("local").unwrap();
    let second = cache.snapshot("local").unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(first.topic("orders").unwrap().name, "orders");
}

#[test]
fn snapshot_looks_up_topics_and_groups_by_name() {
    let snapshot = ClusterSnapshot::from_catalog(
        vec![test_topic("orders"), test_topic("payments")],
        vec![test_group("orders-app"), test_group("payments-app")],
    );
    assert_eq!(snapshot.topic("payments").unwrap().name, "payments");
    assert!(snapshot.topic("missing").is_none());
    assert_eq!(snapshot.group("payments-app").unwrap().id, "payments-app");
    assert!(snapshot.group("missing").is_none());
    assert_eq!(
        snapshot.groups_for_topic(Some("orders"))[0].id,
        "orders-app"
    );
    assert!(snapshot.groups_for_topic(Some("missing")).is_empty());
    assert_eq!(snapshot.groups_for_topic(None).len(), 2);
    assert_eq!(
        snapshot.message_counts(),
        HashMap::from([("orders".into(), 0), ("payments".into(), 0)])
    );
    assert!(snapshot.broker(1).is_none());
    assert_eq!(snapshot.overview.health, ClusterHealth::Offline);
}

#[test]
fn snapshot_looks_up_brokers_by_id() {
    let snapshot = ClusterSnapshot::assemble(
        Vec::new(),
        Vec::new(),
        vec![Broker {
            id: 3,
            host: "broker-c".into(),
            port: 9092,
            rack: None,
            controller: false,
            partition_count: 2,
            leader_count: 1,
        }],
        ClusterOverview::offline(ClusterIdentity {
            name: "local".into(),
            bootstrap_servers: vec!["localhost:9092".into()],
            security_protocol: crate::config::SecurityProtocol::Plaintext,
        }),
    );
    assert_eq!(snapshot.broker(3).unwrap().host, "broker-c");
    assert!(snapshot.broker(1).is_none());
}

#[test]
fn cache_group_lookup_is_by_id() {
    let cache = CatalogCache::new();
    cache.store(
        "local",
        ClusterSnapshot::from_groups(vec![test_group("cached")]),
    );
    assert_eq!(cache.group("local", "cached").unwrap().lag, 4);
    assert!(cache.group("local", "missing").is_none());
    assert!(cache.group("other", "cached").is_none());
}

#[test]
fn cache_broker_lookup_is_by_id() {
    let cache = CatalogCache::new();
    cache.store(
        "local",
        ClusterSnapshot::assemble(
            Vec::new(),
            Vec::new(),
            vec![Broker {
                id: 7,
                host: "cached-broker".into(),
                port: 9093,
                rack: None,
                controller: false,
                partition_count: 4,
                leader_count: 2,
            }],
            ClusterOverview::offline(empty_identity()),
        ),
    );
    assert_eq!(cache.broker("local", 7).unwrap().host, "cached-broker");
    assert!(cache.broker("local", 1).is_none());
    assert!(cache.broker("other", 7).is_none());
}

#[tokio::test(start_paused = true)]
async fn first_poll_runs_immediately_then_respects_the_interval() {
    let polls = Arc::new(AtomicUsize::new(0));
    let cache = CatalogCache::new();
    let counter = Arc::clone(&polls);
    let _poller = CatalogPoller::start_with(
        cache.clone(),
        ["local"],
        Duration::from_secs(5),
        move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok(ClusterSnapshot::from_topics(Vec::new())) }
        },
    );

    wait_until(|| polls.load(Ordering::SeqCst) >= 1).await;
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert!(cache.snapshot("local").is_some());

    tokio::time::advance(Duration::from_secs(4)).await;
    tokio::task::yield_now().await;
    assert_eq!(polls.load(Ordering::SeqCst), 1);

    tokio::time::advance(Duration::from_secs(1) + Duration::from_millis(1)).await;
    wait_until(|| polls.load(Ordering::SeqCst) >= 2).await;
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn kick_runs_a_poll_before_the_interval() {
    let polls = Arc::new(AtomicUsize::new(0));
    let cache = CatalogCache::new();
    let counter = Arc::clone(&polls);
    let poller = CatalogPoller::start_with(cache, ["local"], Duration::from_secs(60), move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        async { Ok(ClusterSnapshot::from_topics(Vec::new())) }
    });

    wait_until(|| polls.load(Ordering::SeqCst) >= 1).await;
    assert_eq!(polls.load(Ordering::SeqCst), 1);

    poller.kick("local");
    wait_until(|| polls.load(Ordering::SeqCst) >= 2).await;
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    poller.kick("missing");
}

#[tokio::test(start_paused = true)]
async fn kick_during_fetch_runs_again_when_it_finishes() {
    let polls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&polls);
    let poller = CatalogPoller::start_with(
        CatalogCache::new(),
        ["local"],
        Duration::from_secs(60),
        move |_| {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            async move {
                if n == 0 {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Ok(ClusterSnapshot::from_topics(Vec::new()))
            }
        },
    );

    tokio::task::yield_now().await;
    poller.kick("local");
    tokio::time::advance(Duration::from_secs(5) + Duration::from_millis(1)).await;
    wait_until(|| polls.load(Ordering::SeqCst) >= 2).await;
    assert_eq!(polls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn invalidate_then_kick_stores_the_catalog_again() {
    let engine = Arc::new(QueryEngine::from_sessions(vec![FakeCluster::local()]));
    let cache = CatalogCache::new();
    let poller = start_from_engine(
        cache.clone(),
        SubjectCache::new(),
        engine,
        RateStore::new(),
        Duration::from_secs(60),
        Duration::from_secs(60),
        Duration::from_secs(60),
    );

    wait_until(|| cache.snapshot("local").is_some()).await;
    let first = cache.snapshot("local").unwrap();
    cache.invalidate("local");
    assert!(cache.snapshot("local").is_none());

    poller.kick("local");
    wait_until(|| cache.snapshot("local").is_some()).await;
    let second = cache.snapshot("local").unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(second.body_eq(&first));
}

#[tokio::test(start_paused = true)]
async fn a_failed_poll_keeps_the_previous_snapshot() {
    let polls = Arc::new(AtomicUsize::new(0));
    let cache = CatalogCache::new();
    cache.store(
        "local",
        ClusterSnapshot::from_catalog(vec![test_topic("kept")], vec![test_group("kept-group")]),
    );
    let counter = Arc::clone(&polls);
    let _poller = CatalogPoller::start_with(
        cache.clone(),
        ["local"],
        Duration::from_secs(5),
        move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Err(KafkaError::UnknownCluster("local".into())) }
        },
    );

    wait_until(|| polls.load(Ordering::SeqCst) >= 1).await;
    assert_eq!(cache.topic("local", "kept").unwrap().name, "kept");
    assert_eq!(cache.group("local", "kept-group").unwrap().id, "kept-group");
}

#[tokio::test(start_paused = true)]
async fn one_cluster_failing_does_not_block_others() {
    let cache = CatalogCache::new();
    let _poller = CatalogPoller::start_with(
        cache.clone(),
        ["good", "bad"],
        Duration::from_secs(5),
        |cluster| async move {
            if cluster == "bad" {
                Err(KafkaError::Admin("broker down".into()))
            } else {
                Ok(ClusterSnapshot::from_topics(vec![test_topic("ok")]))
            }
        },
    );

    wait_until(|| cache.snapshot("good").is_some()).await;
    assert_eq!(cache.topic("good", "ok").unwrap().name, "ok");
    assert!(cache.snapshot("bad").is_none());
}

#[tokio::test(start_paused = true)]
async fn a_slow_cluster_does_not_block_others() {
    let cache = CatalogCache::new();
    let _poller = CatalogPoller::start_with(
        cache.clone(),
        ["slow", "fast"],
        Duration::from_secs(5),
        |cluster| async move {
            if cluster == "slow" {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            Ok(ClusterSnapshot::from_topics(vec![test_topic(&cluster)]))
        },
    );

    wait_until(|| cache.snapshot("fast").is_some()).await;
    assert_eq!(cache.topic("fast", "fast").unwrap().name, "fast");
    assert!(cache.snapshot("slow").is_none());

    tokio::time::advance(Duration::from_secs(30)).await;
    wait_until(|| cache.snapshot("slow").is_some()).await;
    assert_eq!(cache.topic("slow", "slow").unwrap().name, "slow");
}

#[tokio::test(start_paused = true)]
async fn dropping_the_poller_stops_further_polls() {
    let polls = Arc::new(AtomicUsize::new(0));
    let cache = CatalogCache::new();
    let counter = Arc::clone(&polls);
    let poller = CatalogPoller::start_with(cache, ["local"], Duration::from_secs(5), move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
        async { Ok(ClusterSnapshot::from_topics(Vec::new())) }
    });

    wait_until(|| polls.load(Ordering::SeqCst) >= 1).await;
    drop(poller);
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::task::yield_now().await;
    assert_eq!(polls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn start_polls_query_engine_catalog() {
    let engine = Arc::new(QueryEngine::from_sessions(vec![FakeCluster::local()]));
    let cache = CatalogCache::new();
    let rates = RateStore::new();
    let subjects = SubjectCache::new();
    let _poller = start_from_engine(
        cache.clone(),
        subjects.clone(),
        engine,
        rates.clone(),
        Duration::from_secs(60),
        Duration::from_secs(60),
        Duration::from_secs(60),
    );

    wait_until(|| cache.snapshot("local").is_some() && subjects.snapshot("local").is_some()).await;
    let snapshot = cache.snapshot("local").unwrap();
    assert_eq!(snapshot.topics[0].name, "orders.created");
    assert_eq!(snapshot.topics[0].message_count, 16);
    assert_eq!(snapshot.groups[0].id, "order-processor");
    assert_eq!(snapshot.groups[0].lag, 5);
    assert_eq!(snapshot.groups[0].topics, vec!["orders.created"]);
    assert_eq!(snapshot.brokers[0].id, 1);
    assert_eq!(snapshot.brokers[0].partition_count, 2);
    assert_eq!(snapshot.brokers[0].leader_count, 2);
    assert_eq!(snapshot.overview.cluster_id, "test-cluster");
    assert_eq!(snapshot.overview.health, ClusterHealth::Healthy);
    assert_eq!(snapshot.overview.broker_count, 1);
    assert_eq!(snapshot.overview.topic_count, 1);
    assert_eq!(snapshot.overview.consumer_group_count, 1);
    assert!(snapshot.updated_at >= DateTime::<Utc>::UNIX_EPOCH);
    assert_eq!(
        rates
            .topic_rate("local", "orders.created")
            .unwrap()
            .messages_per_sec,
        0.0
    );
    assert_eq!(
        subjects.snapshot("local").unwrap()[0].subject,
        "orders.created-value"
    );
}

#[tokio::test(start_paused = true)]
async fn poller_observes_rate_store_from_catalog_counts() {
    let engine = Arc::new(QueryEngine::from_sessions(vec![FakeCluster::local()]));
    let cache = CatalogCache::new();
    let rates = RateStore::new();
    let _poller = start_from_engine(
        cache.clone(),
        SubjectCache::new(),
        engine,
        rates.clone(),
        Duration::from_secs(5),
        Duration::from_secs(60),
        Duration::from_secs(60),
    );

    wait_until(|| !rates.topic_rates("local").is_empty()).await;
    assert_eq!(rates.cluster_history("local").len(), 1);

    tokio::time::advance(Duration::from_secs(5) + Duration::from_millis(1)).await;
    wait_until(|| rates.cluster_history("local").len() >= 2).await;
    assert_eq!(
        rates
            .topic_rate("local", "orders.created")
            .unwrap()
            .messages_per_sec,
        0.0
    );
}

#[tokio::test(start_paused = true)]
async fn unchanged_catalog_poll_keeps_the_same_arc() {
    let engine = Arc::new(QueryEngine::from_sessions(vec![FakeCluster::local()]));
    let cache = CatalogCache::new();
    let rates = RateStore::new();
    let _poller = start_from_engine(
        cache.clone(),
        SubjectCache::new(),
        engine,
        rates.clone(),
        Duration::from_secs(5),
        Duration::from_secs(60),
        Duration::from_secs(60),
    );

    wait_until(|| cache.snapshot("local").is_some()).await;
    let first = cache.snapshot("local").unwrap();

    tokio::time::advance(Duration::from_secs(5) + Duration::from_millis(1)).await;
    wait_until(|| rates.cluster_history("local").len() >= 2).await;
    let second = cache.snapshot("local").unwrap();
    assert!(Arc::ptr_eq(&first, &second));
}

#[test]
fn subject_store_overwrites_and_seed_does_not() {
    let cache = SubjectCache::new();
    cache.store("local", vec![test_subject("first")]);
    cache.store("local", vec![test_subject("second")]);
    assert_eq!(cache.snapshot("local").unwrap()[0].subject, "second");
    assert!(!cache.seed("local", vec![test_subject("seeded")]));
    assert_eq!(cache.snapshot("local").unwrap()[0].subject, "second");

    let other = SubjectCache::new();
    assert!(other.seed("staging", vec![test_subject("seeded")]));
    assert_eq!(other.snapshot("staging").unwrap()[0].subject, "seeded");
    assert!(other.snapshot("missing").is_none());

    cache.invalidate("local");
    assert!(cache.snapshot("local").is_none());
    assert!(cache.poll_lane("local").updated_at.is_some());
}

#[tokio::test]
async fn failed_subject_poll_keeps_catalog_and_previous_subjects() {
    let engine = Arc::new(QueryEngine::from_sessions(vec![
        FakeCluster::local().with_subjects_error("registry down"),
    ]));
    let cache = CatalogCache::new();
    let subjects = SubjectCache::new();
    subjects.store("local", vec![test_subject("kept")]);
    let _poller = start_from_engine(
        cache.clone(),
        subjects.clone(),
        engine,
        RateStore::new(),
        Duration::from_secs(60),
        Duration::from_secs(60),
        Duration::from_secs(60),
    );

    wait_until(|| cache.snapshot("local").is_some()).await;
    assert_eq!(
        cache.topic("local", "orders.created").unwrap().name,
        "orders.created"
    );
    assert_eq!(subjects.snapshot("local").unwrap()[0].subject, "kept");
}

#[tokio::test]
async fn failed_catalog_poll_still_fills_subjects() {
    let engine = Arc::new(QueryEngine::from_sessions(vec![
        FakeCluster::local().unreachable(),
    ]));
    let cache = CatalogCache::new();
    cache.store(
        "local",
        ClusterSnapshot::from_topics(vec![test_topic("kept")]),
    );
    let subjects = SubjectCache::new();
    let _poller = start_from_engine(
        cache.clone(),
        subjects.clone(),
        engine,
        RateStore::new(),
        Duration::from_secs(60),
        Duration::from_secs(60),
        Duration::from_secs(60),
    );

    wait_until(|| subjects.snapshot("local").is_some()).await;
    assert_eq!(cache.topic("local", "kept").unwrap().name, "kept");
    assert_eq!(
        subjects.snapshot("local").unwrap()[0].subject,
        "orders.created-value"
    );
}
