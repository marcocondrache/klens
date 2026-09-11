//! Seed a Kafka cluster with sample topics and JSON messages for local UI testing.
//!
//! This stays in xtask so the klens service remains read-only.

use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use clap::Args;
use futures::future::try_join_all;
use klens::{ClusterConfig, Config, KafkaClusterConfig};
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use rand::{Rng, SeedableRng};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde_json::{Value, json};

/// Sensible defaults for local compose (single broker, plaintext).
const DEFAULT_TOPICS: usize = 12;
const DEFAULT_MESSAGES: usize = 500;
const DEFAULT_PARTITIONS: i32 = 3;
const DEFAULT_REPLICATION: i32 = 1;
const DEFAULT_BATCH_SIZE: usize = 256;
const DEFAULT_PREFIX: &str = "demo";

/// Realistic topic name stems used before falling back to numbered names.
const TOPIC_CATALOG: &[&str] = &[
    "orders.created",
    "orders.updated",
    "orders.cancelled",
    "payments.authorized",
    "payments.captured",
    "inventory.stock-changed",
    "users.profile-updated",
    "users.signed-in",
    "notifications.email",
    "notifications.push",
    "shipping.events",
    "analytics.page-views",
    "search.queries",
    "audit.security",
    "checkout.cart-abandoned",
    "billing.invoices",
];

#[derive(Debug, Args)]
pub struct SeedArgs {
    /// Path to klens `config.yaml` (or set `KLENS_CONFIG_PATH`).
    #[arg(long, default_value_os_t = Config::path())]
    pub config: PathBuf,

    /// Cluster name from the config file.
    #[arg(long, default_value = "local")]
    pub cluster: String,

    /// Override bootstrap servers (comma-separated). Skips loading cluster
    /// settings from the config file when set.
    #[arg(long, value_name = "HOST:PORT[,...]")]
    pub bootstrap: Option<String>,

    /// Number of topics to ensure.
    #[arg(long, default_value_t = DEFAULT_TOPICS)]
    pub topics: usize,

    /// Messages to produce per topic.
    #[arg(long, default_value_t = DEFAULT_MESSAGES)]
    pub messages: usize,

    /// Partitions for newly created topics.
    #[arg(long, default_value_t = DEFAULT_PARTITIONS)]
    pub partitions: i32,

    /// Replication factor for newly created topics.
    #[arg(long, default_value_t = DEFAULT_REPLICATION)]
    pub replication_factor: i32,

    /// Prefix applied to catalog and numbered topic names.
    #[arg(long, default_value = DEFAULT_PREFIX)]
    pub prefix: String,

    /// RNG seed for reproducible keys and payloads.
    #[arg(long)]
    pub seed: Option<u64>,

    /// How many produce requests to queue before awaiting delivery.
    #[arg(long, default_value_t = DEFAULT_BATCH_SIZE)]
    pub batch_size: usize,

    /// Print the plan without contacting Kafka.
    #[arg(long)]
    pub dry_run: bool,
}

pub fn run(args: SeedArgs) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to start tokio runtime")?;
    runtime.block_on(run_async(args))
}

async fn run_async(args: SeedArgs) -> Result<()> {
    validate(&args)?;

    let cluster = resolve_cluster(&args)?;
    let topic_names = plan_topic_names(&args.prefix, args.topics);
    let rng_seed = args.seed.unwrap_or_else(entropy_seed);
    let total_messages = args.topics.saturating_mul(args.messages);

    println!(
        "seed plan: cluster={} bootstrap={} topics={} messages/topic={} total={} partitions={} rf={} batch={} rng_seed={} dry_run={}",
        cluster.name,
        cluster.bootstrap_servers.join(","),
        topic_names.len(),
        args.messages,
        total_messages,
        args.partitions,
        args.replication_factor,
        args.batch_size,
        rng_seed,
        args.dry_run,
    );

    for name in &topic_names {
        println!("  topic {name}");
    }

    if args.dry_run {
        println!("dry-run: no topics created and no messages produced");
        return Ok(());
    }

    let mut client = KafkaClusterConfig::from(&cluster).into_client_config();
    client.set(
        "client.id",
        format!("klens-{}-seed", cluster.name.trim()),
    );
    // Prefer throughput for bulk seeding; linger a little so batches fill.
    client.set("acks", "1");
    client.set("linger.ms", "5");
    client.set("compression.type", "lz4");
    client.set("message.timeout.ms", "30000");

    let admin: AdminClient<DefaultClientContext> = client
        .create()
        .context("failed to create Kafka admin client")?;
    let producer: FutureProducer = client
        .create()
        .context("failed to create Kafka producer")?;

    create_topics(
        &admin,
        &topic_names,
        args.partitions,
        args.replication_factor,
    )
    .await?;

    let mut rng = StdRng::seed_from_u64(rng_seed);
    let produced = produce_messages(
        &producer,
        &topic_names,
        args.messages,
        args.batch_size,
        &mut rng,
    )
    .await?;

    println!(
        "seeded {} topic(s) with {} message(s) (rng_seed={rng_seed})",
        topic_names.len(),
        produced
    );
    Ok(())
}

fn validate(args: &SeedArgs) -> Result<()> {
    if args.topics == 0 {
        bail!("--topics must be greater than 0");
    }
    if args.messages == 0 {
        bail!("--messages must be greater than 0");
    }
    if args.partitions <= 0 {
        bail!("--partitions must be greater than 0");
    }
    if args.replication_factor <= 0 {
        bail!("--replication-factor must be greater than 0");
    }
    if args.batch_size == 0 {
        bail!("--batch-size must be greater than 0");
    }
    if args.prefix.trim().is_empty() {
        bail!("--prefix must not be empty");
    }
    Ok(())
}

fn resolve_cluster(args: &SeedArgs) -> Result<ClusterConfig> {
    if let Some(bootstrap) = &args.bootstrap {
        let servers = parse_bootstrap(bootstrap)?;
        let cluster = ClusterConfig {
            name: args.cluster.clone(),
            bootstrap_servers: servers,
            security: None,
            schema_registry: None,
            properties: Default::default(),
        };
        cluster
            .validate()
            .context("invalid --bootstrap / --cluster combination")?;
        return Ok(cluster);
    }

    let config = Config::load(&args.config)
        .with_context(|| format!("failed to load config from {}", args.config.display()))?;
    let cluster = config
        .clusters
        .iter()
        .find(|cluster| cluster.name == args.cluster)
        .cloned()
        .with_context(|| {
            format!(
                "cluster '{}' not found in {}; available: {}",
                args.cluster,
                args.config.display(),
                config
                    .clusters
                    .iter()
                    .map(|cluster| cluster.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
    Ok(cluster)
}

fn parse_bootstrap(raw: &str) -> Result<Vec<String>> {
    let servers: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect();
    if servers.is_empty() {
        bail!("--bootstrap must list at least one host:port");
    }
    Ok(servers)
}

fn plan_topic_names(prefix: &str, count: usize) -> Vec<String> {
    let prefix = prefix.trim().trim_matches('.');
    (0..count)
        .map(|index| {
            if let Some(stem) = TOPIC_CATALOG.get(index) {
                format!("{prefix}.{stem}")
            } else {
                let extra = index - TOPIC_CATALOG.len() + 1;
                format!("{prefix}.extra.{extra:04}")
            }
        })
        .collect()
}

fn entropy_seed() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0xC0FFEE)
}

async fn create_topics(
    admin: &AdminClient<DefaultClientContext>,
    names: &[String],
    partitions: i32,
    replication_factor: i32,
) -> Result<()> {
    let topics: Vec<NewTopic<'_>> = names
        .iter()
        .map(|name| {
            NewTopic::new(
                name,
                partitions,
                TopicReplication::Fixed(replication_factor),
            )
        })
        .collect();

    let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));
    let results = admin
        .create_topics(topics.iter(), &options)
        .await
        .context("create_topics request failed")?;

    for result in results {
        match result {
            Ok(name) => println!("created topic {name}"),
            Err((name, RDKafkaErrorCode::TopicAlreadyExists)) => {
                println!("topic {name} already exists");
            }
            Err((name, code)) => {
                bail!("failed to create topic {name}: {code}");
            }
        }
    }

    Ok(())
}

async fn produce_messages(
    producer: &FutureProducer,
    topics: &[String],
    messages_per_topic: usize,
    batch_size: usize,
    rng: &mut StdRng,
) -> Result<usize> {
    let mut produced = 0usize;
    let mut batch = Vec::with_capacity(batch_size);
    let base_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(1_700_000_000_000);

    for topic in topics {
        for index in 0..messages_per_topic {
            let payload = build_payload(topic, index, base_ts, rng);
            batch.push(StagedMessage {
                topic: topic.clone(),
                key: payload.key,
                event_type: payload.event_type,
                timestamp: payload.timestamp,
                value: payload.value.to_string(),
            });

            if batch.len() >= batch_size {
                produced += flush_batch(producer, &mut batch).await?;
            }
        }
    }

    produced += flush_batch(producer, &mut batch).await?;
    Ok(produced)
}

struct StagedMessage {
    topic: String,
    key: String,
    event_type: String,
    timestamp: i64,
    value: String,
}

async fn flush_batch(producer: &FutureProducer, batch: &mut Vec<StagedMessage>) -> Result<usize> {
    if batch.is_empty() {
        return Ok(0);
    }

    let mut deliveries = Vec::with_capacity(batch.len());
    for message in batch.drain(..) {
        let producer = producer.clone();
        deliveries.push(async move {
            let headers = OwnedHeaders::new()
                .insert(rdkafka::message::Header {
                    key: "source",
                    value: Some("xtask-seed"),
                })
                .insert(rdkafka::message::Header {
                    key: "event_type",
                    value: Some(message.event_type.as_str()),
                });
            producer
                .send(
                    FutureRecord::to(&message.topic)
                        .payload(&message.value)
                        .key(&message.key)
                        .headers(headers)
                        .timestamp(message.timestamp),
                    Timeout::After(Duration::from_secs(30)),
                )
                .await
                .map_err(|(error, _)| error)
        });
    }

    let count = deliveries.len();
    try_join_all(deliveries)
        .await
        .context("failed to produce message")?;
    Ok(count)
}

#[derive(Debug)]
struct GeneratedMessage {
    key: String,
    event_type: String,
    timestamp: i64,
    value: Value,
}

fn build_payload(topic: &str, index: usize, base_ts: i64, rng: &mut StdRng) -> GeneratedMessage {
    let timestamp = base_ts + (index as i64) * 37 + i64::from(rng.random_range(0..25));
    let stem = topic.rsplit('.').next().unwrap_or(topic);

    if topic.contains("orders") {
        let order_id = format!("ord_{:08}", index + 1);
        return GeneratedMessage {
            key: order_id.clone(),
            event_type: format!("order.{stem}"),
            timestamp,
            value: json!({
                "orderId": order_id,
                "customerId": format!("cust_{}", rng.random_range(1..50_000)),
                "status": pick(rng, &["created", "paid", "fulfilled", "cancelled"]),
                "currency": pick(rng, &["USD", "EUR", "GBP"]),
                "totalCents": rng.random_range(199..250_000),
                "items": rng.random_range(1..8),
                "channel": pick(rng, &["web", "ios", "android", "pos"]),
            }),
        };
    }

    if topic.contains("payments") {
        let payment_id = format!("pay_{:08}", index + 1);
        return GeneratedMessage {
            key: payment_id.clone(),
            event_type: format!("payment.{stem}"),
            timestamp,
            value: json!({
                "paymentId": payment_id,
                "orderId": format!("ord_{:08}", rng.random_range(1..100_000)),
                "amountCents": rng.random_range(100..500_000),
                "currency": pick(rng, &["USD", "EUR", "GBP"]),
                "method": pick(rng, &["card", "paypal", "bank_transfer", "apple_pay"]),
                "authorized": rng.random_bool(0.92),
            }),
        };
    }

    if topic.contains("inventory") {
        let sku = format!("sku-{}", rng.random_range(1000..9999));
        return GeneratedMessage {
            key: sku.clone(),
            event_type: "inventory.stock_changed".into(),
            timestamp,
            value: json!({
                "sku": sku,
                "warehouse": pick(rng, &["ams-1", "ber-2", "lon-1", "nyc-3"]),
                "delta": rng.random_range(-20..40),
                "onHand": rng.random_range(0..5_000),
            }),
        };
    }

    if topic.contains("users") {
        let user_id = format!("user_{:06}", rng.random_range(1..200_000));
        return GeneratedMessage {
            key: user_id.clone(),
            event_type: format!("user.{stem}"),
            timestamp,
            value: json!({
                "userId": user_id,
                "email": format!("user{}@example.com", rng.random_range(1..200_000)),
                "locale": pick(rng, &["en-US", "en-GB", "de-DE", "fr-FR", "nl-NL"]),
                "plan": pick(rng, &["free", "pro", "enterprise"]),
            }),
        };
    }

    if topic.contains("notifications") {
        let notification_id = format!("ntf_{:08}", index + 1);
        return GeneratedMessage {
            key: notification_id.clone(),
            event_type: format!("notification.{stem}"),
            timestamp,
            value: json!({
                "notificationId": notification_id,
                "userId": format!("user_{:06}", rng.random_range(1..200_000)),
                "channel": if topic.contains("email") { "email" } else { "push" },
                "template": pick(rng, &["welcome", "receipt", "shipping", "password_reset"]),
                "delivered": rng.random_bool(0.97),
            }),
        };
    }

    if topic.contains("shipping") {
        let shipment_id = format!("shp_{:08}", index + 1);
        return GeneratedMessage {
            key: shipment_id.clone(),
            event_type: "shipping.event".into(),
            timestamp,
            value: json!({
                "shipmentId": shipment_id,
                "orderId": format!("ord_{:08}", rng.random_range(1..100_000)),
                "carrier": pick(rng, &["dhl", "ups", "fedex", "postnl"]),
                "status": pick(rng, &["label_created", "in_transit", "out_for_delivery", "delivered"]),
                "trackingNumber": format!("TRK{}", rng.random_range(10_000_000..99_999_999)),
            }),
        };
    }

    if topic.contains("analytics") || topic.contains("page-views") {
        let session_id = format!("ses_{:010}", rng.random_range(1..1_000_000_000u64));
        return GeneratedMessage {
            key: session_id.clone(),
            event_type: "analytics.page_view".into(),
            timestamp,
            value: json!({
                "sessionId": session_id,
                "path": pick(rng, &["/", "/pricing", "/docs", "/app/topics", "/app/groups"]),
                "referrer": pick(rng, &["direct", "google", "newsletter", "twitter"]),
                "userAgent": pick(rng, &["chrome", "firefox", "safari", "edge"]),
                "durationMs": rng.random_range(120..45_000),
            }),
        };
    }

    if topic.contains("search") {
        let query_id = format!("q_{:08}", index + 1);
        return GeneratedMessage {
            key: query_id.clone(),
            event_type: "search.query".into(),
            timestamp,
            value: json!({
                "queryId": query_id,
                "term": pick(rng, &["kafka", "consumer lag", "schema registry", "partition", "acl"]),
                "hits": rng.random_range(0..500),
                "tookMs": rng.random_range(1..250),
            }),
        };
    }

    if topic.contains("audit") || topic.contains("security") {
        let event_id = format!("aud_{:08}", index + 1);
        return GeneratedMessage {
            key: event_id.clone(),
            event_type: "audit.security".into(),
            timestamp,
            value: json!({
                "eventId": event_id,
                "actor": format!("user_{:06}", rng.random_range(1..50_000)),
                "action": pick(rng, &["login", "logout", "role_change", "api_token_created"]),
                "ip": format!(
                    "{}.{}.{}.{}",
                    rng.random_range(1..223),
                    rng.random_range(0..255),
                    rng.random_range(0..255),
                    rng.random_range(1..254)
                ),
                "success": rng.random_bool(0.95),
            }),
        };
    }

    if topic.contains("billing") || topic.contains("invoices") {
        let invoice_id = format!("inv_{:08}", index + 1);
        return GeneratedMessage {
            key: invoice_id.clone(),
            event_type: "billing.invoice".into(),
            timestamp,
            value: json!({
                "invoiceId": invoice_id,
                "customerId": format!("cust_{}", rng.random_range(1..50_000)),
                "amountCents": rng.random_range(999..1_000_000),
                "currency": pick(rng, &["USD", "EUR"]),
                "status": pick(rng, &["open", "paid", "void", "past_due"]),
            }),
        };
    }

    if topic.contains("checkout") || topic.contains("cart") {
        let cart_id = format!("cart_{:08}", index + 1);
        return GeneratedMessage {
            key: cart_id.clone(),
            event_type: "checkout.cart_abandoned".into(),
            timestamp,
            value: json!({
                "cartId": cart_id,
                "customerId": format!("cust_{}", rng.random_range(1..50_000)),
                "items": rng.random_range(1..12),
                "valueCents": rng.random_range(500..80_000),
                "idleMinutes": rng.random_range(15..720),
            }),
        };
    }

    // Generic fallback for numbered extra topics.
    let key = format!("msg_{:08}", index + 1);
    GeneratedMessage {
        key: key.clone(),
        event_type: "demo.event".into(),
        timestamp,
        value: json!({
            "id": key,
            "topic": topic,
            "sequence": index,
            "region": pick(rng, &["eu-west-1", "us-east-1", "ap-southeast-1"]),
            "severity": rng.random_range(0.0..1.0),
            "tags": [
                pick(rng, &["alpha", "beta", "canary", "stable"]),
                pick(rng, &["batch", "realtime", "backfill"]),
            ],
        }),
    }
}

fn pick<'a, T>(rng: &mut StdRng, options: &'a [T]) -> &'a T {
    options
        .choose(rng)
        .expect("pick requires a non-empty options slice")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_catalog_then_numbered_extras() {
        let names = plan_topic_names("demo", TOPIC_CATALOG.len() + 2);
        assert_eq!(names[0], "demo.orders.created");
        assert_eq!(
            names[TOPIC_CATALOG.len() - 1],
            format!("demo.{}", TOPIC_CATALOG[TOPIC_CATALOG.len() - 1])
        );
        assert_eq!(names[TOPIC_CATALOG.len()], "demo.extra.0001");
        assert_eq!(names[TOPIC_CATALOG.len() + 1], "demo.extra.0002");
    }

    #[test]
    fn payloads_are_reproducible_with_seed() {
        let mut a = StdRng::seed_from_u64(42);
        let mut b = StdRng::seed_from_u64(42);
        let left = build_payload("demo.orders.created", 7, 1_700_000_000_000, &mut a);
        let right = build_payload("demo.orders.created", 7, 1_700_000_000_000, &mut b);
        assert_eq!(left.key, right.key);
        assert_eq!(left.value, right.value);
        assert_eq!(left.timestamp, right.timestamp);
    }

    #[test]
    fn parse_bootstrap_splits_hosts() {
        assert_eq!(
            parse_bootstrap("localhost:9092, broker:9093").unwrap(),
            vec!["localhost:9092".to_owned(), "broker:9093".to_owned()]
        );
        assert!(parse_bootstrap(" , ").is_err());
    }

    #[test]
    fn validate_rejects_zero_counts() {
        let mut args = SeedArgs {
            config: PathBuf::from("config.yaml"),
            cluster: "local".into(),
            bootstrap: None,
            topics: 0,
            messages: 10,
            partitions: 1,
            replication_factor: 1,
            prefix: "demo".into(),
            seed: None,
            batch_size: 10,
            dry_run: true,
        };
        assert!(validate(&args).is_err());
        args.topics = 1;
        args.messages = 0;
        assert!(validate(&args).is_err());
    }
}
