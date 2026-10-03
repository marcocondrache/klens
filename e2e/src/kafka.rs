use std::time::Duration;

use krafka::admin::{AdminClient, NewTopic, QuotaAlteration};
use krafka::producer::{Producer, ProducerRecord};
use testcontainers_modules::kafka::apache;
use testcontainers_modules::testcontainers::runners::AsyncRunner as _;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt as _};
use tokio::time::{Instant, sleep};

use crate::{PATIENCE, POLL};

const IMAGE_TAG: &str = "4.1.0";
const CREATE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Kafka {
    bootstrap_servers: String,
    admin: AdminClient,
    producer: Producer,
    _container: ContainerAsync<apache::Kafka>,
}

impl Kafka {
    pub async fn start() -> Self {
        // The native image's setup step segfaults now and then, and the JVM
        // image's default 1 GiB heap would crowd a runner with a broker per test.
        let container = apache::Kafka::default()
            .with_jvm_image()
            .with_tag(IMAGE_TAG)
            .with_env_var("KAFKA_HEAP_OPTS", "-Xms256m -Xmx256m")
            .start()
            .await
            .expect("a kafka container starts");
        let port = container
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("the kafka port is mapped");
        let bootstrap_servers = format!("127.0.0.1:{port}");
        let admin = AdminClient::builder()
            .bootstrap_servers(&bootstrap_servers)
            .build()
            .await
            .expect("admin client");
        let producer = Producer::builder()
            .bootstrap_servers(&bootstrap_servers)
            .build()
            .await
            .expect("producer");

        Self {
            bootstrap_servers,
            admin,
            producer,
            _container: container,
        }
    }

    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    pub async fn topic(&self, name: &str, partitions: i32) {
        self.create(NewTopic::new(name, partitions, 1).expect("a valid topic"))
            .await;
    }

    pub async fn create(&self, topic: NewTopic) {
        let created = self
            .admin
            .create_topics(vec![topic], CREATE_TIMEOUT, false)
            .await
            .expect("create topics");
        for topic in created {
            assert_eq!(topic.error, None, "creating {}", topic.name);
        }
    }

    pub async fn send(&self, record: ProducerRecord) -> i64 {
        self.producer
            .send_record(record)
            .await
            .expect("produce")
            .offset
    }

    pub async fn fill(&self, topic: &str, partition: i32, keys: &[impl AsRef<str>]) {
        for key in keys {
            let key = key.as_ref();
            let record = ProducerRecord::new(topic, format!("{{\"key\":\"{key}\"}}"))
                .with_key(key.to_owned())
                .with_partition(partition);
            self.send(record).await;
        }
    }

    /// A fresh broker answers `NotCoordinator` until it has loaded its
    /// offsets topic, so a commit retries until the coordinator takes it.
    pub async fn commit(&self, group: &str, topic: &str, offsets: &[(i32, i64)]) {
        let deadline = Instant::now() + PATIENCE;
        loop {
            let refused: Vec<String> = match self
                .admin
                .alter_consumer_group_offsets(group, &[(topic, offsets)])
                .await
            {
                Ok(partitions) => partitions
                    .into_iter()
                    .filter_map(|partition| partition.error)
                    .collect(),
                Err(error) => vec![error.to_string()],
            };
            if refused.is_empty() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "committing {group} on {topic} kept failing: {refused:?}"
            );
            sleep(POLL).await;
        }
    }

    pub async fn set_user_quota(&self, user: &str, key: &str, value: f64) {
        let altered = self
            .admin
            .alter_client_quotas(
                &[QuotaAlteration {
                    entity: vec![("user", Some(user))],
                    ops: vec![(key, Some(value))],
                }],
                false,
            )
            .await
            .expect("alter client quotas");
        for entity in altered {
            assert_eq!(entity.error, None, "setting {key} for {user}");
        }
    }
}
