use std::time::Duration;

use krafka::admin::{
    AdminClient, AlterClientQuotasOptions, AlterConsumerGroupOffsetsOptions,
    AlterUserScramCredentialsOptions, ClientQuotaAlteration, CreateTopicsOptions, NewTopic,
    ScramCredentialUpsertion, TopicPartition,
};
use krafka::auth::ScramMechanism;
use krafka::producer::{Producer, Record};
use testcontainers_modules::kafka::apache;
use testcontainers_modules::testcontainers::runners::AsyncRunner as _;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt as _};
use tokio::time::{Instant, sleep};
use zeroize::Zeroizing;

use crate::{PATIENCE, POLL};

const DEFAULT_IMAGE_TAG: &str = "4.1.0";
const CREATE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Kafka {
    bootstrap_servers: String,
    admin: AdminClient,
    producer: Producer,
    _container: ContainerAsync<apache::Kafka>,
}

impl Kafka {
    pub async fn start() -> Self {
        Self::start_with(&[]).await
    }

    /// Every client here connects as `User:ANONYMOUS`, which stays a super
    /// user so no ACL a test creates locks out the harness or klens.
    pub async fn with_authorizer() -> Self {
        Self::start_with(&[
            (
                "KAFKA_AUTHORIZER_CLASS_NAME",
                "org.apache.kafka.metadata.authorizer.StandardAuthorizer",
            ),
            ("KAFKA_SUPER_USERS", "User:ANONYMOUS"),
        ])
        .await
    }

    async fn start_with(env: &[(&str, &str)]) -> Self {
        let tag = std::env::var("KAFKA_IMAGE_TAG").unwrap_or_else(|_| DEFAULT_IMAGE_TAG.to_owned());
        // The native image's setup step segfaults now and then, and the JVM
        // image's default 1 GiB heap would crowd a runner with a broker per test.
        // Kafka 3.9 advertises the controller listener in combined mode and
        // refuses to start when that listener binds 0.0.0.0.
        let mut request = apache::Kafka::default()
            .with_jvm_image()
            .with_tag(tag)
            .with_env_var("KAFKA_HEAP_OPTS", "-Xms256m -Xmx256m")
            .with_env_var(
                "KAFKA_LISTENERS",
                "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://localhost:9094",
            );
        for (name, value) in env {
            request = request.with_env_var(*name, *value);
        }
        let container = request.start().await.expect("a kafka container starts");
        let port = container
            .get_host_port_ipv4(apache::KAFKA_PORT)
            .await
            .expect("the kafka port is mapped");
        let bootstrap_servers = format!("127.0.0.1:{port}");
        let kafka = krafka::Kafka::builder(&bootstrap_servers)
            .connect()
            .await
            .expect("kafka");
        let admin = kafka.admin();
        let producer = kafka.producer().build().await.expect("producer");

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
            .create_topics(
                [topic],
                CreateTopicsOptions::default().timeout(CREATE_TIMEOUT),
            )
            .await
            .expect("create topics");
        for (name, result) in created {
            result.unwrap_or_else(|error| panic!("creating {name}: {error}"));
        }
    }

    pub async fn send(&self, record: Record) -> i64 {
        self.producer.send(record).await.expect("produce").offset
    }

    pub async fn fill(&self, topic: &str, partition: i32, keys: &[impl AsRef<str>]) {
        for key in keys {
            let key = key.as_ref();
            let record = Record::new(topic, format!("{{\"key\":\"{key}\"}}"))
                .key(key.to_owned())
                .partition(partition);
            self.send(record).await;
        }
    }

    /// A fresh broker answers `NotCoordinator` until it has loaded its
    /// offsets topic, so a commit retries until the coordinator takes it.
    pub async fn commit(&self, group: &str, topic: &str, offsets: &[(i32, i64)]) {
        let deadline = Instant::now() + PATIENCE;
        loop {
            let offsets = offsets
                .iter()
                .map(|&(partition, offset)| (TopicPartition::new(topic, partition), offset));
            let refused: Vec<String> = match self
                .admin
                .alter_consumer_group_offsets(
                    group,
                    offsets,
                    AlterConsumerGroupOffsetsOptions::default(),
                )
                .await
            {
                Ok(partitions) => partitions
                    .into_values()
                    .filter_map(|result| result.err().map(|error| error.to_string()))
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
                vec![ClientQuotaAlteration::new(
                    [("user", Some(user))].into_iter().collect(),
                    vec![(key.to_owned(), Some(value))],
                )],
                AlterClientQuotasOptions::default(),
            )
            .await
            .expect("alter client quotas");
        for result in altered.into_values() {
            result.unwrap_or_else(|error| panic!("setting {key} for {user}: {error}"));
        }
    }

    pub async fn set_scram_credential(
        &self,
        user: &str,
        mechanism: ScramMechanism,
        iterations: i32,
    ) {
        let altered = self
            .admin
            .alter_user_scram_credentials(
                Vec::new(),
                vec![ScramCredentialUpsertion {
                    name: user.to_owned(),
                    mechanism,
                    iterations,
                    salt: Zeroizing::new(vec![1; 16]),
                    salted_password: Zeroizing::new(vec![2; mechanism.hash_length()]),
                }],
                AlterUserScramCredentialsOptions::default(),
            )
            .await
            .expect("alter scram credentials");
        for result in altered.into_values() {
            result.unwrap_or_else(|error| {
                panic!("setting {} for {user}: {error}", mechanism.mechanism_name())
            });
        }
    }
}
