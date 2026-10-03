use std::sync::Arc;

use futures::StreamExt;
use schemreg::{
    CachedSchemaRegistry, ConfluentSchemaRegistry, RetryPolicy, Schema, SchemaId, SchemaRegError,
    SchemaRegistryClient as _, SchemaVersion,
};
use secrecy::ExposeSecret;
use tokio::sync::OnceCell;

use crate::config::{SchemaRegistry, SchemaRegistryTuning};
use crate::kafka::error::KafkaError;
use crate::kafka::model::{RegisteredSchema, SchemaCompatibility, SchemaReference, SchemaSubject};

const MAX_CACHED_SCHEMAS: usize = 10_000;

pub(crate) type Registry = CachedSchemaRegistry<ConfluentSchemaRegistry>;

#[derive(Clone)]
pub struct SchemaRegistryClient {
    cluster: String,
    registry: Arc<Registry>,
    fetch_concurrency: usize,
}

impl SchemaRegistryClient {
    pub fn new(
        cluster: impl Into<String>,
        config: &SchemaRegistry,
        tuning: &SchemaRegistryTuning,
    ) -> Result<Self, KafkaError> {
        let cluster = cluster.into();

        let mut builder = ConfluentSchemaRegistry::builder()
            .url(config.url.as_str())
            .request_timeout(tuning.timeout)
            .retry_policy(RetryPolicy::none());

        if let Some(auth) = &config.auth {
            builder = builder.basic_auth(&auth.username, auth.password.expose_secret());
        }

        let inner = builder
            .build()
            .map_err(|error| KafkaError::SchemaRegistry {
                cluster: cluster.clone(),
                message: error.to_string(),
            })?;

        Ok(Self {
            cluster,
            registry: Arc::new(CachedSchemaRegistry::with_max_entries(
                inner,
                MAX_CACHED_SCHEMAS,
            )),
            fetch_concurrency: tuning.subject_fetch_concurrency.get(),
        })
    }

    pub(crate) fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    pub(crate) fn fetch_concurrency(&self) -> usize {
        self.fetch_concurrency
    }

    pub(crate) fn fail(&self, message: impl Into<String>) -> KafkaError {
        KafkaError::SchemaRegistry {
            cluster: self.cluster.clone(),
            message: message.into(),
        }
    }

    pub async fn subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        let names = self
            .registry
            .get_subjects()
            .await
            .map_err(|error| self.fail(error.to_string()))?;

        let global = OnceCell::new();

        let mut subjects: Vec<SchemaSubject> = futures::stream::iter(names)
            .map(|name| {
                let global = &global;
                async move {
                    let loaded = self.load_subject(&name, global).await;
                    if let Err(error) = &loaded {
                        tracing::warn!(subject = %name, %error, "skipping subject");
                    }
                    loaded.ok()
                }
            })
            .buffer_unordered(self.fetch_concurrency)
            .filter_map(std::future::ready)
            .collect()
            .await;

        subjects.sort_by(|left, right| left.subject.cmp(&right.subject));
        Ok(subjects)
    }

    pub async fn schema_by_subject_version(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        let latest = self
            .registry
            .get_schema_by_version(subject, SchemaVersion::new(version))
            .await
            .map_err(|error| self.fail(error.to_string()))?;
        self.registered(&latest)
    }

    async fn load_subject(
        &self,
        name: &str,
        global: &OnceCell<SchemaCompatibility>,
    ) -> Result<SchemaSubject, KafkaError> {
        let (versions, latest, compatibility) = tokio::try_join!(
            async {
                self.registry
                    .get_versions(name)
                    .await
                    .map_err(|error| self.fail(error.to_string()))
            },
            async {
                self.registry
                    .get_latest_schema(name)
                    .await
                    .map_err(|error| self.fail(error.to_string()))
            },
            async { Ok(self.compatibility(name, global).await) },
        )?;

        Ok(SchemaSubject {
            subject: name.to_owned(),
            id: schema_id(&latest).ok_or_else(|| self.fail("schema is missing id"))?,
            schema_type: latest.schema_type.into(),
            latest_version: latest
                .version
                .map(SchemaVersion::as_i32)
                .ok_or_else(|| self.fail("schema is missing version"))?,
            versions: versions.into_iter().map(SchemaVersion::as_i32).collect(),
            compatibility,
        })
    }

    async fn compatibility(
        &self,
        name: &str,
        global: &OnceCell<SchemaCompatibility>,
    ) -> SchemaCompatibility {
        match self.registry.get_compatibility(name).await {
            Ok(level) => level.into(),
            Err(error) if is_unconfigured(&error) => {
                *global.get_or_init(|| self.global_compatibility()).await
            }
            Err(error) => {
                tracing::warn!(subject = %name, %error, "falling back to NONE compatibility");
                SchemaCompatibility::None
            }
        }
    }

    async fn global_compatibility(&self) -> SchemaCompatibility {
        match self.registry.get_compatibility("").await {
            Ok(level) => level.into(),
            Err(error) if is_unconfigured(&error) => SchemaCompatibility::None,
            Err(error) => {
                tracing::warn!(%error, "falling back to NONE compatibility");
                SchemaCompatibility::None
            }
        }
    }

    fn registered(&self, schema: &Schema) -> Result<RegisteredSchema, KafkaError> {
        Ok(RegisteredSchema {
            id: schema_id(schema).ok_or_else(|| self.fail("schema is missing id"))?,
            schema_type: schema.schema_type.into(),
            schema: schema.schema.to_string(),
            references: references(&schema.references),
        })
    }
}

/// Confluent reports a subject with no compatibility override as `40408`, which
/// is a 404 the crate does not classify as not-found because it is not about a
/// missing subject. Registries that do not implement the code answer a bare
/// 404 instead.
fn is_unconfigured(error: &SchemaRegError) -> bool {
    error.is_not_found()
        || error.error_code()
            == Some(schemreg::error::error_code::SUBJECT_COMPATIBILITY_NOT_CONFIGURED)
}

pub(crate) fn schema_id(schema: &Schema) -> Option<i32> {
    i32::try_from(SchemaId::as_u32(schema.id?)).ok()
}

pub(crate) fn references(references: &[schemreg::SchemaReference]) -> Vec<SchemaReference> {
    references
        .iter()
        .map(|reference| SchemaReference {
            name: reference.name.clone(),
            subject: reference.subject.clone(),
            version: reference.version.as_i32(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::model::SchemaType;
    use crate::kafka::registry::testing::{FakeRegistry, Schema};
    use crate::testing::yaml;

    fn offline(tuning: &SchemaRegistryTuning) -> SchemaRegistryClient {
        SchemaRegistryClient::new("local", &yaml("url: http://localhost:8081"), tuning).unwrap()
    }

    fn string() -> Schema {
        Schema::avro(r#""string""#)
    }

    async fn registry_of(subjects: &[&str]) -> FakeRegistry {
        let registry = FakeRegistry::start().await;
        for (id, subject) in (1..).zip(subjects) {
            registry.register(subject, 1, id, string());
        }
        registry
    }

    #[test]
    fn subject_fetches_run_as_wide_as_tuning_allows() {
        let tuning = SchemaRegistryTuning {
            subject_fetch_concurrency: std::num::NonZeroUsize::new(3).unwrap(),
            ..SchemaRegistryTuning::default()
        };

        assert_eq!(offline(&tuning).fetch_concurrency(), 3);
    }

    #[tokio::test]
    async fn lists_subjects_with_their_latest_schema() {
        let registry = FakeRegistry::start().await;
        for (version, id) in [(1, 5), (2, 6), (3, 7)] {
            registry.register("orders-value", version, id, string());
        }
        registry.set_compatibility("orders-value", "FULL");

        let subjects = registry.client().subjects().await.unwrap();

        assert_eq!(subjects.len(), 1);
        let subject = &subjects[0];
        assert_eq!(subject.subject, "orders-value");
        assert_eq!(subject.id, 7);
        assert_eq!(subject.latest_version, 3);
        assert_eq!(subject.versions, vec![1, 2, 3]);
        assert_eq!(subject.schema_type, SchemaType::Avro);
        assert_eq!(subject.compatibility, SchemaCompatibility::Full);
    }

    #[tokio::test]
    async fn subjects_come_back_sorted_by_name() {
        let registry = registry_of(&["b-value", "a-value"]).await;

        let subjects = registry.client().subjects().await.unwrap();

        let names: Vec<&str> = subjects.iter().map(|s| s.subject.as_str()).collect();
        assert_eq!(names, vec!["a-value", "b-value"]);
    }

    #[tokio::test]
    async fn compatibility_comes_from_the_effective_config() {
        let registry = registry_of(&["orders-value"]).await;
        registry.set_compatibility("orders-value", "FORWARD_TRANSITIVE");

        let subjects = registry.client().subjects().await.unwrap();

        assert_eq!(subjects[0].compatibility, SchemaCompatibility::Forward);
        assert_eq!(
            registry.hits("/config/orders-value").await,
            1,
            "one effective-config read per subject"
        );
    }

    #[tokio::test]
    async fn a_registry_that_defaults_to_global_answers_in_one_read() {
        let registry = registry_of(&["orders-value"]).await;
        registry.set_global_compatibility("BACKWARD_TRANSITIVE");

        let subjects = registry.client().subjects().await.unwrap();

        assert_eq!(subjects[0].compatibility, SchemaCompatibility::Backward);
        assert_eq!(registry.hits("/config").await, 0);
    }

    #[tokio::test]
    async fn a_missing_config_is_not_an_error() {
        let registry = registry_of(&["orders-value"]).await;

        let subjects = registry.client().subjects().await.unwrap();

        assert_eq!(subjects.len(), 1);
        assert_eq!(subjects[0].compatibility, SchemaCompatibility::None);
    }

    #[tokio::test]
    async fn a_subject_without_an_override_falls_back_to_the_global_default() {
        let registry = registry_of(&["a-value", "b-value"]).await;
        registry.set_global_compatibility("BACKWARD_TRANSITIVE");
        registry.ignore_default_to_global();

        let subjects = registry.client().subjects().await.unwrap();

        assert_eq!(subjects.len(), 2);
        for subject in &subjects {
            assert_eq!(subject.compatibility, SchemaCompatibility::Backward);
        }
        assert_eq!(
            registry.hits("/config").await,
            1,
            "the global default is read once per sweep"
        );
    }

    #[tokio::test]
    async fn a_failing_subject_is_skipped_not_fatal() {
        let registry = registry_of(&["good-value", "bad-value"]).await;
        registry.fail("/subjects/bad-value/versions");

        let subjects = registry.client().subjects().await.unwrap();

        let names: Vec<&str> = subjects.iter().map(|s| s.subject.as_str()).collect();
        assert_eq!(names, vec!["good-value"]);
    }

    #[tokio::test]
    async fn a_failing_subject_list_is_fatal() {
        let registry = registry_of(&["orders-value"]).await;
        registry.fail("/subjects");

        let error = registry.client().subjects().await.unwrap_err();

        assert!(matches!(error, KafkaError::SchemaRegistry { .. }));
    }

    #[tokio::test]
    async fn reads_one_subject_version_with_its_references() {
        let registry = FakeRegistry::start().await;
        registry.register(
            "orders-value",
            2,
            9,
            Schema::protobuf("syntax = \"proto3\";").referencing("common.proto", "common", 1),
        );

        let schema = registry
            .client()
            .schema_by_subject_version("orders-value", 2)
            .await
            .unwrap();

        assert_eq!(schema.id, 9);
        assert_eq!(schema.schema_type, SchemaType::Protobuf);
        assert_eq!(schema.references.len(), 1);
        assert_eq!(schema.references[0].name, "common.proto");
        assert_eq!(schema.references[0].subject, "common");
        assert_eq!(schema.references[0].version, 1);
    }

    #[tokio::test]
    async fn subject_names_are_percent_encoded() {
        let registry = registry_of(&["orders/v1-value"]).await;

        let subjects = registry.client().subjects().await.unwrap();

        assert_eq!(subjects[0].subject, "orders/v1-value");
        assert_eq!(
            registry.hits("/subjects/orders%2Fv1-value/versions").await,
            1
        );
    }

    #[tokio::test]
    async fn basic_auth_is_sent_when_configured() {
        let registry = FakeRegistry::start().await;
        let client = SchemaRegistryClient::new(
            "local",
            &yaml(&format!(
                "{{url: '{}', auth: {{username: user, password: {{value: secret}}}}}}",
                registry.uri()
            )),
            &SchemaRegistryTuning::default(),
        )
        .unwrap();

        client.subjects().await.unwrap();

        let expected = format!(
            "Basic {}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, "user:secret")
        );
        let sent = registry.requests().await;
        assert_eq!(
            sent[0]
                .headers
                .get("authorization")
                .map(|value| value.to_str().unwrap()),
            Some(expected.as_str())
        );
    }

    #[test]
    fn a_registry_error_names_its_cluster() {
        let error = offline(&SchemaRegistryTuning::default()).fail("boom");

        assert_eq!(
            error.to_string(),
            "schema registry request failed for cluster 'local': boom"
        );
    }
}
