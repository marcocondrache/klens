use std::sync::Arc;

use bytes::Bytes;
use foldhash::{HashMap, HashMapExt, HashSet, HashSetExt};
use futures::StreamExt;
use schemreg::error::error_code;
use schemreg::{
    CachedSchemaRegistry, ConfluentSchemaRegistry, RetryPolicy, Schema, SchemaId, SchemaRegError,
    SchemaRegistryClient as _, SchemaVersion,
};
use secrecy::ExposeSecret;
use tokio::sync::OnceCell;

use crate::config::{SchemaRegistry, SchemaRegistryTuning};
use crate::kafka::error::KafkaError;
use crate::kafka::model::{
    NewSchema, RegisteredSchema, RegisteredVersion, SchemaCompatibility, SchemaDeletion,
    SchemaReference, SchemaSubject,
};

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
                        tracing::warn!(
                            subject = name.as_str(),
                            error = error.to_string().as_str(),
                            "skipping subject"
                        );
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
            .map_err(|error| {
                if error.is_not_found() {
                    KafkaError::UnknownSubject {
                        cluster: self.cluster.clone(),
                        subject: subject.to_owned(),
                        version,
                    }
                } else {
                    self.fail(error.to_string())
                }
            })?;
        self.registered(&latest)
    }

    /// The schema id of each of `versions` the registry answers for. The reads
    /// skip the id cache, which holds the schemas records decode with.
    pub async fn version_ids(
        &self,
        versions: &[(Arc<str>, i32)],
    ) -> Vec<(Arc<str>, RegisteredVersion)> {
        futures::stream::iter(versions.iter().cloned())
            .map(|(subject, version)| async move {
                let read = self
                    .registry
                    .inner()
                    .get_schema_by_version(&subject, SchemaVersion::new(version))
                    .await;
                match read {
                    Ok(schema) => {
                        schema_id(&schema).map(|id| (subject, RegisteredVersion { id, version }))
                    }
                    Err(error) => {
                        tracing::debug!(
                            subject = subject.as_ref(),
                            version,
                            error = error.to_string().as_str(),
                            "no schema id for version"
                        );
                        None
                    }
                }
            })
            .buffer_unordered(self.fetch_concurrency)
            .filter_map(std::future::ready)
            .collect()
            .await
    }

    /// Encodes `json` with the schema the registry holds under `id`.
    pub async fn encode(&self, id: i32, json: &str) -> Result<Bytes, KafkaError> {
        let schema = self
            .registry
            .get_schema_by_id(SchemaId::new(id.cast_unsigned()))
            .await
            .map_err(|error| {
                if error.is_not_found() {
                    KafkaError::UnknownSchema {
                        cluster: self.cluster.clone(),
                        id,
                    }
                } else {
                    self.fail(error.to_string())
                }
            })?;
        let dependencies = self
            .dependencies(&schema.references)
            .await
            .map_err(|error| self.fail(error.to_string()))?;
        RegisteredSchema {
            id,
            schema_type: schema.schema_type.into(),
            schema: schema.schema.to_string(),
            references: references(&schema.references),
        }
        .encode(&dependencies, json)
    }

    /// The name and body of every schema `references` reaches, each after
    /// the schemas it references.
    pub(crate) async fn dependencies(
        &self,
        references: &[schemreg::SchemaReference],
    ) -> Result<Vec<(String, String)>, SchemaRegError> {
        let roots = self::references(references);
        let mut fetched = HashMap::new();
        let mut pending = roots.clone();
        let mut seen = HashSet::new();

        loop {
            let wave: Vec<SchemaReference> = pending
                .drain(..)
                .filter(|reference| seen.insert((reference.subject.clone(), reference.version)))
                .collect();
            if wave.is_empty() {
                break;
            }

            let mut fetches = futures::stream::iter(wave.into_iter().map(|reference| async move {
                let schema = self
                    .registry
                    .get_schema_by_version(
                        &reference.subject,
                        SchemaVersion::new(reference.version),
                    )
                    .await?;
                Ok::<_, SchemaRegError>((reference, schema))
            }))
            .buffer_unordered(self.fetch_concurrency);

            while let Some(result) = fetches.next().await {
                let (reference, schema) = result?;
                pending.extend(self::references(&schema.references));
                fetched.insert((reference.subject, reference.version), schema);
            }
        }

        Ok(in_dependency_order(roots, &fetched))
    }

    pub async fn register(&self, schema: &NewSchema) -> Result<RegisteredVersion, KafkaError> {
        let schema_type = schema.schema_type.into();
        let references: Vec<schemreg::SchemaReference> = schema
            .references
            .iter()
            .map(|reference| {
                schemreg::SchemaReference::new(
                    &reference.name,
                    &reference.subject,
                    SchemaVersion::new(reference.version),
                )
            })
            .collect();
        self.registry
            .register_schema(&schema.subject, &schema.schema, schema_type, &references)
            .await
            .map_err(|error| self.refusal(error))?;
        let registered = self
            .registry
            .lookup_schema(&schema.subject, &schema.schema, schema_type, &references)
            .await
            .map_err(|error| self.fail(error.to_string()))?
            .ok_or_else(|| self.fail("the registry does not hold the schema it registered"))?;
        Ok(RegisteredVersion {
            id: schema_id(&registered).ok_or_else(|| self.fail("schema is missing id"))?,
            version: registered
                .version
                .map(SchemaVersion::as_i32)
                .ok_or_else(|| self.fail("schema is missing version"))?,
        })
    }

    pub async fn delete(&self, deletion: &SchemaDeletion) -> Result<(), KafkaError> {
        match self.delete_once(deletion, false).await {
            Err(error) if deletion.permanent && is_soft_deleted(&error) => {}
            soft => soft.map_err(|error| self.deletion_error(deletion, error))?,
        }
        if deletion.permanent {
            self.delete_once(deletion, true)
                .await
                .map_err(|error| self.deletion_error(deletion, error))?;
        }
        Ok(())
    }

    async fn delete_once(
        &self,
        deletion: &SchemaDeletion,
        permanent: bool,
    ) -> Result<(), SchemaRegError> {
        match deletion.version {
            None => self
                .registry
                .delete_subject(&deletion.subject, permanent)
                .await
                .map(drop),
            Some(version) => self
                .registry
                .delete_version(&deletion.subject, SchemaVersion::new(version), permanent)
                .await
                .map(drop),
        }
    }

    fn deletion_error(&self, deletion: &SchemaDeletion, error: SchemaRegError) -> KafkaError {
        if error.is_not_found() {
            KafkaError::UnknownSubject {
                cluster: self.cluster.clone(),
                subject: deletion.subject.clone(),
                version: deletion.version.unwrap_or(0),
            }
        } else {
            self.refusal(error)
        }
    }

    pub async fn set_compatibility(
        &self,
        subject: &str,
        level: SchemaCompatibility,
    ) -> Result<(), KafkaError> {
        self.registry
            .set_compatibility(subject, level.into())
            .await
            .map_err(|error| self.refusal(error))
    }

    /// A registry answers a write it will not take with a 4xx code and a
    /// message meant for the user. Confluent sends an incompatible schema as
    /// a bare 409 and an invalid one as 42201.
    fn refusal(&self, error: SchemaRegError) -> KafkaError {
        match error {
            SchemaRegError::Api {
                error_code: 400..500 | 40000..50000,
                message,
            } => KafkaError::RegistryRefused(message),
            error => self.fail(error.to_string()),
        }
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
                tracing::warn!(
                    subject = name,
                    error = error.to_string().as_str(),
                    "falling back to NONE compatibility"
                );
                SchemaCompatibility::None
            }
        }
    }

    async fn global_compatibility(&self) -> SchemaCompatibility {
        match self.registry.get_compatibility("").await {
            Ok(level) => level.into(),
            Err(error) if is_unconfigured(&error) => SchemaCompatibility::None,
            Err(error) => {
                tracing::warn!(
                    error = error.to_string().as_str(),
                    "falling back to NONE compatibility"
                );
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

enum Visit {
    Enter(SchemaReference),
    Leave(String, Arc<Schema>),
}

fn in_dependency_order(
    roots: Vec<SchemaReference>,
    fetched: &HashMap<(String, i32), Arc<Schema>>,
) -> Vec<(String, String)> {
    let mut ordered = Vec::with_capacity(fetched.len());
    let mut entered = HashSet::with_capacity(fetched.len());
    let mut visits: Vec<Visit> = roots.into_iter().map(Visit::Enter).collect();

    while let Some(visit) = visits.pop() {
        match visit {
            Visit::Enter(reference) => {
                let key = (reference.subject, reference.version);
                if let Some(schema) = fetched.get(&key)
                    && entered.insert(key)
                {
                    visits.push(Visit::Leave(reference.name, Arc::clone(schema)));
                    visits.extend(references(&schema.references).into_iter().map(Visit::Enter));
                }
            }
            Visit::Leave(name, schema) => ordered.push((name, schema.schema.to_string())),
        }
    }

    ordered
}

fn is_soft_deleted(error: &SchemaRegError) -> bool {
    matches!(
        error.error_code(),
        Some(error_code::SUBJECT_SOFT_DELETED | error_code::VERSION_SOFT_DELETED)
    )
}

/// Confluent reports a subject with no compatibility override as `40408`, which
/// is a 404 the crate does not classify as not-found because it is not about a
/// missing subject. Registries that do not implement the code answer a bare
/// 404 instead.
fn is_unconfigured(error: &SchemaRegError) -> bool {
    error.is_not_found()
        || error.error_code() == Some(error_code::SUBJECT_COMPATIBILITY_NOT_CONFIGURED)
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
    use crate::testing::{LogCapture, yaml};

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

        assert_eq!(offline(&tuning).fetch_concurrency, 3);
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

        assert_eq!(
            subjects[0].compatibility,
            SchemaCompatibility::ForwardTransitive
        );
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

        assert_eq!(
            subjects[0].compatibility,
            SchemaCompatibility::BackwardTransitive
        );
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
            assert_eq!(
                subject.compatibility,
                SchemaCompatibility::BackwardTransitive
            );
        }
        assert_eq!(
            registry.hits("/config").await,
            1,
            "the global default is read once per sweep"
        );
    }

    #[tokio::test]
    async fn a_failing_config_read_falls_back_to_none_not_the_global_default() {
        let registry = registry_of(&["orders-value"]).await;
        registry.set_global_compatibility("BACKWARD");
        registry.fail("/config/orders-value");

        let subjects = registry.client().subjects().await.unwrap();

        assert_eq!(subjects[0].compatibility, SchemaCompatibility::None);
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
    async fn a_subject_with_a_quote_and_a_newline_stays_on_its_skip_warning() {
        let registry = registry_of(&["orders\"\nuser=\"mallory"]).await;
        registry.fail("/subjects/orders%22%0Auser=%22mallory/versions");
        let logs = LogCapture::at(tracing::Level::WARN);

        registry.client().subjects().await.unwrap();

        logs.assert_contains(r#"skipping subject subject="orders\"\nuser=\"mallory""#);
        logs.assert_lacks("\nuser=");
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
    async fn a_version_the_registry_does_not_hold_is_an_unknown_subject() {
        let registry = FakeRegistry::start().await;
        registry.register("orders-value", 1, 9, string());
        let client = registry.client();

        let missing = client.schema_by_subject_version("orders-value", 2).await;
        let unknown = client.schema_by_subject_version("ghost-value", 1).await;
        registry.fail("/subjects/orders-value/versions/1");
        let failed = client.schema_by_subject_version("orders-value", 1).await;

        assert!(
            matches!(
                &missing,
                Err(KafkaError::UnknownSubject { subject, version: 2, .. }) if subject == "orders-value"
            ),
            "{missing:?}"
        );
        assert!(
            matches!(&unknown, Err(KafkaError::UnknownSubject { version: 1, .. })),
            "{unknown:?}"
        );
        assert!(
            matches!(&failed, Err(KafkaError::SchemaRegistry { .. })),
            "{failed:?}"
        );
    }

    #[tokio::test]
    async fn version_ids_answer_for_the_versions_the_registry_holds() {
        let registry = FakeRegistry::start().await;
        for (version, id) in [(1, 5), (2, 6)] {
            registry.register("orders-value", version, id, string());
        }
        let client = registry.client();
        let subject = Arc::<str>::from("orders-value");

        let mut ids: Vec<(i32, i32)> = client
            .version_ids(&[
                (Arc::clone(&subject), 1),
                (Arc::clone(&subject), 2),
                (Arc::clone(&subject), 3),
            ])
            .await
            .into_iter()
            .map(|(read, found)| {
                assert_eq!(read, subject);
                (found.version, found.id)
            })
            .collect();
        ids.sort_unstable();

        assert_eq!(ids, [(1, 5), (2, 6)], "version 3 is not registered");
        assert_eq!(
            client.registry().cache_len(),
            0,
            "the decoder's id cache is left alone"
        );
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

    fn new_schema(subject: &str, text: &str) -> NewSchema {
        NewSchema {
            subject: subject.to_owned(),
            schema_type: SchemaType::Avro,
            schema: text.to_owned(),
            references: Vec::new(),
        }
    }

    #[tokio::test]
    async fn a_registered_schema_becomes_the_subjects_next_version() {
        let registry = registry_of(&["orders-value"]).await;

        let registered = registry
            .client()
            .register(&new_schema("orders-value", r#""int""#))
            .await
            .unwrap();

        assert_eq!(registered, RegisteredVersion { id: 2, version: 2 });
        let subjects = registry.client().subjects().await.unwrap();
        assert_eq!(subjects[0].versions, vec![1, 2]);
    }

    #[tokio::test]
    async fn registering_a_schema_the_subject_holds_answers_its_version() {
        let registry = registry_of(&["orders-value", "payments-value"]).await;

        let registered = registry
            .client()
            .register(&new_schema("payments-value", r#""string""#))
            .await
            .unwrap();

        assert_eq!(registered, RegisteredVersion { id: 2, version: 1 });
    }

    #[tokio::test]
    async fn a_new_subject_starts_at_version_one_with_its_references() {
        let registry = registry_of(&["common"]).await;
        let schema = NewSchema {
            subject: "orders/v1-value".to_owned(),
            schema_type: SchemaType::Protobuf,
            schema: "syntax = \"proto3\";".to_owned(),
            references: vec![SchemaReference {
                name: "common.proto".to_owned(),
                subject: "common".to_owned(),
                version: 1,
            }],
        };

        let registered = registry.client().register(&schema).await.unwrap();

        assert_eq!(registered, RegisteredVersion { id: 2, version: 1 });
        let read = registry
            .client()
            .schema_by_subject_version("orders/v1-value", 1)
            .await
            .unwrap();
        assert_eq!(read.schema_type, SchemaType::Protobuf);
        assert_eq!(read.references, schema.references);
    }

    #[tokio::test]
    async fn a_schema_the_registry_refuses_carries_its_message() {
        let registry = registry_of(&["orders-value"]).await;

        for (status, code, message) in [
            (409, 409, "Schema being registered is incompatible"),
            (422, 42201, "Invalid schema"),
        ] {
            registry.refuse(status, code, message);
            let error = registry
                .client()
                .register(&new_schema("orders-value", r#""int""#))
                .await
                .unwrap_err();

            assert!(
                matches!(&error, KafkaError::RegistryRefused(refused) if refused == message),
                "{error:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_registry_that_fails_a_registration_is_a_registry_error() {
        let registry = registry_of(&["orders-value"]).await;
        registry.fail("/subjects/orders-value/versions");

        let error = registry
            .client()
            .register(&new_schema("orders-value", r#""int""#))
            .await
            .unwrap_err();

        assert!(
            matches!(error, KafkaError::SchemaRegistry { .. }),
            "{error:?}"
        );
    }

    fn deletion(version: Option<i32>, permanent: bool) -> SchemaDeletion {
        SchemaDeletion {
            subject: "orders-value".to_owned(),
            version,
            permanent,
        }
    }

    async fn two_versions() -> FakeRegistry {
        let registry = registry_of(&["orders-value"]).await;
        registry.register("orders-value", 2, 2, Schema::avro(r#""int""#));
        registry
    }

    async fn deletes(registry: &FakeRegistry) -> Vec<String> {
        registry
            .requests()
            .await
            .iter()
            .filter(|request| request.method == wiremock::http::Method::DELETE)
            .map(|request| request.url[url::Position::BeforePath..].to_owned())
            .collect()
    }

    #[tokio::test]
    async fn a_soft_deleted_version_leaves_the_subject() {
        let registry = two_versions().await;

        registry
            .client()
            .delete(&deletion(Some(1), false))
            .await
            .unwrap();

        let subjects = registry.client().subjects().await.unwrap();
        assert_eq!(subjects[0].versions, vec![2]);
        assert_eq!(
            deletes(&registry).await,
            vec!["/subjects/orders-value/versions/1"]
        );
    }

    #[tokio::test]
    async fn a_soft_deleted_subject_leaves_the_listing() {
        let registry = two_versions().await;

        registry
            .client()
            .delete(&deletion(None, false))
            .await
            .unwrap();

        assert!(registry.client().subjects().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_permanent_delete_soft_deletes_first() {
        let registry = two_versions().await;

        registry
            .client()
            .delete(&deletion(Some(2), true))
            .await
            .unwrap();
        registry
            .client()
            .delete(&deletion(None, true))
            .await
            .unwrap();

        assert_eq!(
            deletes(&registry).await,
            vec![
                "/subjects/orders-value/versions/2",
                "/subjects/orders-value/versions/2?permanent=true",
                "/subjects/orders-value",
                "/subjects/orders-value?permanent=true",
            ]
        );
    }

    #[tokio::test]
    async fn a_permanent_delete_finishes_a_soft_deleted_subject() {
        let registry = two_versions().await;
        let client = registry.client();

        client.delete(&deletion(None, false)).await.unwrap();
        client.delete(&deletion(None, true)).await.unwrap();

        let error = client.delete(&deletion(None, true)).await.unwrap_err();
        assert!(
            matches!(&error, KafkaError::UnknownSubject { subject, version: 0, .. } if subject == "orders-value"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn deleting_a_version_the_registry_does_not_hold_is_an_unknown_subject() {
        let registry = two_versions().await;

        let error = registry
            .client()
            .delete(&deletion(Some(9), false))
            .await
            .unwrap_err();

        assert!(
            matches!(error, KafkaError::UnknownSubject { version: 9, .. }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn soft_deleting_twice_carries_the_registry_refusal() {
        let registry = two_versions().await;
        let client = registry.client();

        client.delete(&deletion(Some(1), false)).await.unwrap();
        let error = client.delete(&deletion(Some(1), false)).await.unwrap_err();

        assert!(
            matches!(&error, KafkaError::RegistryRefused(message) if message == "Version was soft deleted."),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_delete_the_registry_refuses_carries_its_message() {
        let registry = two_versions().await;
        registry.refuse(422, 42206, "One or more references exist to the schema");

        let error = registry
            .client()
            .delete(&deletion(Some(1), true))
            .await
            .unwrap_err();

        assert!(
            matches!(&error, KafkaError::RegistryRefused(message) if message.starts_with("One or more references")),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_registry_that_fails_a_delete_is_a_registry_error() {
        let registry = two_versions().await;
        registry.fail("/subjects/orders-value");

        let error = registry
            .client()
            .delete(&deletion(None, false))
            .await
            .unwrap_err();

        assert!(
            matches!(error, KafkaError::SchemaRegistry { .. }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_set_compatibility_level_is_the_subjects_own() {
        let registry = registry_of(&["orders-value"]).await;
        registry.set_global_compatibility("BACKWARD");

        registry
            .client()
            .set_compatibility("orders-value", SchemaCompatibility::FullTransitive)
            .await
            .unwrap();

        let subjects = registry.client().subjects().await.unwrap();
        assert_eq!(
            subjects[0].compatibility,
            SchemaCompatibility::FullTransitive
        );
    }

    #[tokio::test]
    async fn a_compatibility_level_the_registry_refuses_carries_its_message() {
        let registry = registry_of(&["orders-value"]).await;
        registry.refuse(422, 42203, "Invalid compatibility level");

        let error = registry
            .client()
            .set_compatibility("orders-value", SchemaCompatibility::Full)
            .await
            .unwrap_err();

        assert!(
            matches!(&error, KafkaError::RegistryRefused(message) if message == "Invalid compatibility level"),
            "{error:?}"
        );
    }

    const STATUS: &str = r#"{"type": "enum", "name": "Status", "symbols": ["OPEN", "CLOSED"]}"#;

    const LINE: &str =
        r#"{"type": "record", "name": "Line", "fields": [{"name": "status", "type": "Status"}]}"#;

    const SHIPMENT: &str = r#"{
        "type": "record",
        "name": "Shipment",
        "fields": [{"name": "line", "type": "Line"}, {"name": "status", "type": "Status"}]
    }"#;

    async fn shipments() -> FakeRegistry {
        let registry = FakeRegistry::start().await;
        registry.register("status", 1, 1, Schema::avro(STATUS));
        registry.register(
            "line",
            1,
            2,
            Schema::avro(LINE).referencing("Status", "status", 1),
        );
        registry.put(
            3,
            Schema::avro(SHIPMENT)
                .referencing("Line", "line", 1)
                .referencing("Status", "status", 1),
        );
        registry
    }

    #[tokio::test]
    async fn dependencies_come_once_and_after_the_schemas_they_reference() {
        let registry = shipments().await;
        let references = [
            schemreg::SchemaReference::new("Line", "line", SchemaVersion::new(1)),
            schemreg::SchemaReference::new("Status", "status", SchemaVersion::new(1)),
        ];

        let dependencies = registry.client().dependencies(&references).await.unwrap();

        assert_eq!(
            dependencies,
            [
                ("Status".to_owned(), STATUS.to_owned()),
                ("Line".to_owned(), LINE.to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn a_payload_encodes_with_the_schema_its_id_names() {
        let registry = shipments().await;
        let json = serde_json::json!({ "line": { "status": "CLOSED" }, "status": "OPEN" });

        let encoded = registry
            .client()
            .encode(3, &json.to_string())
            .await
            .unwrap();

        let decoded = registry.decoder().decode(&encoded).await;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&decoded).unwrap(),
            json
        );
    }

    #[tokio::test]
    async fn a_schema_id_the_registry_does_not_hold_is_unknown() {
        let registry = FakeRegistry::start().await;

        let error = registry.client().encode(3, "{}").await.unwrap_err();

        assert!(
            matches!(&error, KafkaError::UnknownSchema { cluster, id: 3 } if cluster == "local"),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_registry_that_fails_a_schema_read_is_a_registry_error() {
        let registry = shipments().await;
        registry.fail("/schemas/ids/3");

        let error = registry.client().encode(3, "{}").await.unwrap_err();

        assert!(
            matches!(&error, KafkaError::SchemaRegistry { .. }),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn a_registry_that_fails_a_dependency_read_is_a_registry_error() {
        let registry = shipments().await;
        registry.fail("/subjects/status/versions/1");

        let error = registry.client().encode(3, "{}").await.unwrap_err();

        assert!(
            matches!(&error, KafkaError::SchemaRegistry { .. }),
            "{error:?}"
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
