use std::sync::Arc;

use futures::StreamExt;
use schemreg::{
    CachedSchemaRegistry, ConfluentSchemaRegistry, RetryPolicy, Schema, SchemaId, SchemaRegError,
    SchemaRegistryClient as _, SchemaVersion,
};
use tokio::sync::OnceCell;

use crate::config::SchemaRegistryConfig;
use crate::environment::{SCHEMA_REGISTRY_TIMEOUT, SUBJECT_FETCH_CONCURRENCY};
use crate::kafka::error::KafkaError;
use crate::kafka::model::{RegisteredSchema, SchemaCompatibility, SchemaReference, SchemaSubject};

/// Schema ids are immutable, so the by-id cache never expires; this is the
/// only thing keeping it from growing with the registry.
const MAX_CACHED_SCHEMAS: usize = 10_000;

/// A Confluent-compatible registry behind a process-lifetime schema cache.
pub(crate) type Registry = CachedSchemaRegistry<ConfluentSchemaRegistry>;

/// Catalog port for a Confluent-compatible Schema Registry.
///
/// Wraps [`schemreg`] with the two things that are klens's rather than the
/// crate's: the subject sweep policy, and the mapping onto klens domain types.
#[derive(Clone)]
pub struct SchemaRegistryClient {
    cluster: String,
    registry: Arc<Registry>,
}

impl SchemaRegistryClient {
    pub fn new(
        cluster: impl Into<String>,
        config: &SchemaRegistryConfig,
    ) -> Result<Self, KafkaError> {
        let cluster = cluster.into();

        let mut builder = ConfluentSchemaRegistry::builder()
            .url(config.url.as_str())
            .request_timeout(*SCHEMA_REGISTRY_TIMEOUT)
            // The ingestion lanes have their own sweep cadence; stacking a
            // retry budget under it can make a sweep overrun its interval.
            .retry_policy(RetryPolicy::none());

        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            builder = builder.basic_auth(username, password);
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
        })
    }

    /// The cached registry, shared with the decode pipeline.
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

        // At most one global-config fetch per sweep, and only if some subject
        // needs it.
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
            .buffer_unordered(*SUBJECT_FETCH_CONCURRENCY)
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
        let versions = self
            .registry
            .get_versions(name)
            .await
            .map_err(|error| self.fail(error.to_string()))?;
        let latest = self
            .registry
            .get_latest_schema(name)
            .await
            .map_err(|error| self.fail(error.to_string()))?;

        Ok(SchemaSubject {
            subject: name.to_owned(),
            id: schema_id(&latest).ok_or_else(|| self.fail("schema is missing id"))?,
            schema_type: latest.schema_type.into(),
            latest_version: latest
                .version
                .map(SchemaVersion::as_i32)
                .ok_or_else(|| self.fail("schema is missing version"))?,
            versions: versions.into_iter().map(SchemaVersion::as_i32).collect(),
            compatibility: self.compatibility(name, global).await,
            schema: latest.schema.to_string(),
        })
    }

    /// The subject's *effective* compatibility: normally one call, because
    /// `?defaultToGlobal=true` makes the registry apply the global fallback
    /// itself.
    ///
    /// Registries that ignore that parameter answer 404 for a subject with no
    /// override of its own, so a not-found falls back to the global config —
    /// resolved once per sweep rather than once per subject. A registry with no
    /// configuration at all 404s both, which is not an error: it means nothing
    /// is enforced.
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

    /// The registry-wide default, read straight from `GET /config`.
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

/// Whether the registry is saying "nothing is configured here" rather than
/// failing.
///
/// Confluent reports a subject with no compatibility override as `40408`, which
/// is a 404 the crate does not classify as not-found because it is not about a
/// missing subject. Registries that do not implement the code answer a bare
/// 404 instead.
fn is_unconfigured(error: &SchemaRegError) -> bool {
    error.is_not_found()
        || error.error_code()
            == Some(schemreg::error::error_code::SUBJECT_COMPATIBILITY_NOT_CONFIGURED)
}

/// Registry ids are unsigned on the wire and signed in klens's domain and
/// GraphQL surface; the registry never issues one that does not fit.
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
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn config(url: &str) -> SchemaRegistryConfig {
        SchemaRegistryConfig {
            url: url.to_owned(),
            username: None,
            password: None,
        }
    }

    fn registry_path(segments: &[&str]) -> String {
        let mut url = url::Url::parse("http://localhost/").unwrap();
        url.path_segments_mut().unwrap().extend(segments);
        url.path().to_owned()
    }

    async fn mock_subject(
        server: &MockServer,
        subject: &str,
        id: i32,
        version: i32,
        schema_type: &str,
        schema: &str,
        versions: &[i32],
    ) {
        Mock::given(method("GET"))
            .and(path(registry_path(&["subjects", subject, "versions"])))
            .respond_with(ResponseTemplate::new(200).set_body_json(versions))
            .mount(server)
            .await;

        Mock::given(method("GET"))
            .and(path(registry_path(&[
                "subjects", subject, "versions", "latest",
            ])))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "subject": subject,
                "id": id,
                "version": version,
                "schemaType": schema_type,
                "schema": schema,
            })))
            .mount(server)
            .await;
    }

    async fn mock_compatibility(server: &MockServer, subject: &str, level: &str) {
        Mock::given(method("GET"))
            .and(path(registry_path(&["config", subject])))
            .and(query_param("defaultToGlobal", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "compatibilityLevel": level,
            })))
            .mount(server)
            .await;
    }

    async fn mock_subjects(server: &MockServer, names: &[&str]) {
        Mock::given(method("GET"))
            .and(path("/subjects"))
            .respond_with(ResponseTemplate::new(200).set_body_json(names))
            .mount(server)
            .await;
    }

    fn client(url: &str) -> SchemaRegistryClient {
        SchemaRegistryClient::new("local", &config(url)).unwrap()
    }

    #[tokio::test]
    async fn lists_subjects_with_their_latest_schema() {
        let server = MockServer::start().await;
        mock_subjects(&server, &["orders-value"]).await;
        mock_subject(
            &server,
            "orders-value",
            7,
            3,
            "AVRO",
            r#""string""#,
            &[1, 2, 3],
        )
        .await;
        mock_compatibility(&server, "orders-value", "FULL").await;

        let subjects = client(&server.uri()).subjects().await.unwrap();

        assert_eq!(subjects.len(), 1);
        let subject = &subjects[0];
        assert_eq!(subject.subject, "orders-value");
        assert_eq!(subject.id, 7);
        assert_eq!(subject.latest_version, 3);
        assert_eq!(subject.versions, vec![1, 2, 3]);
        assert_eq!(subject.schema_type, SchemaType::Avro);
        assert_eq!(subject.compatibility, SchemaCompatibility::Full);
        assert_eq!(subject.schema, r#""string""#);
    }

    #[tokio::test]
    async fn subjects_come_back_sorted_by_name() {
        let server = MockServer::start().await;
        mock_subjects(&server, &["b-value", "a-value"]).await;
        for name in ["a-value", "b-value"] {
            mock_subject(&server, name, 1, 1, "AVRO", r#""string""#, &[1]).await;
            mock_compatibility(&server, name, "BACKWARD").await;
        }

        let subjects = client(&server.uri()).subjects().await.unwrap();

        let names: Vec<&str> = subjects.iter().map(|s| s.subject.as_str()).collect();
        assert_eq!(names, vec!["a-value", "b-value"]);
    }

    #[tokio::test]
    async fn compatibility_comes_from_the_effective_config() {
        let server = MockServer::start().await;
        mock_subjects(&server, &["orders-value"]).await;
        mock_subject(&server, "orders-value", 1, 1, "AVRO", r#""string""#, &[1]).await;
        mock_compatibility(&server, "orders-value", "FORWARD_TRANSITIVE").await;

        let subjects = client(&server.uri()).subjects().await.unwrap();

        assert_eq!(subjects[0].compatibility, SchemaCompatibility::Forward);
        let reads = server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| request.url.path().starts_with("/config"))
            .count();
        assert_eq!(reads, 1, "one effective-config read per subject");
    }

    #[tokio::test]
    async fn a_missing_config_is_not_an_error() {
        let server = MockServer::start().await;
        mock_subjects(&server, &["orders-value"]).await;
        mock_subject(&server, "orders-value", 1, 1, "AVRO", r#""string""#, &[1]).await;
        Mock::given(method("GET"))
            .and(path("/config/orders-value"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": 40408,
                "message": "Subject-level compatibility not configured",
            })))
            .mount(&server)
            .await;

        let subjects = client(&server.uri()).subjects().await.unwrap();

        assert_eq!(subjects.len(), 1);
        assert_eq!(subjects[0].compatibility, SchemaCompatibility::None);
    }

    /// Registries that ignore `?defaultToGlobal=true` answer 404 for a subject
    /// with no override of its own; the global default still applies.
    #[tokio::test]
    async fn a_subject_without_an_override_falls_back_to_the_global_default() {
        let server = MockServer::start().await;
        mock_subjects(&server, &["a-value", "b-value"]).await;
        for name in ["a-value", "b-value"] {
            mock_subject(&server, name, 1, 1, "AVRO", r#""string""#, &[1]).await;
            Mock::given(method("GET"))
                .and(path(registry_path(&["config", name])))
                .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                    "error_code": 40408,
                    "message": "Subject-level compatibility not configured",
                })))
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "compatibilityLevel": "BACKWARD_TRANSITIVE",
            })))
            .mount(&server)
            .await;

        let subjects = client(&server.uri()).subjects().await.unwrap();

        assert_eq!(subjects.len(), 2);
        for subject in &subjects {
            assert_eq!(subject.compatibility, SchemaCompatibility::Backward);
        }

        let globals = server
            .received_requests()
            .await
            .unwrap_or_default()
            .into_iter()
            .filter(|request| request.url.path() == "/config")
            .count();
        assert_eq!(globals, 1, "the global default is read once per sweep");
    }

    #[tokio::test]
    async fn a_failing_subject_is_skipped_not_fatal() {
        let server = MockServer::start().await;
        mock_subjects(&server, &["good-value", "bad-value"]).await;
        mock_subject(&server, "good-value", 1, 1, "AVRO", r#""string""#, &[1]).await;
        mock_compatibility(&server, "good-value", "NONE").await;
        Mock::given(method("GET"))
            .and(path("/subjects/bad-value/versions"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let subjects = client(&server.uri()).subjects().await.unwrap();

        let names: Vec<&str> = subjects.iter().map(|s| s.subject.as_str()).collect();
        assert_eq!(names, vec!["good-value"]);
    }

    #[tokio::test]
    async fn a_failing_subject_list_is_fatal() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let error = client(&server.uri()).subjects().await.unwrap_err();

        assert!(matches!(error, KafkaError::SchemaRegistry { .. }));
    }

    #[tokio::test]
    async fn reads_one_subject_version_with_its_references() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/subjects/orders-value/versions/2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "subject": "orders-value",
                "id": 9,
                "version": 2,
                "schemaType": "PROTOBUF",
                "schema": "syntax = \"proto3\";",
                "references": [{"name": "common.proto", "subject": "common", "version": 1}],
            })))
            .mount(&server)
            .await;

        let schema = client(&server.uri())
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
        let server = MockServer::start().await;
        mock_subjects(&server, &["orders/v1-value"]).await;
        mock_subject(
            &server,
            "orders/v1-value",
            1,
            1,
            "AVRO",
            r#""string""#,
            &[1],
        )
        .await;
        mock_compatibility(&server, "orders/v1-value", "NONE").await;

        let subjects = client(&server.uri()).subjects().await.unwrap();

        assert_eq!(subjects[0].subject, "orders/v1-value");
    }

    #[tokio::test]
    async fn basic_auth_is_sent_when_configured() {
        let server = MockServer::start().await;
        mock_subjects(&server, &[] as &[&str]).await;

        let client = SchemaRegistryClient::new(
            "local",
            &SchemaRegistryConfig {
                url: server.uri(),
                username: Some("user".into()),
                password: Some("secret".into()),
            },
        )
        .unwrap();
        client.subjects().await.unwrap();

        let expected = format!(
            "Basic {}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, "user:secret")
        );
        let sent = server.received_requests().await.unwrap_or_default();
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
        let error = client("http://localhost:8081").fail("boom");

        assert_eq!(
            error.to_string(),
            "schema registry request failed for cluster 'local': boom"
        );
    }
}
