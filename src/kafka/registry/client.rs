use std::sync::Arc;

#[cfg(test)]
use schemreg::SchemaId;
use schemreg::SchemaRegistryClient as _;
use schemreg::{
    CachedSchemaRegistry, CompatibilityLevel, ConfluentSchemaRegistry, RetryPolicy, Schema,
    SchemaRegError, SchemaVersion,
};
use tokio::task::JoinSet;

use crate::config::SchemaRegistryConfig;
use crate::environment::SCHEMA_REGISTRY_TIMEOUT;
use crate::kafka::error::KafkaError;
use crate::kafka::model::{SchemaCompatibility, SchemaSubject, SchemaType};
#[cfg(test)]
use crate::kafka::registry::{RegisteredSchema, SchemaReference};

pub(crate) type CachedRegistry = CachedSchemaRegistry<ConfluentSchemaRegistry>;

/// HTTP client for a Confluent-compatible Schema Registry.
#[derive(Clone)]
pub struct SchemaRegistryClient {
    cluster: String,
    inner: Arc<CachedRegistry>,
}

impl SchemaRegistryClient {
    pub fn new(
        cluster: impl Into<String>,
        config: &SchemaRegistryConfig,
    ) -> Result<Self, KafkaError> {
        let cluster = cluster.into();
        let mut builder = ConfluentSchemaRegistry::builder()
            .url(&config.url)
            .request_timeout(*SCHEMA_REGISTRY_TIMEOUT)
            .retry_policy(RetryPolicy::none());
        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            builder = builder.basic_auth(username, password);
        }
        let registry = builder
            .build()
            .map_err(|error| KafkaError::SchemaRegistry {
                cluster: cluster.clone(),
                message: error.to_string(),
            })?;

        Ok(Self {
            cluster,
            inner: Arc::new(CachedSchemaRegistry::with_max_entries(registry, 10_000)),
        })
    }

    pub(crate) fn cached(&self) -> Arc<CachedRegistry> {
        Arc::clone(&self.inner)
    }

    pub async fn subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        let names = self
            .inner
            .get_subjects()
            .await
            .map_err(|error| self.fail_error(error))?;
        let mut join = JoinSet::new();

        for name in names {
            let client = self.clone();
            join.spawn(async move { client.load_subject(&name).await });
        }

        let mut subjects = Vec::new();
        while let Some(result) = join.join_next().await {
            subjects.push(result.map_err(|error| self.fail(error.to_string()))??);
        }

        subjects.sort_by(|left, right| left.subject.cmp(&right.subject));
        Ok(subjects)
    }

    #[cfg(test)]
    pub async fn schema_by_id(&self, id: i32) -> Result<Option<RegisteredSchema>, KafkaError> {
        match self.inner.get_schema_by_id(self.schema_id(id)?).await {
            Ok(schema) => self.registered(&schema).map(Some),
            Err(error) if error.is_not_found() => Ok(None),
            Err(error) => Err(self.fail_error(error)),
        }
    }

    #[cfg(test)]
    pub async fn schema_by_subject_version(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        let schema = self
            .inner
            .get_schema_by_version(subject, SchemaVersion::new(version))
            .await
            .map_err(|error| self.fail_error(error))?;
        self.registered(&schema)
    }

    pub(crate) async fn schema_by_key(
        &self,
        key: schemreg::SchemaKey,
    ) -> Result<Option<Arc<Schema>>, KafkaError> {
        match self.inner.get_schema_by_key(key).await {
            Ok(schema) => Ok(Some(schema)),
            Err(error) if error.is_not_found() => Ok(None),
            Err(error) => Err(self.fail_error(error)),
        }
    }

    pub(crate) async fn schema_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Arc<Schema>, KafkaError> {
        self.inner
            .get_schema_by_version(subject, version)
            .await
            .map_err(|error| self.fail_error(error))
    }

    async fn load_subject(&self, name: &str) -> Result<SchemaSubject, KafkaError> {
        let versions = self
            .inner
            .get_versions(name)
            .await
            .map_err(|error| self.fail_error(error))?;
        let latest = self
            .inner
            .get_latest_schema(name)
            .await
            .map_err(|error| self.fail_error(error))?;
        let compatibility = self.compatibility(name).await?;

        Ok(SchemaSubject {
            subject: name.to_owned(),
            id: required_id(&latest).map_err(|error| self.fail(error))?,
            schema_type: schema_type_from(latest.schema_type),
            latest_version: latest
                .version
                .map(SchemaVersion::as_i32)
                .ok_or_else(|| self.fail("schema is missing version"))?,
            versions: versions.into_iter().map(SchemaVersion::as_i32).collect(),
            compatibility,
            schema: latest.schema.to_string(),
        })
    }

    async fn compatibility(&self, name: &str) -> Result<SchemaCompatibility, KafkaError> {
        match self.inner.get_compatibility(name).await {
            Ok(level) => Ok(compatibility_from(level)),
            Err(error) if error.is_not_found() => match self.inner.get_compatibility("").await {
                Ok(level) => Ok(compatibility_from(level)),
                Err(error) if error.is_not_found() => Ok(SchemaCompatibility::None),
                Err(error) => Err(self.fail_error(error)),
            },
            Err(error) => Err(self.fail_error(error)),
        }
    }

    #[cfg(test)]
    fn schema_id(&self, id: i32) -> Result<SchemaId, KafkaError> {
        u32::try_from(id)
            .map(SchemaId::new)
            .map_err(|_| self.fail("schema id must be non-negative"))
    }

    fn fail_error(&self, error: SchemaRegError) -> KafkaError {
        match error.status() {
            Some(status) => self.fail(format!("{status}: {error}")),
            None => self.fail(error.to_string()),
        }
    }

    fn fail(&self, message: impl Into<String>) -> KafkaError {
        KafkaError::SchemaRegistry {
            cluster: self.cluster.clone(),
            message: message.into(),
        }
    }

    #[cfg(test)]
    fn registered(&self, schema: &Schema) -> Result<RegisteredSchema, KafkaError> {
        Ok(RegisteredSchema {
            id: required_id(schema).map_err(|error| self.fail(error))?,
            schema_type: schema_type_from(schema.schema_type),
            schema: schema.schema.to_string(),
            references: schema
                .references
                .iter()
                .map(|reference| SchemaReference {
                    name: reference.name.clone(),
                    subject: reference.subject.clone(),
                    version: reference.version.as_i32(),
                })
                .collect(),
        })
    }
}

fn required_id(schema: &Schema) -> Result<i32, &'static str> {
    let id = schema.id.ok_or("schema is missing id")?;
    i32::try_from(id.as_u32()).map_err(|_| "schema id does not fit i32")
}

fn schema_type_from(schema_type: schemreg::SchemaType) -> SchemaType {
    match schema_type {
        schemreg::SchemaType::Json => SchemaType::Json,
        schemreg::SchemaType::Protobuf => SchemaType::Protobuf,
        _ => SchemaType::Avro,
    }
}

fn compatibility_from(level: CompatibilityLevel) -> SchemaCompatibility {
    match level {
        CompatibilityLevel::Forward | CompatibilityLevel::ForwardTransitive => {
            SchemaCompatibility::Forward
        }
        CompatibilityLevel::Full | CompatibilityLevel::FullTransitive => SchemaCompatibility::Full,
        CompatibilityLevel::None => SchemaCompatibility::None,
        _ => SchemaCompatibility::Backward,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn config(url: &str) -> SchemaRegistryConfig {
        SchemaRegistryConfig {
            url: url.to_owned(),
            username: None,
            password: None,
        }
    }

    fn registry_path(segments: &[&str]) -> String {
        let mut url = Url::parse("http://localhost/").unwrap();
        url.path_segments_mut().unwrap().extend(segments);
        url.path().to_owned()
    }

    #[allow(clippy::too_many_arguments)]
    async fn mock_subject(
        server: &MockServer,
        subject: &str,
        id: i32,
        version: i32,
        schema_type: &str,
        schema: &str,
        versions: &[i32],
        compatibility: Option<&str>,
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

        match compatibility {
            Some(level) => {
                Mock::given(method("GET"))
                    .and(path(registry_path(&["config", subject])))
                    .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                        "compatibilityLevel": level,
                    })))
                    .mount(server)
                    .await;
            }
            None => {
                Mock::given(method("GET"))
                    .and(path(registry_path(&["config", subject])))
                    .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                        "error_code": 40401,
                        "message": "Subject not found.",
                    })))
                    .mount(server)
                    .await;
            }
        }
    }

    #[tokio::test]
    async fn lists_subjects_from_registry() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/subjects"))
            .respond_with(ResponseTemplate::new(200).set_body_json(["orders-value"]))
            .mount(&server)
            .await;

        mock_subject(
            &server,
            "orders-value",
            12,
            3,
            "AVRO",
            r#"{"type":"string"}"#,
            &[1, 2, 3],
            Some("BACKWARD"),
        )
        .await;

        let client = SchemaRegistryClient::new("local", &config(&server.uri())).unwrap();
        let subjects = client.subjects().await.unwrap();

        assert_eq!(subjects.len(), 1);
        assert_eq!(subjects[0].subject, "orders-value");
        assert_eq!(subjects[0].id, 12);
        assert_eq!(subjects[0].schema_type, SchemaType::Avro);
        assert_eq!(subjects[0].latest_version, 3);
        assert_eq!(subjects[0].versions, vec![1, 2, 3]);
        assert_eq!(subjects[0].compatibility, SchemaCompatibility::Backward);
        assert_eq!(subjects[0].schema, r#"{"type":"string"}"#);
    }

    #[tokio::test]
    async fn falls_back_to_global_config_when_subject_config_is_missing() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/subjects"))
            .respond_with(ResponseTemplate::new(200).set_body_json(["payments-value"]))
            .mount(&server)
            .await;

        mock_subject(&server, "payments-value", 4, 1, "JSON", "{}", &[1], None).await;

        Mock::given(method("GET"))
            .and(path("/config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "compatibilityLevel": "FULL_TRANSITIVE",
            })))
            .mount(&server)
            .await;

        let client = SchemaRegistryClient::new("local", &config(&server.uri())).unwrap();
        let subjects = client.subjects().await.unwrap();

        assert_eq!(subjects[0].schema_type, SchemaType::Json);
        assert_eq!(subjects[0].compatibility, SchemaCompatibility::Full);
    }

    #[tokio::test]
    async fn maps_transitive_compatibility_and_percent_encodes_subjects() {
        let server = MockServer::start().await;
        let subject = "orders value";

        Mock::given(method("GET"))
            .and(path("/subjects"))
            .respond_with(ResponseTemplate::new(200).set_body_json([subject]))
            .mount(&server)
            .await;

        mock_subject(
            &server,
            subject,
            9,
            2,
            "PROTOBUF",
            "syntax = \"proto3\";",
            &[1, 2],
            Some("FORWARD_TRANSITIVE"),
        )
        .await;

        let client = SchemaRegistryClient::new("local", &config(&server.uri())).unwrap();
        let subjects = client.subjects().await.unwrap();

        assert_eq!(subjects[0].subject, subject);
        assert_eq!(subjects[0].schema_type, SchemaType::Protobuf);
        assert_eq!(subjects[0].compatibility, SchemaCompatibility::Forward);
    }

    #[tokio::test]
    async fn returns_schema_registry_error_on_non_success() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/subjects"))
            .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
            .mount(&server)
            .await;

        let client = SchemaRegistryClient::new("prod", &config(&server.uri())).unwrap();
        let error = client.subjects().await.unwrap_err();

        assert!(error.to_string().contains("prod"));
        assert!(error.to_string().contains("503"));
    }

    #[tokio::test]
    async fn fetches_schema_by_id_including_references() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/schemas/ids/20"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schemaType": "AVRO",
                "schema": r#"{"type":"record","name":"Order","fields":[{"name":"status","type":"Status"}]}"#,
                "references": [{
                    "name": "Status",
                    "subject": "Status",
                    "version": 1
                }]
            })))
            .mount(&server)
            .await;

        let client = SchemaRegistryClient::new("local", &config(&server.uri())).unwrap();
        let schema = client.schema_by_id(20).await.unwrap().unwrap();

        assert_eq!(schema.id, 20);
        assert_eq!(schema.schema_type, SchemaType::Avro);
        assert_eq!(
            schema.references,
            vec![SchemaReference {
                name: "Status".into(),
                subject: "Status".into(),
                version: 1,
            }]
        );
    }

    #[tokio::test]
    async fn schema_by_id_returns_none_when_missing() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/schemas/ids/99"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error_code": 40403,
                "message": "Schema not found.",
            })))
            .mount(&server)
            .await;

        let client = SchemaRegistryClient::new("local", &config(&server.uri())).unwrap();
        assert!(client.schema_by_id(99).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn fetches_schema_by_subject_version() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/subjects/Status/versions/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "subject": "Status",
                "id": 4,
                "version": 1,
                "schemaType": "AVRO",
                "schema": r#"{"type":"enum","name":"Status","symbols":["OPEN","CLOSED"]}"#,
            })))
            .mount(&server)
            .await;

        let client = SchemaRegistryClient::new("local", &config(&server.uri())).unwrap();
        let schema = client.schema_by_subject_version("Status", 1).await.unwrap();

        assert_eq!(schema.id, 4);
        assert_eq!(schema.schema_type, SchemaType::Avro);
        assert!(schema.schema.contains("CLOSED"));
    }

    #[test]
    fn defaults_missing_schema_type_to_avro() {
        assert_eq!(SchemaType::from_registry(None), SchemaType::Avro);
        assert_eq!(SchemaType::from_registry(Some("AVRO")), SchemaType::Avro);
    }
}
