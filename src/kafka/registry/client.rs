use std::sync::Arc;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use reqwest::StatusCode;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use tokio::task::JoinSet;
use url::Url;

use crate::config::SchemaRegistryConfig;
use crate::environment::SCHEMA_REGISTRY_TIMEOUT;
use crate::kafka::error::KafkaError;
use crate::kafka::model::{
    RegisteredSchema, SchemaCompatibility, SchemaReference, SchemaSubject, SchemaType,
    SubjectSchema,
};
use schema_registry::{Client, Error as RegistryError};

mod schema_registry {
    #![allow(dead_code, unused_imports, clippy::all)]

    use progenitor_client as _;

    include!(concat!(env!("OUT_DIR"), "/schema_registry.rs"));
}

/// HTTP client for a Confluent-compatible Schema Registry.
#[derive(Clone)]
pub struct SchemaRegistryClient {
    cluster: String,
    inner: Arc<Client>,
}

impl SchemaRegistryClient {
    pub fn new(
        cluster: impl Into<String>,
        config: &SchemaRegistryConfig,
    ) -> Result<Self, KafkaError> {
        let cluster = cluster.into();
        let base = Url::parse(&config.url).map_err(|error| KafkaError::SchemaRegistry {
            cluster: cluster.clone(),
            message: error.to_string(),
        })?;
        let baseurl = base.as_str().trim_end_matches('/').to_owned();

        let mut headers = HeaderMap::new();
        if let (Some(username), Some(password)) = (&config.username, &config.password) {
            let encoded = BASE64.encode(format!("{username}:{password}"));
            let value = HeaderValue::from_str(&format!("Basic {encoded}")).map_err(|error| {
                KafkaError::SchemaRegistry {
                    cluster: cluster.clone(),
                    message: error.to_string(),
                }
            })?;
            headers.insert(AUTHORIZATION, value);
        }

        let http = reqwest::Client::builder()
            .timeout(*SCHEMA_REGISTRY_TIMEOUT)
            .default_headers(headers)
            .build()
            .map_err(|error| KafkaError::SchemaRegistry {
                cluster: cluster.clone(),
                message: error.to_string(),
            })?;

        Ok(Self {
            cluster,
            inner: Arc::new(Client::new_with_client(&baseurl, http)),
        })
    }

    pub async fn subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        let names = self
            .inner
            .list()
            .send()
            .await
            .map_err(|error| self.fail_error(error))?
            .into_inner();
        let fallback = self
            .global_config()
            .await?
            .as_ref()
            .map(compatibility_from_config);
        let mut join = JoinSet::new();

        for name in names {
            let client = self.clone();
            join.spawn(async move { client.load_entry(&name, fallback).await });
        }

        let mut subjects = Vec::new();
        while let Some(result) = join.join_next().await {
            subjects.push(result.map_err(|error| self.fail(error.to_string()))??);
        }

        subjects.sort_by(|left, right| left.subject.cmp(&right.subject));
        Ok(subjects)
    }

    pub async fn subject_schema(&self, name: &str) -> Result<SubjectSchema, KafkaError> {
        let latest = self
            .inner
            .get_schema_by_version()
            .subject(name)
            .version("latest")
            .send()
            .await
            .map_err(|error| self.fail_error(error))?
            .into_inner();
        Ok(SubjectSchema {
            subject: name.to_owned(),
            id: latest.id.ok_or_else(|| self.fail("schema is missing id"))?,
            version: latest
                .version
                .ok_or_else(|| self.fail("schema is missing version"))?,
            schema_type: SchemaType::from_registry(latest.schema_type.as_deref()),
            schema: latest
                .schema
                .ok_or_else(|| self.fail("schema is missing schema body"))?,
        })
    }

    pub async fn schema_by_id(&self, id: i32) -> Result<Option<RegisteredSchema>, KafkaError> {
        match self.inner.get_schema().id(id).send().await {
            Ok(response) => self
                .registered_from_schema_string(id, response.into_inner())
                .map(Some),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(self.fail_error(error)),
        }
    }

    pub async fn schema_by_subject_version(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<RegisteredSchema, KafkaError> {
        let latest = self
            .inner
            .get_schema_by_version()
            .subject(subject)
            .version(version.to_string())
            .send()
            .await
            .map_err(|error| self.fail_error(error))?
            .into_inner();
        let id = latest.id.ok_or_else(|| self.fail("schema is missing id"))?;
        self.registered_from_schema(id, latest)
    }

    async fn load_entry(
        &self,
        name: &str,
        fallback: Option<SchemaCompatibility>,
    ) -> Result<SchemaSubject, KafkaError> {
        let versions = self
            .inner
            .list_versions()
            .subject(name)
            .send()
            .await
            .map_err(|error| self.fail_error(error))?
            .into_inner();
        let compatibility = match self.subject_config(name).await? {
            Some(config) => compatibility_from_config(&config),
            None => fallback.unwrap_or(SchemaCompatibility::None),
        };
        SchemaSubject::from_versions(name.to_owned(), versions, compatibility)
            .map_err(|error| self.fail(error.to_string()))
    }

    async fn subject_config(
        &self,
        name: &str,
    ) -> Result<Option<schema_registry::types::Config>, KafkaError> {
        match self
            .inner
            .get_subject_level_config()
            .subject(name)
            .send()
            .await
        {
            Ok(response) => Ok(Some(response.into_inner())),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(self.fail_error(error)),
        }
    }

    async fn global_config(&self) -> Result<Option<schema_registry::types::Config>, KafkaError> {
        match self.inner.get_top_level_config().send().await {
            Ok(response) => Ok(Some(response.into_inner())),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(self.fail_error(error)),
        }
    }

    fn fail_error<E>(&self, error: RegistryError<E>) -> KafkaError
    where
        RegistryError<E>: std::fmt::Display,
    {
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

    fn registered_from_schema_string(
        &self,
        id: i32,
        value: schema_registry::types::SchemaString,
    ) -> Result<RegisteredSchema, KafkaError> {
        self.registered_schema(
            id,
            value.schema_type.as_deref(),
            value.schema,
            value.references,
        )
    }

    fn registered_from_schema(
        &self,
        id: i32,
        value: schema_registry::types::Schema,
    ) -> Result<RegisteredSchema, KafkaError> {
        self.registered_schema(
            id,
            value.schema_type.as_deref(),
            value.schema,
            value.references,
        )
    }

    fn registered_schema(
        &self,
        id: i32,
        schema_type: Option<&str>,
        schema: Option<String>,
        references: Vec<schema_registry::types::SchemaReference>,
    ) -> Result<RegisteredSchema, KafkaError> {
        Ok(RegisteredSchema {
            id,
            schema_type: SchemaType::from_registry(schema_type),
            schema: schema.ok_or_else(|| self.fail("schema is missing schema body"))?,
            references: schema_references(references),
        })
    }
}

fn schema_references(
    references: Vec<schema_registry::types::SchemaReference>,
) -> Vec<SchemaReference> {
    references
        .into_iter()
        .filter_map(|reference| {
            Some(SchemaReference {
                name: reference.name?,
                subject: reference.subject?,
                version: reference.version?,
            })
        })
        .collect()
}

fn compatibility_from_config(config: &schema_registry::types::Config) -> SchemaCompatibility {
    let level = config.compatibility_level.map(|level| level.to_string());
    SchemaCompatibility::from_registry(level.as_deref())
}

fn is_not_found<E>(error: &RegistryError<E>) -> bool {
    error.status() == Some(StatusCode::NOT_FOUND)
}

#[cfg(test)]
mod tests {
    use super::*;
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

    async fn mock_catalog_subject(
        server: &MockServer,
        subject: &str,
        versions: &[i32],
        compatibility: Option<&str>,
    ) {
        Mock::given(method("GET"))
            .and(path(registry_path(&["subjects", subject, "versions"])))
            .respond_with(ResponseTemplate::new(200).set_body_json(versions))
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

    async fn mock_latest(
        server: &MockServer,
        subject: &str,
        id: i32,
        version: i32,
        schema_type: &str,
        schema: &str,
    ) {
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

    #[tokio::test]
    async fn lists_subjects_from_registry() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/subjects"))
            .respond_with(ResponseTemplate::new(200).set_body_json(["orders-value"]))
            .mount(&server)
            .await;

        mock_catalog_subject(&server, "orders-value", &[1, 2, 3], Some("BACKWARD")).await;

        let client = SchemaRegistryClient::new("local", &config(&server.uri())).unwrap();
        let subjects = client.subjects().await.unwrap();

        assert_eq!(subjects.len(), 1);
        assert_eq!(subjects[0].subject, "orders-value");
        assert_eq!(subjects[0].latest_version, 3);
        assert_eq!(subjects[0].versions, vec![1, 2, 3]);
        assert_eq!(subjects[0].compatibility, SchemaCompatibility::Backward);
    }

    #[tokio::test]
    async fn subject_catalog_never_requests_a_schema_body() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/subjects"))
            .respond_with(ResponseTemplate::new(200).set_body_json(["orders-value"]))
            .mount(&server)
            .await;

        mock_catalog_subject(&server, "orders-value", &[1, 2, 3], Some("BACKWARD")).await;

        Mock::given(method("GET"))
            .and(path("/config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "compatibilityLevel": "BACKWARD",
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(registry_path(&[
                "subjects",
                "orders-value",
                "versions",
                "latest",
            ])))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "subject": "orders-value",
                "id": 12,
                "version": 3,
                "schemaType": "AVRO",
                "schema": r#"{"type":"string"}"#,
            })))
            .expect(0)
            .mount(&server)
            .await;

        let client = SchemaRegistryClient::new("local", &config(&server.uri())).unwrap();
        let subjects = client.subjects().await.unwrap();

        assert_eq!(subjects[0].subject, "orders-value");
        assert_eq!(subjects[0].latest_version, 3);
    }

    #[tokio::test]
    async fn subject_catalog_fetches_global_config_once_for_many_subjects() {
        let server = MockServer::start().await;
        let names = ["alpha-value", "beta-value", "gamma-value"];

        Mock::given(method("GET"))
            .and(path("/subjects"))
            .respond_with(ResponseTemplate::new(200).set_body_json(names))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "compatibilityLevel": "FULL",
            })))
            .expect(1)
            .mount(&server)
            .await;

        for name in names {
            mock_catalog_subject(&server, name, &[1], None).await;
        }

        let client = SchemaRegistryClient::new("local", &config(&server.uri())).unwrap();
        let subjects = client.subjects().await.unwrap();

        assert_eq!(subjects.len(), 3);
        assert!(
            subjects
                .iter()
                .all(|subject| subject.compatibility == SchemaCompatibility::Full)
        );
    }

    #[tokio::test]
    async fn falls_back_to_global_config_when_subject_config_is_missing() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/subjects"))
            .respond_with(ResponseTemplate::new(200).set_body_json(["payments-value"]))
            .mount(&server)
            .await;

        mock_catalog_subject(&server, "payments-value", &[1], None).await;

        Mock::given(method("GET"))
            .and(path("/config"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "compatibilityLevel": "FULL_TRANSITIVE",
            })))
            .mount(&server)
            .await;

        let client = SchemaRegistryClient::new("local", &config(&server.uri())).unwrap();
        let subjects = client.subjects().await.unwrap();

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

        mock_catalog_subject(&server, subject, &[1, 2], Some("FORWARD_TRANSITIVE")).await;

        let client = SchemaRegistryClient::new("local", &config(&server.uri())).unwrap();
        let subjects = client.subjects().await.unwrap();

        assert_eq!(subjects[0].subject, subject);
        assert_eq!(subjects[0].latest_version, 2);
        assert_eq!(subjects[0].compatibility, SchemaCompatibility::Forward);
    }

    #[tokio::test]
    async fn fetches_subject_schema_from_latest() {
        let server = MockServer::start().await;

        mock_latest(
            &server,
            "orders-value",
            12,
            3,
            "JSON",
            r#"{"type":"string"}"#,
        )
        .await;

        let client = SchemaRegistryClient::new("local", &config(&server.uri())).unwrap();
        let schema = client.subject_schema("orders-value").await.unwrap();

        assert_eq!(schema.subject, "orders-value");
        assert_eq!(schema.id, 12);
        assert_eq!(schema.version, 3);
        assert_eq!(schema.schema_type, SchemaType::Json);
        assert_eq!(schema.schema, r#"{"type":"string"}"#);
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
