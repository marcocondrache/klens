use reqwest::StatusCode;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::task::JoinSet;
use url::Url;

use crate::config::SchemaRegistryConfig;
use crate::environment::SCHEMA_REGISTRY_TIMEOUT;
use crate::kafka::error::KafkaError;
use crate::kafka::model::{SchemaCompatibility, SchemaSubject, SchemaType};

/// HTTP client for a Confluent-compatible Schema Registry.
#[derive(Clone)]
pub struct SchemaRegistryClient {
    cluster: String,
    base: Url,
    http: reqwest::Client,
    username: Option<String>,
    password: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VersionedSchema {
    id: i32,
    version: i32,
    #[serde(rename = "schemaType")]
    schema_type: Option<String>,
    schema: String,
}

#[derive(Debug, Deserialize)]
struct CompatibilityConfig {
    #[serde(rename = "compatibilityLevel", alias = "compatibility")]
    compatibility_level: Option<String>,
}

impl SchemaRegistryClient {
    pub fn new(
        cluster: impl Into<String>,
        config: &SchemaRegistryConfig,
    ) -> Result<Self, KafkaError> {
        let cluster = cluster.into();
        let mut base = Url::parse(&config.url).map_err(|error| KafkaError::SchemaRegistry {
            cluster: cluster.clone(),
            message: error.to_string(),
        })?;
        if !base.path().ends_with('/') {
            let path = format!("{}/", base.path());
            base.set_path(&path);
        }

        let http = reqwest::Client::builder()
            .timeout(*SCHEMA_REGISTRY_TIMEOUT)
            .build()
            .map_err(|error| KafkaError::SchemaRegistry {
                cluster: cluster.clone(),
                message: error.to_string(),
            })?;

        Ok(Self {
            cluster,
            base,
            http,
            username: config.username.clone(),
            password: config.password.clone(),
        })
    }

    pub async fn subjects(&self) -> Result<Vec<SchemaSubject>, KafkaError> {
        let names: Vec<String> = self.get(self.join(&["subjects"])?).await?;
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

    async fn load_subject(&self, name: &str) -> Result<SchemaSubject, KafkaError> {
        let versions: Vec<i32> = self
            .get(self.join(&["subjects", name, "versions"])?)
            .await?;
        let latest: VersionedSchema = self
            .get(self.join(&["subjects", name, "versions", "latest"])?)
            .await?;
        let compatibility = self.compatibility(name).await?;

        Ok(SchemaSubject {
            subject: name.to_owned(),
            id: latest.id,
            schema_type: SchemaType::from_registry(latest.schema_type.as_deref()),
            latest_version: latest.version,
            versions,
            compatibility,
            schema: latest.schema,
        })
    }

    async fn compatibility(&self, name: &str) -> Result<SchemaCompatibility, KafkaError> {
        if let Some(config) = self
            .get_optional::<CompatibilityConfig>(self.join(&["config", name])?)
            .await?
        {
            return Ok(SchemaCompatibility::from_registry(
                config.compatibility_level.as_deref(),
            ));
        }

        if let Some(config) = self
            .get_optional::<CompatibilityConfig>(self.join(&["config"])?)
            .await?
        {
            return Ok(SchemaCompatibility::from_registry(
                config.compatibility_level.as_deref(),
            ));
        }

        Ok(SchemaCompatibility::None)
    }

    fn join(&self, segments: &[&str]) -> Result<Url, KafkaError> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|()| self.fail("schema registry URL cannot be a base"))?
            .extend(segments);
        Ok(url)
    }

    fn request(&self, url: Url) -> reqwest::RequestBuilder {
        let request = self.http.get(url).header(
            "Accept",
            "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json",
        );

        match (&self.username, &self.password) {
            (Some(username), Some(password)) => request.basic_auth(username, Some(password)),
            _ => request,
        }
    }

    async fn get<T: DeserializeOwned>(&self, url: Url) -> Result<T, KafkaError> {
        match self.get_optional(url.clone()).await? {
            Some(value) => Ok(value),
            None => Err(self.fail(format!("404 from {url}"))),
        }
    }

    async fn get_optional<T: DeserializeOwned>(&self, url: Url) -> Result<Option<T>, KafkaError> {
        let response = self
            .request(url.clone())
            .send()
            .await
            .map_err(|error| self.fail(error.to_string()))?;
        let status = response.status();

        if status == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(self.fail(format!("{status} from {url}: {body}")));
        }

        response
            .json()
            .await
            .map_err(|error| self.fail(error.to_string()))
            .map(Some)
    }

    fn fail(&self, message: impl Into<String>) -> KafkaError {
        KafkaError::SchemaRegistry {
            cluster: self.cluster.clone(),
            message: message.into(),
        }
    }
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
                    .respond_with(ResponseTemplate::new(404))
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

    #[test]
    fn defaults_missing_schema_type_to_avro() {
        assert_eq!(SchemaType::from_registry(None), SchemaType::Avro);
        assert_eq!(SchemaType::from_registry(Some("AVRO")), SchemaType::Avro);
    }
}
