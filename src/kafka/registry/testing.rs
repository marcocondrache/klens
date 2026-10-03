use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::{Value, json};
use url::Url;
use wiremock::matchers::any;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use super::client::SchemaRegistryClient;
use super::decode::PayloadDecoder;
use crate::config::{SchemaRegistry, SchemaRegistryTuning};

#[derive(Clone)]
pub struct Schema {
    kind: &'static str,
    text: String,
    references: Vec<Value>,
}

impl Schema {
    pub fn avro(text: &str) -> Self {
        Self::of("AVRO", text)
    }

    pub fn json(text: &str) -> Self {
        Self::of("JSON", text)
    }

    pub fn protobuf(text: &str) -> Self {
        Self::of("PROTOBUF", text)
    }

    pub fn referencing(mut self, name: &str, subject: &str, version: i32) -> Self {
        self.references
            .push(json!({ "name": name, "subject": subject, "version": version }));
        self
    }

    fn of(kind: &'static str, text: &str) -> Self {
        Self {
            kind,
            text: text.to_owned(),
            references: Vec::new(),
        }
    }

    fn body(&self) -> Value {
        json!({
            "schemaType": self.kind,
            "schema": self.text,
            "references": self.references,
        })
    }
}

pub struct FakeRegistry {
    server: MockServer,
    contents: Arc<Mutex<Contents>>,
}

#[derive(Default)]
struct Contents {
    schemas: BTreeMap<u32, Schema>,
    subjects: Vec<Subject>,
    global: Option<String>,
    ignores_default_to_global: bool,
    faults: Vec<Fault>,
}

struct Subject {
    name: String,
    versions: BTreeMap<i32, u32>,
    compatibility: Option<String>,
}

struct Fault {
    path: String,
    remaining: Option<usize>,
}

impl FakeRegistry {
    pub async fn start() -> Self {
        let server = MockServer::start().await;
        let contents = Arc::new(Mutex::new(Contents::default()));
        Mock::given(any())
            .respond_with(Answer(Arc::clone(&contents)))
            .mount(&server)
            .await;
        Self { server, contents }
    }

    pub fn uri(&self) -> String {
        self.server.uri()
    }

    pub fn client(&self) -> SchemaRegistryClient {
        let config = SchemaRegistry {
            url: self.uri().parse().expect("registry url"),
            auth: None,
        };
        SchemaRegistryClient::new("local", &config, &SchemaRegistryTuning::default())
            .expect("registry client")
    }

    pub fn decoder(&self) -> PayloadDecoder {
        PayloadDecoder::new(
            self.client(),
            SchemaRegistryTuning::default().missing_schema_ttl,
        )
    }

    pub fn put(&self, id: u32, schema: Schema) {
        self.contents().schemas.insert(id, schema);
    }

    pub fn register(&self, subject: &str, version: i32, id: u32, schema: Schema) {
        let mut contents = self.contents();
        contents.schemas.insert(id, schema);
        match contents
            .subjects
            .iter_mut()
            .find(|known| known.name == subject)
        {
            Some(known) => {
                known.versions.insert(version, id);
            }
            None => contents.subjects.push(Subject {
                name: subject.to_owned(),
                versions: BTreeMap::from([(version, id)]),
                compatibility: None,
            }),
        }
    }

    #[track_caller]
    pub fn set_compatibility(&self, subject: &str, level: &str) {
        let mut contents = self.contents();
        let Some(known) = contents
            .subjects
            .iter_mut()
            .find(|known| known.name == subject)
        else {
            panic!("{subject} is not registered");
        };
        known.compatibility = Some(level.to_owned());
    }

    pub fn set_global_compatibility(&self, level: &str) {
        self.contents().global = Some(level.to_owned());
    }

    pub fn ignore_default_to_global(&self) {
        self.contents().ignores_default_to_global = true;
    }

    pub fn fail(&self, path: &str) {
        self.fault(path, None);
    }

    pub fn fail_once(&self, path: &str) {
        self.fault(path, Some(1));
    }

    pub async fn hits(&self, path: &str) -> usize {
        self.requests()
            .await
            .iter()
            .filter(|request| request.url.path() == path)
            .count()
    }

    pub async fn requests(&self) -> Vec<Request> {
        self.server.received_requests().await.unwrap_or_default()
    }

    fn fault(&self, path: &str, remaining: Option<usize>) {
        self.contents().faults.push(Fault {
            path: path.to_owned(),
            remaining,
        });
    }

    fn contents(&self) -> MutexGuard<'_, Contents> {
        lock(&self.contents)
    }
}

struct Answer(Arc<Mutex<Contents>>);

impl Respond for Answer {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        lock(&self.0).answer(request)
    }
}

impl Contents {
    fn answer(&mut self, request: &Request) -> ResponseTemplate {
        let path = request.url.path();
        if self.trips(path) {
            return ResponseTemplate::new(500);
        }

        let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        match segments.as_slice() {
            ["subjects"] => found(json!(
                self.subjects
                    .iter()
                    .map(|subject| &subject.name)
                    .collect::<Vec<_>>()
            )),
            ["subjects", subject, "versions"] => match self.subject(subject) {
                Some(subject) => found(json!(subject.versions.keys().collect::<Vec<_>>())),
                None => missing(40401, "Subject not found."),
            },
            ["subjects", subject, "versions", version] => self.version(subject, version),
            ["schemas", "ids", id] => match id.parse().ok().and_then(|id| self.schemas.get(&id)) {
                Some(schema) => found(schema.body()),
                None => missing(40403, "Schema not found."),
            },
            ["config"] => level(self.global.as_deref()),
            ["config", subject] => {
                let fallback = !self.ignores_default_to_global
                    && request
                        .url
                        .query_pairs()
                        .any(|(key, value)| key == "defaultToGlobal" && value == "true");
                let own = self
                    .subject(subject)
                    .and_then(|subject| subject.compatibility.as_deref());
                level(own.or(self.global.as_deref().filter(|_| fallback)))
            }
            _ => ResponseTemplate::new(404),
        }
    }

    fn trips(&mut self, path: &str) -> bool {
        let Some(fault) = self.faults.iter_mut().find(|fault| {
            fault.path == path && fault.remaining.is_none_or(|remaining| remaining > 0)
        }) else {
            return false;
        };
        if let Some(remaining) = &mut fault.remaining {
            *remaining -= 1;
        }
        true
    }

    fn subject(&self, segment: &str) -> Option<&Subject> {
        self.subjects
            .iter()
            .find(|subject| encoded(&subject.name) == segment)
    }

    fn version(&self, segment: &str, version: &str) -> ResponseTemplate {
        let Some(subject) = self.subject(segment) else {
            return missing(40401, "Subject not found.");
        };
        let entry = match version {
            "latest" => subject.versions.last_key_value(),
            number => number
                .parse()
                .ok()
                .and_then(|number| subject.versions.get_key_value(&number)),
        };
        let Some((version, id)) = entry else {
            return missing(40402, "Version not found.");
        };
        let mut body = self.schemas[id].body();
        body["subject"] = json!(subject.name);
        body["id"] = json!(id);
        body["version"] = json!(version);
        found(body)
    }
}

fn lock(contents: &Mutex<Contents>) -> MutexGuard<'_, Contents> {
    contents.lock().expect("fake registry contents")
}

fn encoded(subject: &str) -> String {
    let mut url = Url::parse("http://registry/").expect("base url");
    url.path_segments_mut()
        .expect("base url has a path")
        .push(subject);
    url.path().trim_start_matches('/').to_owned()
}

fn found(body: Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(body)
}

fn missing(code: u32, message: &str) -> ResponseTemplate {
    ResponseTemplate::new(404).set_body_json(json!({ "error_code": code, "message": message }))
}

fn level(level: Option<&str>) -> ResponseTemplate {
    match level {
        Some(level) => found(json!({ "compatibilityLevel": level })),
        None => missing(40408, "Subject compatibility level not configured."),
    }
}
