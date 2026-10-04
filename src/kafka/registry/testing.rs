use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard};

use percent_encoding::percent_decode_str;
use serde_json::{Value, json};
use url::Url;
use wiremock::http::Method;
use wiremock::matchers::any;
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use super::client::SchemaRegistryClient;
use super::decode::PayloadDecoder;
use crate::config::{SchemaRegistry, SchemaRegistryTuning};

#[derive(Clone, PartialEq)]
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

    fn parse(body: &[u8]) -> Self {
        let body: Value = serde_json::from_slice(body).expect("a schema body");
        let kind = match body["schemaType"].as_str() {
            Some("JSON") => "JSON",
            Some("PROTOBUF") => "PROTOBUF",
            _ => "AVRO",
        };
        Self {
            kind,
            text: body["schema"].as_str().expect("schema text").to_owned(),
            references: body["references"].as_array().cloned().unwrap_or_default(),
        }
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
    refusal: Option<(u16, u32, String)>,
}

struct Subject {
    name: String,
    versions: BTreeMap<i32, u32>,
    soft_deleted: BTreeSet<i32>,
    compatibility: Option<String>,
}

impl Subject {
    fn live(&self) -> impl DoubleEndedIterator<Item = (&i32, &u32)> {
        self.versions
            .iter()
            .filter(|(version, _)| !self.soft_deleted.contains(version))
    }
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
        self.contents().register(subject, version, id, schema);
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

    /// Refuses the next write with an HTTP status and a registry error code.
    pub fn refuse(&self, status: u16, code: u32, message: &str) {
        self.contents().refusal = Some((status, code, message.to_owned()));
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
        if request.method == Method::DELETE {
            if let Some(refusal) = self.refused() {
                return refusal;
            }
            let permanent = request
                .url
                .query_pairs()
                .any(|(key, value)| key == "permanent" && value == "true");
            return match segments.as_slice() {
                ["subjects", subject] => self.delete_subject(subject, permanent),
                ["subjects", subject, "versions", version] => {
                    self.delete_version(subject, version, permanent)
                }
                _ => ResponseTemplate::new(404),
            };
        }
        if request.method == Method::POST {
            let schema = Schema::parse(&request.body);
            return match segments.as_slice() {
                ["subjects", subject, "versions"] => self.register_version(subject, schema),
                ["subjects", subject] => match self.find(subject, &schema) {
                    Some(version) => self.version(subject, &version.to_string()),
                    None => missing(40403, "Schema not found."),
                },
                _ => ResponseTemplate::new(404),
            };
        }
        match segments.as_slice() {
            ["subjects"] => found(json!(
                self.subjects
                    .iter()
                    .filter(|subject| subject.live().next().is_some())
                    .map(|subject| &subject.name)
                    .collect::<Vec<_>>()
            )),
            ["subjects", subject, "versions"] => match self.subject(subject) {
                Some(subject) if subject.live().next().is_some() => found(json!(
                    subject
                        .live()
                        .map(|(version, _)| version)
                        .collect::<Vec<_>>()
                )),
                _ => missing(40401, "Subject not found."),
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

    fn refused(&mut self) -> Option<ResponseTemplate> {
        let (status, code, message) = self.refusal.take()?;
        Some(
            ResponseTemplate::new(status)
                .set_body_json(json!({ "error_code": code, "message": message })),
        )
    }

    fn delete_subject(&mut self, segment: &str, permanent: bool) -> ResponseTemplate {
        let Some(index) = self
            .subjects
            .iter()
            .position(|subject| encoded(&subject.name) == segment)
        else {
            return missing(40401, "Subject not found.");
        };
        let subject = &mut self.subjects[index];
        let live: Vec<i32> = subject.live().map(|(version, _)| *version).collect();
        match (permanent, live.is_empty()) {
            (false, true) => missing(40404, "Subject was soft deleted."),
            (false, false) => {
                subject.soft_deleted.extend(&live);
                found(json!(live))
            }
            (true, false) => missing(40405, "Subject was not deleted first."),
            (true, true) => {
                let removed = self.subjects.remove(index);
                found(json!(removed.versions.keys().collect::<Vec<_>>()))
            }
        }
    }

    fn delete_version(
        &mut self,
        segment: &str,
        version: &str,
        permanent: bool,
    ) -> ResponseTemplate {
        let Some(subject) = self
            .subjects
            .iter_mut()
            .find(|subject| encoded(&subject.name) == segment)
        else {
            return missing(40401, "Subject not found.");
        };
        let Some(version) = version
            .parse()
            .ok()
            .filter(|version| subject.versions.contains_key(version))
        else {
            return missing(40402, "Version not found.");
        };
        match (permanent, subject.soft_deleted.contains(&version)) {
            (false, true) => missing(40406, "Version was soft deleted."),
            (false, false) => {
                subject.soft_deleted.insert(version);
                found(json!(version))
            }
            (true, false) => missing(40407, "Version was not deleted first."),
            (true, true) => {
                subject.versions.remove(&version);
                subject.soft_deleted.remove(&version);
                found(json!(version))
            }
        }
    }

    fn register_version(&mut self, segment: &str, schema: Schema) -> ResponseTemplate {
        if let Some(refusal) = self.refused() {
            return refusal;
        }
        if let Some(version) = self.find(segment, &schema) {
            let id = self.subject(segment).expect("a found subject").versions[&version];
            return found(json!({ "id": id }));
        }
        let id = self.schemas.keys().last().map_or(1, |id| id + 1);
        let name = percent_decode_str(segment).decode_utf8_lossy().into_owned();
        let version = self
            .subject(segment)
            .and_then(|subject| subject.versions.keys().last())
            .map_or(1, |version| version + 1);
        self.register(&name, version, id, schema);
        found(json!({ "id": id }))
    }

    fn register(&mut self, subject: &str, version: i32, id: u32, schema: Schema) {
        self.schemas.insert(id, schema);
        match self.subjects.iter_mut().find(|known| known.name == subject) {
            Some(known) => {
                known.versions.insert(version, id);
            }
            None => self.subjects.push(Subject {
                name: subject.to_owned(),
                versions: BTreeMap::from([(version, id)]),
                soft_deleted: BTreeSet::new(),
                compatibility: None,
            }),
        }
    }

    fn find(&self, segment: &str, schema: &Schema) -> Option<i32> {
        self.subject(segment)?
            .live()
            .find(|(_, id)| self.schemas.get(id) == Some(schema))
            .map(|(version, _)| *version)
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
            "latest" => subject.live().next_back(),
            number => number
                .parse::<i32>()
                .ok()
                .and_then(|number| subject.live().find(|(version, _)| **version == number)),
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
