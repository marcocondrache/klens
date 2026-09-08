//! Generate a reqwest client from the vendored Confluent Schema Registry OpenAPI spec.

use std::fs;
use std::path::PathBuf;

use progenitor::{GenerationSettings, Generator, InterfaceStyle, TagStyle};
use serde_json::Value;

fn main() {
    let spec_path = PathBuf::from("vendor/schema-registry/schema-registry-api-spec.yaml");
    println!("cargo:rerun-if-changed={}", spec_path.display());

    let mut spec: Value =
        serde_yaml_ng::from_str(&fs::read_to_string(&spec_path).unwrap_or_else(|error| {
            panic!("failed to read {}: {error}", spec_path.display());
        }))
        .unwrap_or_else(|error| {
            panic!("failed to parse {}: {error}", spec_path.display());
        });
    make_progenitor_ready(&mut spec);

    let spec = serde_json::from_value(spec).unwrap_or_else(|error| {
        panic!("failed to deserialize OpenAPI document: {error}");
    });

    let mut settings = GenerationSettings::default();
    settings.with_interface(InterfaceStyle::Builder);
    settings.with_tag(TagStyle::Merged);

    let tokens = Generator::new(&settings)
        .generate_tokens(&spec)
        .unwrap_or_else(|error| {
            panic!("failed to generate Schema Registry client: {error:#}");
        });
    let content = prettyplease::unparse(&syn::parse2(tokens).unwrap_or_else(|error| {
        panic!("failed to parse generated Schema Registry client: {error}");
    }));

    let mut out_file = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    out_file.push("schema_registry.rs");
    fs::write(&out_file, content).unwrap_or_else(|error| {
        panic!("failed to write {}: {error}", out_file.display());
    });
}

/// Confluent's swagger is not valid OpenAPI 3.0 input for progenitor: some
/// responses omit the required `description`, and request bodies list multiple
/// media types (`todo!` in progenitor 0.14).
fn make_progenitor_ready(value: &mut Value) {
    match value {
        Value::Object(map) => {
            if let Some(Value::Object(responses)) = map.get_mut("responses") {
                for response in responses.values_mut() {
                    if let Value::Object(response) = response
                        && !response.contains_key("$ref")
                    {
                        response
                            .entry("description")
                            .or_insert_with(|| Value::String(String::new()));
                    }
                }
            }
            if let Some(Value::String(operation_id)) = map.get_mut("operationId") {
                match operation_id.as_str() {
                    "get" => *operation_id = "root_get".into(),
                    "post" => *operation_id = "root_post".into(),
                    _ => {}
                }
            }
            if let Some(Value::Object(content)) = map.get_mut("content") {
                let body = content
                    .get("application/json")
                    .or_else(|| content.get("application/vnd.schemaregistry.v1+json"))
                    .or_else(|| content.values().next())
                    .cloned();
                if let Some(body) = body {
                    *content = serde_json::Map::from_iter([("application/json".to_owned(), body)]);
                }
            }
            for child in map.values_mut() {
                make_progenitor_ready(child);
            }
        }
        Value::Array(items) => {
            for child in items {
                make_progenitor_ready(child);
            }
        }
        _ => {}
    }
}
