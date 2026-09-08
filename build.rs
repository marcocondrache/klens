//! Generate a reqwest client from the vendored Confluent Schema Registry OpenAPI spec.

use std::fs;
use std::path::PathBuf;

use progenitor::{GenerationSettings, Generator, InterfaceStyle, TagStyle};
use serde_json::Value;

fn main() {
    let spec_path = PathBuf::from("vendor/schema-registry/schema-registry-api-spec.yaml");
    println!("cargo:rerun-if-changed={}", spec_path.display());

    let yaml = fs::read_to_string(&spec_path).unwrap_or_else(|error| {
        panic!("failed to read {}: {error}", spec_path.display());
    });
    let mut value: Value = serde_yaml_ng::from_str(&yaml).unwrap_or_else(|error| {
        panic!("failed to parse {}: {error}", spec_path.display());
    });

    strip_media_type_parameters(&mut value);
    ensure_response_descriptions(&mut value);
    keep_read_operations(&mut value);
    prune_unused_schemas(&mut value);

    let json = serde_json::to_vec(&value).unwrap_or_else(|error| {
        panic!("failed to serialize OpenAPI document as JSON: {error}");
    });
    let mut deserializer = serde_json::Deserializer::from_slice(&json);
    let spec: openapiv3::OpenAPI = serde_path_to_error::deserialize(&mut deserializer)
        .unwrap_or_else(|error| {
            panic!(
                "failed to deserialize OpenAPI document at {}: {error}",
                error.path()
            );
        });

    let mut settings = GenerationSettings::default();
    settings.with_interface(InterfaceStyle::Builder);
    settings.with_tag(TagStyle::Merged);

    let mut generator = Generator::new(&settings);
    let tokens = generator.generate_tokens(&spec).unwrap_or_else(|error| {
        panic!("failed to generate Schema Registry client: {error:#}");
    });
    let ast = syn::parse2(tokens).unwrap_or_else(|error| {
        panic!("failed to parse generated Schema Registry client: {error}");
    });
    let content = prettyplease::unparse(&ast);

    let mut out_file = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    out_file.push("schema_registry.rs");
    fs::write(&out_file, content).unwrap_or_else(|error| {
        panic!("failed to write {}: {error}", out_file.display());
    });
}

/// Confluent's swagger emits content types like `application/json; qs=0.5`.
/// Those parameters are not useful to progenitor and can break generation.
fn strip_media_type_parameters(value: &mut Value) {
    match value {
        Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for key in keys {
                if looks_like_media_type(&key)
                    && let Some((base, _)) = key.split_once(';')
                {
                    let base = base.trim();
                    if base != key
                        && let Some(child) = map.remove(&key)
                    {
                        match map.get_mut(base) {
                            Some(existing) => merge_values(existing, child),
                            None => {
                                map.insert(base.to_owned(), child);
                            }
                        }
                    }
                }
            }

            for child in map.values_mut() {
                strip_media_type_parameters(child);
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_media_type_parameters(item);
            }
        }
        _ => {}
    }
}

const READ_PATHS: &[&str] = &[
    "/subjects",
    "/subjects/{subject}/versions",
    "/subjects/{subject}/versions/{version}",
    "/config",
    "/config/{subject}",
];

/// Keep only the GET operations klens uses. The full Confluent spec includes
/// write endpoints whose request bodies list four media types, which progenitor
/// cannot generate.
fn keep_read_operations(value: &mut Value) {
    let Some(paths) = value.get_mut("paths").and_then(Value::as_object_mut) else {
        return;
    };

    paths.retain(|path, _| READ_PATHS.contains(&path.as_str()));

    for path_item in paths.values_mut() {
        let Some(path_item) = path_item.as_object_mut() else {
            continue;
        };
        path_item.retain(|key, _| {
            key == "get"
                || key == "parameters"
                || key == "summary"
                || key == "description"
                || key.starts_with("x-")
        });
    }
}

fn ensure_response_descriptions(value: &mut Value) {
    let Some(paths) = value.get_mut("paths").and_then(Value::as_object_mut) else {
        return;
    };

    for path_item in paths.values_mut() {
        let Some(path_item) = path_item.as_object_mut() else {
            continue;
        };

        for (key, operation) in path_item.iter_mut() {
            if !is_http_method(key) {
                continue;
            }

            let Some(responses) = operation
                .as_object_mut()
                .and_then(|operation| operation.get_mut("responses"))
                .and_then(Value::as_object_mut)
            else {
                continue;
            };

            for response in responses.values_mut() {
                let Some(response) = response.as_object_mut() else {
                    continue;
                };
                if !response.contains_key("$ref") {
                    response
                        .entry("description")
                        .or_insert_with(|| Value::String(String::new()));
                }
            }
        }
    }
}

fn is_http_method(key: &str) -> bool {
    matches!(
        key,
        "get" | "put" | "post" | "delete" | "options" | "head" | "patch" | "trace"
    )
}

fn looks_like_media_type(key: &str) -> bool {
    key.contains('/') && !key.starts_with('/')
}

fn merge_values(existing: &mut Value, incoming: Value) {
    match (existing, incoming) {
        (Value::Object(dst), Value::Object(src)) => {
            for (key, value) in src {
                match dst.get_mut(&key) {
                    Some(existing) => merge_values(existing, value),
                    None => {
                        dst.insert(key, value);
                    }
                }
            }
        }
        (_, _) => {}
    }
}

fn prune_unused_schemas(value: &mut Value) {
    let mut keep = std::collections::BTreeSet::new();
    if let Some(paths) = value.get("paths") {
        collect_schema_refs(paths, &mut keep);
    }

    loop {
        let Some(schemas) = value
            .pointer("/components/schemas")
            .and_then(Value::as_object)
        else {
            return;
        };

        let mut extra = std::collections::BTreeSet::new();
        for name in &keep {
            if let Some(schema) = schemas.get(name) {
                collect_schema_refs(schema, &mut extra);
            }
        }
        extra.retain(|name| !keep.contains(name));
        if extra.is_empty() {
            break;
        }
        keep.extend(extra);
    }

    if let Some(schemas) = value
        .pointer_mut("/components/schemas")
        .and_then(Value::as_object_mut)
    {
        schemas.retain(|name, _| keep.contains(name));
    }
}

fn collect_schema_refs(value: &Value, keep: &mut std::collections::BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(reference)) = map.get("$ref")
                && let Some(name) = reference.strip_prefix("#/components/schemas/")
            {
                keep.insert(name.to_owned());
            }
            for child in map.values() {
                collect_schema_refs(child, keep);
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_schema_refs(child, keep);
            }
        }
        _ => {}
    }
}
