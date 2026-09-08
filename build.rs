//! Generate a reqwest client from the vendored Confluent Schema Registry OpenAPI spec.

use std::fs;
use std::path::PathBuf;

use progenitor::{GenerationSettings, Generator, InterfaceStyle, TagStyle};

fn main() {
    let spec_path = PathBuf::from("vendor/schema-registry/schema-registry-api-spec.yaml");
    println!("cargo:rerun-if-changed={}", spec_path.display());

    let spec = serde_yaml_ng::from_str(&fs::read_to_string(&spec_path).unwrap_or_else(|error| {
        panic!("failed to read {}: {error}", spec_path.display());
    }))
    .unwrap_or_else(|error| {
        panic!("failed to parse {}: {error}", spec_path.display());
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
