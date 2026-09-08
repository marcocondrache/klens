//! Generate a reqwest client from the vendored Confluent Schema Registry OpenAPI spec.

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use progenitor::{GenerationSettings, Generator, InterfaceStyle, TagStyle};

fn main() -> Result<()> {
    let spec_path = PathBuf::from("vendor/schema-registry/schema-registry-api-spec.yaml");
    println!("cargo:rerun-if-changed={}", spec_path.display());

    let spec = serde_yaml_ng::from_str(
        &fs::read_to_string(&spec_path)
            .with_context(|| format!("failed to read {}", spec_path.display()))?,
    )
    .with_context(|| format!("failed to parse {}", spec_path.display()))?;

    let mut settings = GenerationSettings::default();
    settings.with_interface(InterfaceStyle::Builder);
    settings.with_tag(TagStyle::Merged);

    let tokens = Generator::new(&settings)
        .generate_tokens(&spec)
        .context("failed to generate Schema Registry client")?;
    let content = prettyplease::unparse(
        &syn::parse2(tokens).context("failed to parse generated Schema Registry client")?,
    );

    let mut out_file = PathBuf::from(std::env::var("OUT_DIR").context("OUT_DIR is not set")?);
    out_file.push("schema_registry.rs");
    fs::write(&out_file, content)
        .with_context(|| format!("failed to write {}", out_file.display()))?;
    Ok(())
}
