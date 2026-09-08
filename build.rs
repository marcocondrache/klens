use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use progenitor::{GenerationSettings, Generator, InterfaceStyle, TagStyle};

const SPEC_PATH: &str = "vendor/schema-registry/schema-registry-api-spec.yaml";

fn main() -> Result<()> {
    let spec_path = PathBuf::from(SPEC_PATH);

    println!("cargo:rerun-if-changed={}", spec_path.display());

    let spec = serde_yaml_ng::from_str(&fs::read_to_string(&spec_path)?)
        .with_context(|| format!("failed to parse {}", spec_path.display()))?;

    let mut binding = GenerationSettings::default();
    let settings = binding
        .with_interface(InterfaceStyle::Builder)
        .with_tag(TagStyle::Merged);

    let tokens = Generator::new(settings)
        .generate_tokens(&spec)
        .context("failed to generate Schema Registry client")?;

    let content = prettyplease::unparse(
        &syn::parse2(tokens).context("failed to parse generated Schema Registry client")?,
    );

    let mut out_file = PathBuf::from(std::env::var("OUT_DIR").context("OUT_DIR is not set")?);
    out_file.push("schema_registry.rs");

    fs::write(&out_file, content)?;

    Ok(())
}
