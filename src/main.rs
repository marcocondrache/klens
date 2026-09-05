use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use klens::app::{AppState, router};
use klens::kafka::{ClusterConfig, ClusterRegistry, ClustersConfig};

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(long, env = "BIND", default_value = "0.0.0.0:8080")]
    bind: SocketAddr,

    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log: String,

    #[arg(long, env = "CONFIG", default_value = "config/clusters.yaml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::try_parse()?;
    let _telemetry = klens::telemetry::Telemetry::init(&args.log, env!("CARGO_CRATE_NAME"))?;

    let clusters = load_clusters(&args.config)?;
    let registry =
        Arc::new(ClusterRegistry::build(clusters).context("failed to initialize kafka clusters")?);

    tracing::info!(clusters = ?registry.names(), "configured kafka clusters");

    klens::serve(router(AppState::new(registry)), args.bind).await
}

fn load_clusters(path: &Path) -> anyhow::Result<Vec<ClusterConfig>> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;

    let parsed: ClustersConfig = serde_yaml_ng::from_str(&raw)
        .with_context(|| format!("failed to parse config file {}", path.display()))?;

    Ok(parsed.clusters)
}
