use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use klens::app::{AppState, router};
use klens::config::Config;
use klens::kafka::ClusterRegistry;

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

    let config = Config::load(&args.config)?;
    let registry = Arc::new(
        ClusterRegistry::from_config(&config).context("failed to initialize kafka clusters")?,
    );

    tracing::info!(clusters = ?registry.names(), "configured kafka clusters");

    klens::serve(router(AppState::new(registry)), args.bind).await
}
