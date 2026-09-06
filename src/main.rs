use std::sync::Arc;

use anyhow::Context;
use klens::app::{AppState, router};
use klens::config::Config;
use klens::kafka::ClusterRegistry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load(Config::path())?;
    let _telemetry = klens::telemetry::Telemetry::init(&config.log, env!("CARGO_CRATE_NAME"))?;

    let registry = Arc::new(
        ClusterRegistry::from_config(&config).context("failed to initialize kafka clusters")?,
    );

    tracing::info!(clusters = ?registry.names(), "configured kafka clusters");

    klens::serve(router(AppState::new(registry)), config.bind).await
}
