use std::sync::Arc;

use anyhow::Context;
use klens::app::{AppState, router};
use klens::config::Config;
use klens::kafka::QueryEngine;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load(Config::path())?;
    let _telemetry = klens::telemetry::Telemetry::init(&config.log, env!("CARGO_CRATE_NAME"))?;

    let engine =
        QueryEngine::from_config(&config).context("failed to initialize kafka query engine")?;

    tracing::info!(clusters = ?engine.names(), "configured kafka clusters");

    klens::serve(router(AppState::new(Arc::new(engine))), config.bind).await
}
