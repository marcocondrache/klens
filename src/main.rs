use std::sync::Arc;

use anyhow::Context;
use klens::app::{AppState, router};
use klens::config::Config;
use klens::kafka::Clusters;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load(Config::path())?;
    let _telemetry =
        klens::telemetry::Telemetry::init(&config.log_level, env!("CARGO_CRATE_NAME"))?;

    let clusters = Arc::new(
        Clusters::connect(&config)
            .await
            .context("failed to connect to the configured kafka clusters")?,
    );

    tracing::info!(
        clusters = ?clusters.names().collect::<Vec<_>>(),
        "configured kafka clusters"
    );

    let auth = klens::app::AuthState::from_config(config.auth.as_ref())
        .await
        .context("failed to initialize authentication")?;

    if auth.is_enabled() {
        tracing::info!("oidc authentication enabled");
    }

    let state = AppState::with_auth(clusters, auth).with_ingest();

    klens::serve(router(state), config.bind).await
}
