use anyhow::Context;
use klens::app::{AppState, AuthState, Limits, router};
use klens::config::Config;
use klens::kafka::Clusters;
use klens::kafka::ingest::Ingest;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load(Config::path())?;
    let _telemetry =
        klens::telemetry::Telemetry::init(&config.log_level, env!("CARGO_CRATE_NAME"))?;

    let clusters = Clusters::connect(&config)
        .await
        .context("failed to connect to the configured kafka clusters")?;

    tracing::info!(
        clusters = ?clusters.names().collect::<Vec<_>>(),
        "configured kafka clusters"
    );

    let auth = AuthState::from_config(config.auth.as_ref())
        .await
        .context("failed to initialize authentication")?;

    if auth.is_enabled() {
        tracing::info!("oidc authentication enabled");
    }

    // Dropping the ingest aborts its lanes, so it lives as long as the server.
    let _ingest = Ingest::start(&clusters);
    let state = AppState::new(clusters, auth, Limits::from_env()).with_writes_from(&config);

    klens::serve(router(state), config.bind).await
}
