use std::net::SocketAddr;

use clap::Parser;

#[derive(Debug, Parser)]
#[command(author, version, about)]
struct Args {
    #[arg(long, env = "BIND", default_value = "0.0.0.0:8080")]
    bind: SocketAddr,

    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::try_parse()?;
    let _telemetry = klens_telemetry::Telemetry::init(&args.log, env!("CARGO_CRATE_NAME"))?;

    klens_server::serve(klens_app::router(), args.bind).await
}
