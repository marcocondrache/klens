use anyhow::Result;
use clap::{Parser, Subcommand};
use xshell::Shell;

mod tasks;

#[derive(Parser)]
#[command(name = "xtask", about = "Project automation tasks")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Regenerate schema.graphql from the Rust schema")]
    Schema,
    #[command(about = "Create sample Kafka topics and produce JSON messages for local UI testing")]
    Seed(tasks::seed::SeedArgs),
}

fn main() -> Result<()> {
    let sh = Shell::new()?;
    match Cli::parse().command {
        Command::Schema => tasks::schema::run(&sh)?,
        Command::Seed(args) => tasks::seed::run(args)?,
    }
    Ok(())
}
