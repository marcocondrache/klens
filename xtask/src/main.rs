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
    #[command(about = "Regenerate web/src/api/types.gen.ts from the API types")]
    Types,
}

fn main() -> xshell::Result<()> {
    let sh = Shell::new()?;
    match Cli::parse().command {
        Command::Types => tasks::types::run(&sh),
    }
}
