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
}

fn main() -> xshell::Result<()> {
    let sh = Shell::new()?;
    match Cli::parse().command {
        Command::Schema => tasks::schema::run(&sh),
    }
}
