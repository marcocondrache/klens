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
    #[command(about = "Create random topics on a local broker")]
    Topics(tasks::topics::Args),
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sh = Shell::new()?;
    match Cli::parse().command {
        Command::Schema => tasks::schema::run(&sh)?,
        Command::Topics(args) => tasks::topics::run(&sh, args)?,
    }
    Ok(())
}
