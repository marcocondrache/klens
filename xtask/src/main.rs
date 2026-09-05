use std::{fs, path::Path};

use clap::{Parser, Subcommand};

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

fn main() {
    match Cli::parse().command {
        Command::Schema => schema(),
    }
}

fn schema() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../schema.graphql")
        .canonicalize()
        .expect("failed to resolve schema.graphql path");

    let sdl = klens::schema_sdl();
    fs::write(&path, sdl).expect("failed to write schema.graphql");
    println!("regenerated {}", path.display());
}
