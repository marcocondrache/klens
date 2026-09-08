use std::path::Path;

use xshell::Shell;

pub fn run(sh: &Shell) -> xshell::Result<()> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../schema.graphql");
    let sdl = klens::schema().as_sdl();

    sh.write_file(&path, &sdl)?;
    println!("regenerated {}", path.display());
    Ok(())
}
