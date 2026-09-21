use std::path::Path;

use xshell::Shell;

pub fn run(sh: &Shell) -> xshell::Result<()> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../web/src/api/types.gen.ts");
    sh.write_file(path, klens::typescript())?;
    Ok(())
}
