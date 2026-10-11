use std::path::{Path, PathBuf};

use xshell::Shell;

pub fn run(sh: &Shell) -> xshell::Result<()> {
    sh.write_file(snapshot(), klens::app::mcp::tool_list())
}

fn snapshot() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../src/app/mcp/tools.json")
}

#[cfg(test)]
mod tests {
    use super::snapshot;

    #[test]
    fn the_tool_list_is_checked_in() {
        let actual = std::fs::read_to_string(snapshot()).unwrap_or_default();

        assert_eq!(
            actual,
            klens::app::mcp::tool_list(),
            "the MCP tool list changed; review the diff, then run `cargo xtask mcp-tools`"
        );
    }
}
