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

    /// A client copies an argument marked `x-mcp-header` into an HTTP header,
    /// where proxies log it.
    #[test]
    fn no_tool_copies_an_argument_into_a_header() {
        assert!(!klens::app::mcp::tool_list().contains("x-mcp-header"));
    }
}
