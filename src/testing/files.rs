use std::io::Write;

use tempfile::NamedTempFile;

pub fn temp_file(contents: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("temp file");
    file.write_all(contents.as_bytes())
        .expect("write temp file");
    file
}
