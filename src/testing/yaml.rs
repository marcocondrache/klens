use std::fmt::Debug;

use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::config::parse;

#[derive(Deserialize)]
struct Root<T> {
    value: T,
}

// The config crate reads only a table at the root, so a value under test sits
// one key down, and errors drop that key again.
fn read<T: DeserializeOwned>(source: &str) -> anyhow::Result<T> {
    let indented = source
        .lines()
        .map(|line| format!("  {line}"))
        .collect::<Vec<_>>()
        .join("\n");
    parse::<Root<T>>(&format!("value:\n{indented}"))
        .map(|root| root.value)
        .map_err(|error| {
            anyhow::anyhow!(
                error
                    .to_string()
                    .replace(" for key `value`", "")
                    .replace("`value.", "`")
            )
        })
}

#[track_caller]
pub fn yaml<T: DeserializeOwned>(source: &str) -> T {
    match read(source) {
        Ok(value) => value,
        Err(error) => panic!("{error}\n{source}"),
    }
}

#[track_caller]
pub fn yaml_err<T: DeserializeOwned + Debug>(source: &str) -> String {
    match read::<T>(source) {
        Ok(value) => panic!("parsed {value:?}\n{source}"),
        Err(error) => error.to_string(),
    }
}
