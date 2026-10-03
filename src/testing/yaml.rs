use std::fmt::Debug;

use serde::de::DeserializeOwned;

use crate::config::parse;

#[track_caller]
pub fn yaml<T: DeserializeOwned>(source: &str) -> T {
    match parse(source) {
        Ok(value) => value,
        Err(error) => panic!("{error}\n{source}"),
    }
}

#[track_caller]
pub fn yaml_err<T: DeserializeOwned + Debug>(source: &str) -> String {
    match parse::<T>(source) {
        Ok(value) => panic!("parsed {value:?}\n{source}"),
        Err(error) => error.to_string(),
    }
}
