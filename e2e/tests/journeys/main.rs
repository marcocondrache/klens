use serde_json::Value;

mod access;
mod brokers;
mod cluster;
mod groups;
mod records;
mod topics;
mod updates;

static MISSING: Value = Value::Null;

fn row<'a>(rows: &'a Value, key: &str, value: &str) -> &'a Value {
    rows.as_array()
        .and_then(|rows| rows.iter().find(|row| row[key] == value))
        .unwrap_or(&MISSING)
}
