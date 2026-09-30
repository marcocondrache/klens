use serde::Serialize;
use ts_rs::TS;

use crate::kafka::store::{projections, tables};

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct BrokerRow {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
    pub controller: bool,
    pub partition_count: i32,
    pub leader_count: i32,
    /// Bytes in this broker's log dirs. Null until a log dirs poll reports
    /// the broker.
    pub size_bytes: Option<i64>,
    /// Sorted by path.
    pub log_dirs: Vec<LogDir>,
}

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct LogDir {
    pub path: String,
    /// Set when the directory is offline, as a Kafka error name.
    pub error: Option<String>,
    /// Size of the volume the directory lives on. Null for brokers before
    /// Kafka 3.3.
    pub total_bytes: Option<i64>,
    pub usable_bytes: Option<i64>,
    /// A cordoned directory takes no new partitions.
    pub cordoned: bool,
    /// Every log in the directory, including a replica still moving in.
    pub size_bytes: i64,
    pub replica_count: i32,
}

impl From<tables::LogDirInfo> for LogDir {
    fn from(dir: tables::LogDirInfo) -> Self {
        Self {
            path: dir.path,
            error: dir.error,
            total_bytes: dir.total_bytes,
            usable_bytes: dir.usable_bytes,
            cordoned: dir.cordoned,
            size_bytes: dir.size_bytes,
            replica_count: dir.replica_count,
        }
    }
}

impl From<projections::BrokerRow> for BrokerRow {
    fn from(row: projections::BrokerRow) -> Self {
        Self {
            id: row.id,
            host: row.host,
            port: row.port,
            rack: row.rack,
            controller: row.controller,
            partition_count: row.partition_count,
            leader_count: row.leader_count,
            size_bytes: row.size_bytes,
            log_dirs: row.log_dirs.into_iter().map(LogDir::from).collect(),
        }
    }
}
