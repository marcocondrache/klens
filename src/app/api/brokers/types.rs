use serde::Serialize;
use ts_rs::TS;

use crate::kafka::store::projections;

#[derive(Debug, Clone, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BrokerRow {
    pub id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
    pub controller: bool,
    pub partition_count: i32,
    pub leader_count: i32,
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
        }
    }
}
