/// Mooncake table states.
use serde::{Deserialize, Serialize};

pub(crate) struct TableSnapshotState {
    /// Mooncake table commit LSN.
    pub(crate) table_commit_lsn: u64,
    /// Iceberg flush LSN.
    pub(crate) iceberg_flush_lsn: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TableState {
    /// Database id.
    pub database_id: u32,
    /// Mooncake table id.
    pub table_id: u32,
    /// Mooncake table commit LSN.
    pub table_commit_lsn: u64,
    /// Iceberg flush LSN.
    pub iceberg_flush_lsn: Option<u64>,
    /// Iceberg warehouse location.
    pub iceberg_warehouse_location: String,
}
