/// Mooncake table states.
use serde::{Deserialize, Serialize};

pub(crate) struct TableSnapshotStatus {
    /// Mooncake table commit LSN.
    pub(crate) commit_lsn: u64,
    /// Iceberg flush LSN.
    pub(crate) flush_lsn: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TableStatus {
    /// Database id.
    pub database_id: u32,
    /// Mooncake table id.
    pub table_id: u32,
    /// Mooncake table commit LSN.
    pub commit_lsn: u64,
    /// Iceberg flush LSN.
    pub flush_lsn: Option<u64>,
    /// Iceberg warehouse location.
    pub iceberg_warehouse_location: String,
}
