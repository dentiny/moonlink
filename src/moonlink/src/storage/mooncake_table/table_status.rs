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
    /// Mooncake table id.
    pub mooncake_table_id: String,
    /// Mooncake table commit LSN.
    pub commit_lsn: u64,
    /// Iceberg flush LSN.
    pub flush_lsn: Option<u64>,
    /// Iceberg warehouse location.
    pub iceberg_warehouse_location: String,
}
