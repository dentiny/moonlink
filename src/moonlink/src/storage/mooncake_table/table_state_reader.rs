/// Table state reader is a class, which fetches current table status.
use std::sync::Arc;

use crate::storage::mooncake_table::table_state::TableState;
use crate::storage::IcebergTableConfig;
use crate::storage::MooncakeTable;
use crate::storage::SnapshotTableState;
use crate::Result;

use tokio::sync::RwLock;

pub struct TableStateReader {
    /// Mooncake table id.
    table_id: u32,
    /// Iceberg warehouse location.
    iceberg_warehouse_location: String,
    /// Table snapshot.
    table_snapshot: Arc<RwLock<SnapshotTableState>>,
}

impl TableStateReader {
    pub fn new(
        table_id: u32,
        iceberg_table_config: &IcebergTableConfig,
        table: &MooncakeTable,
    ) -> Self {
        let (table_snapshot, _) = table.get_state_for_reader();
        Self {
            table_id,
            iceberg_warehouse_location: iceberg_table_config.filesystem_config.get_root_path(),
            table_snapshot,
        }
    }

    /// Get current table state.
    pub async fn get_current_table_state(&self) -> Result<TableState> {
        let table_snapshot_state = {
            let mut snapshot_guard = self.table_snapshot.write().await;
            snapshot_guard.get_table_snapshot_states()?
        };
        Ok(TableState {
            table_id: self.table_id,
            table_commit_lsn: table_snapshot_state.table_commit_lsn,
            iceberg_flush_lsn: table_snapshot_state.iceberg_flush_lsn,
            iceberg_warehouse_location: self.iceberg_warehouse_location.clone(),
        })
    }
}
