use crate::storage::deltalake::deltalake_table_manager::DeltalakeTableManager;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::Result;

impl DeltalakeTableManager {
    pub(crate) async fn load_snapshot_from_table_impl(
        &mut self,
    ) -> Result<(u32, MooncakeSnapshot)> {
    }
}
