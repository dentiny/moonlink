use crate::pg_replicate::replication_state::ReplicationState;
use crate::pg_replicate::table::TableSchema;
use crate::pg_replicate::util::postgres_schema_to_moonlink_schema;
use crate::{Error, Result};
use moonlink::{
    EventSyncReceiver, EventSyncSender, FileSystemAccessor, FileSystemConfig, IcebergTableConfig,
    MooncakeTable, MooncakeTableConfig, MoonlinkTableConfig, ObjectStorageCache, ReadStateManager,
    TableEvent, TableEventManager, TableHandler,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, mpsc::Sender, oneshot, watch};

/// Components required to replicate a single table.
/// Components that the [`Sink`] needs for processing CDC events.
pub struct TableComponents {
    pub event_sender: Sender<TableEvent>,
}

/// Resources that should be returned to the caller when a table is initialised.
pub struct TableResources {
    pub event_sender: Sender<TableEvent>,
    pub read_state_manager: ReadStateManager,
    pub table_event_manager: TableEventManager,
    pub commit_lsn_tx: watch::Sender<u64>,
    pub flush_lsn_rx: watch::Receiver<u64>,
}

/// Create table event manager sender and receiver.
fn create_table_event_syncer() -> (EventSyncSender, EventSyncReceiver) {
    let (drop_table_completion_tx, drop_table_completion_rx) = oneshot::channel();
    let (flush_lsn_tx, flush_lsn_rx) = watch::channel(0u64);
    let event_sync_sender = EventSyncSender {
        drop_table_completion_tx,
        flush_lsn_tx,
    };
    let event_sync_receiver = EventSyncReceiver {
        drop_table_completion_rx,
        flush_lsn_rx,
    };
    (event_sync_sender, event_sync_receiver)
}

/// Build all components needed to replicate `table_schema`.
pub async fn build_table_components(
    mooncake_table_id: String,
    table_id: u32,
    table_schema: &TableSchema,
    base_path: &Path,
    table_temp_files_directory: String,
    replication_state: &ReplicationState,
    object_storage_cache: ObjectStorageCache,
) -> Result<(TableResources, MoonlinkTableConfig)> {
    let table_path = PathBuf::from(base_path).join(&mooncake_table_id);
    tokio::fs::create_dir_all(&table_path).await?;
    let (arrow_schema, identity) = postgres_schema_to_moonlink_schema(table_schema);

    let remote_root_directory = base_path.to_str().unwrap().to_string();
    let iceberg_table_config = IcebergTableConfig {
        warehouse_uri: remote_root_directory.clone(),
        namespace: vec![table_schema.table_name.schema.clone()],
        table_name: mooncake_table_id,
        ..Default::default()
    };
    let mooncake_table_config = MooncakeTableConfig::new(table_temp_files_directory);
    let table = MooncakeTable::new(
        arrow_schema,
        table_schema.table_name.to_string(),
        table_id,
        table_path,
        identity,
        iceberg_table_config.clone(),
        mooncake_table_config.clone(),
        object_storage_cache,
        Arc::new(FileSystemAccessor::new(filesystem_config)),
    )
    .await?;

    let (commit_lsn_tx, commit_lsn_rx) = watch::channel(0u64);
    let read_state_manager =
        ReadStateManager::new(&table, replication_state.subscribe(), commit_lsn_rx);
    let (event_sync_sender, event_sync_receiver) = create_table_event_syncer();
    let table_handler =
        TableHandler::new(table, event_sync_sender, replication_state.subscribe()).await;
    let flush_lsn_rx = event_sync_receiver.flush_lsn_rx.clone();
    let table_event_manager =
        TableEventManager::new(table_handler.get_event_sender(), event_sync_receiver);
    let event_sender = table_handler.get_event_sender();

    let table_resource = TableResources {
        event_sender,
        read_state_manager,
        table_event_manager,
        commit_lsn_tx,
        flush_lsn_rx,
    };
    let moonlink_table_config = MoonlinkTableConfig {
        mooncake_table_config,
        iceberg_table_config,
    };

    Ok((table_resource, moonlink_table_config))
}
