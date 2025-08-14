use std::collections::{HashMap, HashSet};

use crate::storage::cache::object_storage::cache_config::ObjectStorageCacheConfig;
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
use crate::storage::mooncake_table::replay::replay_events::{
    BackgroundEventId, MooncakeTableEvent,
};
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::storage::mooncake_table::DiskSliceWriter;
use crate::storage::mooncake_table::MooncakeTable;
use crate::storage::mooncake_table::Snapshot as MooncakeSnapshot;
use crate::storage::mooncake_table_config::DiskSliceWriterConfig;
use crate::table_handler::chaos_table_metadata::ReplayTableMetadata;
use crate::table_notify::TableEvent;
use crate::Result;

use std::sync::Arc;
use tempfile::{tempdir, TempDir};
use tokio::io::AsyncBufReadExt;
use tokio::sync::Notify;
use tokio::sync::{mpsc, Mutex};

#[derive(Clone, Debug)]
struct CompletedFlush {
    // Transaction ID.
    xact_id: Option<u32>,
    /// Result for mem slice flush.
    flush_result: Option<Result<DiskSliceWriter>>,
}
#[allow(dead_code)]
#[derive(Clone, Debug)]
struct CompletedMooncakeSnapshot {
    /// Mooncake snapshot LSN.
    lsn: u64,
    /// Mooncake snapshot dump.
    current_snapshot: MooncakeSnapshot,
}

struct ReplayEnvironment {
    cache_temp_dir: TempDir,
    table_temp_dir: TempDir,
}

fn create_disk_writer_config() -> DiskSliceWriterConfig {
    DiskSliceWriterConfig::default()
}

async fn create_mooncake_table_for_replay(
    replay_env: &ReplayEnvironment,
    lines: &mut tokio::io::Lines<tokio::io::BufReader<tokio::fs::File>>,
) -> MooncakeTable {
    let line = lines.next_line().await.unwrap().unwrap();
    let replay_table_metadata: ReplayTableMetadata = serde_json::from_str(&line).unwrap();
    let table_metadata = create_test_table_metadata_disable_flush(
        replay_env
            .table_temp_dir
            .path()
            .to_str()
            .unwrap()
            .to_string(),
        create_disk_writer_config(),
        replay_table_metadata.identity.clone(),
    );
    let object_storage_cache = if replay_table_metadata.local_filesystem_optimization_enabled {
        let config = ObjectStorageCacheConfig::new(
            /*max_bytes=*/ 1 << 30, // 1GiB
            replay_env
                .cache_temp_dir
                .path()
                .to_str()
                .unwrap()
                .to_string(),
            /*optimize_local_filesystem=*/ true,
        );
        ObjectStorageCache::new(config)
    } else {
        ObjectStorageCache::default_for_test(&replay_env.cache_temp_dir)
    };
    let iceberg_table_config =
        get_iceberg_table_config_with_storage_config(replay_table_metadata.storage_config.clone());
    create_mooncake_table(table_metadata, iceberg_table_config, object_storage_cache).await
}

pub(crate) async fn replay() {
    // TODO(hjiang): Take an command line argument.
    let replay_filepath = "/tmp/chaos_test_iyo52k8tsoj1";
    let cache_temp_dir = tempdir().unwrap();
    let table_temp_dir = tempdir().unwrap();
    let replay_env = ReplayEnvironment {
        cache_temp_dir,
        table_temp_dir,
    };

    // Table event reader.
    let file = tokio::fs::File::open(replay_filepath).await.unwrap();
    let buf_reader = tokio::io::BufReader::new(file);
    let mut lines: tokio::io::Lines<tokio::io::BufReader<tokio::fs::File>> = buf_reader.lines();

    // Used to notify certain background table events have completed.
    let event_notification = Arc::new(Notify::new());
    let event_notification_clone = event_notification.clone();

    // Current table states.
    let mut ongoing_flush_event_id = HashSet::new();
    let completed_flush_events: Arc<Mutex<HashMap<BackgroundEventId, CompletedFlush>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let completed_flush_events_clone = completed_flush_events.clone();

    let mut ongoing_mooncake_snapshot_id = HashSet::new();
    let completed_mooncake_snapshots: Arc<
        Mutex<HashMap<BackgroundEventId, CompletedMooncakeSnapshot>>,
    > = Arc::new(Mutex::new(HashMap::new()));
    let completed_mooncake_snapshots_clone = completed_mooncake_snapshots.clone();

    // Expected table snapshot.
    let mut snapshot_disk_files = HashSet::new();

    let mut table = create_mooncake_table_for_replay(&replay_env, &mut lines).await;
    let (table_event_sender, mut table_event_receiver) = mpsc::channel(100);
    let (event_replay_sender, _event_replay_receiver) = mpsc::unbounded_channel();
    table.register_table_notify(table_event_sender).await;
    table.register_event_replay_tx(Some(event_replay_sender));
    // Start a background thread which continuously read from event receiver.
    tokio::spawn(async move {
        while let Some(table_event) = table_event_receiver.recv().await {
            #[allow(clippy::single_match)]
            match table_event {
                TableEvent::FlushResult {
                    id,
                    xact_id,
                    flush_result,
                } => {
                    let completed_flush = CompletedFlush {
                        xact_id,
                        flush_result,
                    };
                    let mut guard = completed_flush_events_clone.lock().await;
                    assert!(guard.insert(id, completed_flush).is_none());
                    event_notification.notify_waiters();
                }
                TableEvent::MooncakeTableSnapshotResult {
                    mooncake_snapshot_result,
                } => {
                    let completed_mooncake_snapshot = CompletedMooncakeSnapshot {
                        lsn: mooncake_snapshot_result.commit_lsn,
                        current_snapshot: mooncake_snapshot_result.current_snapshot.unwrap(),
                    };
                    let mut guard = completed_mooncake_snapshots_clone.lock().await;
                    assert!(guard
                        .insert(mooncake_snapshot_result.id, completed_mooncake_snapshot)
                        .is_none());
                    event_notification.notify_waiters();
                }
                // TODO(hjiang): Implement other background events.
                _ => {}
            }
        }
    });

    while let Some(serialized_event) = lines.next_line().await.unwrap() {
        let replay_table_event: MooncakeTableEvent =
            serde_json::from_str(&serialized_event).unwrap();
        match replay_table_event {
            // =====================
            // Foreground operations
            // =====================
            MooncakeTableEvent::Append(append_event) => {
                if let Some(xact_id) = append_event.xact_id {
                    table
                        .append_in_stream_batch(append_event.row, xact_id)
                        .unwrap();
                } else {
                    table.append(append_event.row).unwrap();
                }
            }
            MooncakeTableEvent::Delete(delete_event) => {
                if let Some(xact_id) = delete_event.xact_id {
                    table
                        .delete_in_stream_batch(delete_event.row, xact_id)
                        .await;
                } else {
                    table
                        .delete(delete_event.row, delete_event.lsn.unwrap())
                        .await;
                }
            }
            MooncakeTableEvent::Commit(commit_event) => {
                if let Some(xact_id) = commit_event.xact_id {
                    table
                        .commit_transaction_stream_impl(xact_id, commit_event.lsn)
                        .unwrap();
                } else {
                    table.commit(commit_event.lsn);
                }
            }
            MooncakeTableEvent::Abort(abort_event) => {
                let flushed_disk_files =
                    table.get_stream_transaction_disk_files(abort_event.xact_id);
                for cur_disk_file in flushed_disk_files.into_iter() {
                    assert!(snapshot_disk_files.remove(&cur_disk_file));
                }
                table.abort_in_stream_batch(abort_event.xact_id);
            }
            // =====================
            // Flush operation
            // =====================
            MooncakeTableEvent::FlushInitiation(flush_initiation_event) => {
                assert!(ongoing_flush_event_id.insert(flush_initiation_event.id));
                if let Some(xact_id) = flush_initiation_event.xact_id {
                    table
                        .flush_stream(xact_id, flush_initiation_event.lsn)
                        .unwrap();
                } else {
                    table.flush(flush_initiation_event.lsn.unwrap()).unwrap();
                }
            }
            MooncakeTableEvent::FlushCompletion(flush_completion_event) => {
                assert!(ongoing_flush_event_id.remove(&flush_completion_event.id));
                loop {
                    let completed_flush_event = {
                        let mut guard = completed_flush_events.lock().await;
                        guard.remove(&flush_completion_event.id)
                    };
                    if let Some(completed_flush_event) = completed_flush_event {
                        if let Some(disk_slice) = completed_flush_event.flush_result {
                            let disk_slice = disk_slice.unwrap();
                            let new_disk_file_ids = disk_slice
                                .output_files()
                                .iter()
                                .map(|f| f.0.file_id())
                                .collect::<Vec<_>>();
                            if let Some(xact_id) = completed_flush_event.xact_id {
                                table.apply_stream_flush_result(
                                    xact_id,
                                    disk_slice,
                                    flush_completion_event.id,
                                );
                            } else {
                                table.apply_flush_result(disk_slice, flush_completion_event.id);
                            }
                            // Check newly persisted disk files.
                            let expected_disk_files_count =
                                snapshot_disk_files.len() + new_disk_file_ids.len();
                            snapshot_disk_files.extend(new_disk_file_ids);
                            assert_eq!(expected_disk_files_count, snapshot_disk_files.len());
                        }
                        break;
                    }
                    // Otherwise block until the corresponding flush event completes.
                    event_notification_clone.notified().await;
                }
            }
            // =====================
            // Mooncake snapshot
            // =====================
            MooncakeTableEvent::MooncakeSnapshotInitiation(snapshot_initiation_event) => {
                assert!(ongoing_mooncake_snapshot_id.insert(snapshot_initiation_event.id));
                let mut snapshot_option = snapshot_initiation_event.option.clone();
                snapshot_option.dump_snapshot = true;
                // Event only recorded when snapshot gets created in source run.
                assert!(table.create_snapshot(snapshot_option));
            }
            MooncakeTableEvent::MooncakeSnapshotCompletion(snapshot_completion_event) => {
                assert!(ongoing_mooncake_snapshot_id.remove(&snapshot_completion_event.id));
                loop {
                    let completed_mooncake_snapshot = {
                        let mut guard = completed_mooncake_snapshots.lock().await;
                        guard.remove(&snapshot_completion_event.id)
                    };
                    if let Some(completed_mooncake_snapshot) = completed_mooncake_snapshot {
                        let actual_disk_files = completed_mooncake_snapshot
                            .current_snapshot
                            .disk_files
                            .iter()
                            .map(|cur_disk_file| cur_disk_file.0.file_id())
                            .collect::<HashSet<_>>();
                        assert_eq!(snapshot_disk_files, actual_disk_files);

                        // Update mooncake table related states.
                        table.mark_mooncake_snapshot_completed();
                        break;
                    }
                    // Otherwise block until the corresponding flush event completes.
                    event_notification_clone.notified().await;
                }
            }

            // TODO(hjiang): Handle other events.
            _ => {}
        }
    }
}

#[tokio::test]
async fn test_table_event_replay() {
    replay().await;
}
