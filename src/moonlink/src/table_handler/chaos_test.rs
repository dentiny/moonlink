/// This file implements chaos test for table handler.
///
/// System invariants:
/// - Begin events happen only after end events
/// - End events happen only after begin events
/// - Rows to delete comes from committed appended ones
/// - LSN always increases
use crate::event_sync::create_table_event_syncer;
use crate::row::{MoonlinkRow, RowValue};
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::table_handler::test_utils::*;
use crate::table_handler::{TableEvent, TableHandler};
use crate::union_read::ReadStateManager;
use crate::ObjectStorageCache;
use crate::TableEventManager;

use rand::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::{tempdir, TempDir};
use tokio::sync::mpsc;
use tokio::sync::watch;

/// Create a test moonlink row.
fn create_row(id: i32, name: &str, age: i32) -> MoonlinkRow {
    MoonlinkRow::new(vec![
        RowValue::Int32(id),
        RowValue::ByteArray(name.as_bytes().to_vec()),
        RowValue::Int32(age),
    ])
}

/// Events randomly selected for chaos test.
#[derive(Debug)]
struct ChaosEvent {
    table_events: Vec<TableEvent>,
    table_maintenance_event: Option<TableEvent>,
    snapshot_read_lsn: Option<u64>,
    force_snapshot_lsn: Option<u64>,
}

impl ChaosEvent {
    fn create_table_events(table_events: Vec<TableEvent>) -> Self {
        Self {
            table_events,
            table_maintenance_event: None,
            snapshot_read_lsn: None,
            force_snapshot_lsn: None,
        }
    }
    fn create_table_maintenance_event(table_event: TableEvent) -> Self {
        Self {
            table_events: vec![],
            table_maintenance_event: Some(table_event),
            snapshot_read_lsn: None,
            force_snapshot_lsn: None,
        }
    }
    fn create_snapshot_read(lsn: u64) -> Self {
        Self {
            table_events: vec![],
            table_maintenance_event: None,
            snapshot_read_lsn: Some(lsn),
            force_snapshot_lsn: None,
        }
    }
    fn create_force_snapshot(lsn: u64) -> Self {
        Self {
            table_events: vec![],
            table_maintenance_event: None,
            snapshot_read_lsn: None,
            force_snapshot_lsn: Some(lsn),
        }
    }
}

struct ChaosState {
    /// Used to generate random events, with current timestamp as random seed.
    rng: StdRng,
    /// Used to generate rows to insert.
    next_id: i32,
    /// Inserted rows in committed transactions.
    committed_inserted_rows: VecDeque<(i32 /*id*/, MoonlinkRow)>,
    /// Inserted rows in the current uncommitted transaction.
    uncommitted_inserted_rows: VecDeque<(i32 /*id*/, MoonlinkRow)>,
    /// Deleted row ids in the current uncommitted transaction.
    uncommitted_deleted_rows: HashSet<i32>,
    /// Used to indicate whether there's an ongoing transaction.
    has_begun: bool,
    /// LSN to use for the next operation, including update operations and commits.
    cur_lsn: u64,
    /// Used to read snapshot.
    read_state_manager: ReadStateManager,
    /// Last commit LSN.
    last_commit_lsn: Option<u64>,
}

impl ChaosState {
    fn new(read_state_manager: ReadStateManager) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let rng = StdRng::seed_from_u64(nanos as u64);
        Self {
            rng,
            has_begun: false,
            next_id: 0,
            committed_inserted_rows: VecDeque::new(),
            uncommitted_inserted_rows: VecDeque::new(),
            uncommitted_deleted_rows: HashSet::new(),
            read_state_manager,
            cur_lsn: 0,
            last_commit_lsn: None,
        }
    }

    /// Get the current LSN to use for the current operation, and increment.
    fn get_and_update_cur_lsn(&mut self) -> u64 {
        let cur_lsn = self.cur_lsn;
        self.cur_lsn += 1;
        cur_lsn
    }

    fn begin_transaction(&mut self) {
        assert!(!self.has_begun);
        self.has_begun = true;
    }

    fn commit_transaction(&mut self, lsn: u64) {
        // Set chaos test states.
        assert!(self.has_begun);
        self.has_begun = false;
        self.last_commit_lsn = Some(lsn);

        // Set table states.
        self.committed_inserted_rows
            .retain(|(id, _)| !self.uncommitted_deleted_rows.contains(id));
        self.committed_inserted_rows
            .extend(self.uncommitted_inserted_rows.drain(..));
    }

    fn get_next_row_to_append(&mut self) -> MoonlinkRow {
        let row = create_row(self.next_id, /*name=*/ "user", self.next_id % 5);
        self.uncommitted_inserted_rows
            .push_back((self.next_id, row.clone()));
        self.next_id += 1;
        row
    }

    /// Return all [`id`] fields for the moonlink rows which haven't been deleted in the alphabetical order.
    fn get_valid_ids(&self) -> Vec<i32> {
        self.committed_inserted_rows
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>()
    }

    /// Get a random row to delete.
    fn get_random_row_to_delete(&mut self) -> (i32, MoonlinkRow) {
        let candidates: Vec<_> = self
            .committed_inserted_rows
            .iter()
            .filter(|(id, _)| !self.uncommitted_deleted_rows.contains(id))
            .collect();
        assert!(!candidates.is_empty());

        let random_idx = self.rng.random_range(0..candidates.len());
        let (id, row) = candidates[random_idx];
        (*id, row.clone())
    }

    fn generate_random_events(&mut self) -> ChaosEvent {
        #[derive(Debug, Clone)]
        enum EventKind {
            Begin,
            Append,
            Delete,
            EndWithFlush,
            EndNoFlush,
            ReadSnapshot,
            /// Foreground force snapshot only happens after commit operation, otherwise it gets blocked.
            ForegroundForceSnapshot,
            /// Foreground force index merge only happens after commit operation, otherwise it gets blocked.
            ForegroundForceIndexMerge,
        }

        let mut choices = vec![];

        if self.last_commit_lsn.is_some() {
            choices.push(EventKind::ReadSnapshot);
            // Foreground table maintenance operations happen after a commit operation.
            if self.uncommitted_inserted_rows.is_empty()
                && self.uncommitted_inserted_rows.is_empty()
            {
                choices.push(EventKind::ForegroundForceSnapshot);
                choices.push(EventKind::ForegroundForceIndexMerge);
            }
        }
        if !self.has_begun {
            choices.push(EventKind::Begin);
        } else {
            choices.push(EventKind::Append);
            if !self.committed_inserted_rows.is_empty()
                && self.committed_inserted_rows.len() != self.uncommitted_deleted_rows.len()
            {
                choices.push(EventKind::Delete);
            }
            choices.push(EventKind::EndWithFlush);
            choices.push(EventKind::EndNoFlush);
        }

        match *choices.choose(&mut self.rng).unwrap() {
            EventKind::ReadSnapshot => {
                ChaosEvent::create_snapshot_read(self.last_commit_lsn.unwrap())
            }
            EventKind::ForegroundForceSnapshot => {
                ChaosEvent::create_force_snapshot(self.last_commit_lsn.unwrap())
            }
            EventKind::ForegroundForceIndexMerge => {
                ChaosEvent::create_table_maintenance_event(TableEvent::ForceRegularIndexMerge)
            }
            EventKind::Begin => {
                self.begin_transaction();
                let row = self.get_next_row_to_append();
                ChaosEvent::create_table_events(vec![TableEvent::Append {
                    row,
                    xact_id: None,
                    lsn: self.get_and_update_cur_lsn(),
                    is_copied: false,
                }])
            }
            EventKind::Append => {
                let row = self.get_next_row_to_append();
                ChaosEvent::create_table_events(vec![TableEvent::Append {
                    row,
                    xact_id: None,
                    lsn: self.get_and_update_cur_lsn(),
                    is_copied: false,
                }])
            }
            EventKind::Delete => {
                let (id, row) = self.get_random_row_to_delete();
                self.uncommitted_deleted_rows.insert(id);
                ChaosEvent::create_table_events(vec![TableEvent::Delete {
                    row,
                    xact_id: None,
                    lsn: self.get_and_update_cur_lsn(),
                }])
            }
            EventKind::EndWithFlush => {
                let lsn = self.get_and_update_cur_lsn();
                self.commit_transaction(lsn);
                ChaosEvent::create_table_events(vec![TableEvent::CommitFlush {
                    lsn,
                    xact_id: None,
                }])
            }
            EventKind::EndNoFlush => {
                let lsn = self.get_and_update_cur_lsn();
                self.commit_transaction(lsn);
                ChaosEvent::create_table_events(vec![TableEvent::Commit { lsn, xact_id: None }])
            }
        }
    }
}

#[allow(dead_code)]
struct TestEnvironment {
    iceberg_temp_dir: TempDir,
    cache_temp_dir: TempDir,
    table_temp_dir: TempDir,
    object_storage_cache: ObjectStorageCache,
    read_state_manager: ReadStateManager,
    table_event_manager: TableEventManager,
    table_handler: TableHandler,
    event_sender: mpsc::Sender<TableEvent>,
    event_replay_rx: mpsc::UnboundedReceiver<TableEvent>,
    last_commit_lsn_tx: watch::Sender<u64>,
    replication_lsn_tx: watch::Sender<u64>,
}

impl TestEnvironment {
    async fn new() -> Self {
        let iceberg_temp_dir = tempdir().unwrap();
        let iceberg_table_config = get_iceberg_table_config(&iceberg_temp_dir);

        let table_temp_dir = tempdir().unwrap();
        let mooncake_table_metadata = create_test_table_metadata_with_index_merge_disable_flush(
            table_temp_dir.path().to_str().unwrap().to_string(),
        );

        // Local filesystem to store read-through cache.
        let cache_temp_dir = tempdir().unwrap();
        let object_storage_cache = ObjectStorageCache::default_for_test(&cache_temp_dir);

        // Create mooncake table and table event notification receiver.
        let table = create_mooncake_table(
            mooncake_table_metadata,
            iceberg_table_config,
            object_storage_cache.clone(),
        )
        .await;
        let (replication_lsn_tx, replication_lsn_rx) = watch::channel(0u64);
        let (last_commit_lsn_tx, last_commit_lsn_rx) = watch::channel(0u64);
        let read_state_manager =
            ReadStateManager::new(&table, replication_lsn_rx.clone(), last_commit_lsn_rx);
        let (table_event_sync_sender, table_event_sync_receiver) = create_table_event_syncer();
        let (event_replay_tx, event_replay_rx) = mpsc::unbounded_channel();
        let table_handler = TableHandler::new(
            table,
            table_event_sync_sender,
            replication_lsn_rx.clone(),
            Some(event_replay_tx),
        )
        .await;
        let table_event_manager =
            TableEventManager::new(table_handler.get_event_sender(), table_event_sync_receiver);
        let event_sender = table_handler.get_event_sender();

        Self {
            iceberg_temp_dir,
            cache_temp_dir,
            table_temp_dir,
            object_storage_cache,
            table_event_manager,
            read_state_manager,
            table_handler,
            event_sender,
            event_replay_rx,
            replication_lsn_tx,
            last_commit_lsn_tx,
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_chaos() {
    let mut env = TestEnvironment::new().await;
    let event_sender = env.event_sender.clone();
    let read_state_manager = env.read_state_manager;
    let mut table_event_manager = env.table_event_manager;
    let last_commit_lsn_tx = env.last_commit_lsn_tx.clone();
    let replication_lsn_tx = env.replication_lsn_tx.clone();

    let task = tokio::spawn(async move {
        let mut state = ChaosState::new(read_state_manager);

        // TODO(hjiang): Make iteration count a CLI configurable constant.
        for _ in 0..100 {
            let chaos_events = state.generate_random_events();

            // Perform table maintenance operations.
            if let Some(TableEvent::ForceRegularIndexMerge) = &chaos_events.table_maintenance_event
            {
                let mut rx = table_event_manager.initiate_index_merge().await;
                rx.recv().await.unwrap().unwrap();
            }

            // Perform table update operations.
            for cur_event in chaos_events.table_events.into_iter() {
                // For commit events, need to set up corresponding replication and commit LSN.
                if let TableEvent::Commit { lsn, .. } = cur_event {
                    replication_lsn_tx.send(lsn).unwrap();
                    last_commit_lsn_tx.send(lsn).unwrap();
                } else if let TableEvent::CommitFlush { lsn, .. } = cur_event {
                    replication_lsn_tx.send(lsn).unwrap();
                    last_commit_lsn_tx.send(lsn).unwrap();
                }
                event_sender.send(cur_event).await.unwrap();
            }

            // Perform snapshot read operation and check.
            if let Some(read_lsn) = chaos_events.snapshot_read_lsn {
                let requested_read_lsn = if read_lsn == 0 { None } else { Some(read_lsn) };
                let expected_ids = state.get_valid_ids();
                check_read_snapshot(
                    &state.read_state_manager,
                    requested_read_lsn,
                    /*expected_ids=*/ &expected_ids,
                )
                .await;
            }

            // Perform force snapshot and check.
            if let Some(snapshot_lsn) = chaos_events.force_snapshot_lsn {
                let rx = table_event_manager.initiate_snapshot(snapshot_lsn).await;
                TableEventManager::synchronize_force_snapshot_request(rx, snapshot_lsn)
                    .await
                    .unwrap();
            }
        }

        // TODO(hjiang): Temporarily hard code a sleep time to trigger background tasks.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // If anything bad happens in the eventloop, drop table would fail.
        event_sender.send(TableEvent::DropTable).await.unwrap();
    });

    // Await the task directly and handle its result.
    let task_result = task.await;

    // Print out events in order if chaos test fails.
    if let Err(e) = task_result {
        // Display all enqueued events for debugging and replay.
        while let Some(cur_event) = env.event_replay_rx.recv().await {
            println!("{cur_event:?}");
        }
        // Propagate the panic to fail the test.
        if let Ok(panic) = e.try_into_panic() {
            std::panic::resume_unwind(panic);
        }
    }
}
