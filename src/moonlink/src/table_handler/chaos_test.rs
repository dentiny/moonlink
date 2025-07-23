/// This file implements chaos test for table handler.
///
/// System invariants:
/// - Begin events happen only after end events
/// - End events happen only after begin events
/// - Rows to delete comes from appended ones
/// - LSN always increases
use crate::event_sync::create_table_event_syncer;
use crate::row::{MoonlinkRow, RowValue};
use crate::storage::mooncake_table::table_creation_test_utils::*;
use crate::table_handler::{TableEvent, TableHandler};
use crate::union_read::ReadStateManager;
use crate::ObjectStorageCache;
use crate::TableEventManager;

use rand::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::VecDeque;
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
struct ChaosEvent {
    table_events: Vec<TableEvent>,
    snapshot_read_lsn: Option<u64>,
}

impl ChaosEvent {
    fn create_table_events(table_events: Vec<TableEvent>) -> Self {
        Self {
            table_events,
            snapshot_read_lsn: None,
        }
    }
    fn create_snapshot_read(lsn: u64) -> Self {
        Self {
            table_events: vec![],
            snapshot_read_lsn: Some(lsn),
        }
    }
}

struct ChaosState {
    /// Used to generate rows to insert.
    next_id: i32,
    inserted_rows: VecDeque<MoonlinkRow>,
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
        Self {
            has_begun: false,
            next_id: 0,
            inserted_rows: VecDeque::new(),
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
        assert!(self.has_begun);
        self.has_begun = false;
        self.last_commit_lsn = Some(lsn);
    }

    fn next_row(&mut self) -> MoonlinkRow {
        let row = create_row(self.next_id, /*name=*/ "user", self.next_id % 5);
        self.next_id += 1;
        row
    }

    fn generate_random_events(&mut self, rng: &mut impl Rng) -> ChaosEvent {
        #[derive(Debug, Clone)]
        enum EventKind {
            Begin,
            Append,
            Delete,
            EndWithFlush,
            EndNoFlush,
            ReadSnapshot,
        }

        let mut choices = vec![];

        if self.last_commit_lsn.is_some() {
            choices.push(EventKind::ReadSnapshot);
        }
        if !self.has_begun {
            choices.push(EventKind::Begin);
        } else {
            choices.push(EventKind::Append);
            if !self.inserted_rows.is_empty() {
                choices.push(EventKind::Delete);
            }
            choices.push(EventKind::EndWithFlush);
            choices.push(EventKind::EndNoFlush);
        }

        match *choices.choose(rng).unwrap() {
            EventKind::ReadSnapshot => {
                ChaosEvent::create_snapshot_read(self.last_commit_lsn.unwrap())
            }
            EventKind::Begin => {
                self.begin_transaction();
                ChaosEvent::create_table_events(vec![TableEvent::Append {
                    row: self.next_row(),
                    xact_id: None,
                    lsn: self.get_and_update_cur_lsn(),
                    is_copied: false,
                }])
            }
            EventKind::Append => {
                let row = self.next_row();
                self.inserted_rows.push_back(row.clone());
                ChaosEvent::create_table_events(vec![TableEvent::Append {
                    row,
                    xact_id: None,
                    lsn: self.get_and_update_cur_lsn(),
                    is_copied: false,
                }])
            }
            EventKind::Delete => {
                let idx = rng.random_range(0..self.inserted_rows.len());
                let row = self.inserted_rows.remove(idx).unwrap();
                ChaosEvent::create_table_events(vec![TableEvent::Delete {
                    row,
                    xact_id: None,
                    lsn: self.get_and_update_cur_lsn(),
                }])
            }
            EventKind::EndWithFlush => {
                let lsn = self.get_and_update_cur_lsn();
                self.commit_transaction(lsn);
                ChaosEvent::create_table_events(vec![
                    TableEvent::Commit { lsn, xact_id: None },
                    TableEvent::Flush { lsn },
                ])
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
    let last_commit_lsn_tx = env.last_commit_lsn_tx.clone();
    let replication_lsn_tx = env.replication_lsn_tx.clone();

    let task = tokio::spawn(async move {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut rng = StdRng::seed_from_u64(nanos as u64);
        let mut state = ChaosState::new(read_state_manager);

        // TODO(hjiang): Make iteration count a CLI configurable constant.
        for _ in 0..100 {
            let chaos_events = state.generate_random_events(&mut rng);
            for cur_event in chaos_events.table_events.into_iter() {
                if let TableEvent::Commit { lsn, .. } = cur_event {
                    replication_lsn_tx.send(lsn).unwrap();
                    last_commit_lsn_tx.send(lsn).unwrap();
                }
                event_sender.send(cur_event).await.unwrap();
            }
            if let Some(read_lsn) = chaos_events.snapshot_read_lsn {
                let requested_read_lsn = if read_lsn == 0 { None } else { Some(read_lsn) };
                let _read_state = state
                    .read_state_manager
                    .try_read(requested_read_lsn)
                    .await
                    .unwrap();
            }
        }

        // TODO(hjiang): Temporarily hard code a sleep time to trigger background tasks.
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

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
