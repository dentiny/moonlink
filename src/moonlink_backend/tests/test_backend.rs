#[cfg(test)]
mod tests {
    use arrow_array::Int64Array;
    use moonlink_metadata_store::{base_metadata_store::MetadataStoreTrait, PgMetadataStore};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::{collections::HashMap, sync::Arc};
    use tempfile::TempDir;
    use tokio_postgres::{connect, types::PgLsn, Client, NoTls};

    use serial_test::serial;
    use std::{collections::HashSet, fs::File};

    use moonlink::decode_read_state_for_testing;
    use moonlink_backend::{
        recreate_directory, MoonlinkBackend, ReadState, DEFAULT_MOONLINK_TEMP_FILE_PATH,
    };

    const SRC_URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";
    const DST_URI: &str = "postgresql://postgres:postgres@postgres:5432/postgres";
    const CREATE_TABLE_SCHEMA_SQL: &str =
        include_str!("../../moonlink_metadata_store/src/postgres/sql/create_tables.sql");

    type DatabaseId = u32;
    type TableId = u64;
    const TABLE_ID: TableId = 0;

    // ───────────────────── Helper functions & fixtures ─────────────────────

    struct TestGuard {
        backend: Arc<MoonlinkBackend<DatabaseId, TableId>>,
        tmp: Option<TempDir>,
        database_id: DatabaseId,
    }

    impl TestGuard {
        async fn new(table_name: &'static str) -> (Self, Client) {
            let (tmp, backend, client, database_id) = setup_backend(table_name).await;
            let guard = Self {
                backend: Arc::new(backend),
                tmp: Some(tmp),
                database_id,
            };
            (guard, client)
        }
    }

    impl Drop for TestGuard {
        fn drop(&mut self) {
            // move everything we need into the async block
            let backend = Arc::clone(&self.backend);
            let tmp = self.tmp.take();

            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    let _ = backend.drop_table(self.database_id, TABLE_ID).await;
                    let _ = backend.shutdown_connection(SRC_URI).await;
                    let _ = recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH);
                    drop(tmp);
                });
            });
        }
    }

    /// Get current database id.
    async fn get_current_database_id(client: &Client) -> u32 {
        let row = client
            .query_one(
                "SELECT oid FROM pg_database WHERE datname = current_database()",
                &[],
            )
            .await
            .unwrap();
        row.get(0)
    }

    /// Return the current WAL LSN as a simple `u64`.
    async fn current_wal_lsn(client: &Client) -> u64 {
        let row = client
            .query_one("SELECT pg_current_wal_lsn()", &[])
            .await
            .unwrap();
        let lsn: PgLsn = row.get(0);
        lsn.into()
    }

    /// Read the first column of a Parquet file into a `Vec<Option<i64>>`.
    fn read_ids_from_parquet(path: &str) -> Vec<Option<i64>> {
        let file = File::open(path).unwrap_or_else(|_| panic!("open {path}"));
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.into_iter().next().unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..col.len()).map(|i| Some(col.value(i))).collect()
    }

    /// Extract **all** primary-key IDs referenced in `read_state`.
    fn ids_from_state(read_state: &ReadState) -> HashSet<i64> {
        let (files, _, _, _) = decode_read_state_for_testing(read_state);
        files
            .into_iter()
            .flat_map(|f| read_ids_from_parquet(&f).into_iter().flatten())
            .collect()
    }

    /// Spin up a backend + scratch TempDir + psql client, and guarantee
    /// a **fresh table** named `table_name` exists and is registered with
    /// Moonlink.
    async fn setup_backend(
        table_name: &'static str,
    ) -> (
        TempDir,
        MoonlinkBackend<DatabaseId, TableId>,
        Client,
        DatabaseId,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let backend =
            MoonlinkBackend::<DatabaseId, TableId>::new(temp_dir.path().to_str().unwrap().into());

        // Connect to Postgres.
        let (client, connection) = connect(SRC_URI, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Get current database id.
        let database_id = get_current_database_id(&client).await;

        // Clear any leftover replication slot from previous runs.
        let _ = client
            .simple_query(
                "SELECT pg_terminate_backend(active_pid)
             FROM pg_replication_slots
             WHERE slot_name = 'moonlink_slot_postgres';",
            )
            .await;
        let _ = client
            .simple_query("SELECT pg_drop_replication_slot('moonlink_slot_postgres')")
            .await;

        // Re-create the working table.
        client
            .simple_query(&format!(
                "DROP TABLE IF EXISTS {0};
                 CREATE TABLE {0} (id BIGINT PRIMARY KEY, name TEXT);",
                table_name
            ))
            .await
            .unwrap();
        client
            .simple_query("DROP TABLE IF EXISTS mooncake.tables")
            .await
            .unwrap();
        backend
            .create_table(
                database_id,
                /*table_id=*/ TABLE_ID,
                DST_URI.to_string(),
                /*src_table_name=*/ format!("public.{table_name}"),
                SRC_URI.to_string(),
            )
            .await
            .unwrap();

        (temp_dir, backend, client, database_id)
    }

    /// Reusable helper for the "create table / insert rows / detect change"
    /// scenario used in two places.
    async fn smoke_create_and_insert(
        backend: &MoonlinkBackend<DatabaseId, TableId>,
        client: &Client,
        database_id: DatabaseId,
        uri: &str,
    ) {
        client
            .simple_query(
                "DROP TABLE IF EXISTS test;
                CREATE TABLE test (id BIGINT PRIMARY KEY, name TEXT);",
            )
            .await
            .unwrap();
        client
            .simple_query("DROP TABLE IF EXISTS mooncake.tables;")
            .await
            .unwrap();
        client.simple_query(CREATE_TABLE_SCHEMA_SQL).await.unwrap();

        backend
            .create_table(
                database_id,
                TABLE_ID,
                DST_URI.to_string(),
                /*table_name=*/ "public.test".to_string(),
                uri.to_string(),
            )
            .await
            .unwrap();

        // First two rows.
        client
            .simple_query("INSERT INTO test VALUES (1,'foo'),(2,'bar');")
            .await
            .unwrap();

        let old = backend
            .scan_table(database_id, TABLE_ID, /*lsn=*/ None)
            .await
            .unwrap();
        let lsn = current_wal_lsn(client).await;
        let new = backend
            .scan_table(database_id, TABLE_ID, Some(lsn))
            .await
            .unwrap();
        assert_ne!(old.data, new.data);

        recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH).unwrap();
    }

    // ───────────────────────────── Tests ─────────────────────────────

    /// Low-level filesystem helper: directory (re)creation.
    #[test]
    #[serial]
    fn test_recreate_directory() {
        let tmp = TempDir::new().unwrap();
        let file = tmp.path().join("tmp.txt");
        std::fs::write(&file, b"x").unwrap();
        assert!(file.exists());

        // idempotent "wipe" of an existing dir
        recreate_directory(tmp.path().to_str().unwrap()).unwrap();
        assert!(!file.exists());

        // creation of a brand-new path
        let inner = tmp.path().join("sub");
        recreate_directory(inner.to_str().unwrap()).unwrap();
        assert!(inner.exists());
    }

    /// Validate `create_table` and `drop_table` across successive uses.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_moonlink_service() {
        let (guard, client) = TestGuard::new("test").await;
        let backend = &guard.backend;

        smoke_create_and_insert(backend, &client, guard.database_id, SRC_URI).await;
        backend.drop_table(guard.database_id, TABLE_ID).await;
        smoke_create_and_insert(backend, &client, guard.database_id, SRC_URI).await;
    }

    /// End-to-end: inserts should appear in `scan_table`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_scan_returns_inserted_rows() {
        let (guard, client) = TestGuard::new("scan_test").await;
        let backend = &guard.backend;

        client
            .simple_query("INSERT INTO scan_test VALUES (1,'a'),(2,'b');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn))
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1, 2]));

        // Add one more row.
        client
            .simple_query("INSERT INTO scan_test VALUES (3,'c');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn))
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1, 2, 3]));
    }

    /// `scan_table(..., Some(lsn))` should return rows up to that LSN.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_scan_table_with_lsn() {
        let (guard, client) = TestGuard::new("lsn_test").await;
        let backend = &guard.backend;

        client
            .simple_query("INSERT INTO lsn_test VALUES (1,'a');")
            .await
            .unwrap();
        let lsn1 = current_wal_lsn(&client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn1))
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1]));

        client
            .simple_query("INSERT INTO lsn_test VALUES (2,'b');")
            .await
            .unwrap();
        let lsn2 = current_wal_lsn(&client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn2))
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1, 2]));
    }

    /// Validates that `create_iceberg_snapshot` writes Iceberg metadata.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_create_iceberg_snapshot() {
        let (guard, client) = TestGuard::new("snapshot_test").await;
        let backend = &guard.backend;

        client
            .simple_query("INSERT INTO snapshot_test VALUES (1,'a');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        // It's not guaranteed whether "table insertion" or "create iceberg snapshot" reaches table handler eventloop first, add a sleep to reduce flakiness.
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        backend
            .create_snapshot(guard.database_id, TABLE_ID, lsn)
            .await
            .unwrap();

        // Look for any file in the Iceberg metadata dir.
        let meta_dir = guard
            .tmp
            .as_ref()
            .unwrap()
            .path()
            .join("public")
            .join(format!("{}.{}", guard.database_id, TABLE_ID))
            .join("metadata");
        assert!(meta_dir.exists());
        assert!(meta_dir.read_dir().unwrap().next().is_some());
    }

    /// Test that replication connections are properly cleaned up and can be recreated.
    /// This validates that dropping the last table from a connection properly cleans up
    /// the replication slot, allowing new connections to be established.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_replication_connection_cleanup() {
        let (guard, client) = TestGuard::new("repl_test").await;
        let backend = &guard.backend;

        // Drop the table that setup_backend created so we can test the full cycle
        backend.drop_table(guard.database_id, TABLE_ID).await;

        // First cycle: add table, insert data, verify it works
        backend
            .create_table(
                guard.database_id,
                TABLE_ID,
                DST_URI.to_string(),
                "public.repl_test".to_string(),
                SRC_URI.to_string(),
            )
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO repl_test VALUES (1,'first');")
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, None)
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1]));

        // Drop the table (this should clean up the replication connection)
        backend.drop_table(guard.database_id, TABLE_ID).await;

        // Second cycle: add table again, insert different data, verify it works
        backend
            .create_table(
                guard.database_id,
                TABLE_ID,
                DST_URI.to_string(),
                /*table_name=*/ "public.repl_test".to_string(),
                SRC_URI.to_string(),
            )
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO repl_test VALUES (2,'second');")
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, /*lsn=*/ None)
                .await
                .unwrap(),
        );
        // Should only see the new row (2), not the old one (1)
        assert_eq!(ids, HashSet::from([2]));
    }

    /// End-to-end: bulk insert (1M rows)
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_bulk_insert_one_million_rows() {
        let (guard, client) = TestGuard::new("bulk_test").await;
        let backend = &guard.backend;

        client
            .simple_query(
                "INSERT INTO bulk_test (id, name)
             SELECT gs, 'val_' || gs
             FROM generate_series(1, 1000000) AS gs;",
            )
            .await
            .unwrap();

        let lsn_after_insert = current_wal_lsn(&client).await;

        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, Some(lsn_after_insert))
                .await
                .unwrap(),
        );

        assert_eq!(ids.len(), 1_000_000);
        assert!(ids.contains(&1), "row id 1 missing");
        assert!(ids.contains(&1_000_000), "row id 1_000_000 missing");
        assert_eq!(ids.len(), 1_000_000);
    }

    /// Testing scenario: perform table creation and drop operations, and check metadata store table states.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_metadata_store() {
        let (guard, _) = TestGuard::new("metadata_store").await;
        // Till now, table [`metadata_store`] has been created at both row storage and column storage database.
        let backend = &guard.backend;
        let metadata_store = PgMetadataStore::new(DST_URI).await.unwrap();

        // Check metadata storage after table creation.
        let metadata_entries = metadata_store
            .get_all_table_metadata_entries()
            .await
            .unwrap();
        assert_eq!(metadata_entries.len(), 1);
        assert_eq!(metadata_entries[0].table_id, TABLE_ID as u32);
        assert_eq!(
            metadata_entries[0]
                .moonlink_table_config
                .iceberg_table_config
                .table_name,
            format!("{}.{}", guard.database_id, TABLE_ID)
        );

        // Drop table and check metadata storage.
        backend.drop_table(guard.database_id, TABLE_ID).await;
        let metadata_entries = metadata_store
            .get_all_table_metadata_entries()
            .await
            .unwrap();
        assert!(metadata_entries.is_empty());
    }

    /// Test recovery.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_recovery() {
        let (guard, client) = TestGuard::new("recovery").await;
        let backend = &guard.backend;

        // Drop the table that setup_backend created so we can test the full cycle
        backend.drop_table(guard.database_id, TABLE_ID).await;

        // First cycle: add table, insert data, verify it works
        backend
            .create_table(
                guard.database_id,
                TABLE_ID,
                DST_URI.to_string(),
                "public.recovery".to_string(),
                SRC_URI.to_string(),
            )
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO recovery VALUES (1,'first');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;
        // Wait for a while so changes are streamed to mooncake table.
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        // Force create iceberg snapshot to test mooncake/iceberg table recovery.
        backend
            .create_snapshot(guard.database_id, TABLE_ID, lsn)
            .await
            .unwrap();

        // Attempt recovery logic.
        let temp_dir = TempDir::new().unwrap();
        let uris = HashMap::<String, String>::from([(SRC_URI.to_string(), SRC_URI.to_string())]);
        let backend = MoonlinkBackend::<DatabaseId, TableId>::new_with_recovery(
            temp_dir.path().to_str().unwrap().to_string(),
            uris,
        )
        .await
        .unwrap();
        let ids = ids_from_state(
            &backend
                .scan_table(guard.database_id, TABLE_ID, /*lsn=*/ None)
                .await
                .unwrap(),
        );
        assert_eq!(ids, HashSet::from([1]));

        // TODO(hjiang): Add test cases to insert new rows to make sure recovered mooncake table works.
    }
}
