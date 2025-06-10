#[cfg(test)]
mod tests {
    use arrow_array::Int64Array;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::TempDir;
    use tokio_postgres::{connect, types::PgLsn, Client, NoTls};

    use serial_test::serial;
    use std::{collections::HashSet, fs::File};

    use moonlink::decode_read_state_for_testing;
    use moonlink_backend::{
        recreate_directory, MoonlinkBackend, ReadState, DEFAULT_MOONLINK_TEMP_FILE_PATH,
    };

    // ───────────────────── Helper functions & fixtures ─────────────────────

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
    ) -> (TempDir, MoonlinkBackend<&'static str>, Client) {
        let temp_dir = TempDir::new().unwrap();
        let uri = "postgresql://postgres:postgres@postgres:5432/postgres";
        let backend =
            MoonlinkBackend::<&'static str>::new(temp_dir.path().to_str().unwrap().into());

        // Connect to Postgres.
        let (client, connection) = connect(uri, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });

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
        backend
            .create_table(table_name, &format!("public.{table_name}"), uri)
            .await
            .unwrap();

        (temp_dir, backend, client)
    }

    /// Reusable helper for the "create table / insert rows / detect change"
    /// scenario used in two places.
    async fn smoke_create_and_insert(
        backend: &MoonlinkBackend<&'static str>,
        client: &Client,
        uri: &str,
    ) {
        client
            .simple_query(
                "DROP TABLE IF EXISTS test;
                           CREATE TABLE test (id BIGINT PRIMARY KEY, name TEXT);",
            )
            .await
            .unwrap();

        backend
            .create_table("test", "public.test", uri)
            .await
            .unwrap();

        // First two rows.
        client
            .simple_query("INSERT INTO test VALUES (1,'foo'),(2,'bar');")
            .await
            .unwrap();

        let old = backend.scan_table(&"test", None).await.unwrap();
        let lsn = current_wal_lsn(client).await;
        let new = backend.scan_table(&"test", Some(lsn)).await.unwrap();
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
    #[tokio::test]
    #[serial]
    async fn test_moonlink_service() {
        let temp_dir = TempDir::new().unwrap();
        let uri = "postgresql://postgres:postgres@postgres:5432/postgres";
        let backend =
            MoonlinkBackend::<&'static str>::new(temp_dir.path().to_str().unwrap().into());

        let (client, conn) = connect(uri, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });

        smoke_create_and_insert(&backend, &client, uri).await;
        backend.drop_table("test").await.unwrap();
        smoke_create_and_insert(&backend, &client, uri).await;
    }

    /// End-to-end: inserts should appear in `scan_table`.
    #[tokio::test]
    #[serial]
    async fn test_scan_returns_inserted_rows() {
        let (tmp, backend, client) = setup_backend("scan_test").await;

        client
            .simple_query("INSERT INTO scan_test VALUES (1,'a'),(2,'b');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        let ids = ids_from_state(&backend.scan_table(&"scan_test", Some(lsn)).await.unwrap());
        assert_eq!(ids, HashSet::from([1, 2]));

        // Add one more row.
        client
            .simple_query("INSERT INTO scan_test VALUES (3,'c');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        let ids = ids_from_state(&backend.scan_table(&"scan_test", Some(lsn)).await.unwrap());
        assert_eq!(ids, HashSet::from([1, 2, 3]));

        backend.drop_table("scan_test").await.unwrap();
        recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH).unwrap();
        drop(tmp);
    }

    /// `scan_table(..., Some(lsn))` should return rows up to that LSN.
    #[tokio::test]
    #[serial]
    async fn test_scan_table_with_lsn() {
        let (_tmp, backend, client) = setup_backend("lsn_test").await;

        client
            .simple_query("INSERT INTO lsn_test VALUES (1,'a');")
            .await
            .unwrap();
        let lsn1 = current_wal_lsn(&client).await;

        let ids = ids_from_state(&backend.scan_table(&"lsn_test", Some(lsn1)).await.unwrap());
        assert_eq!(ids, HashSet::from([1]));

        client
            .simple_query("INSERT INTO lsn_test VALUES (2,'b');")
            .await
            .unwrap();
        let lsn2 = current_wal_lsn(&client).await;

        let ids = ids_from_state(&backend.scan_table(&"lsn_test", Some(lsn2)).await.unwrap());
        assert_eq!(ids, HashSet::from([1, 2]));

        backend.drop_table("lsn_test").await.unwrap();
        recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH).unwrap();
    }

    /// Validates that `create_iceberg_snapshot` writes Iceberg metadata.
    #[tokio::test]
    #[serial]
    async fn test_create_iceberg_snapshot() {
        let (tmp, backend, client) = setup_backend("snapshot_test").await;

        client
            .simple_query("INSERT INTO snapshot_test VALUES (1,'a');")
            .await
            .unwrap();
        let lsn = current_wal_lsn(&client).await;

        backend
            .create_iceberg_snapshot(&"snapshot_test", lsn)
            .await
            .unwrap();

        // Look for any file in the Iceberg metadata dir.
        let _meta_dir = tmp
            .path()
            .join("default")
            .join("public.snapshot_test")
            .join("metadata");

        // TODO(hjiang): Flaky test, track by issue https://github.com/Mooncake-Labs/moonlink/issues/432
        assert!(meta_dir.exists() && meta_dir.read_dir().unwrap().next().is_some());

        backend.drop_table("snapshot_test").await.unwrap();
        recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH).unwrap();
    }

    /// Test that replication connections are properly cleaned up and can be recreated.
    /// This validates that dropping the last table from a connection properly cleans up
    /// the replication slot, allowing new connections to be established.
    #[tokio::test]
    #[serial]
    async fn test_replication_connection_cleanup() {
        let temp_dir = TempDir::new().unwrap();
        let uri = "postgresql://postgres:postgres@postgres:5432/postgres";
        let backend =
            MoonlinkBackend::<&'static str>::new(temp_dir.path().to_str().unwrap().into());

        let (client, conn) = connect(uri, NoTls).await.unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });

        // Create the test table
        client
            .simple_query(
                "DROP TABLE IF EXISTS repl_test;
                 CREATE TABLE repl_test (id BIGINT PRIMARY KEY, name TEXT);",
            )
            .await
            .unwrap();

        // First cycle: add table, insert data, verify it works
        backend
            .create_table("repl_test", "public.repl_test", uri)
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO repl_test VALUES (1,'first');")
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let ids = ids_from_state(&backend.scan_table(&"repl_test", None).await.unwrap());
        assert_eq!(ids, HashSet::from([1]));

        // Drop the table (this should clean up the replication connection)
        backend.drop_table("repl_test").await.unwrap();

        // Second cycle: add table again, insert different data, verify it works
        backend
            .create_table("repl_test", "public.repl_test", uri)
            .await
            .unwrap();

        client
            .simple_query("INSERT INTO repl_test VALUES (2,'second');")
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let ids = ids_from_state(&backend.scan_table(&"repl_test", None).await.unwrap());
        // Should only see the new row (2), not the old one (1)
        assert_eq!(ids, HashSet::from([2]));

        // Clean up
        backend.drop_table("repl_test").await.unwrap();
        recreate_directory(DEFAULT_MOONLINK_TEMP_FILE_PATH).unwrap();
    }
}
