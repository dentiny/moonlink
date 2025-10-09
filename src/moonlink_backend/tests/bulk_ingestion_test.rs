mod common;

#[cfg(test)]
mod tests {
    use crate::common::ids_from_state;

    use super::common::{current_wal_lsn, TestGuard, DATABASE, TABLE};

    use rand::prelude::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use serial_test::serial;

    use std::time::UNIX_EPOCH;

    /// End-to-end: multiple randomized bulk inserts (up to 50K rows each)
    #[cfg(feature = "bulk-ingestion-test")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_bulk_insert_multiple_iterations() {
        let rand_seed = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        println!("Random seed is {rand_seed}");
        let rng = StdRng::seed_from_u64(rand_seed);

        const NUM_ITERATIONS: usize = 200;
        const MAX_BATCH_SIZE: i64 = 50_000;

        let (guard, client) = TestGuard::new(Some("bulk_insert_multiple_iterations"), true).await;
        let backend = guard.backend();

        let mut total_rows_inserted: i64 = 0;
        for i in 0..NUM_ITERATIONS {
            // Get random number of rows to insert, stacked upon previous insertions.
            let num_rows = rng.random_range(0..=MAX_BATCH_SIZE);
            let start_id = total_rows_inserted + 1;
            let end_id = total_rows_inserted + num_rows;
            total_rows_inserted += num_rows;
            println!(
                "Iteration {}: inserting {} rows (id {}-{})",
                i + 1,
                num_rows,
                start_id,
                end_id
            );

            // Bulk insert rows into the table.
            let insert_sql = format!(
                "INSERT INTO bulk_insert_multiple_iterations (id, name)
                SELECT gs, 'val_' || gs
                FROM generate_series({start_id}, {end_id}) AS gs;"
            );
            client.simple_query(&insert_sql).await.unwrap();

            // Get mooncake snapshot.
            let lsn_after_insert = current_wal_lsn(&client).await;
            let ids = ids_from_state(
                &backend
                    .scan_table(
                        DATABASE.to_string(),
                        TABLE.to_string(),
                        Some(lsn_after_insert),
                    )
                    .await
                    .unwrap(),
            );

            // Validate mooncake snapshot.
            assert_eq!(
                ids.len() as i64,
                total_rows_inserted,
                "Expected {} rows after inserts, got {}",
                total_rows_inserted,
                ids.len()
            );
            assert!(
                ids.contains(&total_rows_inserted),
                "Row ID {total_rows_inserted} missing"
            );
            println!("Successfully inserted and verified {total_rows_inserted} total rows");
        }
    }
}
