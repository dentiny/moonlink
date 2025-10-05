/// This module defines a few iceberg table property related constants and utils.
/// Reference: https://iceberg.apache.org/docs/latest/configuration/#table-properties
use std::collections::HashMap;

/// Compression codec for parquet files.
pub(crate) const PARQUET_COMPRESSION: &str = "write.parquet.compression-codec";
pub(crate) const PARQUET_COMPRESSION_DEFAULT: &str = "snappy";

/// Compression codec for metadata.
pub(crate) const METADATA_COMPRESSION: &str = "write.metadata.compression-codec";
pub(crate) const METADATA_COMPRESSION_DEFAULT: &str = "none";

/// Retry properties.
pub(crate) const TABLE_COMMIT_RETRY_NUM: &str = "commit.retry.num-retries";
pub(crate) const TABLE_COMMIT_RETRY_NUM_DEFAULT: u64 = 5;

pub(crate) const TABLE_COMMIT_RETRY_MIN_MS: &str = "commit.retry.min-wait-ms";
pub(crate) const TABLE_COMMIT_RETRY_MIN_MS_DEFAULT: u64 = 200;

pub(crate) const TABLE_COMMIT_RETRY_MAX_MS: &str = "commit.retry.max-wait-ms";
pub(crate) const TABLE_COMMIT_RETRY_MAX_MS_DEFAULT: u64 = 30000; // 30 second

pub(crate) const TABLE_COMMIT_RETRY_TIMEOUT_MS: &str = "commit.retry.total-timeout-ms";
pub(crate) const TABLE_COMMIT_RETRY_TIMEOUT_MS_DEFAULT: u64 = 120000; // 2 min

// Create iceberg table properties from table config.
pub(crate) fn create_iceberg_table_properties() -> HashMap<String, String> {
    let mut props = HashMap::with_capacity(6);
    // Compression properties.
    props.insert(
        PARQUET_COMPRESSION.to_string(),
        PARQUET_COMPRESSION_DEFAULT.to_string(),
    );
    props.insert(
        METADATA_COMPRESSION.to_string(),
        METADATA_COMPRESSION_DEFAULT.to_string(),
    );
    // Commit retry properties.
    props.insert(
        TABLE_COMMIT_RETRY_NUM.to_string(),
        TABLE_COMMIT_RETRY_NUM_DEFAULT.to_string(),
    );
    props.insert(
        TABLE_COMMIT_RETRY_MIN_MS.to_string(),
        TABLE_COMMIT_RETRY_MIN_MS_DEFAULT.to_string(),
    );
    props.insert(
        TABLE_COMMIT_RETRY_MAX_MS.to_string(),
        TABLE_COMMIT_RETRY_MAX_MS_DEFAULT.to_string(),
    );
    props.insert(
        TABLE_COMMIT_RETRY_TIMEOUT_MS.to_string(),
        TABLE_COMMIT_RETRY_TIMEOUT_MS_DEFAULT.to_string(),
    );
    props
}
