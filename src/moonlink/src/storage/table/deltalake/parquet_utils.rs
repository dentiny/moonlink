use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::ParquetMetaData;
use serde_json::Value;

use crate::storage::table::deltalake::stats::Stats;
use crate::Result;

use std::collections::HashMap;

/// Decode a Parquet min/max byte slice into a json serialized value.
fn decode_parquet_value(phys_type: PhysicalType, bytes: &[u8]) -> Value {
    match phys_type {
        PhysicalType::BOOLEAN => Value::Bool(bytes[0] != 0),
        PhysicalType::INT32 => {
            let v = i32::from_le_bytes(bytes.try_into().unwrap());
            Value::Number(v.into())
        }
        PhysicalType::INT64 => {
            let v = i64::from_le_bytes(bytes.try_into().unwrap());
            Value::Number(v.into())
        }
        PhysicalType::FLOAT => {
            let v = f32::from_le_bytes(bytes.try_into().unwrap());
            Value::Number(serde_json::Number::from_f64(v as f64).unwrap())
        }
        PhysicalType::DOUBLE => {
            let v = f64::from_le_bytes(bytes.try_into().unwrap());
            Value::Number(serde_json::Number::from_f64(v).unwrap())
        }
        PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
            Value::String(String::from_utf8_lossy(bytes).to_string())
        }
        _ => Value::Null,
    }
}

/// Get stats from the given parquet file.
pub(crate) fn collect_parquet_stats(parquet_metadata: &ParquetMetaData) -> Result<Stats> {
    let mut num_records: i64 = 0;
    let mut null_counts = HashMap::new();
    let mut min_values = HashMap::new();
    let mut max_values = HashMap::new();

    for rg in parquet_metadata.row_groups() {
        num_records += rg.num_rows();

        for col in rg.columns() {
            if let Some(stats) = col.statistics() {
                let name = col.column_descr().name().to_string();
                let phys_type = col.column_descr().physical_type();

                if let Some(nulls) = stats.null_count_opt() {
                    *null_counts.entry(name.clone()).or_insert(0) += nulls as i64;
                }
                if let Some(min_bytes) = stats.min_bytes_opt() {
                    min_values.insert(name.clone(), decode_parquet_value(phys_type, min_bytes));
                }
                if let Some(max_bytes) = stats.max_bytes_opt() {
                    max_values.insert(name.clone(), decode_parquet_value(phys_type, max_bytes));
                }
            }
        }
    }

    Ok(Stats {
        num_records,
        tight_bounds: true,
        min_values: Some(min_values),
        max_values: Some(max_values),
        null_count: Some(null_counts),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{
        mooncake_table::table_operation_test_utils::create_local_parquet_file,
        table::iceberg::parquet_utils,
    };
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_collect_parquet_stats() {
        let temp_dir = TempDir::new().unwrap();
        let filepath = create_local_parquet_file(&temp_dir).await;
        let (parquet_metadata, _) = parquet_utils::get_parquet_metadata(&filepath)
            .await
            .unwrap();
        let delta_stats = collect_parquet_stats(&parquet_metadata).unwrap();

        // Check basic stats.
        assert_eq!(delta_stats.num_records, 3);
        assert!(delta_stats.tight_bounds);

        // Check null counts.
        let actual_null_values = delta_stats.null_count.as_ref().unwrap();
        let expected_null_values = HashMap::from([
            ("id".to_string(), 0),
            ("name".to_string(), 1),
            ("age".to_string(), 0),
        ]);
        assert_eq!(*actual_null_values, expected_null_values);

        // Check min values.
        let actual_min_values = delta_stats.min_values.as_ref().unwrap();
        let expected_min_values = HashMap::from([
            ("id".to_string(), serde_json::json!(1)),
            ("name".to_string(), serde_json::json!("Alice")),
            ("age".to_string(), serde_json::json!(10)),
        ]);
        assert_eq!(*actual_min_values, expected_min_values);

        // Check max values.
        let actual_max_values = delta_stats.max_values.as_ref().unwrap();
        let expected_max_values = HashMap::from([
            ("id".to_string(), serde_json::json!(3)),
            ("name".to_string(), serde_json::json!("Bob")),
            ("age".to_string(), serde_json::json!(30)),
        ]);
        assert_eq!(*actual_max_values, expected_max_values);
    }
}
