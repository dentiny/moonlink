use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::basic::Type as PhysicalType;
use parquet::file::metadata::ParquetMetaData;
use serde_json::Value;

use crate::storage::deltalake::stats::Stats;
use crate::storage::iceberg::parquet_utils;
use crate::Result;

use std::collections::HashMap;

/// Decode a Parquet min/max byte slice into a serde_json::Value.
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
        num_records += rg.num_rows() as i64;

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
