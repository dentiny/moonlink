use std::collections::HashMap;
use std::sync::Arc;

use moonlink::decode_serialized_read_state_for_testing;
use serde_json::json;
use bytes::Bytes;
use tokio::net::TcpStream;
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::{DataType, Field};
use tempfile::TempDir;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::io::Cursor;
use reqwest;

use moonlink_rpc::{scan_table_begin, scan_table_end};
use crate::{start_with_config, ServiceConfig, READINESS_PROBE_PORT};

/// Local moonlink REST API IP/port address.
const REST_ADDR: &str = "http://54.245.134.191:3030";
/// Local moonlink server IP/port address.
const MOONLINK_ADDR: &str = "54.245.134.191:3031";
/// Test database name.
const DATABASE: &str = "pg_mooncake";
/// Test table name.
const TABLE: &str = "public.test-table";

/// Send request to readiness endpoint.
async fn test_readiness_probe() {
    let url = format!("http://54.245.134.191:{}/ready", READINESS_PROBE_PORT);
    loop {
        if let Ok(resp) = reqwest::get(&url).await {
            if resp.status() == reqwest::StatusCode::OK {
                return;
            }
        }
        println!("not ready, waiting...");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Util function to load all record batches inside of the given [`path`].
pub async fn read_all_batches(url: &str) -> Vec<RecordBatch> {
    let resp = reqwest::get(url).await.unwrap();
    assert!(resp.status().is_success());

    let data: Bytes = resp.bytes().await.unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(data).unwrap()
        .build().unwrap();

    // Collect all record batches.
    let batches = reader
        .into_iter()
        .map(|b| b.unwrap()) // handle Err properly in production
        .collect();

    batches
}

/// Util function to create test arrow schema.
fn create_test_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, /*nullable=*/false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "0".to_string(),
        )])),
        Field::new("name", DataType::Utf8, /*nullable=*/false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "1".to_string(),
        )])),
        Field::new("email", DataType::Utf8, /*nullable=*/true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "2".to_string(),
        )])),
        Field::new("age", DataType::Int32, /*nullable=*/true).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_string(),
            "3".to_string(),
        )])),
    ]))
}

#[tokio::test]
async fn test_moonlink_standalone() {
    println!("before readiness probe");
    test_readiness_probe().await;
    println!("after readiness probe");

    // Create test table.
    let client = reqwest::Client::new();
    let create_table_payload = json!({
        "mooncake_database": DATABASE,
        "mooncake_table": TABLE,
        "schema": [
            {"name": "id", "data_type": "int32", "nullable": false},
            {"name": "name", "data_type": "string", "nullable": false},
            {"name": "email", "data_type": "string", "nullable": true},
            {"name": "age", "data_type": "int32", "nullable": true}
        ]
    });
    let response = client
        .post(format!("{REST_ADDR}/tables/{TABLE}"))
        .header("content-type", "application/json")
        .json(&create_table_payload)
        .send()
        .await.unwrap();
    println!("response = {:?}", response);
    assert!(response.status().is_success());

    // Ingest some data.
    let insert_payload = json!({
        "operation": "insert",
        "data": {
            "id": 4,
            "name": "Dad",
            "email": "Dad@example.com",
            "age": 40
        }
    });
    let response = client
        .post(format!("{REST_ADDR}/ingest/{TABLE}"))
        .header("content-type", "application/json")
        .json(&insert_payload)
        .send()
        .await.unwrap();
    println!("insert response = {:?}", response);
    assert!(response.status().is_success());

    // Scan table and get data file and puffin files back.
    let mut moonlink_stream = TcpStream::connect(MOONLINK_ADDR).await.unwrap();
    let bytes = scan_table_begin(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
        0,
    ).await.unwrap();
    let (data_file_paths, puffin_file_paths, puffin_deletion, positional_deletion) = decode_serialized_read_state_for_testing(bytes);
    assert_eq!(data_file_paths.len(), 1);
    println!("data file paths = {:?}", data_file_paths);
    let record_batches = read_all_batches(&data_file_paths[0]).await;
    let expected_arrow_batch = RecordBatch::try_new(
        create_test_arrow_schema(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["Alice Johnson".to_string()])),
            Arc::new(StringArray::from(vec!["alice@example.com".to_string()])),
            Arc::new(Int32Array::from(vec![30])),
        ],
    )
    .unwrap();
    assert_eq!(record_batches, vec![expected_arrow_batch]);

    assert!(puffin_file_paths.is_empty());
    assert!(puffin_deletion.is_empty());
    assert!(positional_deletion.is_empty());
    
    scan_table_end(
        &mut moonlink_stream,
        DATABASE.to_string(),
        TABLE.to_string(),
    ).await.unwrap();
}
