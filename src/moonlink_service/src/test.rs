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

/// Moonlink backend directory.
const MOONLINK_BACKEND_DIR: &str = "/tmp/moonlink_service_test";
/// Local nginx server IP/port address.
const NGINX_ADDR: &str = "http://nginx.local:80";
/// Local moonlink REST API IP/port address.
const REST_ADDR: &str = "http://127.0.0.1:3030";
/// Local moonlink server IP/port address.
const MOONLINK_ADDR: &str = "127.0.0.1:3031";
/// Test database name.
const DATABASE: &str = "test-database";
/// Test table name.
const TABLE: &str = "test-table";

/// Util function to delete and re-create the given directory.
fn recreate_directory(dir: &str) {
    // Clean up directory to place moonlink temporary files.
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(e) => {
            if e.kind() != std::io::ErrorKind::NotFound {
                return;
            }
        }
    }
    std::fs::create_dir_all(dir).unwrap();
}

/// Send request to readiness endpoint.
async fn test_readiness_probe() {
    let url = format!("http://127.0.0.1:{}/ready", READINESS_PROBE_PORT);
    loop {
        if let Ok(resp) = reqwest::get(&url).await {
            if resp.status() == reqwest::StatusCode::OK {
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Util function to load all record batches inside of the given [`path`].
pub async fn read_all_batches(url: &str) -> Vec<RecordBatch> {
    let resp = reqwest::get(url).await.unwrap();
    println!("status = {}", resp.status());

    assert!(resp.status().is_success());

    println!("content = {}", resp.text().await.unwrap());

    vec![]

    // let data: Bytes = resp.bytes().await.unwrap();
    // println!("read len = {}", data.len());
    // let mut reader = ParquetRecordBatchReaderBuilder::try_new(data).unwrap()
    //     .build().unwrap();

    // // Collect all record batches
    // let batches = reader
    //     .into_iter()
    //     .map(|b| b.unwrap()) // handle Err properly in production
    //     .collect();

    // batches
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
    recreate_directory(MOONLINK_BACKEND_DIR);
    let config = ServiceConfig {
        base_path: MOONLINK_BACKEND_DIR.to_string(),
        data_server_uri: Some(NGINX_ADDR.to_string()),
        rest_api_port: Some(3030),
        tcp_port: Some(3031),
    };
    tokio::spawn(async move {
        start_with_config(config).await.unwrap();
    });
    test_readiness_probe().await;

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
    assert!(response.status().is_success());

    // Ingest some data.
    let insert_payload = json!({
        "operation": "insert",
        "data": {
            "id": 1,
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 30
        }
    });
    let response = client
        .post(format!("{REST_ADDR}/ingest/{TABLE}"))
        .header("content-type", "application/json")
        .json(&insert_payload)
        .send()
        .await.unwrap();
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
