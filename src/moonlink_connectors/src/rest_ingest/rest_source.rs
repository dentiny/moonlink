use crate::rest_ingest::json_converter::{JsonToMoonlinkRowConverter, JsonToMoonlinkRowError};
use crate::Result;
use arrow_schema::Schema;
use bytes::Bytes;
use moonlink::row::MoonlinkRow;
use moonlink::{
    AccessorConfig, BaseFileSystemAccess, FileSystemAccessor, FsRetryConfig, FsTimeoutConfig,
    StorageConfig,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;
use thiserror::Error;
use tokio::sync::Mutex;

pub type SrcTableId = u32;

#[derive(Debug, Error)]
pub enum RestSourceError {
    #[error("json conversion error: {0}")]
    JsonConversion(#[from] JsonToMoonlinkRowError),
    #[error("unknown table: {0}")]
    UnknownTable(String),
    #[error("invalid operation for table: {0}")]
    InvalidOperation(String),
    #[error("duplicate table created: {0}")]
    DuplicateTable(String),
    #[error("non-existent table to remove: {0}")]
    NonExistentTable(String),
}

/// ======================
/// Row event request
/// ======================
///
#[derive(Debug, Clone)]
pub enum RowEventOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct RowEventRequest {
    pub src_table_name: String,
    pub operation: RowEventOperation,
    pub payload: serde_json::Value,
    pub timestamp: SystemTime,
}

/// ======================
/// File event request
/// ======================
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileEventOperation {
    Upload,
}

#[derive(Debug, Clone)]
pub struct FileEventRequest {
    /// Src table name.
    pub src_table_name: String,
    /// Storage config, which provides access to storage backend.
    pub storage_config: StorageConfig,
    /// Parquet files to upload, which will be processed in order.
    pub files: Vec<String>,
}

/// ======================
/// Event request
/// ======================
///
#[derive(Debug, Clone)]
pub enum EventRequest {
    RowRequest(RowEventRequest),
    FileRequest(FileEventRequest),
}

/// ======================
/// Rest event
/// ======================
///
#[derive(Debug, Clone)]
pub enum RestEvent {
    RowEvent {
        src_table_id: SrcTableId,
        operation: RowEventOperation,
        row: MoonlinkRow,
        lsn: u64,
        timestamp: SystemTime,
    },
    FileEvent {
        operation: FileEventOperation,
        table_events: Arc<Mutex<tokio::sync::mpsc::UnboundedReceiver<RestEvent>>>,
    },
    Commit {
        lsn: u64,
        timestamp: SystemTime,
    },
}

pub struct RestSource {
    table_schemas: HashMap<String, Arc<Schema>>,
    src_table_name_to_src_id: HashMap<String, SrcTableId>,
    lsn_generator: Arc<AtomicU64>,
}

impl Default for RestSource {
    fn default() -> Self {
        Self::new()
    }
}

impl RestSource {
    pub fn new() -> Self {
        Self {
            table_schemas: HashMap::new(),
            src_table_name_to_src_id: HashMap::new(),
            lsn_generator: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn add_table(
        &mut self,
        src_table_name: String,
        src_table_id: SrcTableId,
        schema: Arc<Schema>,
    ) -> Result<()> {
        if self
            .table_schemas
            .insert(src_table_name.clone(), schema)
            .is_some()
        {
            return Err(RestSourceError::DuplicateTable(src_table_name).into());
        }
        // Invariant sanity check.
        assert!(self
            .src_table_name_to_src_id
            .insert(src_table_name, src_table_id)
            .is_none());
        Ok(())
    }

    pub fn remove_table(&mut self, src_table_name: &str) -> Result<()> {
        if self.table_schemas.remove(src_table_name).is_none() {
            return Err(RestSourceError::NonExistentTable(src_table_name.to_string()).into());
        }
        // Invariant sanity check.
        assert!(self
            .src_table_name_to_src_id
            .remove(src_table_name)
            .is_some());
        Ok(())
    }

    /// Process an event request.
    pub fn process_request(&self, request: &EventRequest) -> Result<Vec<RestEvent>> {
        match request {
            EventRequest::FileRequest(request) => self.process_file_request(request),
            EventRequest::RowRequest(request) => self.process_row_request(request),
        }
    }

    /// Read parquet files and send parsed moonlink rows to channel.
    async fn generate_table_events_for_file_upload(
        src_table_id: SrcTableId,
        lsn_generator: Arc<AtomicU64>,
        storage_config: StorageConfig,
        parquet_files: Vec<String>,
        event_sender: tokio::sync::mpsc::UnboundedSender<RestEvent>,
    ) {
        let accessor_config = AccessorConfig {
            storage_config,
            timeout_config: FsTimeoutConfig::default(),
            retry_config: FsRetryConfig::default(),
            chaos_config: None,
        };
        let filesystem_accessor = FileSystemAccessor::new(accessor_config);
        // TODO(hjiang): Handle parallel read and error propagation.
        for cur_file in parquet_files.into_iter() {
            let content = filesystem_accessor.read_object(&cur_file).await.unwrap();
            let content = Bytes::from(content);
            let builder = ParquetRecordBatchReaderBuilder::try_new(content).unwrap();
            let record_batches = builder.build().unwrap();
            for batch in record_batches {
                let batch = batch.unwrap();
                let moonlink_rows = MoonlinkRow::from_record_batch(&batch);
                for cur_row in moonlink_rows.into_iter() {
                    event_sender
                        .send(RestEvent::RowEvent {
                            src_table_id,
                            operation: RowEventOperation::Insert,
                            row: cur_row,
                            lsn: lsn_generator.fetch_add(1, Ordering::SeqCst),
                            timestamp: std::time::SystemTime::now(),
                        })
                        .unwrap();
                }
            }
        }
    }

    /// Process an event request, which is operated on a file.
    fn process_file_request(&self, request: &FileEventRequest) -> Result<Vec<RestEvent>> {
        let src_table_id = self
            .src_table_name_to_src_id
            .get(&request.src_table_name)
            .ok_or_else(|| RestSourceError::UnknownTable(request.src_table_name.clone()))?;
        let src_table_id = *src_table_id;

        let (file_upload_row_tx, file_upload_row_rx) = tokio::sync::mpsc::unbounded_channel();
        let lsn_generator = self.lsn_generator.clone();
        let storage_config = request.storage_config.clone();
        let parquet_files = request.files.clone();

        tokio::task::spawn(async move {
            Self::generate_table_events_for_file_upload(
                src_table_id,
                lsn_generator,
                storage_config,
                parquet_files,
                file_upload_row_tx,
            )
            .await;
        });

        let file_upload_row_rx = Arc::new(Mutex::new(file_upload_row_rx));
        let file_rest_event = RestEvent::FileEvent {
            operation: FileEventOperation::Upload,
            table_events: file_upload_row_rx,
        };
        Ok(vec![file_rest_event])
    }

    /// Process an event request, which is operated on a row.
    fn process_row_request(&self, request: &RowEventRequest) -> Result<Vec<RestEvent>> {
        let schema = self
            .table_schemas
            .get(&request.src_table_name)
            .ok_or_else(|| RestSourceError::UnknownTable(request.src_table_name.clone()))?;

        let src_table_id = self
            .src_table_name_to_src_id
            .get(&request.src_table_name)
            .ok_or_else(|| RestSourceError::UnknownTable(request.src_table_name.clone()))?;

        let converter = JsonToMoonlinkRowConverter::new(schema.clone());
        let row = converter.convert(&request.payload)?;

        let row_lsn = self.lsn_generator.fetch_add(1, Ordering::SeqCst);
        let commit_lsn = self.lsn_generator.fetch_add(1, Ordering::SeqCst);

        // Generate both a row event and a commit event
        let events = vec![
            RestEvent::RowEvent {
                src_table_id: *src_table_id,
                operation: request.operation.clone(),
                row,
                lsn: row_lsn,
                timestamp: request.timestamp,
            },
            RestEvent::Commit {
                lsn: commit_lsn,
                timestamp: request.timestamp,
            },
        ];

        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use arrow_schema::{DataType, Field, Schema};
    use moonlink::row::RowValue;
    use serde_json::json;
    use std::sync::Arc;

    fn make_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    #[test]
    fn test_rest_source_creation() {
        let mut source = RestSource::new();
        assert_eq!(source.table_schemas.len(), 0);
        assert_eq!(source.src_table_name_to_src_id.len(), 0);

        // Test adding table
        let schema = make_test_schema();
        source
            .add_table("test_table".to_string(), 1, schema.clone())
            .unwrap();
        assert_eq!(source.table_schemas.len(), 1);
        assert_eq!(source.src_table_name_to_src_id.len(), 1);
        assert!(source.table_schemas.contains_key("test_table"));
        assert_eq!(source.src_table_name_to_src_id.get("test_table"), Some(&1));

        // Test removing table
        source.remove_table("test_table").unwrap();
        assert_eq!(source.table_schemas.len(), 0);
        assert_eq!(source.src_table_name_to_src_id.len(), 0);
    }

    #[test]
    fn test_process_request_success() {
        let mut source = RestSource::new();
        let schema = make_test_schema();
        source
            .add_table("test_table".to_string(), 1, schema)
            .unwrap();

        let request = RowEventRequest {
            src_table_name: "test_table".to_string(),
            operation: RowEventOperation::Insert,
            payload: json!({
                "id": 42,
                "name": "test"
            }),
            timestamp: SystemTime::now(),
        };

        let events = source.process_row_request(&request).unwrap();
        assert_eq!(events.len(), 2); // Should have row event + commit event

        // Check row event (first event)
        match &events[0] {
            RestEvent::RowEvent {
                src_table_id,
                operation,
                row,
                lsn,
                ..
            } => {
                assert_eq!(*src_table_id, 1);
                assert!(matches!(operation, RowEventOperation::Insert));
                assert_eq!(row.values.len(), 2);
                assert_eq!(row.values[0], RowValue::Int32(42));
                assert_eq!(row.values[1], RowValue::ByteArray(b"test".to_vec()));
                assert_eq!(*lsn, 1);
            }
            _ => panic!("Expected RowEvent"),
        }

        // Check commit event (second event)
        match &events[1] {
            RestEvent::Commit { lsn, .. } => {
                assert_eq!(*lsn, 2);
            }
            _ => panic!("Expected Commit event"),
        }
    }

    #[test]
    fn test_create_existing_table() {
        let mut source = RestSource::new();
        let schema = make_test_schema();
        source
            .add_table(
                "test_table".to_string(),
                /*src_table_id=*/ 1,
                schema.clone(),
            )
            .unwrap();

        let res = source.add_table("test_table".to_string(), /*src_table_id=*/ 1, schema);
        assert!(res.is_err());
    }

    #[test]
    fn test_drop_non_existent_table() {
        let mut source = RestSource::new();
        let res = source.remove_table("test_table");
        assert!(res.is_err());
    }

    #[test]
    fn test_process_request_unknown_table() {
        let source = RestSource::new();
        // No schema added

        let request = RowEventRequest {
            src_table_name: "unknown_table".to_string(),
            operation: RowEventOperation::Insert,
            payload: json!({"id": 1}),
            timestamp: SystemTime::now(),
        };

        let err = source.process_row_request(&request).unwrap_err();
        match err {
            Error::RestSource(es) => {
                let inner = es
                    .source
                    .as_deref()
                    .and_then(|e| e.downcast_ref::<RestSourceError>())
                    .expect("expected inner RestSourceError");

                match inner {
                    RestSourceError::UnknownTable(table_name) => {
                        assert_eq!(table_name, "unknown_table");
                    }
                    other => panic!("Expected UnknownTable, got {other:?}"),
                }
            }
            other => panic!("Expected Error::RestSource, got {other:?}"),
        }
    }

    #[test]
    fn test_lsn_generation() {
        let mut source = RestSource::new();
        let schema = make_test_schema();
        source
            .add_table("test_table".to_string(), 1, schema)
            .unwrap();

        let request1 = RowEventRequest {
            src_table_name: "test_table".to_string(),
            operation: RowEventOperation::Insert,
            payload: json!({"id": 1, "name": "first"}),
            timestamp: SystemTime::now(),
        };

        let request2 = RowEventRequest {
            src_table_name: "test_table".to_string(),
            operation: RowEventOperation::Insert,
            payload: json!({"id": 2, "name": "second"}),
            timestamp: SystemTime::now(),
        };

        let events1 = source.process_row_request(&request1).unwrap();
        let events2 = source.process_row_request(&request2).unwrap();

        // Each request should generate 2 events (row + commit)
        assert_eq!(events1.len(), 2);
        assert_eq!(events2.len(), 2);

        // Check first request events
        match &events1[0] {
            RestEvent::RowEvent { lsn, .. } => assert_eq!(*lsn, 1),
            _ => panic!("Expected RowEvent"),
        }
        match &events1[1] {
            RestEvent::Commit { lsn, .. } => assert_eq!(*lsn, 2),
            _ => panic!("Expected Commit"),
        }

        // Check second request events (LSNs should continue incrementing)
        match &events2[0] {
            RestEvent::RowEvent { lsn, .. } => assert_eq!(*lsn, 3),
            _ => panic!("Expected RowEvent"),
        }
        match &events2[1] {
            RestEvent::Commit { lsn, .. } => assert_eq!(*lsn, 4),
            _ => panic!("Expected Commit"),
        }
    }
}
