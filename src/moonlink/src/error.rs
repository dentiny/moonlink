use arrow::error::ArrowError;
use iceberg::Error as IcebergError;
use parquet::errors::ParquetError;
use snafu::{prelude::*, Location};
use std::io;
use std::path::PathBuf;
use std::result;
use std::string::FromUtf8Error;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;

/// Custom error type for moonlink
#[derive(Clone, Debug, Snafu)]
pub enum Error {
    #[snafu(display("Arrow error: {source}"))]
    Arrow { source: Arc<ArrowError> },

    #[snafu(display("IO error: {source}"))]
    Io { source: Arc<io::Error> },

    #[snafu(display("Parquet error: {source}"))]
    Parquet { source: Arc<ParquetError> },

    #[snafu(display("Transaction {id} not found"))]
    TransactionNotFound { id: u32 },

    #[snafu(display("Watch channel receiver error: {source}"))]
    WatchChannelRecvError { source: watch::error::RecvError },

    #[snafu(display("Tokio join error: {message}. This typically occurs when a spawned task fails to complete successfully. Check the task's execution or panic status for more details."))]
    TokioJoinError { message: String },

    #[snafu(display("Iceberg error: {source}"))]
    IcebergError { source: Arc<IcebergError> },

    #[snafu(display("Iceberg error: {message}"))]
    IcebergMessage { message: String, location: Location },

    #[snafu(display("OpenDAL error: {source}"))]
    OpenDal { source: Arc<opendal::Error> },

    #[snafu(display("UTF-8 conversion error: {source}"))]
    Utf8 { source: std::string::FromUtf8Error },

    #[snafu(display("Join error: {source}"))]
    JoinError { source: Arc<tokio::task::JoinError> },

    #[snafu(display("JSON serialization/deserialization error: {source}"))]
    Json { source: Arc<serde_json::Error> },
}

pub type Result<T> = result::Result<T, Error>;

impl From<FromUtf8Error> for Error {
    fn from(source: FromUtf8Error) -> Self {
        Error::Utf8 { source }
    }
}

impl From<ArrowError> for Error {
    fn from(source: ArrowError) -> Self {
        Error::Arrow {
            source: Arc::new(source),
        }
    }
}

impl From<IcebergError> for Error {
    fn from(source: IcebergError) -> Self {
        Error::IcebergError {
            source: Arc::new(source),
        }
    }
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        Error::Io {
            source: Arc::new(source),
        }
    }
}

impl From<opendal::Error> for Error {
    fn from(source: opendal::Error) -> Self {
        Error::OpenDal {
            source: Arc::new(source),
        }
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(source: tokio::task::JoinError) -> Self {
        Error::JoinError {
            source: Arc::new(source),
        }
    }
}

impl From<ParquetError> for Error {
    fn from(source: ParquetError) -> Self {
        Error::Parquet {
            source: Arc::new(source),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(source: serde_json::Error) -> Self {
        Error::Json {
            source: Arc::new(source),
        }
    }
}

////////==========
#[derive(Clone, Debug, Snafu)]
pub enum SnafuError {
    #[snafu(display("Unable to create file {}", path.display()))]
    TestError {
        source: Arc<std::io::Error>,
        path: std::path::PathBuf,
        location: Location,
    },
}

pub type SnafuResult<T> = result::Result<T, SnafuError>;

impl SnafuError {
    pub fn io_error(source: std::io::Error, path: impl Into<PathBuf>, location: Location) -> Self {
        SnafuError::TestError {
            source: Arc::new(source),
            path: path.into(),
            location,
        }
    }
}
