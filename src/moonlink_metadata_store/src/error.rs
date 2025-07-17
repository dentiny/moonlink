use serde_json::Error as SerdeJsonError;
use std::sync::Arc;
use thiserror::Error;
use tokio_postgres::Error as TokioPostgresError;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("tokio postgres error: {source}")]
    TokioPostgres { source: Arc<TokioPostgresError> },

    #[error("metadata operation row number doesn't match {0} vs {1}")]
    PostgresRowCountError(u32, u32),

    #[error("sqlx error: {source}")]
    Sqlx { source: Arc<sqlx::Error> },

    #[error("metadata operation row number doesn't match {0} vs {1}")]
    SqliteRowCountError(u32, u32),

    #[error("metadata access failed precondition: {0}")]
    MetadataStoreFailedPrecondition(String),

    #[error("serde json error: {source}")]
    SerdeJson { source: Arc<SerdeJsonError> },

    #[error("table id with id {0} not found")]
    TableIdNotFound(u32),

    #[error("required field {0} not exist in serialized config json")]
    ConfigFieldNotExist(String),

    #[error("IO error: {source}")]
    Io { source: Arc<std::io::Error> },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<TokioPostgresError> for Error {
    fn from(source: TokioPostgresError) -> Self {
        Error::TokioPostgres {
            source: Arc::new(source),
        }
    }
}

impl From<sqlx::Error> for Error {
    fn from(source: sqlx::Error) -> Self {
        Error::Sqlx {
            source: Arc::new(source),
        }
    }
}

impl From<SerdeJsonError> for Error {
    fn from(source: SerdeJsonError) -> Self {
        Error::SerdeJson {
            source: Arc::new(source),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(source: std::io::Error) -> Self {
        Error::Io {
            source: Arc::new(source),
        }
    }
}
