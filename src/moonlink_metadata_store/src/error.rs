use serde_json::Error as SerdeJsonError;
use thiserror::Error;
use tokio_postgres::Error as TokioPostgresError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("tokio postgres error: {0}")]
    TokioPostgresError(#[from] TokioPostgresError),

    #[error("metadata operation row number doesn't match {0} vs {1}")]
    PostgresRowCountError(u32, u32),

    #[error("serde json error: {0}")]
    SerdeJsonError(#[from] SerdeJsonError),

    #[error("table id with id {0} not found")]
    TableIdNotFound(u32),

    #[error("required field {0} not exist in serialized config json")]
    ConfigFieldNotExist(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
