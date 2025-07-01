use crate::pg_replicate::postgres_source::{CdcStreamError, PostgresSourceError};
use moonlink::Error as MoonlinkError;
use std::result;
use thiserror::Error;
use tokio_postgres::Error as TokioPostgresError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Postgres source error: {0}")]
    PostgresSourceError(#[from] PostgresSourceError),

    #[error("tokio postgres error: {0}")]
    TokioPostgresError(#[from] TokioPostgresError),

    #[error("Postgres cdc stream error: {0}")]
    CdcStreamError(#[from] CdcStreamError),

    #[error("Moonlink source error: {source}")]
    MoonlinkError { source: MoonlinkError },
}

pub type Result<T> = result::Result<T, Error>;

impl From<MoonlinkError> for Error {
    fn from(source: MoonlinkError) -> Self {
        Error::MoonlinkError { source }
    }
}
