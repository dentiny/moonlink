use thiserror::Error;
use tokio_postgres::Error as TokioPostgresError;

use crate::error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("tokio postgres error: {0}")]
    TokioPostgresError(#[from] TokioPostgresError),
}

pub type Result<T> = std::result::Result<T, Error>;
