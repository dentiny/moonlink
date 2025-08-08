#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Arrow schema error: {0}")]
    ArrowSchema(#[from] arrow_schema::ArrowError),
    #[error("Backend error: {0}")]
    Backend(#[from] moonlink_backend::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("RPC error: {0}")]
    Rpc(#[from] moonlink_rpc::Error),
    #[error("Join error: {0}.")]
    TokioTaskJoin(#[from] tokio::task::JoinError),
}

pub type Result<T> = std::result::Result<T, Error>;
