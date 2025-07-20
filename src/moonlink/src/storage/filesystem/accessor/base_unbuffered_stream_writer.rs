/// This module defines the interface for unbuffered stream writer.
///
/// There're two modes for the writer:
/// - non-blocking write, which returns immediately without waiting for the IO operation to complete.
/// - blocking write, which blocks wait until completion.
/// WARNING: These two modes cannot be used together.
use async_trait::async_trait;

use crate::Result;

#[cfg(test)]
use mockall::*;

#[async_trait]
#[cfg_attr(test, automock)]
pub trait BaseUnbufferedStreamWriter: Send {
    /// Append the given buffer to the writer in non-blocking style.
    async fn append_non_blocking(&mut self, data: Vec<u8>) -> Result<()>;

    /// Flush all pending writes.
    async fn finalize(mut self) -> Result<()>;
}
