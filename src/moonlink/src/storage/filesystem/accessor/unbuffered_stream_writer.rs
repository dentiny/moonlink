/// A stream writer, which doesn't maintain a buffer inside.
use crate::storage::filesystem::accessor::base_unbuffered_stream_writer::BaseUnbufferedStreamWriter;
use crate::Result;

use async_trait::async_trait;
use futures::io::WriteAll;
use futures::AsyncWriteExt;
use opendal::raw::oio::MultipartWriter;
use opendal::raw::AccessDyn;
use opendal::raw::AccessorInfo;
use opendal::FuturesAsyncWriter;
use opendal::Operator;
use opendal::Writer;
use tokio::sync::mpsc::{Receiver, Sender};

use futures::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Max number of outstanding multipart writes.
const MAX_CONCURRENT_WRITRS: usize = 32;
/// Channel size for foreground/background communication.
const CHANNEL_SIZE: usize = 32;

pub struct UnbufferedStreamWriter {
    request_tx: Sender<Vec<u8>>,
    background_task: tokio::task::JoinHandle<Result<()>>,
}

impl UnbufferedStreamWriter {
    /// # Arguments
    ///
    /// * object_filepath: filepath relative to operator root path.
    pub async fn new(operator: Operator, object_filepath: String) -> Result<Self> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(CHANNEL_SIZE);
        let background_task = tokio::spawn(async move {
            let mut writer = operator
                .writer_with(&object_filepath)
                .concurrent(MAX_CONCURRENT_WRITRS)
                .await?;
            while let Some(buf) = rx.recv().await {
                writer.write(buf).await?;
            }
            writer.close().await?;
            Ok(())
        });

        Ok(Self {
            request_tx: tx,
            background_task,
        })
    }
}

#[async_trait]
impl BaseUnbufferedStreamWriter for UnbufferedStreamWriter {
    async fn append_non_blocking(&mut self, data: Vec<u8>) -> Result<()> {
        self.request_tx.send(data).await.unwrap();
        Ok(())
    }

    async fn finalize(mut self) -> Result<()> {
        drop(self.request_tx);
        self.background_task.await??;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::filesystem::accessor::{operator_utils, test_utils::*};

    #[tokio::test]
    async fn test_unbuffered_stream_writer() {
        const FILE_SIZE: usize = 10;

        let temp_dir = tempfile::tempdir().unwrap();
        let root_directory = temp_dir.path().to_str().unwrap().to_string();

        // Prepare src file.
        let src_filename = "src".to_string();
        let src_filepath = format!("{}/{}", &root_directory, src_filename);
        let expected_content = create_local_file(&src_filepath, FILE_SIZE).await;

        // Create an operator.
        let filesystem_config = &crate::FileSystemConfig::FileSystem { root_directory };
        let operator = operator_utils::create_opendal_operator(filesystem_config).unwrap();

        // Create writer and append in blocks.
        let mut writer = UnbufferedStreamWriter::new(operator.clone(), src_filename)
            .await
            .unwrap();
        writer
            .append_non_blocking(expected_content[..FILE_SIZE / 2].as_bytes().to_vec())
            .await
            .unwrap();
        writer
            .append_non_blocking(expected_content[FILE_SIZE / 2..].as_bytes().to_vec())
            .await
            .unwrap();
        writer.finalize().await.unwrap();

        // Verify content.
        let actual_content = tokio::fs::read(&src_filepath).await.unwrap();
        assert_eq!(actual_content, expected_content.into_bytes());
    }
}
