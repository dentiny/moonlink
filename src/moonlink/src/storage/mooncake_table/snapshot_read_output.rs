use crate::storage::cache::object_storage::base_cache::CacheTrait;
use crate::storage::cache::object_storage::object_storage_cache::ObjectStorageCache;
use crate::storage::storage_utils::FileId;
use crate::storage::PuffinDeletionBlobAtRead;
use crate::table_notify::TableNotify;
use crate::NonEvictableHandle;
use crate::ReadState;

use std::sync::Arc;

use tokio::sync::mpsc::Sender;

/// Mooncake snapshot for read.

/// Pass out two types of data files to read.
#[derive(Clone)]
pub enum DataFileForRead {
    /// Temporary data file for in-memory unpersisted data, used for union read.
    TemporaryDataFile(String),
    /// If the data file has already been pinned, directly add a reference count, otherwise it could be deferenced later.
    PinnedLocalWriteCache(NonEvictableHandle),
    /// If the data file is not pinned, pass out (file id, remote file path) and rely on read-through cache.
    RemoteFilePath((FileId, String)),
}

impl DataFileForRead {
    /// Get a file path to read.
    #[cfg(test)]
    pub fn get_file_path(&self) -> String {
        match self {
            Self::TemporaryDataFile(file) => file.clone(),
            Self::PinnedLocalWriteCache(cache_handle) => {
                cache_handle.cache_entry.cache_filepath.clone()
            }
            Self::RemoteFilePath((_, file)) => file.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ReadOutput {
    /// Data files contains two parts:
    /// 1. Committed and persisted data files, which consists of file id and remote path (if any).
    /// 2. Associated files, which include committed but un-persisted records.
    pub data_file_paths: Vec<DataFileForRead>,
    /// Puffin file paths.
    pub puffin_file_paths: Vec<String>,
    /// Deletion vectors persisted in puffin files.
    pub deletion_vectors: Vec<PuffinDeletionBlobAtRead>,
    /// Committed but un-persisted positional deletion records.
    pub position_deletes: Vec<(u32 /*file_index*/, u32 /*row_index*/)>,
    /// Contains committed but non-persisted record batches, which are persisted as temporary data files on local filesystem.
    pub associated_files: Vec<String>,
    /// Data files involved in the current snapshot.
    pub involved_data_files: Vec<FileId>,
    /// Table notifier for query completion; could be none for empty read output.
    pub table_notifier: Option<Sender<TableNotify>>,
    /// Data file cache, to pin local file cache, could be none for empty read output.
    pub data_file_cache: Option<ObjectStorageCache>,
}

impl ReadOutput {
    pub fn default() -> Self {
        Self {
            data_file_paths: vec![],
            puffin_file_paths: vec![],
            deletion_vectors: vec![],
            position_deletes: vec![],
            associated_files: vec![],
            involved_data_files: vec![],
            table_notifier: None,
            data_file_cache: None,
        }
    }

    /// Resolve all remote filepaths and convert into [`ReadState`] for query usage.
    ///
    /// TODO(hjiang): Parallelize download and pin.
    pub async fn take_as_read_state(mut self) -> Arc<ReadState> {
        // Resolve remote data files.
        let mut resolved_data_files = Vec::with_capacity(self.data_file_paths.len());
        let mut cache_handles = vec![];
        for cur_data_file in self.data_file_paths.into_iter() {
            match cur_data_file {
                DataFileForRead::TemporaryDataFile(file) => resolved_data_files.push(file),
                DataFileForRead::PinnedLocalWriteCache(cache_handle) => {
                    resolved_data_files.push(cache_handle.get_cache_filepath().to_string());
                    cache_handles.push(cache_handle);
                }
                DataFileForRead::RemoteFilePath((file_id, remote_filepath)) => {
                    // TODO(hjiang):
                    // 1. Delete evicted data files.
                    // 2. Better error propagation.
                    let (cache_handle, _files_to_delete) = self
                        .data_file_cache
                        .as_mut()
                        .unwrap()
                        .get_cache_entry(file_id, &remote_filepath)
                        .await
                        .unwrap();
                    if let Some(cache_handle) = cache_handle {
                        resolved_data_files.push(cache_handle.get_cache_filepath().to_string());
                        cache_handles.push(cache_handle);
                    } else {
                        resolved_data_files.push(remote_filepath);
                    }
                }
            }
        }

        // Construct read state.
        Arc::new(ReadState::new(
            // Data file and positional deletes for query.
            resolved_data_files,
            self.puffin_file_paths,
            self.deletion_vectors,
            self.position_deletes,
            // Fields used for read state cleanup after query completion.
            self.associated_files,
            self.involved_data_files,
            cache_handles,
            self.table_notifier,
        ))
    }
}
