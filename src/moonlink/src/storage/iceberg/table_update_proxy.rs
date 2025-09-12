use std::collections::{HashMap, HashSet};

use crate::storage::iceberg::{
    moonlink_catalog::PuffinBlobType, puffin_writer_proxy::PuffinBlobMetadataProxy,
};

/// iceberg-rust doesn't support a few requirement features for moonlink, for example, deletion vector, data files to remove, etc.
/// TableUpdateProxy records these unsupported content which will be used in [`Catalog::update_table`].
///
/// Used to record puffin blob metadata in one transaction, and cleaned up after transaction commits.
#[derive(Debug, Default)]
pub(crate) struct TableUpdateProxy {
    /// Maps from "puffin filepath" to "puffin blob metadata".
    pub(crate) deletion_vector_blobs_to_add: HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    pub(crate) file_index_blobs_to_add: HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    /// A vector of "puffin filepath"s.
    pub(crate) puffin_blobs_to_remove: HashSet<String>,
    /// A set of data files to remove, along with their corresponding deletion vectors and file indices.
    pub(crate) data_files_to_remove: HashSet<String>,
}

impl TableUpdateProxy {
    /// Notice: it should be only set once, otherwise panic.
    pub(crate) fn set_data_files_to_remove(&mut self, data_files: HashSet<String>) {
        assert!(self.data_files_to_remove.is_empty());
        self.data_files_to_remove = data_files;
    }
    /// Notice: it should be only set once, otherwise panic.
    pub(crate) fn set_index_puffin_files_to_remove(&mut self, puffin_filepaths: HashSet<String>) {
        assert!(self.puffin_blobs_to_remove.is_empty());
        self.puffin_blobs_to_remove = puffin_filepaths;
    }
    /// Notice: given puffin filepath should correspond to one metadata, and should be set only once.
    pub(crate) fn record_puffin_metadata(
        &mut self,
        puffin_filepath: String,
        puffin_metadata: Vec<PuffinBlobMetadataProxy>,
        puffin_blob_type: PuffinBlobType,
    ) {
        match &puffin_blob_type {
            PuffinBlobType::DeletionVector => assert!(self
                .deletion_vector_blobs_to_add
                .insert(puffin_filepath, puffin_metadata)
                .is_none()),
            PuffinBlobType::FileIndex => assert!(self
                .file_index_blobs_to_add
                .insert(puffin_filepath, puffin_metadata)
                .is_none()),
        };
    }

    pub(crate) fn clear(&mut self) {
        self.deletion_vector_blobs_to_add.clear();
        self.file_index_blobs_to_add.clear();
        self.puffin_blobs_to_remove.clear();
        self.data_files_to_remove.clear();
    }
}
