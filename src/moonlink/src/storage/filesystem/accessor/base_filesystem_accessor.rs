/// This module defines the interface for filesystem accessor.
use crate::Result;

use async_trait::async_trait;

#[cfg(test)]
use mockall::*;

#[async_trait]
#[cfg_attr(test, automock)]
pub trait BaseFileSystemAccess: std::fmt::Debug + Send + Sync {
    /// ===============================
    /// Directory operations
    /// ===============================
    ///
    /// List all direct sub-directory under the given directory.
    ///
    /// For example, we have directory "a", "a/b", "a/b/c", listing direct subdirectories for "a" will return "a/b".
    async fn list_direct_subdirectories(&self, folder: &str) -> Result<Vec<String>>;

    /// Remove the whole directory recursively.
    async fn remove_directory(&self, directory: &str) -> Result<()>;

    /// ===============================
    /// Object operations
    /// ===============================
    ///
    /// Return whether the given object exists.
    async fn object_exists(&self, object: &str) -> Result<bool>;

    /// Read the whole content for the given object.
    /// Notice, it's not suitable to read large files; as of now it's made for metadata files.
    async fn read_object(&self, object: &str) -> Result<String>;

    /// Write the whole content to the given object.
    async fn write_object(&self, object_filepath: &str, content: &str) -> Result<()>;

    /// Delete the given object.
    async fn delete_object(&self, object_filepath: &str) -> Result<()>;

    /// Copy from local file [`src`] to remote file [`dst`].
    async fn copy_from_local_to_remote(&self, src: &str, dst: &str) -> Result<()>;
}
