use super::puffin_writer_proxy::append_puffin_metadata_and_rewrite;
use crate::storage::iceberg::moonlink_catalog::PuffinWrite;
use crate::storage::iceberg::puffin_writer_proxy::{
    get_puffin_metadata_and_close, PuffinBlobMetadataProxy,
};
use crate::storage::iceberg::utils::to_iceberg_error;
use crate::storage::iceberg::utils;

use futures::future::join_all;
use futures::TryStreamExt;
use std::collections::{HashMap, HashSet};
/// This module contains the file-based catalog implementation, which relies on version hint file to decide current version snapshot.
/// Dispite a few limitation (i.e. atomic rename for local filesystem), it's not a problem for moonlink, which guarantees at most one writer at the same time (for nows).
/// It leverages `opendal` and iceberg `FileIO` as an abstraction layer to operate on all possible storage backends.
///
/// Iceberg table format from object storage's perspective:
/// - namespace_indicator.txt
///   - An empty file, indicates it's a valid namespace
/// - data
///   - parquet files
/// - metdata
///   - version hint file
///     + version-hint.text
///     + contains the latest version number for metadata
///   - metadata file
///     + v0.metadata.json ... vn.metadata.json
///     + records table schema
///   - snapshot file / manifest list
///     + snap-<snapshot-id>-<attempt-id>-<commit-uuid>.avro
///     + points to manifest files and record actions
///   - manifest files
///     + <commit-uuid>-m<manifest-counter>.avro
///     + which points to data files and stats
///
/// TODO(hjiang):
/// 1. Before release we should support not only S3, but also R2, GCS, etc; necessary change should be minimal, only need to setup configuration like secret id and secret key.
/// 2. Add integration test to actual object storage before pg_mooncake release.
use std::error::Error;
use std::path::PathBuf;
use std::vec;

use async_trait::async_trait;
use iceberg::io::{FileIO, FileIOBuilder};
use iceberg::puffin::PuffinWriter;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::table::Table;
use iceberg::Result as IcebergResult;
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, TableUpdate,
};
use iceberg::{Error as IcebergError, TableRequirement};
use opendal::layers::RetryLayer;
use opendal::services;
use opendal::Operator;
use tokio::sync::OnceCell;

// Object storage usually doesn't have "folder" concept, when creating a new namespace, we create an indicator file under certain folder.
pub(super) const NAMESPACE_INDICATOR_OBJECT_NAME: &str = "indicator.text";

// Retry related constants.
static MIN_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(500);
static MAX_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(10);
static RETRY_DELAY_FACTOR: f32 = 1.5;
static MAX_RETRY_COUNT: usize = 2;

#[derive(Clone, Debug, PartialEq)]
pub enum CatalogConfig {
    #[cfg(feature = "storage-fs")]
    FileSystem,
    #[cfg(feature = "storage-s3")]
    S3 {
        access_key_id: String,
        secret_access_key: String,
        region: String,
        bucket: String,
        endpoint: String,
    },
    #[cfg(feature = "storage-gcs")]
    GCS {
        bucket: String,
        endpoint: String,
    },
}

#[derive(Debug)]
pub struct FileCatalog {
    /// Catalog configurations.
    config: CatalogConfig,
    /// Similar to opendal operator, which also provides an abstraction above different storage backends.
    file_io: FileIO,
    /// Operator to manager all IO operations.
    operator: OnceCell<Operator>,
    /// Table location.
    warehouse_location: String,
    /// Used to record puffin blob metadata in one transaction, and cleaned up after transaction commits.
    ///
    /// Maps from "puffin filepath" to "puffin blob metadata".
    puffin_blobs_to_add: HashMap<String, Vec<PuffinBlobMetadataProxy>>,
    /// A vector of "puffin filepath"s.
    puffin_blobs_to_remove: HashSet<String>,
    /// A set of data files to remove, along with their corresponding deletion vectors and file indices.
    data_files_to_remove: HashSet<String>,
}

impl FileCatalog {
    /// Create a file catalog, which gets initialized lazily.
    pub fn new(warehouse_location: String, config: CatalogConfig) -> IcebergResult<Self> {
        let file_io = utils::create_file_io(&config)?;
        Ok(Self {
            config,
            file_io,
            operator: OnceCell::new(),
            warehouse_location,
            puffin_blobs_to_add: HashMap::new(),
            puffin_blobs_to_remove: HashSet::new(),
            data_files_to_remove: HashSet::new(),
        })
    }

    /// Get warehouse uri.
    #[allow(dead_code)]
    pub(super) fn get_warehouse_location(&self) -> &str {
        &self.warehouse_location
    }

    /// Get IO operator from the catalog.
    pub(crate) async fn get_operator(&self) -> IcebergResult<&Operator> {
        let retry_layer = RetryLayer::new()
            .with_max_times(MAX_RETRY_COUNT)
            .with_jitter()
            .with_factor(RETRY_DELAY_FACTOR)
            .with_min_delay(MIN_RETRY_DELAY)
            .with_max_delay(MAX_RETRY_DELAY);

        self.operator
            .get_or_try_init(|| async {
                match &self.config {
                    #[cfg(feature = "storage-fs")]
                    CatalogConfig::FileSystem => {
                        let builder = services::Fs::default().root(&self.warehouse_location);
                        let op = Operator::new(builder)
                            .expect("failed to create fs operator")
                            .layer(retry_layer)
                            .finish();
                        Ok(op)
                    }
                    #[cfg(feature = "storage-gcs")]
                    CatalogConfig::GCS { 
                        bucket,
                        endpoint, 
                    } => {
                        let builder = services::Gcs::default()
                            .root("/")
                            .bucket(bucket)
                            .endpoint(endpoint)
                            .disable_config_load()
                            .disable_vm_metadata()
                            .allow_anonymous();
                        let op = Operator::new(builder)
                            .expect("failed to create gcs operator")
                            .layer(retry_layer)
                            .finish();
                        Ok(op)
                    }
                    #[cfg(feature = "storage-s3")]
                    CatalogConfig::S3 {
                        access_key_id,
                        secret_access_key,
                        region,
                        bucket,
                        endpoint,
                        ..
                    } => {
                        let builder = services::S3::default()
                            .bucket(bucket)
                            .region(region)
                            .endpoint(endpoint)
                            .access_key_id(access_key_id)
                            .secret_access_key(secret_access_key);
                        let op = Operator::new(builder)
                            .expect("failed to create s3 operator")
                            .layer(retry_layer)
                            .finish();
                        Ok(op)
                    }
                }
            })
            .await
    }

    /// Get object name of the indicator object for the given namespace.
    fn get_namespace_indicator_name(namespace: &iceberg::NamespaceIdent) -> String {
        let mut path = PathBuf::new();
        for part in namespace.as_ref() {
            path.push(part);
        }
        path.push(NAMESPACE_INDICATOR_OBJECT_NAME);
        path.to_str().unwrap().to_string()
    }

    async fn object_exists(&self, object: &str) -> Result<bool, Box<dyn Error>> {
        match self.get_operator().await?.stat(object).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// List all direct sub-directory under the given directory.
    ///
    /// For example, we have directory "a", "a/b", "a/b/c", listing direct subdirectories for "a" will return "a/b".
    #[tracing::instrument(name = "list_subdirs", skip_all)]
    async fn list_direct_subdirectories(
        &self,
        folder: &str,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let prefix = format!("{}/", folder);
        let mut dirs = Vec::new();
        let lister = self.get_operator().await?.list(&prefix).await?;

        let entries = lister;
        for cur_entry in entries.iter() {
            // Both directories and objects will be returned, here we only care about sub-directories.
            if !cur_entry.path().ends_with('/') {
                continue;
            }
            let dir_name = cur_entry
                .path()
                .trim_start_matches(&prefix)
                .trim_end_matches('/')
                .to_string();
            if !dir_name.is_empty() {
                dirs.push(dir_name);
            }
        }

        Ok(dirs)
    }

    /// Read the whole content for the given object.
    /// Notice, it's not suitable to read large files; as of now it's made for metadata files.
    async fn read_object(&self, object: &str) -> Result<String, Box<dyn Error>> {
        let content = self.get_operator().await?.read(object).await?;
        Ok(String::from_utf8(content.to_vec())?)
    }

    /// Write the whole content to the given file.
    async fn write_object(
        &self,
        object_filepath: &str,
        content: &str,
    ) -> Result<(), Box<dyn Error>> {
        let data = content.as_bytes().to_vec();
        let operator = self.get_operator()
            .await?;
        operator.write(object_filepath, data)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write content: {}", e),
                )
            })?;
        Ok(())
    }

    /// Remove the whole directory.
    #[tracing::instrument(name = "remove_directory", skip_all)]
    #[cfg(feature = "storage-gcs")]
    async fn remove_directory(&self, directory: &str) -> Result<(), Box<dyn Error>> {
        let path = if directory.ends_with('/') {
            directory.to_string()
        } else {
            format!("{}/", directory)
        };
    
        let operator = self.get_operator().await?;
        let mut lister = operator.lister(&path).await?;
        let mut entries = Vec::new();
        
        while let Some(entry) = lister.try_next().await? {
            // List operation returns target path.
            if entry.path() != path {
                entries.push(entry.path().to_string());
            }
        }
        for entry_path in entries {
            if entry_path == path {
                continue;
            }
            if entry_path.ends_with('/') {
                Box::pin(self.remove_directory(&entry_path)).await?;
            } else {
                operator.delete(&entry_path).await?;
            }
        }
    
        if !path.is_empty() && path != "/" {
            operator.remove_all(&path).await?;
        }
    
        Ok(())
    }

    #[tracing::instrument(name = "remove_directory", skip_all)]
    #[cfg(not(feature = "storage-gcs"))]
    async fn remove_directory(&self, directory: &str) -> Result<(), Box<dyn Error>> {
        let op = self.get_operator().await.unwrap().clone();
        op.remove_all(directory).await?;
        Ok(())
    }

    /// Load metadata and its location foe the given table.
    pub(super) async fn load_metadata(
        &self,
        table_ident: &TableIdent,
    ) -> IcebergResult<(String /*metadata_filepath*/, TableMetadata)> {
        // Read version hint for the table to get latest version.
        let version_hint_filepath = format!(
            "{}/{}/metadata/version-hint.text",
            table_ident.namespace().to_url_string(),
            table_ident.name(),
        );
        let version_str = self
            .read_object(&version_hint_filepath)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to read version hint file on load table: {}", e),
                )
            })?;
        let version = version_str
            .trim()
            .parse::<u32>()
            .map_err(|e| IcebergError::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        // Read and parse table metadata.
        let metadata_filepath = format!(
            "{}/{}/metadata/v{}.metadata.json",
            table_ident.namespace().to_url_string(),
            table_ident.name(),
            version,
        );
        let metadata_str = self.read_object(&metadata_filepath).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to read table metadata file on load table: {}", e),
            )
        })?;
        let metadata = serde_json::from_slice::<TableMetadata>(metadata_str.as_bytes())
            .map_err(|e| IcebergError::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        Ok((metadata_filepath, metadata))
    }

    /// Validate table commit requirements.
    fn validate_table_requirements(
        table_requirements: Vec<TableRequirement>,
        table_metadata: &TableMetadata,
    ) -> IcebergResult<()> {
        for cur_requirment in table_requirements.into_iter() {
            cur_requirment.check(Some(table_metadata))?;
        }
        Ok(())
    }

    /// Reflect table updates to table metadata builder.
    fn reflect_table_updates(
        mut builder: TableMetadataBuilder,
        table_updates: Vec<TableUpdate>,
    ) -> IcebergResult<TableMetadataBuilder> {
        for update in &table_updates {
            match update {
                TableUpdate::AddSnapshot { snapshot } => {
                    builder = builder.add_snapshot(snapshot.clone())?;
                }
                TableUpdate::SetSnapshotRef {
                    ref_name,
                    reference,
                } => {
                    builder = builder.set_ref(ref_name, reference.clone())?;
                }
                TableUpdate::SetProperties { updates } => {
                    builder = builder.set_properties(updates.clone())?;
                }
                TableUpdate::RemoveProperties { removals } => {
                    builder = builder.remove_properties(removals)?;
                }
                _ => {
                    unreachable!("Only snapshot updates are expected in this implementation");
                }
            }
        }
        Ok(builder)
    }
}

#[async_trait]
impl PuffinWrite for FileCatalog {
    async fn record_puffin_metadata_and_close(
        &mut self,
        puffin_filepath: String,
        puffin_writer: PuffinWriter,
    ) -> IcebergResult<()> {
        let puffin_metadata = get_puffin_metadata_and_close(puffin_writer).await?;
        self.puffin_blobs_to_add
            .insert(puffin_filepath, puffin_metadata);
        Ok(())
    }

    fn set_data_files_to_remove(&mut self, data_files: HashSet<String>) {
        assert!(self.data_files_to_remove.is_empty());
        self.data_files_to_remove = data_files;
    }

    fn set_puffin_files_to_remove(&mut self, puffin_filepaths: HashSet<String>) {
        assert!(self.puffin_blobs_to_remove.is_empty());
        self.puffin_blobs_to_remove = puffin_filepaths;
    }

    fn clear_puffin_metadata(&mut self) {
        self.puffin_blobs_to_add.clear();
        self.puffin_blobs_to_remove.clear();
        self.data_files_to_remove.clear();
    }
}

#[async_trait]
impl Catalog for FileCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        // Check parent namespace existence.
        if let Some(namespace_ident) = parent {
            let exists = self.namespace_exists(namespace_ident).await?;
            if !exists {
                return Err(IcebergError::new(
                    iceberg::ErrorKind::NamespaceNotFound,
                    format!(
                        "When list namespace, parent namespace {:?} doesn't exist.",
                        namespace_ident
                    ),
                ));
            }
        }

        let parent_directory = if let Some(namespace_ident) = parent {
            namespace_ident.to_url_string()
        } else {
            "/".to_string()
        };
        let subdirectories = self
            .list_direct_subdirectories(&parent_directory)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to list namespaces: {}", e),
                )
            })?;

        // Start multiple async functions in parallel to check whether namespace.
        let mut futures = Vec::with_capacity(subdirectories.len());
        for cur_subdir in subdirectories.iter() {
            let cur_namespace_ident = if let Some(parent_namespace_ident) = parent {
                let mut parent_namespace_segments = parent_namespace_ident.clone().to_vec();
                parent_namespace_segments.push(cur_subdir.to_string());
                NamespaceIdent::from_vec(parent_namespace_segments).unwrap()
            } else {
                NamespaceIdent::new(cur_subdir.to_string())
            };
            futures.push(async move { self.namespace_exists(&cur_namespace_ident).await });
        }

        // Wait for all operations to complete and collect results.
        let exists_results = join_all(futures).await;
        let mut res: Vec<NamespaceIdent> = Vec::new();
        for (exists_result, cur_subdir) in exists_results.into_iter().zip(subdirectories.iter()) {
            let is_namespace = exists_result?;
            if is_namespace {
                let cur_namespace_ident = if let Some(parent_namespace_ident) = parent {
                    let mut parent_namespace_segments = parent_namespace_ident.clone().to_vec();
                    parent_namespace_segments.push(cur_subdir.to_string());
                    NamespaceIdent::from_vec(parent_namespace_segments).unwrap()
                } else {
                    NamespaceIdent::new(cur_subdir.to_string())
                };
                res.push(cur_namespace_ident);
            }
        }

        Ok(res)
    }

    /// Create a new namespace inside the catalog, return error if namespace already exists, or any parent namespace doesn't exist.
    ///
    /// TODO(hjiang): Implement properties handling.
    async fn create_namespace(
        &self,
        namespace_ident: &iceberg::NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<iceberg::Namespace> {
        let segments = namespace_ident.clone().inner();
        let mut segment_vec = vec![];
        for cur_segment in &segments[..segments.len().saturating_sub(1)] {
            segment_vec.push(cur_segment.clone());
            let parent_namespace_ident = NamespaceIdent::from_vec(segment_vec.clone())?;
            let exists = self.namespace_exists(&parent_namespace_ident).await?;
            if !exists {
                return Err(IcebergError::new(
                    iceberg::ErrorKind::NamespaceNotFound,
                    format!(
                        "Parent Namespace {:?} doesn't exists",
                        parent_namespace_ident
                    ),
                ));
            }
        }
        self.write_object(
            &FileCatalog::get_namespace_indicator_name(namespace_ident),
            /*content=*/ "",
        )
        .await
        .map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to write metadata file at namespace creation: {}", e),
            )
        })?;

        Ok(Namespace::new(namespace_ident.clone()))
    }

    /// Get a namespace information from the catalog, return error if requested namespace doesn't exist.
    async fn get_namespace(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<Namespace> {
        let exists = self.namespace_exists(namespace_ident).await?;
        if exists {
            return Ok(Namespace::new(namespace_ident.clone()));
        }
        Err(IcebergError::new(
            iceberg::ErrorKind::NamespaceNotFound,
            format!("Namespace {:?} does not exist", namespace_ident),
        ))
    }

    /// Check if namespace exists in catalog.
    async fn namespace_exists(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<bool> {
        match self
            .get_operator()
            .await?
            .stat(&FileCatalog::get_namespace_indicator_name(namespace_ident))
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => return Ok(false),
        }
    }

    /// Drop a namespace from the catalog.
    async fn drop_namespace(&self, namespace_ident: &NamespaceIdent) -> IcebergResult<()> {
        let key = FileCatalog::get_namespace_indicator_name(namespace_ident);

        self.get_operator().await?.delete(&key).await.map_err(|e| {
            IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to drop namespace: {}", e),
            )
        })?;

        Ok(())
    }

    /// List tables from namespace, return error if the given namespace doesn't exist.
    async fn list_tables(
        &self,
        namespace_ident: &NamespaceIdent,
    ) -> IcebergResult<Vec<TableIdent>> {
        // Check if the given namespace exists.
        let exists = self.namespace_exists(namespace_ident).await?;
        if !exists {
            return Err(IcebergError::new(
                iceberg::ErrorKind::NamespaceNotFound,
                format!(
                    "Namespace {:?} doesn't exist when list tables within.",
                    namespace_ident
                ),
            ));
        }

        let parent_directory = namespace_ident.to_url_string();
        let subdirectories = self
            .list_direct_subdirectories(&parent_directory)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to list tables: {}", e),
                )
            })?;

        let mut table_idents: Vec<TableIdent> = Vec::with_capacity(subdirectories.len());
        for cur_subdir in subdirectories.iter() {
            let cur_table_ident = TableIdent::new(namespace_ident.clone(), cur_subdir.clone());
            let exists = self.table_exists(&cur_table_ident).await?;
            if exists {
                table_idents.push(cur_table_ident);
            }
        }
        Ok(table_idents)
    }

    async fn update_namespace(
        &self,
        _namespace_ident: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        todo!()
    }

    /// Create a new table inside the namespace.
    async fn create_table(
        &self,
        namespace_ident: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        let directory = namespace_ident.to_url_string();
        let table_ident = TableIdent::new(namespace_ident.clone(), creation.name.clone());

        // Create version hint file.
        let version_hint_filepath =
            format!("{}/{}/metadata/version-hint.text", directory, creation.name);
        self.write_object(&version_hint_filepath, /*content=*/ "0")
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write version hint file at table creation: {}", e),
                )
            })?;

        // Create metadata file.
        let metadata_filepath = format!(
            "{}/{}/metadata/v0.metadata.json",
            directory,
            creation.name.clone()
        );

        let table_metadata = TableMetadataBuilder::from_table_creation(creation)?.build()?;
        let metadata_json = serde_json::to_string(&table_metadata.metadata)?;
        self.write_object(&metadata_filepath, /*content=*/ &metadata_json)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write metadata file at table creation: {}", e),
                )
            })?;

        let table = Table::builder()
            .metadata(table_metadata.metadata)
            .identifier(table_ident)
            .file_io(self.file_io.clone())
            .build()?;
        Ok(table)
    }

    /// Load table from the catalog.
    async fn load_table(&self, table_ident: &TableIdent) -> IcebergResult<Table> {
        let (metadata_filepath, metadata) = self.load_metadata(table_ident).await?;

        // Build and return the table.
        let metadata_path = format!("{}/{}", self.warehouse_location, metadata_filepath);
        let table = Table::builder()
            .metadata_location(metadata_path)
            .metadata(metadata)
            .identifier(table_ident.clone())
            .file_io(self.file_io.clone())
            .build()?;
        Ok(table)
    }

    /// Drop a table from the catalog.
    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        let directory = format!("{}/{}", table.namespace().to_url_string(), table.name());
        self.remove_directory(&directory)
            .await
            .map_err(|e| IcebergError::new(
                iceberg::ErrorKind::Unexpected, 
                format!("Failed to delete directory {}: {:?}", directory, e),
            ))?;
        Ok(())
    }

    /// Check if a table exists in the catalog.
    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        let mut version_hint_filepath = PathBuf::from(table.namespace.to_url_string());
        version_hint_filepath.push(table.name());
        version_hint_filepath.push("metadata");
        version_hint_filepath.push("version-hint.text");

        let exists = self
            .object_exists(version_hint_filepath.to_str().unwrap())
            .await
            .map_err(|e| IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to check version hint file existencee {:?}: {:?}", version_hint_filepath, e),
            ))?;
        Ok(exists)
    }

    /// Rename a table in the catalog.
    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        todo!()
    }

    /// Update a table to the catalog, which writes metadata file and version hint file.
    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<Table> {
        let (metadata_filepath, metadata) = self.load_metadata(commit.identifier()).await?;
        let version = metadata.next_sequence_number();
        let builder = TableMetadataBuilder::new_from_metadata(
            metadata.clone(),
            /*current_file_location=*/ Some(metadata_filepath.clone()),
        );

        // Validate existing table metadata with requirements.
        Self::validate_table_requirements(commit.take_requirements(), &metadata)?;

        // Construct new metadata with updates.
        let updates = commit.take_updates();
        let builder = Self::reflect_table_updates(builder, updates)?;
        let metadata = builder.build()?.metadata;

        // Write metadata file.
        let metadata_directory = format!(
            "{}/{}/metadata",
            commit.identifier().namespace().to_url_string(),
            commit.identifier().name()
        );
        let new_metadata_filepath = format!("{}/v{}.metadata.json", metadata_directory, version,);
        let metadata_json = serde_json::to_string(&metadata)?;
        self.write_object(&new_metadata_filepath, &metadata_json)
            .await
            .map_err(|e| {
                IcebergError::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("Failed to write metadata file at table update: {}", e),
                )
            })?;

        // Manifest files and manifest list has persisted into storage, make modifications based on puffin blobs.
        //
        // TODO(hjiang): Add unit test for update and check manifest population.
        append_puffin_metadata_and_rewrite(
            &metadata,
            &self.file_io,
            &self.data_files_to_remove,
            &self.puffin_blobs_to_add,
            &self.puffin_blobs_to_remove,
        )
        .await?;

        // Write version hint file.
        let version_hint_path = format!("{}/version-hint.text", metadata_directory);
        self.write_object(&version_hint_path, &format!("{version}"))
            .await
            .map_err(|e| IcebergError::new(
                iceberg::ErrorKind::Unexpected,
                format!("Failed to write version hint file existencee {}: {:?}", version_hint_path, e),
            ))?;

        Table::builder()
            .identifier(commit.identifier().clone())
            .file_io(self.file_io.clone())
            .metadata(metadata)
            .metadata_location(metadata_filepath)
            .build()
    }
}
