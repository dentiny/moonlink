use crate::pg_replicate::table::TableId;
use crate::Result;
use crate::{PostgresSourceError, ReplicationConnection};
use moonlink::{IcebergTableEventManager, ObjectStorageCache, ReadStateManager};
use std::collections::HashMap;
use std::hash::Hash;
use tracing::{debug, info};

/// Manage replication sources keyed by their connection URI.
///
/// This struct abstracts the lifecycle of `MoonlinkPostgresSource` and
/// provides a single entry point to add new tables to a running
/// replication. A new replication will automatically be started when a
/// table is added for a URI that is not currently being replicated.
pub struct ReplicationManager<T: Eq + Hash> {
    /// Maps from uri to replication connection.
    connections: HashMap<String, ReplicationConnection>,
    /// Maps from table id (string format) to (uri, table id).
    table_info: HashMap<T, (String, TableId)>,
    /// Base directory for mooncake tables.
    table_base_path: String,
    /// Base directory for temporary files used in union read.
    table_temp_files_directory: String,
    /// Data file cache.
    data_file_cache: ObjectStorageCache,
}

impl<T: Eq + Hash> ReplicationManager<T> {
    pub fn new(
        table_base_path: String,
        table_temp_files_directory: String,
        data_file_cache: ObjectStorageCache,
    ) -> Self {
        Self {
            connections: HashMap::new(),
            table_info: HashMap::new(),
            table_base_path,
            table_temp_files_directory,
            data_file_cache,
        }
    }

    /// Add a table to be replicated from the given `uri`.
    ///
    /// If replication for this `uri` is not yet running a new replication
    /// source will be created.
    pub async fn add_table(
        &mut self,
        uri: &str,
        external_table_id: T,
        table_name: &str,
    ) -> Result<()> {
        info!(%uri, table_name, "adding table through manager");
        if !self.connections.contains_key(uri) {
            debug!(%uri, "creating replication connection");
            // Lazily create the directory that will hold all tables.
            // This will not overwrite any existing directory.
            tokio::fs::create_dir_all(&self.table_base_path)
                .await
                .map_err(PostgresSourceError::Io)?;
            let base_path = tokio::fs::canonicalize(&self.table_base_path)
                .await
                .map_err(PostgresSourceError::Io)?;
            let replication_connection = ReplicationConnection::new(
                uri.to_owned(),
                base_path.to_str().unwrap().to_string(),
                self.table_temp_files_directory.clone(),
                self.data_file_cache.clone(),
            )
            .await?;
            self.connections
                .insert(uri.to_string(), replication_connection);
        }
        let replication_connection = self.connections.get_mut(uri).unwrap();

        let table_id = replication_connection.add_table(table_name).await?;
        self.table_info
            .insert(external_table_id, (uri.to_string(), table_id));

        if !replication_connection.replication_started() {
            replication_connection.start_replication().await?;
        }

        info!(table_id, "table added through manager");

        Ok(())
    }

    /// Drop table specified by the given table id.
    /// Precondition: the table has been registered, otherwise panics.
    pub async fn drop_table(&mut self, external_table_id: T) -> Result<()> {
        let (table_uri, table_id) = &self.table_info.get(&external_table_id).unwrap();
        info!(table_id, %table_uri, "dropping table through manager");
        let repl_conn = self.connections.get_mut(table_uri).unwrap();
        repl_conn.drop_table(*table_id).await?;
        if repl_conn.table_readers_count() == 0 {
            repl_conn.drop_publication().await?;
            repl_conn.drop_replication_slot().await?;
            self.connections.remove(table_uri);
        }

        info!(table_id, "table dropped through manager");
        Ok(())
    }

    pub fn get_table_reader(&self, table_id: &T) -> &ReadStateManager {
        let (uri, table_id) = self.table_info.get(table_id).expect("table not found");
        let connection = self.connections.get(uri).expect("connection not found");
        connection.get_table_reader(*table_id)
    }

    pub fn get_iceberg_table_event_manager(
        &mut self,
        table_id: &T,
    ) -> &mut IcebergTableEventManager {
        let (uri, table_id) = self.table_info.get(table_id).expect("table not found");
        let connection = self.connections.get_mut(uri).expect("connection not found");
        connection.get_iceberg_table_event_manager(*table_id)
    }
}
