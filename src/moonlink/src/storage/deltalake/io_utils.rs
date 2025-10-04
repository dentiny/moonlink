use deltalake::DeltaTable;

use crate::{BaseFileSystemAccess, Result};

/// Get a unique filepath for iceberg table data filepath.
fn generate_unique_data_filepath(table: &DeltaTable, local_filepath: &String) -> String {
    let filename_without_suffix = std::path::Path::new(local_filepath)
        .file_stem()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let table_uri = table.table_uri();
    let remote_filepath = format!(
        "{}.{}-{}.parquet",
        table_uri,
        filename_without_suffix,
        uuid::Uuid::now_v7()
    );
    remote_filepath
}

/// Upload the given data file to delta table.
pub(crate) async fn upload_data_file_to_delta(
    table: &DeltaTable,
    local_filepath: &String,
    filesystem_accessor: &dyn BaseFileSystemAccess,
) -> Result<String> {
    let remote_filepath = generate_unique_data_filepath(table, local_filepath);
    // Import local parquet file to remote.
    filesystem_accessor
        .copy_from_local_to_remote(local_filepath, &remote_filepath)
        .await?;
    Ok(remote_filepath)
}
