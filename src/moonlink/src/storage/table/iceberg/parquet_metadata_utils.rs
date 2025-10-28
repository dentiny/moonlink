use iceberg::{arrow::ArrowFileReader, io::FileMetadata, io::InputFile, Result as IcebergResult};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};

/// Get parquet metadata from the given file.
pub(crate) async fn get_parquet_metadata(
    file_metadata: FileMetadata,
    input_file: InputFile,
) -> IcebergResult<ParquetMetaData> {
    let file_size_in_bytes = file_metadata.size;
    let reader = input_file.reader().await?;
    let mut arrow_file_reader = ArrowFileReader::new(file_metadata, reader);

    // TODO(hjiang): Check IO operation number and decide reader options.
    // As of now it's only accessing local files and will cached by filesystem.
    let parquet_meta_data_reader = ParquetMetaDataReader::new()
        .with_prefetch_hint(None)
        .with_column_index_policy(PageIndexPolicy::Optional)
        .with_page_index_policy(PageIndexPolicy::Optional)
        .with_offset_index_policy(PageIndexPolicy::Optional);
    let parquet_metadata = parquet_meta_data_reader
        .load_and_finish(&mut arrow_file_reader, file_size_in_bytes)
        .await?;

    Ok(parquet_metadata)
}
