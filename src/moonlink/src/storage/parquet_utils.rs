/// This module contains parquet related constants and utils.
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

/// Default compression.
const DEFAULT_COMPRESSION: Compression = parquet::basic::Compression::SNAPPY;

/// Get the parquet write properties for disk slices.
pub fn get_default_parquet_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(DEFAULT_COMPRESSION)
        .build()
}

/// Get the parquet write properties for compacted files.
pub fn get_compaction_parquet_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(4).unwrap()))
        .build()
}
