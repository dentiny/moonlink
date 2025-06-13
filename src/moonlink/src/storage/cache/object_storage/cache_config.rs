/// Configuration for object storage cache.
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStorageCacheConfig {
    /// Max number of bytes for cache entries at local filesystem.
    pub max_bytes: u64,
    /// Directory to store local cache files.
    pub cache_directory: String,
}

impl ObjectStorageCacheConfig {
    pub fn new(max_bytes: u64, cache_directory: String) -> Self {
        Self {
            max_bytes,
            cache_directory,
        }
    }

    /// Provide a default option for ease of testing.
    /// For unit test, by default disable on-disk caching so it provides reproducibility in all cases,
    /// and useful to check write-through cache is not accessed and store after iceberg snapshot persistence.
    #[cfg(test)]
    pub fn default_for_test() -> Self {
        const DEFAULT_MAX_BYTES_FOR_TEST: u64 = 0;
        const DEFAULT_CACHE_DIRECTORY: &str = "/tmp/moonlink_test_cache";

        // Re-create default cache directory for testing.
        match std::fs::remove_dir_all(DEFAULT_CACHE_DIRECTORY) {
            Ok(()) => {}
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    panic!("Failed to remove directory: {:?}", e);
                }
            }
        }
        std::fs::create_dir_all(DEFAULT_CACHE_DIRECTORY).unwrap();

        Self {
            max_bytes: DEFAULT_MAX_BYTES_FOR_TEST,
            cache_directory: DEFAULT_CACHE_DIRECTORY.to_string(),
        }
    }

    /// Provide a default option for ease of benchmark.
    /// For unit test, by default disable on-disk caching so it provides reproducibility in all cases,
    /// and useful to check write-through cache is not accessed and store after iceberg snapshot persistence.
    #[cfg(feature = "bench")]
    pub fn default_for_bench() -> Self {
        const DEFAULT_MAX_BYTES_FOR_TEST: u64 = 1 << 30; // 30GiB
        const DEFAULT_CACHE_DIRECTORY: &str = "/tmp/moonlink_test_cache";

        // Re-create default cache directory for testing.
        match std::fs::remove_dir_all(DEFAULT_CACHE_DIRECTORY) {
            Ok(()) => {}
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    panic!("Failed to remove directory: {:?}", e);
                }
            }
        }
        std::fs::create_dir_all(DEFAULT_CACHE_DIRECTORY).unwrap();

        Self {
            max_bytes: DEFAULT_MAX_BYTES_FOR_TEST,
            cache_directory: DEFAULT_CACHE_DIRECTORY.to_string(),
        }
    }
}
