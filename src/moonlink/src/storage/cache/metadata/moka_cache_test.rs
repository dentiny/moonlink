use std::time::Duration;

use crate::storage::cache::metadata::base_cache::MetadataCacheTrait;
use crate::storage::cache::metadata::moka_cache::MokaCache;
use crate::storage::cache::metadata::test_utils::MokaCacheTestBuilder;

#[cfg(test)]
impl<K, V> MokaCache<K, V>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub async fn dump_all_for_test(&self) -> Vec<(K, V)> {
        self.cache
            .iter()
            .map(|(k_arc, v)| ((*k_arc).clone(), v.clone()))
            .collect()
    }
}

#[tokio::test]
async fn test_get_values() {
    let cache = MokaCacheTestBuilder::new().build();
    cache.put("key1".to_string(), "value1".to_string()).await;
    cache.put("key2".to_string(), "value2".to_string()).await;

    assert_eq!(
        cache.get(&"key1".to_string()).await,
        Some("value1".to_string())
    );
}

#[tokio::test]
async fn test_evict_by_ttl() {
    let cache = MokaCacheTestBuilder::new()
        .ttl(Duration::from_secs(0))
        .build();
    cache.put("key1".to_string(), "value1".to_string()).await;
    cache.put("key2".to_string(), "value2".to_string()).await;

    let all_entries = cache.dump_all_for_test().await;
    assert!(!all_entries.contains(&("key1".to_string(), "value1".to_string())));
    assert!(!all_entries.contains(&("key2".to_string(), "value2".to_string())));
}

#[tokio::test]
async fn test_put_values() {
    let cache = MokaCacheTestBuilder::new().build();
    cache.put("key1".to_string(), "value1".to_string()).await;
    cache.put("key2".to_string(), "value2".to_string()).await;

    let all_entries = cache.dump_all_for_test().await;
    assert!(all_entries.contains(&("key1".to_string(), "value1".to_string())));
    assert!(all_entries.contains(&("key2".to_string(), "value2".to_string())));
}

#[tokio::test]
async fn test_replace_entry_when_max_size_exceeds() {
    let cache = MokaCacheTestBuilder::new().build();
    cache.put("key1".to_string(), "value1".to_string()).await;
    cache.put("key2".to_string(), "value2".to_string()).await;
    cache.put("key3".to_string(), "value3".to_string()).await;

    let all_entries = cache.dump_all_for_test().await;
    assert!(all_entries.contains(&("key2".to_string(), "value2".to_string())));
    assert!(all_entries.contains(&("key3".to_string(), "value3".to_string())));
}

#[tokio::test]
async fn test_evict_value() {
    let cache = MokaCacheTestBuilder::new().build();

    cache.put("key1".to_string(), "value1".to_string()).await;
    cache.put("key2".to_string(), "value2".to_string()).await;

    let removed = cache.remove(&"key1".to_string()).await;
    assert_eq!(removed, Some("value1".to_string()));

    let all_entries = cache.dump_all_for_test().await;
    assert!(!all_entries.contains(&("key1".to_string(), "value1".to_string())));
    assert!(all_entries.contains(&("key2".to_string(), "value2".to_string())));
}

#[tokio::test]
async fn test_evict_non_existing_key() {
    use crate::storage::cache::metadata::test_utils::MokaCacheTestBuilder;

    let cache = MokaCacheTestBuilder::new().build::<String, String>();
    cache.put("key1".to_string(), "value1".to_string()).await;
    cache.put("key2".to_string(), "value2".to_string()).await;

    let removed = cache.remove(&"not_exist".to_string()).await;
    assert_eq!(removed, None);

    let all_entries = cache.dump_all_for_test().await;
    assert!(all_entries.contains(&("key1".to_string(), "value1".to_string())));
    assert!(all_entries.contains(&("key2".to_string(), "value2".to_string())));
}

#[tokio::test]
async fn test_clear_all_values() {
    let cache = MokaCacheTestBuilder::new().build();

    cache.put("key1".to_string(), "value1".to_string()).await;
    cache.put("key2".to_string(), "value2".to_string()).await;
    cache.clear().await;

    let all_entries = cache.dump_all_for_test().await;
    assert!(!all_entries.contains(&("key1".to_string(), "value1".to_string())));
    assert!(!all_entries.contains(&("key2".to_string(), "value2".to_string())));
}
