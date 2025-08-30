use crate::storage::iceberg::rest_catalog::RestCatalog;
use crate::storage::iceberg::rest_catalog_test_guard::RestCatalogTestGuard;
use crate::storage::iceberg::rest_catalog_test_utils::*;
use iceberg::Catalog;
use serial_test::serial;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_drop_table() {
    let namespace = get_random_string();
    let table = get_random_string();
    let config = get_random_rest_catalog_config();
    let mut guard =
        RestCatalogTestGuard::new(config.clone(), namespace.clone(), Some(table.clone()))
            .await
            .unwrap();
    let catalog = RestCatalog::new(config).await.unwrap();
    let table_ident = guard.table.clone().unwrap();
    guard.table = None;
    assert!(catalog
        .table_exists(&table_ident)
        .await
        .unwrap_or_else(|_| panic!(
            "Table exist function fail, namespace={namespace} table={table}"
        )));
    catalog
        .drop_table(&table_ident)
        .await
        .unwrap_or_else(|_| panic!("Table creation fail, namespace={namespace} table={table}"));
    assert!(!catalog
        .table_exists(&table_ident)
        .await
        .unwrap_or_else(|_| panic!(
            "Table exist function fail, namespace={namespace} table={table}"
        )));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_table_exists() {
    let namespace = get_random_string();
    let table = get_random_string();
    let config = get_random_rest_catalog_config();
    let guard = RestCatalogTestGuard::new(config, namespace.clone(), Some(table.clone()))
        .await
        .unwrap();
    let config = get_random_rest_catalog_config();
    let catalog = RestCatalog::new(config).await.unwrap();

    // Check table existence.
    let table_ident = guard.table.clone().unwrap();
    assert!(catalog.table_exists(&table_ident).await.unwrap());

    // List tables and validate.
    let tables = catalog.list_tables(table_ident.namespace()).await.unwrap();
    assert_eq!(tables, vec![table_ident]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_load_table() {
    let namespace = get_random_string();
    let table = get_random_string();
    let config = get_random_rest_catalog_config();
    let guard = RestCatalogTestGuard::new(config, namespace.clone(), Some(table.clone()))
        .await
        .unwrap();
    let config = get_random_rest_catalog_config();
    let catalog = RestCatalog::new(config).await.unwrap();
    let table_ident = guard.table.clone().unwrap();
    let result = catalog.load_table(&table_ident).await.unwrap();
    let result_table_ident = result.identifier().clone();
    assert_eq!(table_ident, result_table_ident);
}
