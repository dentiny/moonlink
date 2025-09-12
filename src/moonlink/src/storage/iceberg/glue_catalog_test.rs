use crate::storage::filesystem::s3::test_guard::TestGuard as S3TestGuard;
use crate::storage::iceberg::catalog_test_impl::*;
use crate::storage::iceberg::file_catalog_test_utils::get_test_schema;
use crate::storage::iceberg::glue_catalog_test_utils::*;
use iceberg::{Catalog, NamespaceIdent, TableIdent};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_table_creation_and_drop() {
    let (bucket_name, warehouse_uri) =
        crate::storage::filesystem::s3::s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _guard = S3TestGuard::new(bucket_name.clone()).await;
    let glue_catalog = create_glue_catalog(warehouse_uri.clone()).await;

    let namespace_ident = NamespaceIdent::new(get_random_namespace());
    let table_ident = TableIdent::new(namespace_ident.clone(), get_random_table());
    create_namespace(&glue_catalog, namespace_ident.clone()).await;
    create_table(&glue_catalog, namespace_ident.clone(), table_ident.clone()).await;

    // Check table existence.
    let exists = glue_catalog.table_exists(&table_ident).await.unwrap();
    assert!(exists);

    // Check schema.
    let table = glue_catalog.load_table(&table_ident).await.unwrap();
    let actual_schema = table.metadata().current_schema();
    assert_eq!(**actual_schema, get_test_schema());

    // Drop table.
    glue_catalog.drop_table(&table_ident).await.unwrap();
    let exists = glue_catalog.table_exists(&table_ident).await.unwrap();
    assert!(!exists);

    // Drop namespace after test.
    glue_catalog.drop_namespace(&namespace_ident).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_table() {
    let (bucket_name, warehouse_uri) =
        crate::storage::filesystem::s3::s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _guard = S3TestGuard::new(bucket_name.clone()).await;
    let mut glue_catalog = create_glue_catalog(warehouse_uri.clone()).await;

    let namespace_ident = NamespaceIdent::new(get_random_namespace());
    let table_ident = TableIdent::new(namespace_ident.clone(), get_random_table());
    // create_namespace(&glue_catalog, namespace_ident.clone()).await;

    // Update table.
    let namespace = namespace_ident.to_url_string();
    let table = table_ident.name.clone();
    test_update_table_impl(&mut glue_catalog, namespace, table).await;

    // Drop namespace and table after test.
    glue_catalog.drop_table(&table_ident).await.unwrap();
    glue_catalog.drop_namespace(&namespace_ident).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_schema() {
    let (bucket_name, warehouse_uri) =
        crate::storage::filesystem::s3::s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _guard = S3TestGuard::new(bucket_name.clone()).await;
    let mut glue_catalog = create_glue_catalog(warehouse_uri.clone()).await;

    let namespace_ident = NamespaceIdent::new(get_random_namespace());
    let table_ident = TableIdent::new(namespace_ident.clone(), get_random_table());
    // create_namespace(&glue_catalog, namespace_ident.clone()).await;

    // Update table.
    let namespace = namespace_ident.to_url_string();
    let table = table_ident.name.clone();
    test_update_schema_impl(&mut glue_catalog, namespace, table).await;

    // Drop namespace and table after test.
    glue_catalog.drop_table(&table_ident).await.unwrap();
    glue_catalog.drop_namespace(&namespace_ident).await.unwrap();
}
