use crate::storage::filesystem::s3::s3_test_utils::*;
use crate::storage::filesystem::s3::test_guard::TestGuard as S3TestGuard;
use crate::storage::iceberg::catalog_test_impl::*;
use crate::storage::iceberg::file_catalog_test_utils::get_test_schema;
use crate::storage::iceberg::glue_catalog::GlueCatalog;
use crate::storage::iceberg::iceberg_table_config::GlueCatalogConfig;
use crate::storage::iceberg::moonlink_catalog::CatalogAccess;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_glue::{
    AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, GLUE_CATALOG_PROP_URI,
    GLUE_CATALOG_PROP_WAREHOUSE,
};
use rand::{distr::Alphanumeric, Rng};
use std::collections::HashMap;

/// Test AWS access id.
const TEST_AWS_ACCESS_ID: &str = "moonlink_test_access_id";
/// Test AWS secret.
const TEST_AWS_ACCESS_SECRET: &str = "moonlink_test_secret";
/// Test AWS region.
const TEST_AWS_RETION: &str = "us-east-1";
/// Test glue endpoint.
const TEST_GLUE_ENDPOINT: &str = "http://moto-glue.local:5000";

/// Test util function to get glue catalog name.
fn get_random_glue_catalog_name() -> String {
    format!("glue-catalog-{}", uuid::Uuid::new_v4().to_string())
}

/// Test util function to get a random string.
fn get_random_string() -> String {
    let rng = rand::rng();
    rng.sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

/// Test util function to get a random namespace.
fn get_random_namespace() -> String {
    get_random_string()
}

/// Test util function to get a random table.
fn get_random_table() -> String {
    get_random_string()
}

/// Test util function to create glue catalog properties.
fn create_glue_catalog_properties(warehouse_uri: String) -> HashMap<String, String> {
    HashMap::from([
        // AWS configs.
        (
            AWS_ACCESS_KEY_ID.to_string(),
            TEST_AWS_ACCESS_ID.to_string(),
        ),
        (
            AWS_SECRET_ACCESS_KEY.to_string(),
            TEST_AWS_ACCESS_SECRET.to_string(),
        ),
        (AWS_REGION_NAME.to_string(), TEST_AWS_RETION.to_string()),
        // S3 configs.
        (S3_ENDPOINT.to_string(), S3_TEST_ENDPOINT.to_string()),
        (
            S3_ACCESS_KEY_ID.to_string(),
            S3_TEST_ACCESS_KEY_ID.to_string(),
        ),
        (
            S3_SECRET_ACCESS_KEY.to_string(),
            S3_TEST_SECRET_ACCESS_KEY.to_string(),
        ),
        (S3_REGION.to_string(), S3_TEST_REGION.to_string()),
        // Glue configs.
        (
            GLUE_CATALOG_PROP_URI.to_string(),
            TEST_GLUE_ENDPOINT.to_string(),
        ),
        (
            GLUE_CATALOG_PROP_WAREHOUSE.to_string(),
            warehouse_uri.clone(),
        ),
    ])
}

/// Test util function to create a glue catalog.
async fn create_glue_catalog(warehouse_uri: String) -> GlueCatalog {
    let props = create_glue_catalog_properties(warehouse_uri.clone());
    let glue_config = GlueCatalogConfig {
        name: get_random_glue_catalog_name(),
        uri: TEST_GLUE_ENDPOINT.to_string(),
        catalog_id: None,
        warehouse: warehouse_uri.clone(),
        props,
    };
    let accessor_config = create_s3_storage_config(&warehouse_uri);
    let glue_catalog = GlueCatalog::new(glue_config, accessor_config, get_test_schema())
        .await
        .unwrap();
    glue_catalog
}

/// Test util function to create a namespace for the given glue catalog.
async fn create_namespace(glue_catalog: &GlueCatalog, namespace_ident: NamespaceIdent) {
    glue_catalog
        .create_namespace(&namespace_ident, /*properties=*/ HashMap::new())
        .await
        .unwrap();
}
/// Test util function to create a table for the given glue catalog.
async fn create_table(
    glue_catalog: &GlueCatalog,
    namespace_ident: NamespaceIdent,
    table_ident: TableIdent,
) {
    let table_creation = TableCreation::builder()
        .name(table_ident.name().to_string())
        .location(format!(
            "{}/{}/{}",
            glue_catalog.get_warehouse_location(),
            namespace_ident.to_url_string(),
            table_ident.name(),
        ))
        .schema(get_test_schema())
        .build();
    glue_catalog
        .create_table(&namespace_ident, table_creation)
        .await
        .unwrap();
}

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
