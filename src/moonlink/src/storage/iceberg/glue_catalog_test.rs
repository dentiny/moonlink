use std::collections::HashMap;
use chrono;
use crate::storage::{filesystem::s3::s3_test_utils::create_s3_storage_config, iceberg::{file_catalog_test_utils::get_test_schema, glue_catalog::GlueCatalog, iceberg_table_config::GlueCatalogConfig}};
use iceberg_catalog_glue::{
    AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY, GLUE_CATALOG_PROP_URI,
    GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalogBuilder,
};
use crate::storage::filesystem::s3::test_guard::TestGuard as S3TestGuard;
use crate::storage::iceberg::moonlink_catalog::CatalogAccess;
use iceberg::{io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY}, Catalog, NamespaceIdent, TableCreation, TableIdent};

/// Test AWS access id.
const TEST_AWS_ACCESS_ID: &str = "moonlink_test_access_id";
/// Test AWS secret.
const TEST_AWS_ACCESS_SECRET: &str = "moonlink_test_secret";
/// Test AWS region.
const TEST_AWS_RETION: &str = "moonlink_aws_region";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_table_creation() {
    // Use a unique bucket name for each test run to avoid conflicts
    let (bucket_name, warehouse_uri) = crate::storage::filesystem::s3::s3_test_utils::get_test_s3_bucket_and_warehouse();
    let _guard = S3TestGuard::new(bucket_name.clone()).await;
    let props = HashMap::from([
        (AWS_ACCESS_KEY_ID.to_string(), TEST_AWS_ACCESS_ID.to_string()),
        (
            AWS_SECRET_ACCESS_KEY.to_string(),
            TEST_AWS_ACCESS_SECRET.to_string(),
        ),
        (AWS_REGION_NAME.to_string(), TEST_AWS_RETION.to_string()),
        (
            S3_ENDPOINT.to_string(),
            format!("http://s3.local:9000"),
        ),
        (S3_ACCESS_KEY_ID.to_string(), "minioadmin".to_string()),
        (S3_SECRET_ACCESS_KEY.to_string(), "minioadmin".to_string()),
        (S3_REGION.to_string(), "us-east-1".to_string()),
        (GLUE_CATALOG_PROP_URI.to_string(), format!("http://moto-glue.local:5000")),
        (GLUE_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_uri.clone()),
    ]);

    // Generate a unique catalog name to avoid conflicts
    let catalog_name = format!("glue-catalog-{}", chrono::Utc::now().timestamp_millis());
    let glue_config = GlueCatalogConfig {
        name: catalog_name,
        uri: "http://moto-glue.local:5000".to_string(),
        catalog_id: "catalog-id".to_string(),
        warehouse: warehouse_uri.clone(),
        props,
    };
    let accessor_config = create_s3_storage_config(&warehouse_uri);
    let glue_catalog = GlueCatalog::new(glue_config, accessor_config, get_test_schema()).await.unwrap();

    let namespace_ident = NamespaceIdent::new("ns-2".to_string());
    glue_catalog.create_namespace(&namespace_ident, /*properties=*/HashMap::new()).await.unwrap();

    let table_ident = TableIdent::new(namespace_ident.clone(), "tbl-2".to_string());

    let table_creation = TableCreation::builder()
        .name("tbl-2".to_string())
        .location(format!(
            "{}/{}/{}",
            glue_catalog.get_warehouse_location(),
            namespace_ident.to_url_string(),
            "tbl-2",
        ))
        .schema(get_test_schema())
        .build();


    glue_catalog.create_table(&namespace_ident, table_creation).await.unwrap();
    let actual_table = glue_catalog.load_table(&table_ident).await.unwrap();
    println!("namespaces = {:?}", actual_table);
}
