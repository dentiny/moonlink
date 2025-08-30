use crate::storage::iceberg::iceberg_table_config::RestCatalogConfig;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::TableCreation;
use rand::{distr::Alphanumeric, Rng};
use std::collections::HashMap;

const DEFAULT_REST_CATALOG_URI: &str = "http://iceberg_rest.local:8181";

pub(crate) fn get_random_string() -> String {
    let rng = rand::rng();
    rng.sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

/// Get rest catalog with random catalog name.
pub(crate) fn get_random_rest_catalog_config() -> RestCatalogConfig {
    RestCatalogConfig {
        name: get_random_string(),
        uri: DEFAULT_REST_CATALOG_URI.to_string(),
        warehouse: "file:///tmp/iceberg-test".to_string(),
        props: HashMap::new(),
    }
}

pub(crate) fn default_table_creation(_namespace: String, table: String) -> TableCreation {
    TableCreation::builder()
        .name(table.clone())
        .schema(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                    NestedField::optional(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
                ])
                .build()
                .unwrap(),
        )
        // .location(format!("file:///tmp/iceberg-test/{}/{}", namespace, table))
        .build()
}
