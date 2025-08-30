/// A RAII-style test guard, which creates namespace ident, table ident at construction, and deletes at destruction.
use crate::storage::iceberg::rest_catalog_test_utils::*;
use crate::storage::iceberg::{iceberg_table_config::RestCatalogConfig, rest_catalog::RestCatalog};
use iceberg::{Catalog, NamespaceIdent, Result, TableIdent};
use std::collections::HashMap;

pub(crate) struct RestCatalogTestGuard {
    pub(crate) namespace: NamespaceIdent,
    pub(crate) table: Option<TableIdent>,
    pub(crate) config: RestCatalogConfig,
}

impl RestCatalogTestGuard {
    pub(crate) async fn new(
        config: RestCatalogConfig,
        namespace: String,
        table: Option<String>,
    ) -> Result<Self> {
        let catalog = RestCatalog::new(config.clone()).await.unwrap();
        let ns_ident = NamespaceIdent::new(namespace.clone());
        catalog.create_namespace(&ns_ident, HashMap::new()).await?;
        let table_ident = if let Some(table) = table {
            let tc = default_table_creation(namespace.clone(), table.clone());
            catalog.create_table(&ns_ident, tc).await?;
            Some(TableIdent {
                namespace: ns_ident.clone(),
                name: table,
            })
        } else {
            None
        };
        Ok(Self {
            namespace: ns_ident,
            table: table_ident,
            config,
        })
    }
}

impl Drop for RestCatalogTestGuard {
    fn drop(&mut self) {
        let table = self.table.take();
        let config = self.config.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let catalog = RestCatalog::new(config).await.unwrap();
                if let Some(t) = table {
                    catalog.drop_table(&t).await.unwrap();
                }
                catalog.drop_namespace(&self.namespace).await.unwrap();
            });
        })
    }
}
