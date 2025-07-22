#[cfg(test)]
use crate::storage::mooncake_table::TableMetadata as MooncakeTableMetadata;
#[cfg(test)]
use iceberg::spec::Schema as IcebergSchema;
use iceberg::spec::DEFAULT_SCHEMA_ID;
use iceberg::table::Table as IcebergTable;

/// Schema related utils.
///
#[cfg(test)]
pub(crate) fn assert_is_same_schema(lhs: IcebergSchema, rhs: IcebergSchema) {
    let lhs_highest_field_id = lhs.highest_field_id();
    let rhs_highest_field_id = rhs.highest_field_id();
    assert_eq!(lhs_highest_field_id, rhs_highest_field_id);

    for cur_field_id in 0..=lhs_highest_field_id {
        let lhs_name = lhs.name_by_field_id(cur_field_id);
        let rhs_name = rhs.name_by_field_id(cur_field_id);
        assert_eq!(lhs_name, rhs_name);
    }
}

/// Validate iceberg table metadata matches the given schema.
#[cfg(test)]
pub(crate) fn assert_table_schema_consistent(
    table: &IcebergTable,
    mooncake_table_metadata: &MooncakeTableMetadata,
) {
    use iceberg::arrow as IcebergArrow;

    let iceberg_schema_1 = table.metadata().current_schema();
    let iceberg_schema_2 =
        IcebergArrow::arrow_schema_to_schema(mooncake_table_metadata.schema.as_ref()).unwrap();
    assert_is_same_schema(iceberg_schema_1.as_ref().clone(), iceberg_schema_2);
}

/// Validate iceberg schema id has been assigned.
pub(crate) fn assert_table_schema_id(table: &IcebergTable) {
    let schema_id = table.metadata().current_schema_id();
    assert_ne!(schema_id, DEFAULT_SCHEMA_ID);
}
