#[cfg(test)]
use iceberg::spec::Schema as IcebergSchema;

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
