use iceberg::spec::{NestedField, Schema, Type};
use iceberg::spec::{NameMapping, MappedField};

fn build_name_mapping_impl(mapped_fields: &mut Vec<MappedField>, cur_field_id: i32, cur_field: &NestedField) {
    // If current type is a composite type, build name mapping recursively.
    if let Type::Struct(struct_value) = cur_field.field_type.as_ref() {
        let sub_fields = struct_value.fields();
        let mut sub_mapped_fields = vec![];
        for cur_sub_field in sub_fields {
            build_name_mapping_impl(&mut sub_mapped_fields, cur_sub_field.id, &cur_sub_field);
        }
        mapped_fields.push(MappedField::new(Some(cur_field_id), vec![cur_field.name.clone()], sub_mapped_fields));
        return;
    }

    // Otherwise, directly construct a new mapped field.
    mapped_fields.push(MappedField::new(Some(cur_field_id), vec![cur_field.name.clone()], /*fields=*/ vec![]));
}

pub(crate) fn build_name_mapping(schema: &Schema) -> NameMapping {
    let mut name_mapping_fields = vec![];
    let highest_field_id = schema.highest_field_id();
    for cur_field_id in 0..=highest_field_id {
        if let Some(cur_field) = schema.field_by_id(cur_field_id) {
            build_name_mapping_impl(&mut name_mapping_fields, cur_field_id, cur_field.as_ref());
        }
    }
    NameMapping::new(name_mapping_fields)
}
