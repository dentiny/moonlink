use std::fmt::{Display, Formatter, Result as FmtResult};
use std::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MooncakeTableId<D, T> {
    pub database_id: D,
    pub table_id: T,
}

impl<D, T> Display for MooncakeTableId<D, T>
where
    D: Display,
    T: Display,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}.{}", self.database_id, self.table_id)
    }
}

impl<D, T> MooncakeTableId<D, T>
where
    T: Display,
    D: Display,
{
    /// Get integer value of database id.
    pub fn get_database_id_value(&self) -> u32 {
        self.database_id.to_string().parse::<u32>().unwrap()
    }

    /// Get integer version of table id.
    pub fn get_table_id_value(&self) -> u32 {
        self.table_id.to_string().parse::<u32>().unwrap()
    }
}
