use std::fmt::{Display, Formatter, Result as FmtResult};
use std::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MooncakeTableId {
    pub mooncake_database: String,
    pub mooncake_table: String,
}

impl Display for MooncakeTableId {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}.{}", self.mooncake_database, self.mooncake_table)
    }
}
