use iceberg::{TableCommit, TableIdent, TableRequirement, TableUpdate};

/// Struct which mimics [`TableCommit`], to workaround the limitation that [`TableCommit`] is not exposed to public.
#[repr(C)]
pub(crate) struct TableCommitProxy {
    pub(crate) ident: TableIdent,
    pub(crate) requirements: Vec<TableRequirement>,
    pub(crate) updates: Vec<TableUpdate>,
}

impl TableCommitProxy {
    /// Take as [`TableCommit`].
    pub(crate) fn take_as_table_commit(self) -> TableCommit {
        unsafe { std::mem::transmute::<TableCommitProxy, TableCommit>(self) }
    }
}
