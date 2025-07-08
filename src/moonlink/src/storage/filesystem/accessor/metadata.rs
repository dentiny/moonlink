/// Metadata for an object.
#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct ObjectMetadata {
    /// Object size.
    pub(crate) size: u64,
}
