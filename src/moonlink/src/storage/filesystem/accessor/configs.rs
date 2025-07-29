// Retry related constants.
pub(crate) const MIN_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(50);
pub(crate) const MAX_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(10);
pub(crate) const RETRY_DELAY_FACTOR: f32 = 1.5;
pub(crate) const MAX_RETRY_COUNT: usize = 5;
