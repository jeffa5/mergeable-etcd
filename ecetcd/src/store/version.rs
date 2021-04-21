use std::num::NonZeroU64;

/// The version of a resource
///
/// - `None` if the resource has been deleted at the revision
/// - `Some(n)` otherwise and n will be the number of changes since creation
pub type Version = Option<NonZeroU64>;
