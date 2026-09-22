//! Provider-neutral object-store transport for Cellule.

pub mod error;
pub mod error_map;
pub mod identity;
pub mod multipart;
mod observation;
pub mod provider_options;
pub mod provider_store;
mod read_admission;
pub use read_admission::ReadAdmission;
pub mod retry;
pub mod store;
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;
mod transport_read_admission;

#[doc(hidden)]
pub mod read_transport {
    pub use crate::transport_read_admission::{TransportReadReceipt, request};
}

pub use error::{Result, StorageError, read_rejection};
pub use error_map::{classify_auth_error, map_object_store_error};
pub use identity::{BucketIdentity, StorageProviderKind};
pub use observation::{StorageObservation, StorageObserver, StorageOperation, StorageOutcome};
pub use provider_store::{ObjectStoreCredentials, build_explicit_store};
pub use retry::{RetryClass, RetryPolicy, retry, retry_class};
pub use store::{
    ETag, MultipartUploadSource, StagedWrite, StorageObjectStream, StorageReadKind, Store,
};
