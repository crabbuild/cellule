//! Validated application composition shared by authorized tenant scopes.

use crate::{
    ApplicationHandle, ApplicationId, BlobArtifactStore, CellApplication, CellClient,
    CompiledApplication, Error, PhantomData, Result, TenantId,
};
use std::sync::Arc;

/// A validated application/client binding that creates tenant-scoped capabilities.
///
/// Construct once during trusted service startup and share it with authorization
/// code. [`Self::scope`] grants access to the supplied tenant: callers must
/// authenticate and authorize that tenant before calling it. This factory does
/// not open Cells, start a runtime, or perform authorization.
pub struct ApplicationBinding<A> {
    client: CellClient,
    compiled: Arc<CompiledApplication>,
    application: ApplicationId,
    marker: PhantomData<fn() -> A>,
}

impl<A> Clone for ApplicationBinding<A> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            compiled: Arc::clone(&self.compiled),
            application: self.application,
            marker: PhantomData,
        }
    }
}

impl<A: CellApplication> ApplicationBinding<A> {
    /// Validates the author type and client registry against the compiled artifact.
    ///
    /// The embedding application constructs and configures the client transport.
    /// `cellule-host` can bind a local client to its existing runtime and layout.
    pub fn new(
        client: CellClient,
        compiled: Arc<CompiledApplication>,
        application: ApplicationId,
    ) -> Result<Self> {
        if compiled.name() != A::NAME {
            return Err(Error::Registry(
                "application type differs from compiled application",
            ));
        }
        if client.registry_digest() != compiled.registry().release_digest() {
            return Err(Error::Registry(
                "client registry differs from compiled application",
            ));
        }
        Ok(Self {
            client,
            compiled,
            application,
            marker: PhantomData,
        })
    }

    /// Creates a capability for an application-authorized tenant using the bound client.
    ///
    /// This clones the validated resources without reconstructing a client or
    /// rereading registry descriptors. It does not validate a caller's permissions.
    #[must_use]
    pub fn scope(&self, authorized_tenant: TenantId) -> ApplicationHandle<A> {
        ApplicationHandle {
            client: self.client.clone(),
            compiled: Arc::clone(&self.compiled),
            tenant: authorized_tenant,
            application: self.application,
            marker: PhantomData,
        }
    }

    /// Returns the immutable artifact used by every scoped capability.
    #[must_use]
    pub fn compiled(&self) -> &CompiledApplication {
        &self.compiled
    }

    /// Returns the stable application installation identity bound to the client.
    #[must_use]
    pub const fn application_id(&self) -> ApplicationId {
        self.application
    }

    /// Returns a binding with the explicit typed-query policy applied to all scopes.
    ///
    /// Commands and outcome resolution retain owner order. Replica queries
    /// require configured readers and never silently fall back to owner reads.
    #[must_use]
    pub fn with_read_policy(&self, policy: cellule_runtime::client::ReadPolicy) -> Self {
        let mut binding = self.clone();
        binding.client = binding.client.with_read_policy(policy);
        binding
    }

    /// Returns a binding whose scoped Blob capabilities share the supplied artifact store.
    #[must_use]
    pub fn with_blob_artifact_store(&self, store: BlobArtifactStore) -> Self {
        let mut binding = self.clone();
        binding.client = binding.client.with_blob_artifact_store(store);
        binding
    }
}
