//! A taskboard whose project is one atomic, durably published Cell.
//!
//! Domain operations expose neither arbitrary SQL nor framework internals.
//! Canonical project keys, schema, operation IDs, and wire codecs are stable
//! application contracts. Infrastructure is assembled in the executable.

mod application;
mod commands;
mod model;
mod queries;

pub use application::{Taskboard, Tasks, compile};
pub use commands::ChangeTask;
pub use model::{Change, Page, PageRequest, ProjectKey, Task, TaskOutcome};
pub use queries::ListTasks;

use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, Committed, InvocationError, MutationIdentity, Observed, Receipt,
};

/// Domain client bound to exactly one project and authorized application handle.
#[derive(Clone)]
pub struct TaskboardClient {
    handle: ApplicationHandle<Taskboard>,
    target: CellTarget,
}

impl TaskboardClient {
    /// Derives the stable project Cell from a validated canonical key.
    pub fn new(
        handle: ApplicationHandle<Taskboard>,
        project: &ProjectKey,
    ) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(application::NAMESPACE, project.as_bytes())?;
        Ok(Self { handle, target })
    }

    /// Returns the resolved project target for application-owned provisioning.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }

    /// Prepares a command whose evidence remains available after caller cancellation.
    pub async fn prepare(
        &self,
        identity: MutationIdentity,
        change: Change,
    ) -> Result<cellule_runtime::PreparedCommand<ChangeTask>, InvocationError<TaskOutcome>> {
        self.handle
            .prepare_command::<ChangeTask>(&self.target, identity, change)
            .await
    }

    /// Applies one logical command; reuse the identity unchanged on retry.
    pub async fn change(
        &self,
        identity: MutationIdentity,
        change: Change,
    ) -> Result<Committed<TaskOutcome>, InvocationError<TaskOutcome>> {
        self.handle
            .command::<ChangeTask>(&self.target, identity, change)
            .await
    }

    /// Reads a bounded page at or beyond a receipt from this project Cell.
    pub async fn list(
        &self,
        minimum: Option<Receipt>,
        page: PageRequest,
    ) -> Result<Observed<Page>, InvocationError<Page>> {
        self.handle
            .query::<ListTasks>(&self.target, minimum, page)
            .await
    }

    /// Resolves the original evidence rather than inventing a new command identity.
    pub async fn resolve(
        &self,
        pending: &cellule_runtime::PendingMutation,
    ) -> Result<cellule_runtime::Resolution, InvocationError<Vec<u8>>> {
        self.handle.resolve(pending).await
    }
}
