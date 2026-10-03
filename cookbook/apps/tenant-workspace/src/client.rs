use crate::{
    ChangeProject, Credentials, Error, GetProject, Mutation, Preference, PreferenceChange,
    PreferenceKey, Preferences, Principal, Project, ProjectChange, ProjectInput, ProjectKey,
    ProjectOutcome, Projects, Role, Version, WorkspaceApplication,
    application::{PREFERENCES, PROJECTS},
    model::PreferenceValue,
};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::LocalNode;
use cellule_runtime::{
    CellTarget, Observed, PreparedCommand, Receipt, Resolution,
    primitives::kv::{
        KvAtomicCommand, KvAtomicRequest, KvCheck, KvCondition, KvListQuery, KvListRequest,
        KvMutation,
    },
};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Authenticated application facade. The node and raw handles remain private.
pub struct Workspace {
    node: Arc<LocalNode>,
    credentials: Arc<Credentials>,
    dispatches: Arc<AtomicU64>,
    pub(crate) admission: Arc<tokio::sync::Semaphore>,
}
impl Workspace {
    /// Assembles a bounded service over an already enrolled, leased node.
    pub fn new(node: Arc<LocalNode>, credentials: Arc<Credentials>) -> Self {
        Self {
            node,
            credentials,
            dispatches: Arc::new(AtomicU64::new(0)),
            admission: Arc::new(tokio::sync::Semaphore::new(32)),
        }
    }
    /// Verifies an opaque credential before deriving any target.
    pub fn authenticate(&self, token: &str) -> Result<Principal, Error> {
        self.credentials.authenticate(token)
    }
    /// Checks the claimed tenant before creating an application handle.
    pub fn client(
        &self,
        principal: Principal,
        claimed_tenant: &str,
    ) -> Result<WorkspaceClient, Error> {
        principal.authorize_tenant(claimed_tenant)?;
        let handle = self
            .node
            .application_handle::<WorkspaceApplication>(principal.tenant.id())?;
        Ok(WorkspaceClient {
            node: self.node.clone(),
            handle,
            principal,
            dispatches: self.dispatches.clone(),
        })
    }
    /// Pages only the verified admin's own tenant memberships.
    pub fn members(
        &self,
        principal: &Principal,
        claimed_tenant: &str,
        after: Option<&str>,
        limit: u32,
    ) -> Result<crate::MemberPage, Error> {
        principal.authorize_tenant(claimed_tenant)?;
        self.credentials.members(principal, after, limit)
    }
    /// Returns local diagnostic evidence of authorized native operations, never an HTTP metric.
    /// Denied requests must leave this count unchanged.
    pub fn dispatch_count(&self) -> u64 {
        self.dispatches.load(Ordering::Relaxed)
    }
    /// Stops HTTP admission before the embedding drains its listener and node.
    /// Requests already holding a permit may finish their accepted operation.
    pub fn stop_admission(&self) {
        self.admission.close();
    }
    /// Reports the underlying node's storage, lease, and supervised-task readiness.
    pub fn is_ready(&self) -> bool {
        self.node.is_ready()
    }
}
/// Domain client bound to one verified membership; callers cannot supply another tenant or target.
#[derive(Clone)]
pub struct WorkspaceClient {
    node: Arc<LocalNode>,
    handle: ApplicationHandle<WorkspaceApplication>,
    principal: Principal,
    dispatches: Arc<AtomicU64>,
}
impl WorkspaceClient {
    fn project_target(&self, project: &ProjectKey) -> Result<CellTarget, Error> {
        Ok(self.handle.target_for_scope(PROJECTS, project.bytes())?)
    }
    fn preference_target(&self) -> Result<CellTarget, Error> {
        Ok(self.handle.target_for_scope(PREFERENCES, b"workspace")?)
    }
    fn minimum(target: &CellTarget, minimum: Option<Receipt>) -> Result<(), Error> {
        if minimum.is_some_and(|r| r.cell != target.cell_id()) {
            return Err(Error::Invalid("receipt belongs to another target"));
        }
        Ok(())
    }
    async fn open_project(&self, target: &CellTarget) -> Result<(), Error> {
        self.dispatches.fetch_add(1, Ordering::Relaxed);
        self.node.open_cell(target, &Projects).await?;
        Ok(())
    }
    async fn open_preferences(&self, target: &CellTarget) -> Result<(), Error> {
        self.dispatches.fetch_add(1, Ordering::Relaxed);
        self.node.open_cell(target, &Preferences).await?;
        Ok(())
    }
    fn project_identity(
        &self,
        project: &ProjectKey,
        mutation: &Mutation<ProjectChange>,
    ) -> Result<cellule_runtime::MutationIdentity, Error> {
        self.principal.authorize_project_write()?;
        let identity = mutation.authorize(&self.principal)?;
        if mutation.resource
            != (crate::Resource::Project {
                key: project.as_str().into(),
            })
        {
            return Err(Error::Invalid("retained project differs from route"));
        }
        mutation.change.validate()?;
        Ok(identity)
    }
    fn preference_identity(
        &self,
        mutation: &Mutation<PreferenceChange>,
    ) -> Result<cellule_runtime::MutationIdentity, Error> {
        self.principal.authorize_admin()?;
        let identity = mutation.authorize(&self.principal)?;
        if mutation.resource != crate::Resource::Preferences {
            return Err(Error::Invalid(
                "retained preference resource differs from route",
            ));
        }
        mutation.change.validate()?;
        Ok(identity)
    }
    fn admit(identity: cellule_runtime::MutationIdentity) -> Result<(), Error> {
        if identity.expires_at_ms <= cellule_cookbook_support::now_ms()? {
            return Err(Error::Invalid(
                "expired mutation; resolve without assuming absence",
            ));
        }
        Ok(())
    }
    /// Authorizes subject and editor role, then freezes the stamped project command.
    /// Retain its evidence before dispatch; expiry does not prove absence.
    pub async fn prepare_project(
        &self,
        project: &ProjectKey,
        mutation: &Mutation<ProjectChange>,
    ) -> Result<PreparedCommand<ChangeProject>, Error> {
        let identity = self.project_identity(project, mutation)?;
        Self::admit(identity)?;
        let target = self.project_target(project)?;
        self.open_project(&target).await?;
        Ok(self
            .handle
            .prepare_command::<ChangeProject>(
                &target,
                identity,
                ProjectInput {
                    actor: self.principal.subject.clone(),
                    change: mutation.change.clone(),
                },
            )
            .await?)
    }
    /// Reads at or beyond a receipt scoped to this tenant and exact project.
    pub async fn project(
        &self,
        project: &ProjectKey,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Project>>, Error> {
        let target = self.project_target(project)?;
        Self::minimum(&target, minimum)?;
        self.open_project(&target).await?;
        Ok(self
            .handle
            .query::<GetProject>(&target, minimum, ())
            .await?)
    }
    /// Resolves a frozen project command without redispatching it.
    pub async fn resolve_project(
        &self,
        project: &ProjectKey,
        mutation: &Mutation<ProjectChange>,
    ) -> Result<Resolution, Error> {
        let identity = self.project_identity(project, mutation)?;
        // Expiry is admission metadata, not a claim about committed state.
        // Report it without manufacturing replacement evidence or a new identity.
        if identity.expires_at_ms <= cellule_cookbook_support::now_ms()? {
            return Ok(Resolution::Expired);
        }
        let prepared = self.prepare_project(project, mutation).await?;
        Ok(self.handle.resolve(prepared.evidence()).await?)
    }
    /// Authorizes tenant admin and retained subject before selecting a KV shard.
    pub async fn prepare_preference(
        &self,
        mutation: &Mutation<PreferenceChange>,
    ) -> Result<PreparedCommand<KvAtomicCommand<Preferences>>, Error> {
        let identity = self.preference_identity(mutation)?;
        Self::admit(identity)?;
        let key = mutation.change.key.text().as_bytes().to_vec();
        let value = serde_json::to_vec(&PreferenceValue {
            value: mutation.change.value.clone(),
            updated_by: self.principal.subject.clone(),
        })?;
        let request = KvAtomicRequest {
            scope: b"workspace".to_vec(),
            checks: vec![KvCheck {
                key: key.clone(),
                condition: mutation
                    .change
                    .expected
                    .map_or(KvCondition::Absent, |v| KvCondition::Version(v.0)),
            }],
            mutations: vec![KvMutation::Put {
                key,
                value,
                expires_at_ms: None,
            }],
        };
        let target = self.preference_target()?;
        self.open_preferences(&target).await?;
        Ok(self
            .handle
            .prepare_command::<KvAtomicCommand<Preferences>>(&target, identity, request)
            .await?)
    }
    /// Resolves the original admin preference edit without redispatching it.
    pub async fn resolve_preference(
        &self,
        mutation: &Mutation<PreferenceChange>,
    ) -> Result<Resolution, Error> {
        let identity = self.preference_identity(mutation)?;
        if identity.expires_at_ms <= cellule_cookbook_support::now_ms()? {
            return Ok(Resolution::Expired);
        }
        let prepared = self.prepare_preference(mutation).await?;
        Ok(self.handle.resolve(prepared.evidence()).await?)
    }
    /// Reads both supported preferences in one native KV query and one receipt domain.
    pub async fn preferences(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Vec<Preference>>, Error> {
        let target = self.preference_target()?;
        Self::minimum(&target, minimum)?;
        self.open_preferences(&target).await?;
        let result = self
            .handle
            .query::<KvListQuery<Preferences>>(
                &target,
                minimum,
                KvListRequest {
                    scope: b"workspace".to_vec(),
                    prefix: vec![],
                    after_key: None,
                    limit: 3,
                },
            )
            .await?;
        if result.output.next_after.is_some() || result.output.entries.len() > 2 {
            return Err(Error::Invalid("unexpected stored preference set"));
        }
        let mut values = Vec::new();
        for entry in result.output.entries {
            let key = match entry.key.as_slice() {
                b"theme" => PreferenceKey::Theme,
                b"locale" => PreferenceKey::Locale,
                _ => return Err(Error::Invalid("unknown stored preference")),
            };
            let value: PreferenceValue = serde_json::from_slice(&entry.value)?;
            crate::model::canonical_slug(&value.updated_by, 48)?;
            PreferenceChange {
                key,
                expected: None,
                value: value.value.clone(),
            }
            .validate()?;
            values.push(Preference {
                key,
                value: value.value,
                updated_by: value.updated_by,
                version: Version(entry.version),
            });
        }
        Ok(Observed {
            output: values,
            receipt: result.receipt,
        })
    }
    /// Returns the verified role for embedding-side presentation; it cannot be changed by ingress.
    pub fn role(&self) -> Role {
        self.principal.role
    }
}
/// Decodes a retained project outcome using its bounded public wire contract.
pub(crate) fn project_outcome(bytes: &[u8]) -> Result<ProjectOutcome, Error> {
    use cellule_runtime::codec::WireValue as _;
    let mut decoder = cellule_runtime::codec::BoundedDecoder::new(bytes, 1024)?;
    let output = ProjectOutcome::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(output)
}
