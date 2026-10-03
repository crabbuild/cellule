use crate::{
    AccountBinding, AccountKey, AccountProgress, AccountReady, AccountSnapshot, Artifact,
    CloseAccount, CloseCompletion, CloseRequest, LedgerReport, PeriodDecision, PeriodIdentity,
    PeriodSpec, PeriodView, ReconcileAccount, StartDecision, UsageDecision, UsageEvent,
    UsageLedger,
    account::{CloseUsage, GetAccount, RecordUsage},
    application::{Files, Runs},
    commands::StartClose,
    period::{CreatePeriod, GetPeriod, ReconcileAccountCommand, SealPeriod},
    wire,
};
use cellule_app::ApplicationHandle;
use cellule_runtime::{
    CellTarget, Committed, InvocationError, MutationIdentity, Observed, Receipt,
    primitives::{
        blob::{BlobQuery, BlobQueryResult},
        workflow::WorkflowStatus,
    },
};

/// Retains framework, provider, codec, and task error sources.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Typed source capability bound to one canonical account entity Cell.
#[derive(Clone)]
pub struct AccountClient {
    handle: ApplicationHandle<UsageLedger>,
    key: AccountKey,
    target: CellTarget,
}

impl AccountClient {
    /// Binds an embedding-authorized account key to its stable entity target.
    pub fn new(
        handle: ApplicationHandle<UsageLedger>,
        key: AccountKey,
    ) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(crate::ACCOUNTS, key.as_bytes())?;
        Ok(Self {
            handle,
            key,
            target,
        })
    }

    /// Stable account target for explicit startup and native work.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }

    /// Permanently binds this account to a period before that period is created.
    pub async fn bind(
        &self,
        identity: MutationIdentity,
        spec: PeriodSpec,
    ) -> Result<Committed<AccountProgress>, InvocationError<AccountProgress>> {
        spec.validate().map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<crate::account::BindAccount>(
                &self.target,
                identity,
                AccountBinding {
                    spec,
                    account: self.key.clone(),
                },
            )
            .await?
            .execute()
            .await
    }

    /// Activates a prepared account and publishes its signed readiness Effect.
    pub async fn activate(
        &self,
        identity: MutationIdentity,
        spec: PeriodSpec,
    ) -> Result<Committed<AccountProgress>, InvocationError<AccountProgress>> {
        spec.validate().map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<crate::account::ActivateAccount>(
                &self.target,
                identity,
                AccountBinding {
                    spec,
                    account: self.key.clone(),
                },
            )
            .await?
            .execute()
            .await
    }

    /// Commits a permanent source event and its period projection Effect atomically.
    pub async fn record(
        &self,
        identity: MutationIdentity,
        event: UsageEvent,
    ) -> Result<Committed<UsageDecision>, InvocationError<UsageDecision>> {
        event.validate().map_err(InvocationError::NotStarted)?;
        if event.account != self.key {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::Identity("foreign usage-ledger account event"),
            ));
        }
        self.handle
            .prepare_command::<RecordUsage>(&self.target, identity, event)
            .await?
            .execute()
            .await
    }

    /// Fences this account and returns the exact durable source set at that fence.
    pub async fn close(
        &self,
        identity: MutationIdentity,
        period_id: [u8; 16],
    ) -> Result<Committed<AccountSnapshot>, InvocationError<AccountSnapshot>> {
        self.handle
            .prepare_command::<CloseUsage>(
                &self.target,
                identity,
                CloseAccount {
                    period_id,
                    account: self.key.clone(),
                },
            )
            .await?
            .execute()
            .await
    }

    /// Reads account state and source totals at a native receipt boundary.
    pub async fn get(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<AccountProgress>>, InvocationError<Option<AccountProgress>>> {
        self.handle
            .query::<GetAccount>(&self.target, minimum, self.key.clone())
            .await
    }
}

/// Typed period capability for lifecycle, projection, and immutable report access.
#[derive(Clone)]
pub struct PeriodClient {
    handle: ApplicationHandle<UsageLedger>,
    id: [u8; 16],
    target: CellTarget,
}

impl PeriodClient {
    /// Binds a permanent period entity identity.
    pub fn new(
        handle: ApplicationHandle<UsageLedger>,
        id: [u8; 16],
    ) -> cellule_runtime::Result<Self> {
        if id == [0; 16] {
            return Err(cellule_runtime::Error::Identity(
                "zero usage-ledger period ID",
            ));
        }
        let target = handle.target_for_scope(crate::PERIODS, &id)?;
        Ok(Self { handle, id, target })
    }

    /// Stable period Cell for domain setup and coordinator work.
    pub fn target(&self) -> &CellTarget {
        &self.target
    }

    /// Permanently creates the full roster in the Opening state.
    pub async fn create(
        &self,
        identity: MutationIdentity,
        spec: PeriodSpec,
    ) -> Result<Committed<PeriodDecision>, InvocationError<PeriodDecision>> {
        spec.validate().map_err(InvocationError::NotStarted)?;
        if spec.id != self.id {
            return Err(InvocationError::NotStarted(
                cellule_runtime::Error::Identity("foreign usage-ledger period specification"),
            ));
        }
        self.handle
            .prepare_command::<CreatePeriod>(&self.target, identity, spec)
            .await?
            .execute()
            .await
    }

    /// Records one source account's activation after the account Cell committed its binding.
    pub async fn confirm_ready(
        &self,
        identity: MutationIdentity,
        account: AccountKey,
    ) -> Result<Committed<PeriodDecision>, InvocationError<PeriodDecision>> {
        self.handle
            .prepare_command::<crate::period::ConfirmReady>(
                &self.target,
                identity,
                AccountReady {
                    period_id: self.id,
                    account,
                },
            )
            .await?
            .execute()
            .await
    }

    /// Raises the global close state; per-account commands establish actual source fences.
    pub async fn begin_close(
        &self,
        identity: MutationIdentity,
    ) -> Result<Committed<PeriodDecision>, InvocationError<PeriodDecision>> {
        self.handle
            .prepare_command::<crate::period::BeginClose>(
                &self.target,
                identity,
                PeriodIdentity(self.id),
            )
            .await?
            .execute()
            .await
    }

    /// Reconciles one complete account snapshot into the period's bounded projection.
    pub async fn reconcile(
        &self,
        identity: MutationIdentity,
        spec: PeriodSpec,
        snapshot: AccountSnapshot,
    ) -> Result<Committed<PeriodDecision>, InvocationError<PeriodDecision>> {
        self.handle
            .prepare_command::<ReconcileAccountCommand>(
                &self.target,
                identity,
                ReconcileAccount { spec, snapshot },
            )
            .await?
            .execute()
            .await
    }

    /// Seals from every immutable account snapshot after the reconciliation barrier.
    pub async fn seal(
        &self,
        identity: MutationIdentity,
    ) -> Result<Committed<LedgerReport>, InvocationError<LedgerReport>> {
        self.handle
            .prepare_command::<SealPeriod>(&self.target, identity, PeriodIdentity(self.id))
            .await?
            .execute()
            .await
    }

    /// Reads lifecycle and projection progress. Empty means the period is not initialized.
    pub async fn get(
        &self,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<PeriodView>>, InvocationError<Option<PeriodView>>> {
        self.handle
            .query::<GetPeriod>(&self.target, minimum, PeriodIdentity(self.id))
            .await
    }
}

/// Receipt-gated native close Workflow status.
#[derive(Clone, Debug, serde::Serialize)]
pub struct CloseView {
    /// Native Workflow run identity.
    pub run_id: [u8; 16],
    /// Current native Workflow lifecycle.
    pub status: String,
    /// Durable close Activity progress or final report receipt.
    pub state: crate::WorkflowState,
}

/// Typed native Workflow start, inspection, and immutable statement Blob access.
#[derive(Clone)]
pub struct CloseClient {
    handle: ApplicationHandle<UsageLedger>,
    target: CellTarget,
}

impl CloseClient {
    /// Binds the application's fixed workflow namespace.
    pub fn new(handle: ApplicationHandle<UsageLedger>) -> cellule_runtime::Result<Self> {
        let target = handle.target_for_scope(crate::RUNS, b"fixed-usage-ledger-close-runs")?;
        Ok(Self { handle, target })
    }

    /// Starts or resolves the uniquely bound close workflow for this period.
    pub async fn start(
        &self,
        identity: MutationIdentity,
        request: CloseRequest,
    ) -> Result<Committed<StartDecision>, InvocationError<StartDecision>> {
        request.validate().map_err(InvocationError::NotStarted)?;
        self.handle
            .prepare_command::<StartClose>(&self.target, identity, request)
            .await?
            .execute()
            .await
    }

    /// Reads current native run state by permanent period identity.
    pub async fn get(
        &self,
        period_id: [u8; 16],
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<CloseView>>, BoxError> {
        if period_id == [0; 16] {
            return Err("zero usage-ledger period identity".into());
        }
        let observed = self
            .handle
            .workflow::<Runs>()?
            .state(period_id.to_vec(), minimum)
            .await?;
        let result = observed
            .output
            .map(|run| {
                let state: crate::WorkflowState = wire::decode(&run.state, crate::MAX_WIRE_BYTES)?;
                if state.period_id != period_id {
                    return Err(cellule_runtime::Error::Identity(
                        "usage-ledger Workflow period identity differs",
                    ));
                }
                let status = match run.status {
                    WorkflowStatus::Running => "running",
                    WorkflowStatus::Completed => "completed",
                    WorkflowStatus::Failed => "failed",
                    WorkflowStatus::Cancelled => "cancelled",
                    WorkflowStatus::Paused => "paused",
                };
                Ok(CloseView {
                    run_id: run.run_id,
                    status: status.into(),
                    state,
                })
            })
            .transpose()?;
        Ok(Observed {
            receipt: observed.receipt,
            output: result,
        })
    }
}

/// Immutable one-object statement capability.
#[derive(Clone)]
pub struct StatementFiles {
    handle: ApplicationHandle<UsageLedger>,
}

impl StatementFiles {
    /// Binds statement access to the embedding's authorized application and tenant.
    pub fn new(handle: ApplicationHandle<UsageLedger>) -> Self {
        Self { handle }
    }

    /// Reads and verifies a complete immutable report object by its stable key.
    pub async fn read(
        &self,
        artifact: &Artifact,
        minimum: Option<Receipt>,
    ) -> Result<Observed<Option<Vec<u8>>>, BoxError> {
        artifact.validate()?;
        let namespace = self.handle.blob::<Files>()?;
        let value = namespace
            .query(
                BlobQuery::Read {
                    key: artifact.key.to_vec(),
                    offset: 0,
                    limit: artifact.bytes,
                },
                minimum,
            )
            .await?;
        let BlobQueryResult::Read(range) = value.output else {
            return Err("unexpected usage-ledger Blob response".into());
        };
        let bytes = range
            .map(|range| -> Result<Vec<u8>, BoxError> {
                if range.offset != 0
                    || range.metadata.key != artifact.key
                    || range.metadata.size != artifact.bytes as u64
                    || range.metadata.content_type.as_deref() != Some("text/csv; charset=utf-8")
                    || range.bytes.len() != artifact.bytes as usize
                    || *blake3::hash(&range.bytes).as_bytes() != artifact.digest
                    || range.metadata.etag != artifact.etag
                {
                    return Err("usage-ledger Blob manifest differs from its close receipt".into());
                }
                Ok(range.bytes)
            })
            .transpose()?;
        Ok(Observed {
            receipt: value.receipt,
            output: bytes,
        })
    }
}

/// Frozen close completion retained by a native run.
pub fn completed_report(view: &CloseView) -> Option<&CloseCompletion> {
    view.state.completion.as_ref()
}
