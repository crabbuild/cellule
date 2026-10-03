use crate::{
    AccountClient, AccountKey, Accounts, Files, PeriodClient, PeriodSpec, PeriodStatus, Periods,
    Runs, UsageLedger, adapter, engine::CloseEngine, wire,
};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, NodeConfig, local_s3_store, new_identity};
use cellule_runtime::{
    ApplicationId, BlobArtifactStore, CellTarget, Command, Error,
    peer::{PeerAuthorizer, PeerPrincipal, VerifiedPeerRequest, wire as peer_wire},
    primitives::{
        effects::{EffectRunOutcome, EffectSupervisor},
        workflow::ActivityRunOutcome,
    },
};
use cellule_store::Store;
use object_store::path::Path;
use std::{sync::Arc, time::Duration};

const APPLICATION: ApplicationId = ApplicationId::from_bytes([0xa5; 16]);
const STORAGE_BUCKET: &str = "cellule-cookbook";

/// Owned app node, authorized tenant handle, and stable loopback Activity endpoint.
pub struct Service {
    /// Native runtime owner; drain this node before process exit.
    pub node: Arc<LocalNode>,
    /// Application capability already bound to the selected tenant and private Blob store.
    pub handle: ApplicationHandle<UsageLedger>,
    /// Stable loopback endpoint frozen into close Workflow requests.
    pub endpoint: String,
}

impl Service {
    /// Starts the native runtime, opens fixed domains, and owns close/activity workers.
    pub async fn start(
        state: std::path::PathBuf,
        tenant_name: &str,
    ) -> Result<Self, crate::BoxError> {
        let tenant = tenant(tenant_name)?;
        let storage_endpoint = match std::env::var("CELLULE_COOKBOOK_ENDPOINT") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => "http://127.0.0.1:19000".into(),
            Err(source) => return Err(source.into()),
        };
        let endpoint = match std::env::var("CELLULE_USAGE_LEDGER_ADAPTER_ENDPOINT") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => "http://127.0.0.1:19031/".into(),
            Err(source) => return Err(source.into()),
        };
        let store = local_s3_store(&storage_endpoint, STORAGE_BUCKET)?;
        let parts = Store::new(Arc::new(object_store::prefix::PrefixStore::new(
            store.inner().clone(),
            "cookbook/usage-ledger/parts",
        )));
        let node = Arc::new(
            LocalNode::start(
                crate::compile()?,
                store,
                NodeConfig {
                    state_directory: state,
                    storage_prefix: Path::from("cookbook/usage-ledger/cells"),
                    application_id: APPLICATION,
                },
            )
            .await?,
        );
        let setup = async {
            let handle = node
                .application_handle::<UsageLedger>(tenant)?
                .with_blob_artifact_store(BlobArtifactStore::new(parts));
            let files = handle.target_for_scope(crate::FILES, b"fixed-usage-ledger-files")?;
            node.open_cell(&files, &Files).await?;
            let runs = handle.target_for_scope(crate::RUNS, b"fixed-usage-ledger-close-runs")?;
            node.open_cell(&runs, &Runs).await?;
            let engine = CloseEngine::new(node.clone(), handle.clone());
            adapter::spawn(&node, engine, &endpoint).await?;
            spawn_close_supervisor(&node, handle.clone())?;
            Ok::<_, crate::BoxError>((handle,))
        }
        .await;
        match setup {
            Ok((handle,)) => Ok(Self {
                node,
                handle,
                endpoint,
            }),
            Err(source) => {
                if let Err(cleanup) = node.shutdown().await {
                    tracing::error!(%cleanup,"usage-ledger startup drain failed");
                }
                Err(source)
            }
        }
    }

    /// Opens accounts, creates the immutable roster, then waits for every account-ready proof.
    pub async fn open_period(&self, spec: PeriodSpec) -> Result<PeriodClient, crate::BoxError> {
        spec.validate()?;
        let period = PeriodClient::new(self.handle.clone(), spec.id)?;
        for account in &spec.accounts {
            let client = AccountClient::new(self.handle.clone(), account.clone())?;
            self.node.open_cell(client.target(), &Accounts).await?;
            client.bind(new_identity()?, spec.clone()).await?;
        }
        self.node.open_cell(period.target(), &Periods).await?;
        let created = period.create(new_identity()?, spec.clone()).await?;
        if matches!(created.output, crate::PeriodDecision::Conflict) {
            return Err("usage-ledger period ID is bound to another roster".into());
        }
        for account in &spec.accounts {
            let client = AccountClient::new(self.handle.clone(), account.clone())?;
            client.activate(new_identity()?, spec.clone()).await?;
            period
                .confirm_ready(new_identity()?, account.clone())
                .await?;
        }
        let opened = period.get(None).await?.output;
        if opened.is_none_or(|view| {
            !matches!(
                view.status,
                PeriodStatus::Open | PeriodStatus::Closing | PeriodStatus::Sealed
            ) || view.ready_accounts as usize != spec.accounts.len()
        }) {
            return Err("usage-ledger roster did not complete its opening barrier".into());
        }
        Ok(period)
    }

    /// Starts one signed source-to-period Effect runner for a bound account.
    pub async fn spawn_effects(&self, key: &AccountKey) -> Result<(), crate::BoxError> {
        let source = AccountClient::new(self.handle.clone(), key.clone())?;
        self.node.open_cell(source.target(), &Accounts).await?;
        let progress = source
            .get(None)
            .await?
            .output
            .ok_or("usage-ledger account is absent")?;
        let target = self
            .handle
            .target_for_scope(crate::PERIODS, &progress.period_id)?;
        let destination_cell = self.node.open_cell(&target, &Periods).await?;
        let authorizer = Arc::new(AccountEffectAuthorizer {
            source: source.target().clone(),
            destination: target.clone(),
        });
        let peer = cellule_cookbook_support::LocalPeer::new(
            self.handle.compiled().registry(),
            target,
            destination_cell,
            authorizer,
        );
        let effect_source = self.handle.effects::<Accounts>(source.target().clone())?;
        let principal = PeerPrincipal {
            issuer: "usage-ledger".into(),
            subject: format!("account-projector-{}", key.as_str()),
            actions: vec![
                "cell.read".into(),
                "cell.write".into(),
                "cookbook.usage-ledger.project".into(),
            ],
        };
        self.node.spawn_worker(move |cancel| async move {
            let supervisor =
                EffectSupervisor::new(effect_source, peer.effect_client(principal), 15_000)
                    .map_err(std::io::Error::other)?;
            let mut delay = 50_u64;
            while !cancel.is_cancelled() {
                let result = supervisor.run_once().await;
                let idle = match result.map_err(std::io::Error::other)? {
                    EffectRunOutcome::Failed { .. } => {
                        return Err(std::io::Error::other(Error::Command(
                            "usage-ledger projection failed; source Effect evidence retained",
                        )));
                    }
                    EffectRunOutcome::Delivered { destination, .. } => {
                        let outcome: crate::PeriodDecision =
                            wire::decode_value(destination.result(), 4096)
                                .map_err(std::io::Error::other)?;
                        if matches!(
                            outcome,
                            crate::PeriodDecision::Conflict
                                | crate::PeriodDecision::InvalidState
                                | crate::PeriodDecision::Capacity
                        ) {
                            return Err(std::io::Error::other(Error::Command(
                                "usage-ledger projection was rejected",
                            )));
                        }
                        false
                    }
                    EffectRunOutcome::Idle { .. } => true,
                    EffectRunOutcome::Retrying { .. } | EffectRunOutcome::LeaseLost { .. } => false,
                };
                delay = if idle { (delay * 2).min(500) } else { 50 };
                tokio::select! {
                    () = cancel.cancelled() => {},
                    () = tokio::time::sleep(Duration::from_millis(delay)) => {},
                }
            }
            Ok::<_, std::io::Error>(())
        })?;
        Ok(())
    }
}

pub(crate) fn tenant(name: &str) -> Result<cellule_runtime::TenantId, crate::BoxError> {
    AccountKey::new(name.to_owned())?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"cellule-cookbook-usage-ledger/tenant/v1\0");
    hash.update(name.as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    if bytes == [0; 16] {
        return Err("zero usage-ledger tenant identity".into());
    }
    Ok(cellule_runtime::TenantId::from_bytes(bytes))
}

struct AccountEffectAuthorizer {
    source: CellTarget,
    destination: CellTarget,
}

impl PeerAuthorizer for AccountEffectAuthorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        if request.target() != &self.destination
            || !request.permits("cookbook.usage-ledger.project")
        {
            return Err(Error::PeerAuthorization(
                "usage-ledger Effect destination differs",
            ));
        }
        match request.operation() {
            Some(peer_wire::peer_request::Operation::Read(read))
                if matches!(
                    read.operation,
                    Some(peer_wire::read_request::Operation::Describe(true))
                ) =>
            {
                Ok(())
            }
            Some(peer_wire::peer_request::Operation::ResolveEffect(resolve))
                if resolve.identity.as_ref().is_some_and(|identity| {
                    identity.source_cell == self.source.cell_id().as_bytes()
                }) =>
            {
                Ok(())
            }
            Some(peer_wire::peer_request::Operation::DeliverEffect(effect)) => {
                let identity = effect
                    .identity
                    .as_ref()
                    .ok_or(Error::Peer("missing usage-ledger Effect identity"))?;
                let Some(peer_wire::effect_request::Operation::CellCommand(command)) =
                    &effect.operation
                else {
                    return Err(Error::PeerAuthorization(
                        "usage-ledger requires a typed projection command",
                    ));
                };
                if identity.source_cell != self.source.cell_id().as_bytes()
                    || self.source.namespace() != crate::ACCOUNTS
                    || self.destination.namespace() != crate::PERIODS
                    || !matches!(command.command_id, crate::period::ProjectUsage::ID)
                    || command.codec_version != 1
                {
                    return Err(Error::PeerAuthorization(
                        "usage-ledger Effect binding differs",
                    ));
                }
                Ok(())
            }
            _ => Err(Error::PeerAuthorization(
                "unsupported usage-ledger peer operation",
            )),
        }
    }
}

fn spawn_close_supervisor(
    node: &LocalNode,
    handle: ApplicationHandle<UsageLedger>,
) -> Result<(), crate::BoxError> {
    let supervisor = cellule_runtime::primitives::workflow::ActivitySupervisor::new(
        handle.activities::<Runs>()?,
        60_000,
    )?;
    node.spawn_worker(move |cancel| async move {
        let mut delay = 50_u64;
        while !cancel.is_cancelled() {
            let outcome = supervisor
                .run_once(0, None)
                .await
                .map_err(std::io::Error::other)?;
            if matches!(outcome, ActivityRunOutcome::IdentityConflict { .. }) {
                return Err(std::io::Error::other(Error::Command(
                    "usage-ledger Activity completion identity conflict",
                )));
            }
            delay = if matches!(outcome, ActivityRunOutcome::Idle { .. }) {
                (delay * 2).min(1000)
            } else {
                50
            };
            tokio::select! {
                () = cancel.cancelled() => {},
                () = tokio::time::sleep(Duration::from_millis(delay)) => {},
            }
        }
        Ok::<_, std::io::Error>(())
    })?;
    Ok(())
}
