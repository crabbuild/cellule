//! Durability gate: commit tickets and fleet or object proofs.
use std::sync::Arc;

use futures_util::future::BoxFuture;
use tokio::sync::{Mutex, OnceCell};

use crate::identity::NodeId;
use crate::identity::SessionId;
use crate::node::lease::NodeLeaseGuard;
use crate::node::log::{
    CommitTicket, DurabilityGate, DurabilityProof, DurabilitySource, NodeLogRetirementObservation,
    NodeLogRetirementProof,
};
use crate::node::log_shipper::{NodeLogShipper, NodeLogSubmission};
use crate::node::log_transport::NodeLogTransport;
use crate::{Error, Result};

mod object_coverage;
use object_coverage::ObjectCoverage;
mod publication;
pub use publication::{BundleCheckpoint, NodeBundlePublicationAuthority};

/// Original node authority used to enroll and close the Cells of an installed
/// shared publication feed. Implementations serialize these mutations and shared
/// selection with their original heartbeat/enrollment state. The host owns and
/// joins the feed; the runtime still verifies Cell departure against origin.
pub trait NodeBundleAuthority: Send + Sync {
    /// Pins the Serving Cell before its SQL admission opens.
    fn bind<'a>(
        &'a self,
        authority: &'a crate::control::authority::CellAuthority,
        observed: &'a crate::control::authority::VersionedControl,
    ) -> BoxFuture<'a, Result<crate::control::authority::VersionedControl>>;

    /// Joins complete issued coverage, materialization/checkpoint and catalog
    /// closure after SQL/capture tasks close, including earlier Fleet ACKs.
    fn close<'a>(
        &'a self,
        authority: &'a crate::control::authority::CellAuthority,
        observed: &'a crate::control::authority::VersionedControl,
        issued: crate::node::log::CellIssuedRange,
    ) -> BoxFuture<'a, Result<()>>;
}

/// Authoritative node-session mutations required by follower durability.
///
/// Implementations must serialize these mutations with heartbeat refreshes and
/// reconcile an ambiguous CAS only when the exact session and log epoch match.
pub trait NodeLogAuthority: Send + Sync {
    /// Requires complete native member fences for every closure when fleet
    /// enrollment retirement depends on them. Ordinary authorities retain
    /// their best-effort rotation contract.
    fn requires_confirmed_retirement(&self) -> bool {
        false
    }
    /// Observes joined member responses before confirmation and closure.
    /// This callback may retain diagnostics; it grants no retirement authority.
    fn observe_retirement(&self, _observation: Arc<NodeLogRetirementObservation>) -> Result<()> {
        Ok(())
    }
    /// Retains an original shutdown failure while a managed drain keeps retrying.
    /// The error includes failures before member observation, such as pending
    /// object coverage. This callback grants no closure authority.
    fn observe_shutdown_failure(&self, _error: Arc<Error>) -> Result<()> {
        Ok(())
    }
    /// Activates one log epoch for this session.
    fn activate<'a>(&'a self, log_epoch: u64) -> BoxFuture<'a, Result<()>>;

    /// Advances the tiered coverage watermark for one log epoch.
    fn advance_coverage<'a>(
        &'a self,
        log_epoch: u64,
        tiered_through: u64,
    ) -> BoxFuture<'a, Result<()>>;

    /// Closes one log epoch at its rotation barrier.
    fn close<'a>(
        &'a self,
        retirement: &'a NodeLogRetirementObservation,
    ) -> BoxFuture<'a, Result<()>>;
}

/// Provider-neutral inputs for constructing one node-log durability epoch.
///
/// Providers own enrollment and the authority/transport implementations. The
/// host owns turning these inputs into the runtime durability object so the
/// product boundary cannot accidentally create a second shipping path.
pub struct NodeDurabilityConfig {
    session: SessionId,
    node: NodeId,
    log_epoch: u64,
    members: Vec<NodeId>,
    transport: Arc<dyn NodeLogTransport>,
    authority: Arc<dyn NodeLogAuthority>,
    node_lease: NodeLeaseGuard,
    limits: cellule_ltx::Limits,
    telemetry: crate::fleet::telemetry::CellTelemetryHandle,
}

impl NodeDurabilityConfig {
    /// Binds provider enrollment to one exact node session and log epoch.
    #[expect(
        clippy::too_many_arguments,
        reason = "the provider-neutral boundary keeps every enrollment contract explicit"
    )]
    pub fn new(
        session: SessionId,
        node: NodeId,
        log_epoch: u64,
        members: Vec<NodeId>,
        transport: Arc<dyn NodeLogTransport>,
        authority: Arc<dyn NodeLogAuthority>,
        node_lease: NodeLeaseGuard,
        limits: cellule_ltx::Limits,
        telemetry: crate::fleet::telemetry::CellTelemetryHandle,
    ) -> Result<Self> {
        if session.as_bytes().iter().all(|byte| *byte == 0)
            || node.as_bytes().iter().all(|byte| *byte == 0)
            || log_epoch == 0
            || members.is_empty()
        {
            return Err(Error::Node("invalid node-log durability configuration"));
        }
        Ok(Self {
            session,
            node,
            log_epoch,
            members,
            transport,
            authority,
            node_lease,
            limits,
            telemetry,
        })
    }

    /// Returns the configured boot, physical node and epoch before construction.
    /// The provider still owns authenticated enrollment and authority validation.
    #[must_use]
    pub const fn identity(&self) -> (SessionId, NodeId, u64) {
        (self.session, self.node, self.log_epoch)
    }

    /// Constructs the runtime-owned durability object for this epoch.
    pub fn build(self) -> Result<Arc<NodeDurability>> {
        let gate = DurabilityGate::new(self.session, self.node, self.log_epoch, self.members)?;
        let shipper = NodeLogShipper::new_with_telemetry(
            gate.clone(),
            Arc::clone(&self.transport),
            self.limits,
            self.telemetry,
        )?;
        Ok(Arc::new(NodeDurability::new(
            gate,
            shipper,
            self.authority,
            self.transport,
            self.node_lease,
        )))
    }

    /// Checks construction bounds without spawning a shipper or enrolling any
    /// follower. Managed producers call this before accepting journal obligations.
    pub fn validate(&self) -> Result<()> {
        DurabilityGate::new(
            self.session,
            self.node,
            self.log_epoch,
            self.members.iter().copied(),
        )?;
        NodeLogShipper::validate_limits(self.limits)?;
        Ok(())
    }
}

/// One enrolled node-log epoch and its non-forgeable durability proof boundary.
///
/// The runtime can submit captured cuts immediately after enrollment, but no
/// fleet proof becomes visible until the first ticket is fsynced by every
/// member and the authoritative inactive-to-active CAS succeeds.
pub struct NodeDurability {
    gate: DurabilityGate,
    shipper: NodeLogShipper,
    authority: Arc<dyn NodeLogAuthority>,
    transport: Arc<dyn NodeLogTransport>,
    node_lease: NodeLeaseGuard,
    activated: OnceCell<()>,
    object_coverage: ObjectCoverage,
    shutdown: Mutex<()>,
    retirement: std::sync::Mutex<Option<Arc<NodeLogRetirementObservation>>>,
    retirement_proof: OnceCell<Arc<NodeLogRetirementProof>>,
    closed: std::sync::atomic::AtomicBool,
    selection_resources: std::sync::OnceLock<crate::fleet::resource::ResourceLedger>,
    bundle_authority: std::sync::OnceLock<Arc<dyn NodeBundleAuthority>>,
    publisher: std::sync::OnceLock<publication::Publisher>,
}

impl NodeDurability {
    pub(crate) fn check_lease(&self) -> Result<()> {
        self.node_lease.check()
    }

    pub(crate) async fn wait_fenced(&self) {
        self.node_lease.wait_fenced().await;
    }

    /// Observes the epoch's durability frontiers; this does not issue a proof.
    pub fn progress(&self) -> Result<crate::node::log::NodeLogProgress> {
        self.gate.progress()
    }

    /// Installs this epoch's sole ordered publication consumer before issuance.
    /// The original host must join selection/fallback for every accepted capture
    /// before retiring this epoch. Receiving captures grants no durability proof.
    pub fn take_publication_feed(&self) -> Result<crate::node::log_shipper::NodePublicationFeed> {
        self.node_lease.check()?;
        self.shipper.take_publication_feed()
    }

    /// Creates one node-log durability epoch over its gate, shipper, authority,
    /// transport, and node lease.
    #[must_use]
    pub fn new(
        gate: DurabilityGate,
        shipper: NodeLogShipper,
        authority: Arc<dyn NodeLogAuthority>,
        transport: Arc<dyn NodeLogTransport>,
        node_lease: NodeLeaseGuard,
    ) -> Self {
        Self {
            gate,
            shipper,
            authority,
            transport,
            node_lease,
            activated: OnceCell::new(),
            object_coverage: ObjectCoverage::default(),
            shutdown: Mutex::new(()),
            retirement: std::sync::Mutex::new(None),
            retirement_proof: OnceCell::new(),
            closed: std::sync::atomic::AtomicBool::new(false),
            selection_resources: std::sync::OnceLock::new(),
            bundle_authority: std::sync::OnceLock::new(),
            publisher: std::sync::OnceLock::new(),
        }
    }

    /// Returns whether shipping stopped or the epoch reached its rotation threshold.
    #[must_use]
    pub fn needs_rotation(&self, max_issued_frames: u64) -> bool {
        self.gate.shipping_scope().is_err() || self.gate.issued_through() >= max_issued_frames
    }

    /// Assigns and asynchronously ships one captured commit to every member.
    pub async fn submit(&self, submission: NodeLogSubmission) -> Result<CommitTicket> {
        self.submit_assigned(submission)
            .await
            .map(|(ticket, _)| ticket)
    }

    /// Uses the same bounded native lane and retains the complete assigned range
    /// witness required by shared bundle selection.
    pub async fn submit_assigned(
        &self,
        submission: NodeLogSubmission,
    ) -> Result<(CommitTicket, crate::node::log::AssignedCommitRange)> {
        self.submit_capture(submission)
            .await
            .map(|capture| (capture.assignment.ticket(), capture.assignment))
    }

    pub(crate) async fn submit_capture(
        &self,
        submission: NodeLogSubmission,
    ) -> Result<crate::node::log_shipper::SubmittedCapture> {
        self.node_lease.check()?;
        let ticket = tokio::select! {
            result = self.shipper.submit_capture(submission) => result?,
            () = self.node_lease.wait_fenced() => return Err(Error::Fenced),
        };
        self.node_lease.check()?;
        Ok(ticket)
    }

    pub(crate) fn attach_selection_resources(
        &self,
        resources: crate::fleet::resource::ResourceLedger,
    ) -> Result<()> {
        let original = self.selection_resources.get_or_init(|| resources.clone());
        if !original.same_ledger(&resources) {
            return Err(Error::Node("bundle selection resource ledger changed"));
        }
        Ok(())
    }

    /// Installs this epoch's shared feed and original binding/closure authority
    /// before native issuance or actor activation. The host must keep accepting
    /// and joining complete captures until runtime drain finishes.
    pub fn enable_bundle_publication(
        &self,
        authority: Arc<dyn NodeBundleAuthority>,
    ) -> Result<crate::node::log_shipper::NodePublicationFeed> {
        self.node_lease.check()?;
        let feed = self.shipper.take_publication_feed()?;
        self.bundle_authority
            .set(authority)
            .map_err(|_| Error::Node("bundle authority already installed"))?;
        Ok(feed)
    }

    /// Starts and retains the sole bounded node bundle producer. Install this
    /// after the runtime resource ledger and before any Cell/native issuance.
    /// Selection and checkpoints use the same original binding authority.
    pub fn start_bundle_publication(
        self: &Arc<Self>,
        authority: Arc<dyn NodeBundlePublicationAuthority>,
    ) -> Result<()> {
        if self.selection_resources.get().is_none() {
            return Err(Error::Node(
                "bundle publication has no installed runtime resource ledger",
            ));
        }
        // Fallible construction precedes installation of the irreversible
        // original feed. Rejected startup leaves the ordinary lane untouched.
        let runtime = tokio::runtime::Handle::try_current().map_err(Error::RuntimeStart)?;
        let working = publication::Publisher::reserve_working(self)?;
        let original: Arc<dyn NodeBundleAuthority> = authority.clone();
        let feed = self.enable_bundle_publication(original)?;
        let publisher = publication::Publisher::start(self, authority, feed, working, runtime);
        self.publisher
            .set(publisher)
            .map_err(|_| Error::Node("bundle producer already installed"))
    }

    pub(crate) fn capture_prefix(
        &self,
        selected: Arc<crate::node::log_shipper::SelectedBundle>,
        assignment: &crate::node::log::AssignedCommitRange,
    ) -> Result<Arc<crate::node::log_shipper::SelectedBundle>> {
        selected.proof.check_live_assignment(assignment)?;
        let (_, commit, position) = assignment.endpoint();
        if selected.proof.commit_sequence() == commit && selected.proof.position() == position {
            return Ok(selected);
        }
        let resources = self
            .selection_resources
            .get()
            .ok_or(Error::PendingPublication)?;
        // Reserve before cloning locator metadata; the original cohort remains
        // separately charged until its other original consumers release it.
        let memory = resources.try_reserve(
            crate::fleet::resource::ResourceCost::zero()
                .with_retained_bytes(selected.proof.retained_metadata_bytes()?),
        )?;
        let proof = selected.proof.original_capture_prefix(assignment)?;
        Ok(Arc::new(crate::node::log_shipper::SelectedBundle {
            proof,
            _memory: memory,
        }))
    }

    pub(crate) async fn checkpoint_materialized(
        &self,
        authority: crate::control::authority::CellAuthority,
        root: cellule_ltx::RootRef,
        selected: Arc<crate::node::log_shipper::SelectedBundle>,
    ) -> Result<()> {
        if let Some(publisher) = self.publisher.get() {
            publisher
                .checkpoint(BundleCheckpoint {
                    authority,
                    root,
                    selected,
                })
                .await?;
        }
        Ok(())
    }

    pub(crate) fn managed_bundle_publication(&self) -> bool {
        self.publisher.get().is_some()
    }

    pub(crate) async fn bind_bundle_cell(
        &self,
        authority: &crate::control::authority::CellAuthority,
        observed: &crate::control::authority::VersionedControl,
    ) -> Result<crate::control::authority::VersionedControl> {
        let Some(bundle) = self.bundle_authority.get() else {
            return Ok(observed.clone());
        };
        self.node_lease.check()?;
        let bound = bundle.bind(authority, observed).await?;
        self.node_lease.check()?;
        observed
            .value()
            .validate_transition(bound.value(), crate::control::Transition::BindBundle)?;
        let pin = bound
            .value()
            .bundle_binding
            .ok_or(Error::Node("bundle enrollment lacks original Cell pin"))?;
        let (session, _, epoch) = self.identity()?;
        if pin.session != session || pin.epoch != epoch {
            return Err(Error::Fenced);
        }
        Ok(bound)
    }

    pub(crate) async fn close_bundle_cell(
        &self,
        authority: &crate::control::authority::CellAuthority,
        observed: &crate::control::authority::VersionedControl,
    ) -> Result<()> {
        if observed.value().bundle_binding.is_none() {
            return Ok(());
        }
        let bundle = self
            .bundle_authority
            .get()
            .ok_or(Error::Node("bound Cell lost original bundle authority"))?;
        self.node_lease.check()?;
        let scope = crate::node::log::CellLogScope {
            application: crate::identity::ApplicationId::from_bytes(
                *authority.layout().application_id(),
            ),
            cell: observed.value().cell,
            incarnation: observed.value().incarnation,
            cell_epoch: observed.value().epoch,
        };
        let issued = self.close_cell_issuance(
            scope,
            observed
                .value()
                .ltx_root()
                .ok_or(Error::PendingPublication)?,
        )?;
        // Never hold the provider's heartbeat/CAS mutex while waiting for the
        // ordered producer: selecting the complete prior Fleet suffix needs it.
        if let Some(publisher) = self.publisher.get() {
            publisher.wait_through(issued.last_node_sequence()).await?;
        }
        bundle.close(authority, observed, issued).await?;
        self.node_lease.check()
    }

    /// Freezes exact Cell issuance after the original SQL and capture tasks join.
    pub fn close_cell_issuance(
        &self,
        scope: crate::node::log::CellLogScope,
        base: cellule_ltx::RootRef,
    ) -> Result<crate::node::log::CellIssuedRange> {
        self.node_lease.check()?;
        self.gate.close_cell_issuance(scope, base)
    }

    /// Returns fleet proof only after follower fsync and authoritative activation.
    pub async fn prove_fleet(&self, ticket: CommitTicket) -> Result<DurabilityProof> {
        self.node_lease.check()?;
        tokio::select! {
            result = self.gate.wait_followers(ticket) => result?,
            () = self.node_lease.wait_fenced() => return Err(Error::Fenced),
        }
        self.activated
            .get_or_try_init(|| async {
                self.node_lease.check()?;
                tokio::select! {
                    result = self.authority.activate(ticket.log_epoch()) => result?,
                    () = self.node_lease.wait_fenced() => return Err(Error::Fenced),
                }
                self.node_lease.check()?;
                self.gate.activate_fleet()?;
                Ok::<(), Error>(())
            })
            .await?;
        let proof = tokio::select! {
            result = self.gate.prove(ticket) => result?,
            () = self.node_lease.wait_fenced() => return Err(Error::Fenced),
        };
        self.node_lease.check()?;
        if proof.source() != DurabilitySource::Fleet {
            return Err(Error::Node(
                "fleet proof was superseded by object durability",
            ));
        }
        Ok(proof)
    }

    /// Returns the first valid follower or object proof for one ticket.
    pub async fn prove(&self, ticket: CommitTicket) -> Result<DurabilityProof> {
        self.node_lease.check()?;
        let activate = async {
            self.gate.wait_followers(ticket).await?;
            self.activated
                .get_or_try_init(|| async {
                    self.node_lease.check()?;
                    tokio::select! {
                        result = self.authority.activate(ticket.log_epoch()) => result?,
                        () = self.node_lease.wait_fenced() => return Err(Error::Fenced),
                    }
                    self.node_lease.check()?;
                    self.gate.activate_fleet()?;
                    Ok::<(), Error>(())
                })
                .await?;
            Ok::<(), Error>(())
        };
        tokio::pin!(activate);
        let proof = tokio::select! {
            proof = self.gate.prove(ticket) => proof?,
            activated = &mut activate => {
                activated?;
                self.gate.prove(ticket).await?
            }
            () = self.node_lease.wait_fenced() => return Err(Error::Fenced),
        };
        self.node_lease.check()?;
        Ok(proof)
    }

    /// Records an already-published object root and persists its contiguous watermark.
    ///
    /// Callers must complete the exact Cell root CAS before invoking this method.
    /// Concurrent completions share coverage updates. A new contiguous frontier
    /// becomes visible only after its authority CAS succeeds under the original
    /// node lease. Sparse roots grant their own exact object proofs without a
    /// node mutation; they cannot advance reclamation or close an unpublished gap.
    pub async fn prove_object(&self, ticket: CommitTicket) -> Result<DurabilityProof> {
        self.confirm_objects(&[ticket]).await?;
        let proof = self.gate.confirmed_object_proof(ticket)?;
        self.node_lease.check()?;
        if proof.source() != DurabilitySource::Object {
            return Err(Error::Node("object proof lost its durability race"));
        }
        Ok(proof)
    }

    pub(crate) async fn confirm_objects(&self, tickets: &[CommitTicket]) -> Result<()> {
        self.node_lease.check()?;
        if self.object_coverage.stage(&self.gate, tickets)? {
            self.object_coverage
                .flush(
                    &self.gate,
                    self.authority.as_ref(),
                    &self.node_lease,
                    tickets,
                )
                .await?;
        }
        self.node_lease.check()?;
        if !self.gate.objects_are_covered(tickets)? {
            return Err(Error::Node("object coverage batch remains unconfirmed"));
        }
        Ok(())
    }

    /// Confirms exact original captures after verified shared bundle selection.
    ///
    /// This performs no storage I/O or authority CAS. Selection already persisted
    /// the bundle and native-log frontier together. Cold reconstruction proofs,
    /// foreign gates and replacement lease guards cannot authorize local ACKs.
    /// The caller must still join actor/read/retry visibility before responding.
    pub fn confirm_bundle(
        &self,
        proofs: &[crate::node::bundle::BundleCoverageProof],
    ) -> Result<u64> {
        crate::node::bundle::confirm_selected_coverage(&self.gate, &self.node_lease, proofs)
    }

    /// Confirms a complete admitted feed cohort and returns its exact selected
    /// metadata to the original commands. The installed runtime's ledger pays
    /// for each Cell proof once until all command/materialization consumers join.
    /// Missing, duplicate, cold or foreign assignments cannot wake siblings.
    /// Captures still require their separate ordinary root cleanup and drain.
    pub fn confirm_selected_captures(
        &self,
        captures: &[crate::node::log_shipper::AssignedCapture],
        proofs: Vec<crate::node::bundle::BundleCoverageProof>,
    ) -> Result<crate::node::log_shipper::SelectedBundlePublication> {
        self.node_lease.check()?;
        if captures.is_empty() || captures.len() > 64 || proofs.len() > 64 {
            return Err(Error::Capacity("selected capture cohort"));
        }
        let assignments = proofs
            .iter()
            .map(|proof| proof.assignment_count())
            .sum::<usize>();
        if assignments != captures.len() || proofs.iter().any(|proof| proof.assignment_count() == 0)
        {
            return Err(Error::Node("selection omits original captured assignments"));
        }
        for (index, capture) in captures.iter().enumerate() {
            let assignment = capture.assignment();
            if captures[..index]
                .iter()
                .any(|before| before.assignment() == assignment)
                || proofs
                    .iter()
                    .filter(|proof| proof.contains_assignment(&assignment))
                    .count()
                    != 1
            {
                return Err(Error::Node(
                    "selection differs from original captured cohort",
                ));
            }
        }
        let resources = self.selection_resources.get().ok_or(Error::Node(
            "bundle publication has no installed runtime resource ledger",
        ))?;
        let memories = proofs
            .iter()
            .map(|proof| {
                resources.try_reserve(
                    crate::fleet::resource::ResourceCost::zero()
                        .with_retained_bytes(proof.retained_metadata_bytes()?),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        // Validate all original gates and leases before sending any receipt.
        // Receipt waiters also require the gate's confirmed Bundle source.
        let through =
            crate::node::bundle::confirm_selected_coverage(&self.gate, &self.node_lease, &proofs)?;
        let selected = proofs
            .into_iter()
            .zip(memories)
            .map(|(proof, memory)| {
                Arc::new(crate::node::log_shipper::SelectedBundle {
                    proof,
                    _memory: memory,
                })
            })
            .collect::<Vec<_>>();
        for capture in captures {
            let proof = selected
                .iter()
                .find(|selected| selected.proof.contains_assignment(&capture.assignment()))
                .ok_or(Error::Node("selected capture lost original assignment"))?;
            capture.confirm_selection(Arc::clone(proof));
        }
        Ok(crate::node::log_shipper::SelectedBundlePublication { through, selected })
    }

    /// Returns this binding's exact enrolled log epoch.
    pub fn log_epoch(&self) -> Result<u64> {
        self.gate.log_epoch()
    }

    /// Returns immutable configured scope, including after retirement. This
    /// metadata does not establish current authority or fleet readiness.
    pub fn identity(&self) -> Result<(SessionId, NodeId, u64)> {
        self.gate.identity()
    }

    /// Returns the latest joined member results, preserving individual errors.
    /// This observation does not assert complete retirement or current authority.
    pub fn retirement_observation(&self) -> Result<Option<Arc<NodeLogRetirementObservation>>> {
        Ok(self
            .retirement
            .lock()
            .map_err(|_| Error::Node("node-log retirement lock poisoned"))?
            .clone())
    }

    /// Drains accepted frames and closes this epoch. Ordinary authorities use
    /// best-effort member retirement; managed fleet authorities retain strict
    /// retries through caller deadlines. Inspect evidence before finalization.
    pub async fn shutdown(&self) -> Result<()> {
        if !self.requires_confirmed_retirement() {
            return self.shutdown_epoch(false).await.map(|_| ());
        }
        // Runtime drain is a retained task. Returning a transient member error
        // here would cache a terminal shutdown failure and strand this epoch.
        // Keep its canonical barrier and observations through caller deadlines.
        loop {
            match self.shutdown_epoch(true).await {
                Ok(_) => return Ok(()),
                Err(error) => self.authority.observe_shutdown_failure(Arc::new(error))?,
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Returns the authority's closure policy for retained supervisor retries.
    #[must_use]
    pub fn requires_confirmed_retirement(&self) -> bool {
        self.authority.requires_confirmed_retirement()
    }

    /// Closes this epoch only after every member confirms its exact append fence.
    /// Object coverage still precedes retirement. A failed member blocks the
    /// authority close and remains retryable; healthy siblings are joined first.
    /// Ordinary closure without complete receipts cannot later manufacture proof.
    pub async fn shutdown_for_maintenance(&self) -> Result<Arc<NodeLogRetirementProof>> {
        self.shutdown_epoch(true).await?.ok_or(Error::Node(
            "node-log member retirement remains unconfirmed",
        ))
    }

    async fn shutdown_epoch(
        &self,
        require_confirmation: bool,
    ) -> Result<Option<Arc<NodeLogRetirementProof>>> {
        let _shutdown = self.shutdown.lock().await;
        if self.closed.load(std::sync::atomic::Ordering::Acquire) {
            let proof = self.retirement_proof.get().cloned();
            if require_confirmation && proof.is_none() {
                return Err(Error::Node(
                    "node-log member retirement remains unconfirmed",
                ));
            }
            return Ok(proof);
        }
        let shipping = self.shipper.shutdown().await;
        if let Some(publisher) = self.publisher.get() {
            // Join both tasks even when fencing stopped the shipper. Preserve
            // the producer's original cause rather than its secondary fence.
            publisher.join().await?;
        }
        shipping?;
        self.object_coverage
            .flush(&self.gate, self.authority.as_ref(), &self.node_lease, &[])
            .await?;
        let barrier = self.gate.begin_rotation()?;
        // A complete member fence precedes authority closure. Retain it before
        // awaiting that CAS: an accepted close with a lost reply makes further
        // retire RPCs unauthorized, although the original fences remain valid.
        let retained = self.retirement_observation()?;
        let observation = match retained {
            Some(observation) if observation.confirmed().is_ok() => {
                if observation.barrier() != &barrier {
                    return Err(Error::Node("retained node-log retirement barrier differs"));
                }
                observation
            }
            _ => {
                let observation = Arc::new(
                    crate::node::log::retire_node_log(Arc::clone(&self.transport), &barrier)
                        .await?,
                );
                *self
                    .retirement
                    .lock()
                    .map_err(|_| Error::Node("node-log retirement lock poisoned"))? =
                    Some(Arc::clone(&observation));
                observation
            }
        };
        self.authority
            .observe_retirement(Arc::clone(&observation))?;
        let confirmation = observation.confirmed();
        let proof = if require_confirmation {
            Some(Arc::new(confirmation?))
        } else {
            confirmation.ok().map(Arc::new)
        };
        self.authority.close(&observation).await?;
        if let Some(proof) = &proof {
            self.retirement_proof
                .set(Arc::clone(proof))
                .map_err(|_| Error::Node("node-log retirement proof already installed"))?;
        }
        self.closed
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(proof)
    }
}

#[cfg(test)]
mod tests;
