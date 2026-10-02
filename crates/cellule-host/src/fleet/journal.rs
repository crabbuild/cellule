use std::{future::Future, pin::Pin};

use cellule_runtime::fleet::operations::{
    AcceptedFleetAction, AcquisitionBasis, AttemptId, FleetAction, FleetActionOutcome,
    FleetInspectionRequest, FleetScope, MovementAction, RecoveryBasis, RecoveryEvidence,
};
use cellule_runtime::identity::{NodeId, SessionId};

/// Provider-neutral fleet adapter call preserving the original source error.
pub type FleetAdapterFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send + 'a>>;

/// Atomic first acceptance or a previously committed exact action.
pub enum FleetActionAcceptance {
    /// The backend atomically validated the head and committed this acceptance.
    /// Only this result permits starting its effect for the first time.
    New(AcceptedFleetAction),
    /// The exact action was already accepted. A missing result is unresolved;
    /// it is never permission to repeat an unobserved effect.
    Existing {
        /// Original immutable acceptance, retained across controller changes.
        accepted: AcceptedFleetAction,
        /// Latest committed checked result, if available.
        result: Option<Box<FleetActionOutcome>>,
    },
}

/// Strongly consistent application journal for local fleet actions.
///
/// First acceptance must linearize its fresh head, controller epoch, permit,
/// intent revision, endpoint and deadline checks with record publication.
/// Use `AcceptedFleetAction::new` inside that transaction. An unconditional
/// write following a separate head read is insufficient. Compare complete
/// execution inputs with `validate_replay`, not only the stable action key.
/// Applications authenticate the caller before invoking the node.
pub trait FleetActionJournal: Send + Sync + 'static {
    /// Checks a native page request against its full current head/registry and
    /// endpoint intent in one transaction using `request.authorize_against`.
    /// Applications authenticate the transport. This read can publish no effect.
    fn authorize_snapshot<'a>(
        &'a self,
        request: &'a super::FleetSnapshotRequest,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, ()>;

    /// Checks a read-only inspection against the current head, registry version
    /// and endpoint intent in one transaction. Use `request.authorize_against`; never satisfy this
    /// with a cached acceptance or effect result. No effect or result is published.
    fn authorize_inspection<'a>(
        &'a self,
        request: &'a FleetInspectionRequest,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, ()>;

    /// Atomically accepts new work or returns its original durable acceptance.
    /// Ambiguous writes are reconciled by the same identity on a later call.
    fn accept_action<'a>(
        &'a self,
        action: &'a FleetAction,
        node: NodeId,
        session: SessionId,
        now_ms: i64,
    ) -> FleetAdapterFuture<'a, FleetActionAcceptance>;

    /// Publishes a checked result bound to the original acceptance.
    ///
    /// Identical terminal results are idempotent; incompatible terminal results
    /// conflict. Unknown observations may be replaced by checked terminal
    /// evidence. A successful return means the result is durably reachable,
    /// including after backend/client reconstruction. Retiring a fleet permit
    /// remains a separate controller transition requiring all cleanup evidence.
    fn publish_action_result<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
        result: &'a FleetActionOutcome,
    ) -> FleetAdapterFuture<'a, ()>;

    /// Looks up original accepted work by immutable attempt, effect and endpoint.
    /// Return Existing records only, with the original result or unresolved
    /// marker. Inspection does not authorize first acceptance.
    fn load_movement_action<'a>(
        &'a self,
        scope: FleetScope,
        attempt: AttemptId,
        effect: MovementAction,
        node: NodeId,
        session: SessionId,
    ) -> FleetAdapterFuture<'a, Option<FleetActionAcceptance>>;

    /// Durably records the exact checked input before receiver acquisition.
    ///
    /// Bind it to the original acceptance atomically. Identical accepted/control
    /// inputs return the original record and capture time. Changed controls
    /// conflict; do not erase the basis after later owner publication. A lost
    /// response must prevent takeover until the same basis is confirmed.
    fn record_acquisition_basis<'a>(
        &'a self,
        basis: &'a AcquisitionBasis,
    ) -> FleetAdapterFuture<'a, AcquisitionBasis>;

    /// Loads the retained basis without inferring it from a successor's root.
    fn load_acquisition_basis<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<AcquisitionBasis>>;

    /// Atomically binds immutable recovery input to the original acceptance.
    /// Identical inputs return their original capture time; changed controls
    /// conflict. Unknown write replies prevent ownership CAS until confirmed.
    fn record_recovery_basis<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
        basis: &'a RecoveryBasis,
    ) -> FleetAdapterFuture<'a, RecoveryBasis>;

    /// Loads original recovery input without reconstructing it from a successor.
    fn load_recovery_basis<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<RecoveryBasis>>;

    /// Confirms the canonical materialized recovery position before admission.
    /// Bind to the exact original basis; retain it through later publication.
    /// Identical writes return the original record/time, incompatible ones fail.
    fn record_recovery_evidence<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
        evidence: &'a RecoveryEvidence,
    ) -> FleetAdapterFuture<'a, RecoveryEvidence>;

    /// Loads the confirmed pre-admission recovery position, or explicit absence.
    fn load_recovery_evidence<'a>(
        &'a self,
        accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<RecoveryEvidence>>;
}
