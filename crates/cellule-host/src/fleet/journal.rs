use std::{future::Future, pin::Pin};

use cellule_runtime::fleet::operations::{
    AcceptedFleetAction, AcquisitionBasis, AttemptId, FleetAction, FleetActionOutcome,
    FleetInspectionRequest, FleetScope, MoveAttempt, MovementAction, ReceiverRecoveryBasis,
    ReceiverRecoveryEvidence, RecoveryBasis, RecoveryEvidence,
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
/// Use `AcceptedFleetAction::new_with_registry` inside that transaction. This
/// also binds receiver continuations to the current registry revision. An
/// unconditional write following a separate head read is insufficient.
/// Compare complete execution inputs with `validate_replay`, not only the
/// stable action key.
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

    /// Atomically first-accepts a routed action after checking its final hop
    /// against this fresh typed closed-boot proof and the same current snapshot.
    /// The default refuses; adapters must implement the proof comparison before
    /// any remote effect can start. Replays of an already accepted action use
    /// `accept_action` and retain the original acceptance.
    fn accept_closed_receiver_action<'a>(
        &'a self,
        _action: &'a FleetAction,
        _node: NodeId,
        _session: SessionId,
        _now_ms: i64,
        _closure: &'a super::FleetFailedBootClosure,
    ) -> FleetAdapterFuture<'a, FleetActionAcceptance> {
        Box::pin(async {
            Err(Box::new(std::io::Error::other(
                "journal does not support closed receiver continuations",
            )) as Box<dyn std::error::Error + Send + Sync>)
        })
    }

    /// Publishes a checked result bound to the original acceptance.
    ///
    /// Identical terminal results are idempotent; incompatible terminal results
    /// conflict. Unknown observations may be replaced by checked terminal
    /// evidence. A successful return means the result is durably reachable,
    /// including after backend/client reconstruction. Retiring a fleet permit
    /// remains a separate controller transition requiring all cleanup evidence.
    /// `RolesSettledAt` must compare its head revision and registry version with
    /// the exact current snapshot in this same publication transaction.
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

    /// Loads every accepted receiver action for this immutable attempt/effect.
    /// Implementations return a complete bounded set so a new controller can
    /// find the latest durable route after an ambiguous response. The default
    /// preserves older adapters by checking only the originally preferred
    /// endpoint; production continuation adapters must override it.
    fn load_movement_actions<'a>(
        &'a self,
        scope: FleetScope,
        attempt: &'a MoveAttempt,
        effect: MovementAction,
    ) -> FleetAdapterFuture<'a, Vec<FleetActionAcceptance>> {
        Box::pin(async move {
            let spec = attempt.spec();
            let (node, session) = if effect.is_source_release() {
                (spec.source_node, spec.source)
            } else {
                (spec.destination_node, spec.destination)
            };
            Ok(self
                .load_movement_action(scope, spec.id, effect, node, session)
                .await?
                .into_iter()
                .collect())
        })
    }

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

    /// Confirms immutable failed-receiver input before the canonical takeover.
    /// Bind atomically to the original routed activation; changed controls
    /// conflict and identical writes return the original observation time.
    fn record_receiver_recovery_basis<'a>(
        &'a self,
        _basis: &'a ReceiverRecoveryBasis,
    ) -> FleetAdapterFuture<'a, ReceiverRecoveryBasis> {
        Box::pin(async {
            Err(std::io::Error::other("receiver recovery journal is not configured").into())
        })
    }
    /// Returns retained input without inferring it from successor state.
    fn load_receiver_recovery_basis<'a>(
        &'a self,
        _accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<ReceiverRecoveryBasis>> {
        Box::pin(async { Ok(None) })
    }
    /// Confirms the canonical recovered position before actor admission.
    /// Require the exact retained basis; incompatible/ambiguous writes cannot
    /// authorize activation. Preserve this record through later publication.
    fn record_receiver_recovery_evidence<'a>(
        &'a self,
        _evidence: &'a ReceiverRecoveryEvidence,
    ) -> FleetAdapterFuture<'a, ReceiverRecoveryEvidence> {
        Box::pin(async {
            Err(std::io::Error::other("receiver recovery journal is not configured").into())
        })
    }
    /// Returns immutable pre-admission evidence for the accepted route.
    fn load_receiver_recovery_evidence<'a>(
        &'a self,
        _accepted: &'a AcceptedFleetAction,
    ) -> FleetAdapterFuture<'a, Option<ReceiverRecoveryEvidence>> {
        Box::pin(async { Ok(None) })
    }
}
