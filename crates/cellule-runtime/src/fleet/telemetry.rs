//! Bounded operational telemetry emitted by the runtime.
use std::{sync::Arc, time::Duration};

use crate::CellId;
use crate::fleet::pressure::PressureState;
use crate::node::log::DurabilitySource;

/// Evidence used for one returned durable command or effect outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandResponseSource {
    /// An already durable outcome was replayed without another commit.
    Recorded,
    /// Follower durability proved the new commit.
    Fleet,
    /// Object publication proved the new commit.
    Object,
}

/// One completed object publication, which may finish after a follower-proof
/// response has already been released for the same commit sequence.
#[derive(Clone, Copy, Debug)]
pub struct PublicationTiming {
    /// Time queued for this Cell's publisher and shared preparation admission.
    pub queue_wait: Duration,
    /// Time spent preparing the immutable root, including bounded retries.
    pub preparation: Duration,
    /// Time spent publishing the prepared root through authority CAS.
    pub authority: Duration,
    /// Time from queued publication to terminal completion.
    pub total: Duration,
    /// Whether object publication completed and the worker confirmed the root.
    pub succeeded: bool,
    /// Sequence used to correlate this observation with a request trace.
    pub commit_sequence: u64,
    /// Logical commits included in this publication attempt. Count these only
    /// when `succeeded` is true when calculating commands per selected root.
    pub covered_commits: u64,
}

/// Native follower timing for one dispatched append batch.
#[derive(Clone, Copy, Debug, Default)]
pub struct FollowerAppendTiming {
    /// Time until the blocking worker starts; excludes its subsequent lane lock.
    pub worker_queue: Duration,
    /// Worker lifetime, including lane locking, verification, writes and barriers.
    pub worker: Duration,
    /// Time in append-file and rotation `sync_data` calls; excludes directory sync.
    pub data_sync: Duration,
    /// Number of attempted append-file and rotation barriers, including failures.
    pub data_sync_calls: u64,
    /// Number of input frames, including retries and already covered frames.
    pub frames: u64,
    /// Whether the append returned a durable receipt.
    pub succeeded: bool,
}

/// Outcome of an actor-owned resident route lookup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResidentRouteOutcome {
    /// The route was resident and usable.
    Hit,
    /// No resident route exists for the Cell.
    Miss,
    /// A resident route exists but refused the request.
    Refused,
}

/// Outcome of a client-side local route cache lookup.
///
/// A hit means the invocation skipped the catalog and authority reads that a
/// cold route needs. A miss means those reads ran.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RouteCacheOutcome {
    /// A cached route still matched the live actor.
    Hit,
    /// No cached route applied, so ownership was re-read from storage.
    Miss,
}

/// Outcome of one commit's attempt to use the node's enrolled follower lane.
///
/// A commit that cannot use its lane still succeeds through object coverage, so
/// `Unavailable` and `Rejected` are the only signals that a node intended fleet
/// durability and silently fell back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilitySubmissionOutcome {
    /// An enrolled lane accepted the captured commit for shipping.
    Fleet,
    /// This host installs no node-log durability provider at all.
    Unsupported,
    /// A provider exists, but no lane is enrolled yet.
    Unavailable,
    /// The enrolled lane refused or fenced the submission.
    Rejected,
}

/// Kind of one registered primitive call observed at the execution boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrimitiveOperationKind {
    /// A registered command.
    Command,
    /// A registered query.
    Query,
}

/// Kind of one catalog object read.
///
/// Catalog heads and immutable pages are read on the routing and due-scan hot
/// paths, and they bypass the LTX origin counters, so this bounded pair is the
/// only production signal for the metadata plane's object-store calls.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogReadKind {
    /// One shard-head observation.
    Head,
    /// One immutable page observation.
    Page,
}

/// One phase of acquiring and activating a Cell on this node.
///
/// A cold route pays ownership, root open, and local restore before the Cell
/// can answer; a warm route pays none of them. Timing the phases apart turns a
/// tail-latency report into a statement about which path to fix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ActivationPhase {
    /// The conditional ownership transition that claims the Cell.
    Ownership,
    /// Continuing a local database that already holds the observed root.
    Resume,
    /// Verifying the immutable root graph through the origin.
    RootOpen,
    /// Materializing the verified root into local disk.
    Restore,
    /// Opening the local database and publishing the serving control.
    Activate,
}

/// Terminal outcome of one registered primitive call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrimitiveOperationOutcome {
    /// The handler committed a result the caller consumes as success.
    Success,
    /// The handler committed a rejection the caller consumes as the result.
    Rejected,
    /// The operation failed before a committed result existed.
    Failed,
}

impl From<&crate::Result<crate::cell::executor::HandlerOutcome>> for PrimitiveOperationOutcome {
    fn from(result: &crate::Result<crate::cell::executor::HandlerOutcome>) -> Self {
        match result {
            Ok(crate::cell::executor::HandlerOutcome::Success(_)) => Self::Success,
            Ok(crate::cell::executor::HandlerOutcome::Rejected(_)) => Self::Rejected,
            Err(_) => Self::Failed,
        }
    }
}

/// Bounded operational events emitted by the Cell durability runtime.
///
/// Implementations must keep labels finite and must not block the Cell actor.
pub trait CellTelemetry: Send + Sync {
    /// Records one registered primitive call by owning module and outcome.
    fn primitive_operation(
        &self,
        _module: &'static str,
        _kind: PrimitiveOperationKind,
        _outcome: PrimitiveOperationOutcome,
        _elapsed: Duration,
    ) {
    }

    /// Records one completed fleet or object durability proof.
    fn durability_proof(&self, _source: DurabilitySource, _waited: Duration) {}

    /// Records one durable command or effect outcome sent to its runtime caller.
    ///
    /// Elapsed time starts at admitted enqueue; confirmation is the final SQL
    /// worker wait after proof, or zero for a recorded result. Transport, queries,
    /// migrations, failed results, and abandoned receivers are excluded.
    fn command_response(
        &self,
        _source: CommandResponseSource,
        _elapsed: Duration,
        _confirmation: Duration,
    ) {
    }

    /// Records the actor queue wait and SQL worker round trip for one command.
    /// The outcome describes worker execution, before durability proof.
    fn command_execution(
        &self,
        _queue_wait: Duration,
        _worker_round_trip: Duration,
        _succeeded: bool,
    ) {
    }

    /// Records an owner query's actor queue wait and SQL worker round trip.
    /// The round trip includes worker admission, read-only setup, and the handler.
    /// Queries refused before dispatch are excluded; deadline failures are included.
    fn query_execution(
        &self,
        _queue_wait: Duration,
        _worker_round_trip: Duration,
        _succeeded: bool,
    ) {
    }

    /// Records background root progress separately from the response winner.
    /// Cell IDs and sequences are for local trace correlation, never metric labels.
    fn publication_completed(&self, _cell: CellId, _timing: PublicationTiming) {}

    /// Records how one commit's node-log submission resolved.
    fn durability_submission(&self, _outcome: DurabilitySubmissionOutcome) {}

    /// Records the immutable objects and bytes one preparation attempt uploaded.
    ///
    /// The count covers every object a Cell root needs — segment bodies,
    /// indexes, directory nodes, root documents, segment pages, bundle bodies,
    /// and compaction outputs — so an operator can size object-store cost per
    /// command instead of inferring it from the database size. Failed attempts
    /// record the objects they did upload.
    fn publication_cost(&self, _objects: u64, _bytes: u64) {}

    /// Records bytes sent to follower append lanes and whether every lane acknowledged them.
    fn node_log_append(&self, _acknowledged: bool, _bytes: u64) {}

    /// Reports native follower work separately from peer HTTP and authorization.
    fn follower_append(&self, _timing: FollowerAppendTiming) {}

    /// Records a bounded-cardinality resident route result.
    fn resident_route(&self, _outcome: ResidentRouteOutcome) {}

    /// Records whether a client invocation reused a cached local route.
    fn route_cache(&self, _outcome: RouteCacheOutcome) {}

    /// Records one finite LTX phase outcome.
    fn ltx_phase(&self, _phase: cellule_ltx::LtxPhase, _elapsed: Duration, _succeeded: bool) {}

    /// Records one catalog object read and its outcome.
    fn catalog_read(&self, _kind: CatalogReadKind, _elapsed: Duration, _succeeded: bool) {}

    /// Records one control-record read and its outcome.
    ///
    /// One control record is read per cold route, per due-scan cell, and per
    /// recovery step, so this bounded counter is what makes the metadata
    /// plane's dominant cost visible.
    fn control_read(&self, _elapsed: Duration, _succeeded: bool) {}

    /// Records the duration of one Cell activation phase.
    fn activation_phase(&self, _phase: ActivationPhase, _elapsed: Duration) {}

    /// Records one logical read attributed to a bounded residency class.
    fn ltx_logical_read(&self, _origin: cellule_ltx::LtxReadOrigin) {}

    /// Records one provider attempt and the bytes returned before its outcome.
    fn ltx_origin_request(
        &self,
        _origin: cellule_ltx::LtxReadOrigin,
        _outcome: cellule_ltx::LtxRequestOutcome,
        _bytes: u64,
    ) {
    }

    /// Aggregates one fixed-size capture ledger without dynamic labels.
    fn ltx_capture(&self, _timing: &cellule_ltx::CaptureTiming, _succeeded: bool) {}

    /// Records the node's current hysteretic pressure tier.
    ///
    /// One node reports one tier at a time, and the classifier only hands out
    /// `Normal`, `Constrained`, `Shedding`, or `Critical`, so a sink can render
    /// this as a bounded gauge family instead of a growing label set. The tier a
    /// node reports is the one that decides whether it sheds settled Cells.
    fn pressure_state(&self, _state: PressureState) {}
}

/// Shared late-bound telemetry sink used by runtime components.
#[derive(Clone, Default)]
pub struct CellTelemetryHandle {
    inner: Arc<std::sync::OnceLock<Arc<dyn CellTelemetry>>>,
}

impl CellTelemetryHandle {
    pub(crate) fn install(&self, telemetry: Arc<dyn CellTelemetry>) -> crate::Result<()> {
        self.inner
            .set(telemetry)
            .map_err(|_| crate::Error::Control("Cell telemetry was initialized twice"))
    }

    /// Creates a handle bound to one sink.
    ///
    /// Runtime components that do not install through `CellRuntime` — a
    /// standalone catalog, for example — use this to share the node's sink.
    #[must_use]
    pub fn from_sink(telemetry: Arc<dyn CellTelemetry>) -> Self {
        let handle = Self::default();
        // The lock is empty at construction, so this set cannot lose a race.
        let _ = handle.inner.set(telemetry);
        handle
    }

    pub(crate) fn durability_proof(&self, source: DurabilitySource, waited: Duration) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.durability_proof(source, waited);
        }
    }

    pub(crate) fn command_response(
        &self,
        source: CommandResponseSource,
        elapsed: Duration,
        confirmation: Duration,
    ) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.command_response(source, elapsed, confirmation);
        }
    }

    pub(crate) fn command_execution(
        &self,
        queue_wait: Duration,
        worker_round_trip: Duration,
        succeeded: bool,
    ) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.command_execution(queue_wait, worker_round_trip, succeeded);
        }
    }

    pub(crate) fn query_execution(
        &self,
        queue_wait: Duration,
        worker_round_trip: Duration,
        succeeded: bool,
    ) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.query_execution(queue_wait, worker_round_trip, succeeded);
        }
    }

    pub(crate) fn publication_completed(&self, cell: CellId, timing: PublicationTiming) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.publication_completed(cell, timing);
        }
    }

    pub(crate) fn catalog_read(&self, kind: CatalogReadKind, elapsed: Duration, succeeded: bool) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.catalog_read(kind, elapsed, succeeded);
        }
    }

    pub(crate) fn control_read(&self, elapsed: Duration, succeeded: bool) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.control_read(elapsed, succeeded);
        }
    }

    pub(crate) fn activation_phase(&self, phase: ActivationPhase, elapsed: Duration) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.activation_phase(phase, elapsed);
        }
    }

    pub(crate) fn primitive_operation(
        &self,
        module: &'static str,
        kind: PrimitiveOperationKind,
        outcome: PrimitiveOperationOutcome,
        elapsed: Duration,
    ) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.primitive_operation(module, kind, outcome, elapsed);
        }
    }

    pub(crate) fn durability_submission(&self, outcome: DurabilitySubmissionOutcome) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.durability_submission(outcome);
        }
    }

    pub(crate) fn publication_cost(&self, objects: u64, bytes: u64) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.publication_cost(objects, bytes);
        }
    }

    pub(crate) fn node_log_append(&self, acknowledged: bool, bytes: u64) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.node_log_append(acknowledged, bytes);
        }
    }

    pub(crate) fn follower_append(&self, timing: FollowerAppendTiming) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.follower_append(timing);
        }
    }

    pub(crate) fn pressure_state(&self, state: PressureState) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.pressure_state(state);
        }
    }

    pub(crate) fn resident_route(&self, outcome: ResidentRouteOutcome) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.resident_route(outcome);
            if outcome == ResidentRouteOutcome::Hit {
                telemetry.ltx_logical_read(cellule_ltx::LtxReadOrigin::Resident);
            }
        }
    }

    pub(crate) fn route_cache(&self, outcome: RouteCacheOutcome) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.route_cache(outcome);
        }
    }

    fn ltx_phase(&self, phase: cellule_ltx::LtxPhase, elapsed: Duration, succeeded: bool) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.ltx_phase(phase, elapsed, succeeded);
        }
    }

    fn ltx_logical_read(&self, origin: cellule_ltx::LtxReadOrigin) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.ltx_logical_read(origin);
        }
    }

    fn ltx_origin_request(
        &self,
        origin: cellule_ltx::LtxReadOrigin,
        outcome: cellule_ltx::LtxRequestOutcome,
        bytes: u64,
    ) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.ltx_origin_request(origin, outcome, bytes);
        }
    }

    fn ltx_capture(&self, timing: &cellule_ltx::CaptureTiming, succeeded: bool) {
        if let Some(telemetry) = self.inner.get() {
            telemetry.ltx_capture(timing, succeeded);
        }
    }
}

impl cellule_ltx::LtxTelemetry for CellTelemetryHandle {
    fn phase(&self, phase: cellule_ltx::LtxPhase, elapsed: Duration, succeeded: bool) {
        self.ltx_phase(phase, elapsed, succeeded);
    }

    fn logical_read(&self, origin: cellule_ltx::LtxReadOrigin) {
        self.ltx_logical_read(origin);
    }

    fn origin_request(
        &self,
        origin: cellule_ltx::LtxReadOrigin,
        outcome: cellule_ltx::LtxRequestOutcome,
        bytes: u64,
    ) {
        self.ltx_origin_request(origin, outcome, bytes);
    }

    fn capture(&self, timing: &cellule_ltx::CaptureTiming, succeeded: bool) {
        tracing::debug!(
            target: "cellule_runtime::action",
            event = "cell_capture_completed",
            capture_ns = timing.total_nanos,
            encode_ns = timing.encode_nanos,
            write_ns = timing.local_write_nanos,
            fsync_ns = timing.fsync_nanos,
            parent_sync_ns = timing.parent_sync_nanos,
            checkpoint_ns = timing.checkpoint_nanos,
            wal_read_bytes = timing.wal_read_bytes,
            ltx_bytes = timing.ltx_bytes,
            succeeded,
        );
        self.ltx_capture(timing, succeeded);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingTelemetry {
        phases: Mutex<Vec<(cellule_ltx::LtxPhase, bool)>>,
        logical_reads: Mutex<Vec<cellule_ltx::LtxReadOrigin>>,
        requests: Mutex<
            Vec<(
                cellule_ltx::LtxReadOrigin,
                cellule_ltx::LtxRequestOutcome,
                u64,
            )>,
        >,
        submissions: Mutex<Vec<DurabilitySubmissionOutcome>>,
    }

    impl CellTelemetry for RecordingTelemetry {
        fn durability_submission(&self, outcome: DurabilitySubmissionOutcome) {
            self.submissions.lock().unwrap().push(outcome);
        }

        fn ltx_phase(&self, phase: cellule_ltx::LtxPhase, _: Duration, succeeded: bool) {
            self.phases.lock().unwrap().push((phase, succeeded));
        }

        fn ltx_logical_read(&self, origin: cellule_ltx::LtxReadOrigin) {
            self.logical_reads.lock().unwrap().push(origin);
        }

        fn ltx_origin_request(
            &self,
            origin: cellule_ltx::LtxReadOrigin,
            outcome: cellule_ltx::LtxRequestOutcome,
            bytes: u64,
        ) {
            self.requests.lock().unwrap().push((origin, outcome, bytes));
        }
    }

    #[test]
    fn ltx_bridge_preserves_only_finite_runtime_dimensions() {
        let handle = CellTelemetryHandle::default();
        let recording = Arc::new(RecordingTelemetry::default());
        handle.install(recording.clone()).unwrap();

        cellule_ltx::LtxTelemetry::phase(
            &handle,
            cellule_ltx::LtxPhase::Directory,
            Duration::from_millis(2),
            true,
        );
        cellule_ltx::LtxTelemetry::origin_request(
            &handle,
            cellule_ltx::LtxReadOrigin::Hydrating,
            cellule_ltx::LtxRequestOutcome::Failed,
            4_096,
        );
        handle.resident_route(ResidentRouteOutcome::Hit);
        handle.durability_submission(DurabilitySubmissionOutcome::Unavailable);
        handle.durability_submission(DurabilitySubmissionOutcome::Fleet);

        assert_eq!(
            *recording.phases.lock().unwrap(),
            vec![(cellule_ltx::LtxPhase::Directory, true)]
        );
        assert_eq!(
            *recording.logical_reads.lock().unwrap(),
            vec![cellule_ltx::LtxReadOrigin::Resident]
        );
        assert_eq!(
            *recording.requests.lock().unwrap(),
            vec![(
                cellule_ltx::LtxReadOrigin::Hydrating,
                cellule_ltx::LtxRequestOutcome::Failed,
                4_096,
            )]
        );
        assert_eq!(
            *recording.submissions.lock().unwrap(),
            vec![
                DurabilitySubmissionOutcome::Unavailable,
                DurabilitySubmissionOutcome::Fleet,
            ]
        );
    }
}
