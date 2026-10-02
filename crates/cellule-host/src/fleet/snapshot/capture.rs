use super::super::actions::{FleetActionExecutor, journal_error, operation, wall_time_ms};
use super::*;

impl FleetActionExecutor {
    pub(in crate::fleet) async fn execute_snapshot(
        &self,
        request: FleetSnapshotRequest,
        owners: Arc<SnapshotOwners>,
    ) -> Result<Arc<FleetNodeSnapshot>, Arc<Error>> {
        let admitted = wall_time_ms().map_err(Arc::new)?;
        self.journal
            .authorize_snapshot(&request, admitted)
            .await
            .map_err(journal_error)
            .map_err(Arc::new)?;
        let started = wall_time_ms().map_err(Arc::new)?;
        if started >= request.deadline_ms() {
            return Err(Arc::new(operation(
                cellule_runtime::fleet::operations::OperationError::Deadline,
            )));
        }
        let state_before = owners.state().map_err(Arc::new)?;
        let page = match request.subject() {
            FleetSnapshotSubject::Host => FleetSnapshotNativePage::Host,
            FleetSnapshotSubject::Cells(cursor) => FleetSnapshotNativePage::Cells(
                self.runtime
                    .fleet_cells_page(*cursor, request.limit())
                    .await
                    .map_err(Arc::new)?,
            ),
            FleetSnapshotSubject::Readers(cursor) => match &owners.readers {
                Some(reader) => FleetSnapshotNativePage::Readers(
                    reader
                        .fleet_readers_page(*cursor, request.limit(), started)
                        .await
                        .map_err(Arc::new)?,
                ),
                None => FleetSnapshotNativePage::Unbound,
            },
            FleetSnapshotSubject::ReaderEnrollments(cursor) => match &owners.readers {
                Some(reader) => match reader
                    .fleet_reader_enrollments_page(*cursor, request.limit(), started)
                    .map_err(Arc::new)?
                {
                    Some(page) => FleetSnapshotNativePage::ReaderEnrollments(page),
                    None => FleetSnapshotNativePage::Unbound,
                },
                None => FleetSnapshotNativePage::Unbound,
            },
            FleetSnapshotSubject::FollowerLanes(cursor) => match &owners.followers {
                Some(store) => FleetSnapshotNativePage::FollowerLanes(
                    store
                        .fleet_lanes_page(*cursor, request.limit(), started)
                        .await
                        .map_err(Arc::new)?,
                ),
                None => FleetSnapshotNativePage::Unbound,
            },
            FleetSnapshotSubject::FollowerEnrollments(cursor) => match &owners.producer {
                Some(producer) => FleetSnapshotNativePage::FollowerEnrollments(
                    producer
                        .page(*cursor, request.limit(), started)
                        .map_err(Arc::new)?,
                ),
                None => FleetSnapshotNativePage::Unbound,
            },
            FleetSnapshotSubject::DurabilitySupervisor => match &owners.supervisor {
                Some(owner) => FleetSnapshotNativePage::DurabilitySupervisor(
                    owner.observe(started).map_err(Arc::new)?,
                ),
                None => FleetSnapshotNativePage::Unbound,
            },
        };
        // Keep the native read owned even if its RPC waiter or deadline expires.
        // Post-authorization refuses changed head/registry instead of restamping
        // the original page under a newer barrier. It can start no native effect.
        let node_log = self
            .runtime
            .try_node_durability()
            .map_err(Arc::new)?
            .map(|(application, durability)| {
                if application != self.scope.application {
                    return Err(Error::Fenced);
                }
                durability.identity()
            })
            .transpose()
            .map_err(Arc::new)?;
        let captured = wall_time_ms().map_err(Arc::new)?;
        self.journal
            .authorize_snapshot(&request, captured)
            .await
            .map_err(journal_error)
            .map_err(Arc::new)?;
        let state_after = owners.state().map_err(Arc::new)?;
        let mode = self.runtime.node_admission().mode().map_err(Arc::new)?;
        let finished = wall_time_ms().map_err(Arc::new)?;
        let result = FleetNodeSnapshot {
            request,
            started_at_ms: started,
            finished_at_ms: finished,
            state_before,
            state_after,
            mode,
            bindings: owners.bindings(),
            node_log,
            page,
        };
        result
            .validate(result.request(), finished)
            .map_err(Arc::new)?;
        Ok(Arc::new(result))
    }
}
