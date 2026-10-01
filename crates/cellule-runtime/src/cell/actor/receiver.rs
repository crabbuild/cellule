//! Runtime-owned receiver credit and cancellation-independent acquisition.

use std::sync::{Mutex, Weak};

use super::*;
use crate::cell::worker::WorkerJobReservation;
use crate::fleet::operations::{
    AttemptId, MAX_ACTIVE_ATTEMPTS, MAX_RECORD_BYTES, MAX_RESTORE_BYTES, MoveAttemptSpec,
    OperationError, ReceiverReservation,
};

/// Local receiver lifecycle hint. Authority and actor observations still prove serving.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReceiverState {
    /// The runtime holds unused resource credit for the exact attempt.
    Prepared,
    /// Acquisition was accepted and owns its work independently of its waiter.
    Activating,
    /// Acquisition returned a live handle; recheck authority and actor readiness.
    Activated,
    /// Accepted work finished unsuccessfully; reconcile authority before retrying.
    Failed,
    /// Unused credit was synchronously returned without an authority transition.
    Cancelled,
}

/// Opaque reference to runtime-owned preparation. Dropping it does not cancel work.
///
/// The runtime retains the reservation after a lost preparation reply. Shutdown
/// cancels unused credit and joins accepted activation even when callers retain
/// this reference. This local API supplies no operator or peer authorization.
#[derive(Clone)]
pub struct PreparedCellReceiver {
    credit: Weak<ReceiverCredit>,
}

impl PreparedCellReceiver {
    /// Returns the immutable movement identity while the runtime retains it.
    pub fn spec(&self) -> crate::Result<MoveAttemptSpec> {
        Ok(self.credit()?.spec.clone())
    }

    /// Returns the exact session and expiry of the retained credit.
    pub fn reservation(&self) -> crate::Result<ReceiverReservation> {
        Ok(self.credit()?.reservation)
    }

    /// Inspects a local lifecycle hint, never a durable movement result.
    pub fn state(&self) -> crate::Result<ReceiverState> {
        Ok(self
            .credit()?
            .state
            .lock()
            .map_err(|_| Error::RuntimeClosed)?
            .phase)
    }

    fn credit(&self) -> crate::Result<Arc<ReceiverCredit>> {
        self.credit.upgrade().ok_or(Error::RuntimeClosed)
    }
}

#[derive(Default)]
pub(super) struct ReceiverRegistry {
    credits: HashMap<AttemptId, Arc<ReceiverCredit>>,
    tasks: Vec<(AttemptId, tokio::task::JoinHandle<()>)>,
}

struct ReceiverCredit {
    spec: MoveAttemptSpec,
    reservation: ReceiverReservation,
    catalog: CatalogProof,
    replica: cellule_ltx::CellReplica,
    destination: PathBuf,
    state: Mutex<CreditState>,
    // Bounded receipt state remains charged until retirement or shutdown.
    _retained: ResourceReservation,
}

struct CreditState {
    phase: ReceiverState,
    resources: Option<ReceiverResources>,
}

struct ReceiverResources {
    cell: CellReservation,
    job: WorkerJobReservation,
    disk: cellule_ltx::DiskBudget,
}

fn operation(error: OperationError) -> Error {
    Error::FleetOperation(Box::new(error))
}

impl CellRuntime {
    /// Reserves real Cell, memory, descriptor, disk, and affine SQL-job capacity.
    ///
    /// Call before releasing the source. Duplicate exact preparation returns the
    /// same credit without another charge. At most two local receipts are kept;
    /// the application retires joined terminal receipts after durable journaling.
    /// Expiry permits cancellation; it never silently returns accepted credit.
    pub fn prepare_receiver(
        &self,
        spec: MoveAttemptSpec,
        catalog: CatalogProof,
        replica: cellule_ltx::CellReplica,
        destination: PathBuf,
        expires_at_ms: i64,
        now_ms: i64,
    ) -> crate::Result<PreparedCellReceiver> {
        spec.validate().map_err(operation)?;
        if spec.destination != self.inner.session
            || catalog.target()? != spec.target
            || replica.scope()
                != (
                    *spec.target.cell_id().as_bytes(),
                    *spec.incarnation.as_bytes(),
                )
        {
            return Err(Error::Fenced);
        }
        self.check_application_limits(&catalog, replica.limits())?;
        if destination.parent().is_none() {
            return Err(Error::Control("Cell activation destination has no parent"));
        }
        let minimum = inventory::transfer_cost(replica.limits(), 1)?;
        if spec.cost.memory_bytes < minimum.memory_bytes
            || spec.cost.disk_bytes < minimum.disk_bytes
            || spec.cost.file_descriptors < minimum.file_descriptors
            || spec.cost.job_credits != 1
            || spec.cost.disk_bytes > MAX_RESTORE_BYTES
        {
            return Err(Error::Capacity(
                "receiver demand does not cover validated Cell bounds",
            ));
        }
        let mut registry = self
            .inner
            .receivers
            .lock()
            .map_err(|_| Error::RuntimeClosed)?;
        self.ensure_acquiring()?;
        if let Some(old) = registry.credits.get(&spec.id) {
            if old.spec != spec
                || old.reservation.expires_at_ms != expires_at_ms
                || old.catalog.entry() != catalog.entry()
                || old.catalog.revision() != catalog.revision()
                || limits_identity(old.replica.limits()) != limits_identity(replica.limits())
                || old.destination != destination
            {
                return Err(operation(OperationError::Conflict));
            }
            return Ok(PreparedCellReceiver {
                credit: Arc::downgrade(old),
            });
        }
        if now_ms < 0 || now_ms >= spec.deadline_ms || expires_at_ms <= now_ms {
            return Err(operation(OperationError::Deadline));
        }
        if registry.credits.len() >= MAX_ACTIVE_ATTEMPTS {
            return Err(operation(OperationError::Budget));
        }
        let memory = usize::try_from(spec.cost.memory_bytes)
            .map_err(|_| Error::Capacity("receiver memory cost overflow"))?;
        let descriptors = usize::try_from(spec.cost.file_descriptors)
            .map_err(|_| Error::Capacity("receiver descriptor cost overflow"))?;
        let retained = self
            .inner
            .resources
            .try_reserve(ResourceCost::zero().with_retained_bytes(MAX_RECORD_BYTES as usize))?;
        let cell = self.inner.pool.reserve_activation_cost(
            ResourceCost::active_cell()
                .with_resident_bytes(memory)
                .with_file_descriptors(descriptors),
        )?;
        let job = self.inner.pool.try_reserve_job(spec.target.cell_id())?;
        let disk = self
            .local_disk_budget()
            .try_reserve(spec.cost.disk_bytes)?
            .into_budget();
        let credit = Arc::new(ReceiverCredit {
            reservation: ReceiverReservation {
                session: spec.destination,
                expires_at_ms,
            },
            spec,
            catalog,
            replica,
            destination,
            state: Mutex::new(CreditState {
                phase: ReceiverState::Prepared,
                resources: Some(ReceiverResources { cell, job, disk }),
            }),
            _retained: retained,
        });
        let handle = PreparedCellReceiver {
            credit: Arc::downgrade(&credit),
        };
        registry.credits.insert(credit.spec.id, credit);
        Ok(handle)
    }

    /// Recovers a lost preparation reply for this runtime's exact attempt.
    pub fn prepared_receiver(&self, id: AttemptId) -> crate::Result<Option<PreparedCellReceiver>> {
        let registry = self
            .inner
            .receivers
            .lock()
            .map_err(|_| Error::RuntimeClosed)?;
        self.ensure_running()?;
        Ok(registry
            .credits
            .get(&id)
            .map(|credit| PreparedCellReceiver {
                credit: Arc::downgrade(credit),
            }))
    }

    /// Cancels only unused preparation. Accepted activation must be reconciled.
    pub fn cancel_prepared_receiver(&self, prepared: &PreparedCellReceiver) -> crate::Result<()> {
        let registry = self
            .inner
            .receivers
            .lock()
            .map_err(|_| Error::RuntimeClosed)?;
        let credit = checked_credit(&registry, prepared)?;
        let mut state = credit.state.lock().map_err(|_| Error::RuntimeClosed)?;
        match state.phase {
            ReceiverState::Prepared | ReceiverState::Cancelled => {
                state.resources.take();
                state.phase = ReceiverState::Cancelled;
                Ok(())
            }
            _ => Err(operation(OperationError::Busy)),
        }
    }

    /// Transfers preparation into canonical Idle takeover and exact-root activation.
    ///
    /// The current Idle control must name the attempt's incarnation and an epoch
    /// at least as new as its source. Intervening canonical acquisition/release
    /// may advance that epoch. Admission is rechecked before CAS and after restore. A dropped
    /// waiter cannot cancel accepted acquisition. Inspect the local lifecycle,
    /// authority and actor after a lost reply; repeated dispatch never reacquires.
    pub async fn activate_prepared_receiver(
        &self,
        prepared: &PreparedCellReceiver,
        authority: CellAuthority,
        observed: VersionedControl,
        owner: Owner,
        now_ms: i64,
    ) -> crate::Result<CellHandle> {
        let response = {
            let mut registry = self
                .inner
                .receivers
                .lock()
                .map_err(|_| Error::RuntimeClosed)?;
            self.ensure_acquiring()?;
            let credit = checked_credit(&registry, prepared)?;
            let control = observed.value();
            if control.cell != credit.spec.target.cell_id()
                || control.incarnation != credit.spec.incarnation
                || control.epoch < credit.spec.source_epoch
                || control.state != crate::control::ControlState::Idle
                || control.owner.is_some()
                || control.root.is_none()
                || owner.session != credit.spec.destination
            {
                return Err(Error::Fenced);
            }
            if now_ms < 0 || now_ms >= credit.reservation.expires_at_ms {
                return Err(operation(OperationError::Deadline));
            }
            let mut state = credit.state.lock().map_err(|_| Error::RuntimeClosed)?;
            if state.phase != ReceiverState::Prepared {
                return Err(operation(OperationError::Busy));
            }
            let resources = state.resources.take().ok_or(Error::RuntimeClosed)?;
            state.phase = ReceiverState::Activating;
            drop(state);
            let runtime = self.clone();
            let (reply, response) = oneshot::channel();
            let id = credit.spec.id;
            // The registry owns completion, and the task owns the runtime until
            // it joins all accepted preparation. Neither depends on the RPC.
            let task = tokio::spawn(async move {
                let completion = ActivationCompletion(Arc::clone(&credit));
                let result = runtime
                    .activate_receiver_credit(&credit, resources, authority, observed, owner)
                    .await;
                if let Ok(mut state) = credit.state.lock() {
                    state.phase = if result.is_ok() {
                        ReceiverState::Activated
                    } else {
                        ReceiverState::Failed
                    };
                }
                let _ = reply.send(result);
                drop(completion);
            });
            registry.tasks.push((id, task));
            response
        };
        response.await.map_err(|_| Error::RuntimeClosed)?
    }

    async fn activate_receiver_credit(
        &self,
        credit: &ReceiverCredit,
        resources: ReceiverResources,
        authority: CellAuthority,
        observed: VersionedControl,
        owner: Owner,
    ) -> crate::Result<CellHandle> {
        let ReceiverResources { cell, job, disk } = resources;
        let host = self
            .inner
            .replica_host
            .clone()
            .with_local_disk_budget(disk.clone());
        let result = async {
            let replica = Self::replica_with_host_cache(
                credit.replica.clone(),
                host.clone(),
                &credit.destination,
            )
            .await?;
            self.acquire_idle_reserved(
                credit.catalog.clone(),
                replica,
                authority,
                observed,
                credit.destination.clone(),
                owner,
                cell,
                Some(job),
                None,
            )
            .await
        }
        .await;
        host.drain_cache_fills().await;
        let finished = disk.finish_preparation();
        match result {
            Ok(handle) => {
                finished?;
                Ok(handle)
            }
            Err(error) => {
                let _ = finished;
                Err(error)
            }
        }
    }

    /// Retires a joined terminal local receipt after its result is durably recorded.
    ///
    /// The caller must reconcile failed/unknown authority outcomes first. This
    /// never closes a serving actor, and never establishes journal permit cleanup.
    /// Attempt identities must never be reused after retirement.
    pub async fn retire_prepared_receiver(
        &self,
        prepared: &PreparedCellReceiver,
    ) -> crate::Result<()> {
        let task = {
            let mut registry = self
                .inner
                .receivers
                .lock()
                .map_err(|_| Error::RuntimeClosed)?;
            let credit = checked_credit(&registry, prepared)?;
            let state = credit.state.lock().map_err(|_| Error::RuntimeClosed)?;
            if matches!(
                state.phase,
                ReceiverState::Prepared | ReceiverState::Activating
            ) {
                return Err(operation(OperationError::Busy));
            }
            let index = registry
                .tasks
                .iter()
                .position(|(id, _)| *id == credit.spec.id);
            if index.is_some_and(|index| !registry.tasks[index].1.is_finished()) {
                return Err(operation(OperationError::Busy));
            }
            let task = index.map(|index| registry.tasks.swap_remove(index).1);
            registry.credits.remove(&credit.spec.id);
            task
        };
        if let Some(task) = task {
            task.await.map_err(Error::WorkerJoin)?;
        }
        Ok(())
    }

    pub(super) async fn drain_prepared_receivers(&self) -> crate::Result<()> {
        let tasks = {
            let mut registry = self
                .inner
                .receivers
                .lock()
                .map_err(|_| Error::RuntimeClosed)?;
            for credit in registry.credits.values() {
                let mut state = credit.state.lock().map_err(|_| Error::RuntimeClosed)?;
                if state.phase == ReceiverState::Prepared {
                    state.resources.take();
                    state.phase = ReceiverState::Cancelled;
                }
            }
            std::mem::take(&mut registry.tasks)
        };
        let mut failure = None;
        for (_, task) in tasks {
            if let Err(error) = task.await {
                failure.get_or_insert(Error::WorkerJoin(error));
            }
        }
        self.inner
            .receivers
            .lock()
            .map_err(|_| Error::RuntimeClosed)?
            .credits
            .clear();
        failure.map_or(Ok(()), Err)
    }
}

fn checked_credit(
    registry: &ReceiverRegistry,
    prepared: &PreparedCellReceiver,
) -> crate::Result<Arc<ReceiverCredit>> {
    let credit = prepared.credit()?;
    if registry
        .credits
        .get(&credit.spec.id)
        .is_none_or(|current| !Arc::ptr_eq(current, &credit))
    {
        return Err(Error::Fenced);
    }
    Ok(credit)
}

// A panic leaves an inspectable unknown result, never a reusable preparation.
struct ActivationCompletion(Arc<ReceiverCredit>);
impl Drop for ActivationCompletion {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock()
            && state.phase == ReceiverState::Activating
        {
            state.phase = ReceiverState::Failed;
        }
    }
}

fn limits_identity(limits: cellule_ltx::Limits) -> (u64, u64, u64, u64, usize) {
    (
        limits.max_database_bytes,
        limits.max_capture_bytes,
        limits.max_file_bytes,
        limits.max_plan_bytes,
        limits.max_segments,
    )
}
