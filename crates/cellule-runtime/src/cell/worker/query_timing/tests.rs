use super::*;
use crate::fleet::telemetry::CellTelemetry;

struct Sink;
impl CellTelemetry for Sink {}

fn trace() -> Arc<QueryTrace> {
    QueryTrace::new(
        &CellTelemetryHandle::from_sink(Arc::new(Sink)),
        &ResourceLedger::new(ResourceCost::zero().with_retained_bytes(4096)),
    )
    .unwrap()
    .unwrap()
}

#[test]
fn disabled_telemetry_allocates_no_trace() {
    assert!(
        QueryTrace::new(
            &CellTelemetryHandle::default(),
            &ResourceLedger::new(ResourceCost::zero())
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn query_trace_reservation_remains_until_the_last_worker_reference_drops() {
    let sink = CellTelemetryHandle::from_sink(Arc::new(Sink));
    let bytes = std::mem::size_of::<QueryTrace>() + 2 * std::mem::size_of::<usize>();
    let ledger = ResourceLedger::new(ResourceCost::zero().with_retained_bytes(bytes));
    let trace = QueryTrace::new(&sink, &ledger).unwrap().unwrap();
    let worker = trace.clone();
    assert!(QueryTrace::new(&sink, &ledger).is_err());
    assert_eq!(ledger.snapshot().unwrap().used.retained_bytes(), bytes);
    drop(trace);
    assert_eq!(ledger.snapshot().unwrap().used.retained_bytes(), bytes);
    drop(worker);
    assert_eq!(ledger.snapshot().unwrap().used.retained_bytes(), 0);
    assert!(QueryTrace::new(&sink, &ledger).unwrap().is_some());
}

#[test]
fn completed_phases_partition_total() {
    let trace = trace();
    trace.received();
    trace.enqueued(QueryActorState::Ready);
    trace.dispatched();
    trace.started();
    trace.admitted(Some(7));
    trace.dequeued();
    trace.completed();
    let timing = trace.snapshot(true);
    assert_eq!(
        timing.actor_queue.unwrap()
            + timing.worker_admission.unwrap()
            + timing.worker_queue.unwrap()
            + timing.execution.unwrap()
            + timing.reply_queue.unwrap(),
        timing.total
    );
    assert!(timing.succeeded);
    assert_eq!(timing.job_id, Some(7));
    assert_eq!(
        timing.actor_ingress.unwrap() + timing.cell_queue.unwrap() + timing.task_start.unwrap(),
        timing.actor_queue.unwrap()
    );
    assert_eq!(timing.actor_state, Some(QueryActorState::Ready));
}

#[test]
fn deadline_before_worker_completion_keeps_unreached_phases_absent() {
    let trace = trace();
    let rejected = trace.snapshot(false);
    assert!(rejected.actor_queue.is_none() && rejected.worker_admission.is_none());
    assert!(rejected.actor_ingress.is_none() && rejected.actor_state.is_none());
    trace.received();
    trace.enqueued(QueryActorState::Renewal);
    let queued = trace.snapshot(false);
    assert!(queued.actor_ingress.is_some());
    assert!(queued.cell_queue.is_none() && queued.task_start.is_none());
    assert_eq!(queued.actor_state, Some(QueryActorState::Renewal));
    trace.dispatched();
    let dispatched = trace.snapshot(false);
    assert!(dispatched.cell_queue.is_some() && dispatched.task_start.is_none());
    trace.started();
    // A deadline between the ID and admission stores must not invent a
    // reached admission boundary in the terminal observation.
    trace.job_id.store(8, Ordering::Release);
    let acquiring = trace.snapshot(false);
    assert!(acquiring.job_id.is_none() && acquiring.worker_admission.is_none());
    trace.admitted(Some(8));
    trace.dequeued();
    let deadline = trace.snapshot(false);
    assert!(
        deadline.actor_queue.is_some()
            && deadline.worker_admission.is_some()
            && deadline.worker_queue.is_some()
    );
    assert!(deadline.execution.is_none() && deadline.reply_queue.is_none());
    trace.completed();
    assert!(trace.snapshot(false).execution.is_some());
    assert!(deadline.execution.is_none()); // The emitted deadline observation is immutable.
}
