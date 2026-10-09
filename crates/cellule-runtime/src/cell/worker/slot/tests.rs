use super::*;
use crate::fleet::telemetry::{CellTelemetry, SqlJobKind, SqlSlotTiming};

#[derive(Default)]
struct Sink(Mutex<Vec<SqlSlotTiming>>);
impl CellTelemetry for Sink {
    fn sql_slot_released(&self, timing: SqlSlotTiming) {
        self.0.lock().unwrap().push(timing);
    }
}

#[tokio::test]
async fn reservations_link_previous_holders_and_preserve_idle_snapshot_borrowing() {
    let pool = SqlWorkerPool::new(2, 2).unwrap();
    pool.configure_retained_capacity(4096).unwrap();
    let sink = Arc::new(Sink::default());
    pool.telemetry_handle().install(sink.clone()).unwrap();
    let first_cell = CellId::from_bytes([0; 32]);
    let mut first = pool
        .reserve_job(first_cell, SqlJobKind::Query)
        .await
        .unwrap();
    first.trace.as_mut().unwrap().started();
    let first_id = first.trace.as_ref().unwrap().id;
    // A busy lane must not reserve the other lane's idle slot.
    let snapshot = pool.reserve_snapshot_job().await.unwrap();
    assert_eq!(snapshot.trace.as_ref().unwrap().shard, 1);
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .worker_jobs(),
        2
    );
    drop(snapshot);
    drop(first);
    let second = pool
        .reserve_job(first_cell, SqlJobKind::Inventory)
        .await
        .unwrap();
    assert_eq!(second.trace.as_ref().unwrap().previous_job, first_id);
    drop(second);
    {
        let values = sink.0.lock().unwrap();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0].kind, SqlJobKind::Snapshot);
        assert!(values[0].handoff.is_none() && values[0].native.is_none());
        assert_eq!(
            values[1].handoff.unwrap() + values[1].native.unwrap(),
            values[1].held
        );
        assert_eq!(values[2].previous_job, Some(first_id));
        assert!(values[2].after_last_release.unwrap() <= values[2].admission);
        for value in values.iter() {
            assert!(
                value.requested_ns <= value.acquired_ns && value.acquired_ns <= value.released_ns
            );
        }
    }
    assert_eq!(
        pool.resource_ledger().snapshot().unwrap().used,
        ResourceCost::zero()
    );
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn disabled_observation_has_no_clock_trace_or_retained_charge() {
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    let reservation = pool
        .reserve_job(CellId::from_bytes([0; 32]), SqlJobKind::Query)
        .await
        .unwrap();
    assert!(reservation.trace.is_none());
    assert_eq!(
        pool.resource_ledger()
            .snapshot()
            .unwrap()
            .used
            .retained_bytes(),
        0
    );
    drop(reservation);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn telemetry_observes_resources_after_the_slot_is_released() {
    struct ReleasingSink {
        ledger: ResourceLedger,
        permit: Arc<Semaphore>,
        count: AtomicU64,
    }
    impl CellTelemetry for ReleasingSink {
        fn sql_slot_released(&self, _: SqlSlotTiming) {
            assert_eq!(self.ledger.snapshot().unwrap().used.worker_jobs(), 0);
            assert_eq!(self.ledger.snapshot().unwrap().used.retained_bytes(), 0);
            assert_eq!(self.permit.available_permits(), 1);
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    pool.configure_retained_capacity(4096).unwrap();
    let sink = Arc::new(ReleasingSink {
        ledger: pool.resource_ledger(),
        permit: pool.inner.slots.permits[0].clone(),
        count: AtomicU64::new(0),
    });
    pool.telemetry_handle().install(sink.clone()).unwrap();
    let reservation = pool
        .reserve_job(CellId::from_bytes([0; 32]), SqlJobKind::Query)
        .await
        .unwrap();
    assert!(
        sink.ledger.snapshot().unwrap().used.retained_bytes() >= std::mem::size_of::<JobTrace>()
    );
    drop(reservation);
    assert_eq!(sink.count.load(Ordering::Relaxed), 1);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn exhausted_trace_ids_release_admission_without_creating_a_job() {
    let pool = SqlWorkerPool::new(1, 1).unwrap();
    pool.configure_retained_capacity(4096).unwrap();
    let sink = Arc::new(Sink::default());
    pool.telemetry_handle().install(sink.clone()).unwrap();
    pool.inner
        .slots
        .probe
        .next_id
        .store(u64::MAX, Ordering::Relaxed);
    assert!(matches!(
        pool.reserve_job(CellId::from_bytes([0; 32]), SqlJobKind::Query)
            .await,
        Err(Error::Capacity("SQL slot trace IDs"))
    ));
    assert_eq!(
        pool.resource_ledger().snapshot().unwrap().used,
        ResourceCost::zero()
    );
    assert_eq!(pool.inner.slots.permits[0].available_permits(), 1);
    assert!(sink.0.lock().unwrap().is_empty());
    pool.shutdown().await.unwrap();
}
