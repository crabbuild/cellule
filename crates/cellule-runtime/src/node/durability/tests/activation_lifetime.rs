use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct CommittedActivation {
    state: tokio::sync::Mutex<()>,
    remote: AtomicBool,
    local: AtomicBool,
    calls: AtomicUsize,
    committed: Notify,
    resume: Notify,
    joined: Notify,
}

impl NodeLogAuthority for CommittedActivation {
    fn activate<'a>(&'a self, _epoch: u64) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let _state = self.state.lock().await;
            self.calls.fetch_add(1, Ordering::AcqRel);
            // The provider has accepted the CAS, but its response and the
            // adapter's acknowledged version have not yet joined.
            self.remote.store(true, Ordering::Release);
            self.committed.notify_one();
            self.resume.notified().await;
            self.local.store(true, Ordering::Release);
            self.joined.notify_one();
            Ok(())
        })
    }

    fn advance_coverage<'a>(&'a self, _epoch: u64, _through: u64) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let _state = self.state.lock().await;
            if self.remote.load(Ordering::Acquire) != self.local.load(Ordering::Acquire) {
                return Err(Error::Node(
                    "coverage CAS used a cancelled activation version",
                ));
            }
            Ok(())
        })
    }

    fn close<'a>(
        &'a self,
        _retirement: &'a crate::node::log::NodeLogRetirementObservation,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn cancelled_proof_waiter_keeps_accepted_activation_joined() {
    for fleet_only in [true, false] {
        cancelled_waiter(fleet_only).await;
    }
}

fn runtime(authority: Arc<dyn NodeLogAuthority>) -> Arc<NodeDurability> {
    let gate = DurabilityGate::new(session(1), node(1), 2, [node(2)]).unwrap();
    let transport: Arc<dyn NodeLogTransport> = Arc::new(ImmediateTransport);
    let shipper = NodeLogShipper::new(
        gate.clone(),
        Arc::clone(&transport),
        cellule_ltx::Limits::default(),
    )
    .unwrap();
    Arc::new(NodeDurability::new(
        gate,
        shipper,
        authority,
        transport,
        lease(),
    ))
}

async fn cancelled_waiter(fleet_only: bool) {
    let authority = Arc::new(CommittedActivation::default());
    let durability = runtime(authority.clone());
    let (_directory, cuts) = capture();
    let ticket = durability.submit(submission(&cuts)).await.unwrap();
    let waiter = tokio::spawn({
        let durability = durability.clone();
        async move {
            if fleet_only {
                durability.prove_fleet(ticket).await
            } else {
                durability.prove(ticket).await
            }
        }
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        authority.committed.notified(),
    )
    .await
    .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    authority.resume.notify_one();
    tokio::time::timeout(
        std::time::Duration::from_millis(250),
        authority.joined.notified(),
    )
    .await
    .expect("accepted authority I/O was cancelled with its proof waiter");
    assert!(authority.local.load(Ordering::Acquire));
    assert_eq!(
        durability.prove_fleet(ticket).await.unwrap().source(),
        DurabilitySource::Fleet
    );
    assert_eq!(authority.calls.load(Ordering::Acquire), 1);
    durability.prove_object(ticket).await.unwrap();
    durability.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_joins_cancelled_waiters_activation_before_coverage() {
    let authority = Arc::new(CommittedActivation::default());
    let durability = runtime(authority.clone());
    let (_directory, cuts) = capture();
    let ticket = durability.submit(submission(&cuts)).await.unwrap();
    let waiter = tokio::spawn({
        let durability = durability.clone();
        async move { durability.prove(ticket).await }
    });
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        authority.committed.notified(),
    )
    .await
    .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let mut shutdown = tokio::spawn({
        let durability = durability.clone();
        async move { durability.shutdown().await }
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut shutdown)
            .await
            .is_err()
    );
    authority.resume.notify_one();
    // Joining the callback does not invent bucket coverage for the Fleet tail.
    assert!(matches!(
        shutdown.await.unwrap(),
        Err(Error::PendingPublication)
    ));
    assert!(authority.local.load(Ordering::Acquire));
    durability.prove_object(ticket).await.unwrap();
    durability.shutdown().await.unwrap();
    assert_eq!(authority.calls.load(Ordering::Acquire), 1);
}

#[derive(Default)]
struct RetryActivation(AtomicUsize);

impl NodeLogAuthority for RetryActivation {
    fn activate<'a>(&'a self, _epoch: u64) -> BoxFuture<'a, Result<()>> {
        Box::pin(async {
            if self.0.fetch_add(1, Ordering::AcqRel) == 0 {
                Err(Error::Node("injected activation failure"))
            } else {
                Ok(())
            }
        })
    }

    fn advance_coverage<'a>(&'a self, _epoch: u64, _through: u64) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn close<'a>(
        &'a self,
        _retirement: &'a crate::node::log::NodeLogRetirementObservation,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn completed_activation_failure_can_retry_without_reissuing_the_ticket() {
    let authority = Arc::new(RetryActivation::default());
    let durability = runtime(authority.clone());
    let (_directory, cuts) = capture();
    let ticket = durability.submit(submission(&cuts)).await.unwrap();
    let error = durability.prove_fleet(ticket).await.unwrap_err();
    assert!(
        matches!(error, Error::Shared(source) if matches!(source.as_ref(), Error::Node("injected activation failure")))
    );
    assert_eq!(
        durability.prove_fleet(ticket).await.unwrap().source(),
        DurabilitySource::Fleet
    );
    assert_eq!(authority.0.load(Ordering::Acquire), 2);
    durability.prove_object(ticket).await.unwrap();
    durability.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_activation_can_drain_through_complete_object_coverage() {
    let authority = Arc::new(RetryActivation::default());
    let durability = runtime(authority.clone());
    let (_directory, cuts) = capture();
    let ticket = durability.submit(submission(&cuts)).await.unwrap();
    durability.prove_fleet(ticket).await.unwrap_err();
    assert_eq!(
        durability.prove_object(ticket).await.unwrap().source(),
        DurabilitySource::Object
    );
    durability.shutdown().await.unwrap();
    assert_eq!(authority.0.load(Ordering::Acquire), 1);
}
