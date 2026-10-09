//! Join the real retained task after request-bank bookkeeping fails.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

struct Provider {
    panic: bool,
    entered: AtomicBool,
}
impl NodeDurabilityProvider for Provider {
    fn rotation_required(
        self: Arc<Self>,
        _live_node_limit: usize,
    ) -> Pin<Box<dyn Future<Output = FacilityResult<bool>> + Send>> {
        Box::pin(async { Ok(false) })
    }

    fn recruit(
        self: Arc<Self>,
        _: ReplicaLimits,
        _: u64,
        _: usize,
    ) -> Pin<Box<dyn Future<Output = FacilityResult<Option<NodeDurabilityConfig>>> + Send>> {
        self.entered.store(true, Ordering::Release);
        assert!(!self.panic, "original native supervisor panic");
        Box::pin(async { Ok(None) })
    }
}

async fn joined_poisoned_bank(panic: bool) {
    let session = SessionId::from_bytes([7; 16]);
    let runtime = CellRuntime::new(SqlWorkerPool::new(1, 2).unwrap(), 1 << 20, session).unwrap();
    let provider = Arc::new(Provider {
        panic,
        entered: AtomicBool::new(false),
    });
    let config = NodeDurabilitySupervisorConfig::new(
        ApplicationId::from_bytes([3; 16]),
        ReplicaLimits::default(),
        1,
        2,
        Duration::from_millis(10),
        Duration::from_secs(3600),
        u64::MAX,
    )
    .unwrap();
    let owner = Arc::new(
        owner::DurabilitySupervisor::new(
            provider.clone(),
            runtime.clone(),
            config,
            session,
            CancellationToken::new(),
        )
        .unwrap(),
    );
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = owner.requests.bank.lock().unwrap();
        panic!("poison original request bank");
    }));
    assert!(poisoned.is_err());
    let joining = owner.clone();
    let waiter = tokio::spawn(async move { joining.join().await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !provider.entered.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    owner.cancellation.cancel();
    let first = waiter.await.unwrap().unwrap_err();
    let original = owner.observe(17).unwrap();
    assert_eq!(
        original.state,
        NodeDurabilitySupervisorState::JoinedUnsettled
    );
    let stopped = original.requests_error.unwrap();
    assert!(matches!(
        stopped.as_ref(),
        Error::Control("node-log rotation request lock poisoned")
    ));
    assert!(Arc::ptr_eq(
        original.rotations.as_ref().unwrap_err(),
        &stopped
    ));
    assert_eq!(original.supervisor_error.is_some(), panic);
    let assert_source = |error: &(dyn std::error::Error + 'static)| {
        let source = error.source().unwrap();
        // Compare concrete source addresses: trait-object vtables can be
        // duplicated across codegen units even for the same original source.
        if panic {
            let native = original
                .supervisor_error
                .as_ref()
                .unwrap()
                .downcast_ref::<tokio::task::JoinError>()
                .unwrap();
            assert!(native.is_panic());
            assert!(std::ptr::eq(
                source.downcast_ref::<tokio::task::JoinError>().unwrap(),
                native
            ));
        } else {
            assert!(std::ptr::eq(
                source.downcast_ref::<Error>().unwrap(),
                stopped.as_ref()
            ));
        }
    };
    assert_source(first.as_ref());
    for _ in 0..3 {
        // The JoinHandle was consumed once; a failed stop cannot repoll it.
        let retry = owner.drain().await.unwrap_err();
        assert_source(retry.as_ref());
        let observed = owner.observe(18).unwrap();
        assert_eq!(
            observed.state,
            NodeDurabilitySupervisorState::JoinedUnsettled
        );
        assert!(Arc::ptr_eq(
            observed.requests_error.as_ref().unwrap(),
            &stopped
        ));
        assert!(Arc::ptr_eq(
            observed.rotations.as_ref().unwrap_err(),
            &stopped
        ));
        if let Some(native) = &original.supervisor_error {
            assert!(Arc::ptr_eq(
                observed.supervisor_error.as_ref().unwrap(),
                native
            ));
        }
    }
    assert_eq!(runtime.stats().retained_bytes(), 4 * 1024);
    drop(owner);
    assert_eq!(runtime.stats().retained_bytes(), 0);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn successful_task_keeps_original_stop_error_across_repeated_joins() {
    joined_poisoned_bank(false).await;
}

#[tokio::test]
async fn panicked_task_keeps_native_error_separate_from_original_stop_error() {
    joined_poisoned_bank(true).await;
}
