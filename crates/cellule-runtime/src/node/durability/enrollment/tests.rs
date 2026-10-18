//! Callback ownership and memory admission survive cancelled waiters and joins.
use super::*;
use crate::cell::catalog::{CatalogEntry, CatalogRole, CellCatalog};
use crate::control::Owner;
use crate::fleet::resource::{ResourceCost, ResourceLedger};
use crate::identity::{
    ApplicationId, CellTarget, Digest, IncarnationId, NamespaceId, SessionId, TenantId,
};
use cellule_ltx::CellStorageLayout;
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Default)]
struct Held {
    entered: Notify,
    resume: Notify,
    calls: AtomicUsize,
    completed: AtomicUsize,
    cohort: AtomicUsize,
    serial: tokio::sync::Mutex<()>,
}

impl NodeBundleAuthority for Held {
    fn bind_batch<'a>(
        &'a self,
        cells: &'a [(&'a CellAuthority, &'a VersionedControl)],
    ) -> BoxFuture<'a, Option<Vec<Result<VersionedControl>>>> {
        Box::pin(async move {
            self.cohort.store(cells.len(), Ordering::SeqCst);
            None
        })
    }

    fn bind<'a>(
        &'a self,
        _: &'a CellAuthority,
        observed: &'a VersionedControl,
    ) -> BoxFuture<'a, Result<VersionedControl>> {
        Box::pin(async move {
            let _serial = self.serial.lock().await;
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.resume.notified().await;
            self.completed.fetch_add(1, Ordering::SeqCst);
            Ok(observed.clone())
        })
    }
    fn close<'a>(
        &'a self,
        _: &'a CellAuthority,
        _: &'a VersionedControl,
        _: crate::node::log::CellIssuedRange,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

async fn fixture() -> (CellAuthority, VersionedControl) {
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("enrollment-lifetime"),
        [1; 16],
    );
    let target = CellTarget::new(
        TenantId::from_bytes([2; 16]),
        ApplicationId::from_bytes([1; 16]),
        NamespaceId::from_bytes([3; 16]),
        b"cell",
    )
    .unwrap();
    let proof = CellCatalog::new(layout.clone(), target.tenant())
        .provision(
            CatalogEntry::new(
                &target,
                CatalogRole::Application,
                Digest::from_bytes([4; 32]),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let authority = CellAuthority::new(layout);
    let control = authority
        .create_initial(
            &proof,
            IncarnationId::from_bytes([5; 16]),
            Owner {
                session: SessionId::from_bytes([6; 16]),
                endpoint: "https://enrollment.internal".into(),
            },
        )
        .await
        .unwrap();
    (authority, control)
}

#[tokio::test]
async fn cancelled_waiter_and_cancelled_join_cannot_abandon_the_accepted_callback() {
    let (cell_authority, control) = fixture().await;
    let enrollment = Arc::new(Enrollment::default());
    let original = Arc::new(Held::default());
    let authority: Arc<dyn NodeBundleAuthority> = original.clone();
    let resources = ResourceLedger::new(ResourceCost::zero().with_retained_bytes(2 << 20));
    let lease = NodeLeaseGuard::new(1_000, 31_000).unwrap();
    let caller = {
        let enrollment = enrollment.clone();
        let authority = authority.clone();
        let lease = lease.clone();
        let resources = resources.clone();
        let cell_authority = cell_authority.clone();
        let control = control.clone();
        tokio::spawn(async move {
            enrollment
                .bind(
                    &authority,
                    &lease,
                    Some(&resources),
                    &cell_authority,
                    &control,
                )
                .await
        })
    };
    original.entered.notified().await;
    caller.abort();
    assert!(matches!(caller.await, Err(source) if source.is_cancelled()));
    assert_eq!(original.completed.load(Ordering::SeqCst), 0);
    assert!(resources.snapshot().unwrap().used.retained_bytes() > 0);
    let joining = {
        let enrollment = enrollment.clone();
        tokio::spawn(async move { enrollment.join().await })
    };
    // Join must close admission before awaiting the still-held callback.
    loop {
        if enrollment.state.lock().unwrap().joining {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(!joining.is_finished());
    joining.abort();
    assert!(joining.await.unwrap_err().is_cancelled());
    assert!(matches!(
        enrollment
            .bind(
                &authority,
                &lease,
                Some(&resources),
                &cell_authority,
                &control
            )
            .await,
        Err(Error::RuntimeClosed)
    ));
    original.resume.notify_one();
    enrollment.join().await.unwrap();
    assert_eq!(original.calls.load(Ordering::SeqCst), 1);
    assert_eq!(original.completed.load(Ordering::SeqCst), 1);
    assert_eq!(resources.snapshot().unwrap().used.retained_bytes(), 0);
}

#[tokio::test]
async fn insufficient_memory_refuses_before_accepting_callback_or_cloning_a_queue() {
    let (cell_authority, control) = fixture().await;
    let enrollment = Enrollment::default();
    let original = Arc::new(Held::default());
    let authority: Arc<dyn NodeBundleAuthority> = original.clone();
    let resources = ResourceLedger::new(ResourceCost::zero().with_retained_bytes(1));
    let lease = NodeLeaseGuard::new(1_000, 31_000).unwrap();
    assert!(matches!(
        enrollment
            .bind(
                &authority,
                &lease,
                Some(&resources),
                &cell_authority,
                &control
            )
            .await,
        Err(Error::Capacity(_))
    ));
    assert_eq!(original.calls.load(Ordering::SeqCst), 0);
    assert_eq!(resources.snapshot().unwrap().used.retained_bytes(), 0);
    enrollment.join().await.unwrap();
}

#[tokio::test]
async fn legacy_callback_releases_first_result_while_the_second_callback_is_held() {
    let (cell_authority, control) = fixture().await;
    let enrollment = Arc::new(Enrollment::default());
    let original = Arc::new(Held::default());
    let authority: Arc<dyn NodeBundleAuthority> = original.clone();
    let lease = NodeLeaseGuard::new(1_000, 31_000).unwrap();
    let spawn = || {
        let enrollment = enrollment.clone();
        let authority = authority.clone();
        let lease = lease.clone();
        let cell_authority = cell_authority.clone();
        let control = control.clone();
        tokio::spawn(async move {
            enrollment
                .bind(&authority, &lease, None, &cell_authority, &control)
                .await
        })
    };
    let first = spawn();
    let second = spawn();
    original.entered.notified().await;
    assert_eq!(original.cohort.load(Ordering::SeqCst), 2);
    original.resume.notify_one();
    original.entered.notified().await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), first)
            .await
            .unwrap()
            .unwrap()
            .is_ok()
    );
    assert!(!second.is_finished());
    original.resume.notify_one();
    assert!(second.await.unwrap().is_ok());
    enrollment.join().await.unwrap();
}
