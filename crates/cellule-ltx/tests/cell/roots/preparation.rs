use super::*;
use cellule_ltx::{
    FailureClass, LtxError, RootPreparation, RootPreparationFuture, RootPreparationMetadata,
};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize},
};
use tokio::sync::Notify;

#[derive(Default)]
struct Metadata {
    observed: Mutex<Vec<RootPreparation>>,
    pause: AtomicBool,
    active: AtomicUsize,
    entered: Notify,
    resume: Notify,
}
struct Active<'a>(&'a AtomicUsize);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl RootPreparationMetadata for Metadata {
    fn retain(&self, preparation: RootPreparation) -> RootPreparationFuture<'_> {
        Box::pin(async move {
            self.active.fetch_add(1, Ordering::SeqCst);
            let _active = Active(&self.active);
            self.observed.lock().unwrap().push(preparation);
            if self.pause.load(Ordering::SeqCst) {
                self.entered.notify_one();
                self.resume.notified().await;
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn preparation_metadata_has_exact_native_inputs_and_no_detached_lifetime() {
    let directory = tempfile::tempdir().unwrap();
    let mut database =
        Db::open(&directory.path().join("native.sqlite"), Limits::default()).unwrap();
    database
        .transaction(|transaction| {
            transaction
                .execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(7)")
        })
        .unwrap();
    let metadata = Arc::new(Metadata::default());
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let replica = replica(Store::new(Arc::new(InMemory::new())), [1; 32], [2; 16])
        .with_host(Host::default().with_io_slots(slots.clone()))
        .with_root_metadata(metadata.clone());
    let cuts = database.capture().unwrap();
    metadata.pause.store(true, Ordering::SeqCst);
    {
        let preparation = replica.prepare(None, &cuts, 1, 1);
        tokio::pin!(preparation);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::select! {
                result = &mut preparation => panic!("paused preparation escaped: {}", result.is_ok()),
                _ = metadata.entered.notified() => {}
            }
        }).await.unwrap();
        assert_eq!(metadata.active.load(Ordering::SeqCst), 1);
        assert_eq!(slots.available_permits(), 0);
    }
    assert_eq!(
        metadata.active.load(Ordering::SeqCst),
        0,
        "dropping caller future must drop inline metadata work"
    );
    assert_eq!(slots.available_permits(), 1);
    metadata.pause.store(false, Ordering::SeqCst);
    let first = replica.prepare(None, &cuts, 1, 1).await.unwrap();
    assert_eq!(metadata.observed.lock().unwrap()[0], first.preparation());
    assert_eq!(first.preparation().root(), first.root());
    assert_eq!(first.preparation().predecessor(), None);
    database
        .transaction(|transaction| {
            transaction.execute("UPDATE counter SET value = 8", [])?;
            Ok(())
        })
        .unwrap();
    let second = replica
        .prepare(Some(&first.root()), &database.capture().unwrap(), 2, 1)
        .await
        .unwrap();
    assert_eq!(
        metadata.observed.lock().unwrap().last().copied(),
        Some(second.preparation())
    );
    assert_eq!(second.preparation().predecessor(), Some(first.root()));
    assert_eq!(metadata.active.load(Ordering::SeqCst), 0);
    assert_eq!(slots.available_permits(), 1);
    database.close().unwrap();
}

struct FailedMetadata;
impl RootPreparationMetadata for FailedMetadata {
    fn retain(&self, _preparation: RootPreparation) -> RootPreparationFuture<'_> {
        Box::pin(async {
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "original metadata error",
            )) as Box<dyn std::error::Error + Send + Sync>)
        })
    }
}

#[tokio::test]
async fn metadata_error_preserves_source_and_refuses_ready_root() {
    let directory = tempfile::tempdir().unwrap();
    let mut database =
        Db::open(&directory.path().join("native.sqlite"), Limits::default()).unwrap();
    database
        .transaction(|transaction| transaction.execute_batch("CREATE TABLE events(value INTEGER)"))
        .unwrap();
    let replica = replica(Store::new(Arc::new(InMemory::new())), [1; 32], [2; 16])
        .with_root_metadata(Arc::new(FailedMetadata));
    let error = match replica
        .prepare(None, &database.capture().unwrap(), 1, 1)
        .await
    {
        Ok(_) => panic!("failed metadata returned a Ready proposal"),
        Err(error) => error,
    };
    assert_eq!(error.classify(), FailureClass::Ambiguous);
    let LtxError::RootPreparation { source } = error else {
        panic!("lost original metadata source")
    };
    assert_eq!(
        source.downcast::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    database.close().unwrap();
}
