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
    let compacted = replica
        .prepare_compaction(&second.root(), 0..2, 1, directory.path())
        .await
        .unwrap();
    assert_ne!(compacted.root(), second.root());
    database
        .transaction(|transaction| {
            transaction.execute("UPDATE counter SET value = 9", [])?;
            Ok(())
        })
        .unwrap();
    let third = replica
        .prepare_after_compaction(&compacted, &database.capture().unwrap(), 3, 1)
        .await
        .unwrap();
    assert_eq!(third.predecessor(), Some(second.root()));
    assert_eq!(
        metadata.observed.lock().unwrap().last().copied(),
        Some(third.preparation()),
        "native metadata must observe the final rebased predecessor"
    );
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
async fn composed_metadata_has_only_final_derivation_and_preserves_failure() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let mut database =
        Db::open(&directory.path().join("composed.sqlite"), Limits::default()).unwrap();
    let metadata = Arc::new(Metadata::default());
    let replica = replica(Store::new(Arc::new(InMemory::new())), [5; 32], [6; 16]);
    let mut root = None;
    for sequence in 1..=8 {
        database
            .transaction(|tx| {
                if sequence == 1 {
                    tx.execute_batch("CREATE TABLE counter(value); INSERT INTO counter VALUES(0)")?;
                }
                tx.execute("UPDATE counter SET value=?1", [sequence])?;
                Ok(())
            })
            .unwrap();
        root = Some(
            replica
                .prepare(root.as_ref(), &database.capture().unwrap(), sequence, 1)
                .await
                .unwrap()
                .root(),
        );
    }
    let root = root.unwrap();
    database
        .transaction(|tx| tx.execute_batch("UPDATE counter SET value=9"))
        .unwrap();
    let cuts = database.capture().unwrap();
    let failed = replica.clone().with_root_metadata(Arc::new(FailedMetadata));
    let error = match failed
        .prepare_scheduled_compaction_append(&root, &cuts, 9, 1, 32, scratch.path())
        .await
    {
        Ok(_) => panic!("failed composed metadata returned a proposal"),
        Err(error) => error,
    };
    let LtxError::RootPreparation { source } = error else {
        panic!("lost composed metadata source")
    };
    assert_eq!(
        source.downcast::<std::io::Error>().unwrap().kind(),
        std::io::ErrorKind::PermissionDenied
    );
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
    let prepared = replica
        .with_root_metadata(metadata.clone())
        .prepare_scheduled_compaction_append(&root, &cuts, 9, 1, 32, scratch.path())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prepared.predecessor(), Some(root));
    assert_eq!(
        *metadata.observed.lock().unwrap(),
        [prepared.preparation()],
        "no intermediate compaction derivation may be retained"
    );
    assert_eq!(metadata.active.load(Ordering::SeqCst), 0);
    assert_eq!(std::fs::read_dir(scratch.path()).unwrap().count(), 0);
    database.close().unwrap();
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
