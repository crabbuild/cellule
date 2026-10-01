#[cfg(feature = "replica")]
use std::sync::atomic::AtomicU64;

use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use super::*;
#[cfg(feature = "replica")]
use crate::environment::directory_cache::DirectoryCache;
#[cfg(feature = "replica")]
use crate::environment::executor::TokioExecutor;

#[cfg(feature = "replica")]
#[tokio::test]
async fn directory_cache_fill_does_not_queue_bytes_behind_busy_jobs() {
    let directory = tempfile::TempDir::new().unwrap();
    let jobs = Arc::new(tokio::sync::Semaphore::new(1));
    let host = Host::default()
        .with_job_slots(jobs.clone())
        .with_directory_cache(directory.path().join("cache"))
        .await
        .unwrap();
    let occupied = jobs.acquire().await.unwrap();
    host.directory_cache_put("node".into(), b"verified".to_vec(), 64)
        .unwrap();
    assert_eq!(host.directory_cache_stats().unwrap().entries(), 0);
    drop(occupied);
    host.directory_cache_put("node".into(), b"verified".to_vec(), 64)
        .unwrap();
    host.drain_cache_fills().await;
    assert_eq!(host.directory_cache_stats().unwrap().entries(), 1);
}

#[cfg(feature = "replica")]
#[tokio::test]
async fn cache_fills_deduplicate_bound_dispatch_and_release_dropped_jobs() {
    #[derive(Default)]
    struct HeldExecutor(std::sync::Mutex<Vec<Box<dyn FnOnce() + Send>>>);
    impl Executor for HeldExecutor {
        fn dispatch(&self, job: Box<dyn FnOnce() + Send>) -> io::Result<()> {
            self.0.lock().unwrap().push(job);
            Ok(())
        }
        fn start_worker(&self, job: Box<dyn FnOnce() + Send>) -> io::Result<Box<dyn Worker>> {
            TokioExecutor.start_worker(job)
        }
    }
    let directory = tempfile::TempDir::new().unwrap();
    let jobs = Arc::new(tokio::sync::Semaphore::new(2));
    let filesystem = Arc::new(FaultFs::default());
    let executor = Arc::new(HeldExecutor::default());
    let host = Host::default()
        .with_filesystem(filesystem.clone())
        .with_job_slots(jobs.clone())
        .with_directory_cache(directory.path().join("cache"))
        .await
        .unwrap()
        .with_executor(executor.clone());
    for _ in 0..100 {
        host.directory_cache_put("same".into(), b"verified".to_vec(), 64)
            .unwrap();
    }
    assert_eq!(executor.0.lock().unwrap().len(), 1);
    host.directory_cache_put("second".into(), b"verified".to_vec(), 64)
        .unwrap();
    host.directory_cache_put("overflow".into(), b"verified".to_vec(), 64)
        .unwrap();
    assert_eq!(executor.0.lock().unwrap().len(), 2);
    assert_eq!(jobs.available_permits(), 0);
    assert!(
        host.directory_cache_get("same".into(), 64)
            .await
            .unwrap()
            .is_none()
    );
    executor.0.lock().unwrap().clear();
    tokio::time::timeout(Duration::from_secs(1), host.drain_cache_fills())
        .await
        .unwrap();
    assert_eq!(jobs.available_permits(), 2);
    assert_eq!(host.directory_cache_stats().unwrap().entries(), 0);

    for (reject, panic) in [(true, false), (false, true), (false, false)] {
        filesystem.reject_create.store(reject, Ordering::SeqCst);
        filesystem.panic_create.store(panic, Ordering::SeqCst);
        host.directory_cache_put("same".into(), b"verified".to_vec(), 64)
            .unwrap();
        let job = executor.0.lock().unwrap().pop().unwrap();
        job();
        tokio::time::timeout(Duration::from_secs(1), host.drain_cache_fills())
            .await
            .unwrap();
        assert_eq!(jobs.available_permits(), 2);
    }
    assert_eq!(host.directory_cache_stats().unwrap().entries(), 1);
}

#[test]
fn disk_budget_reservations_resize_and_release_exact_bytes() {
    let budget = DiskBudget::new(10);
    let first = budget.try_reserve(4).unwrap();
    let second = budget.try_reserve(6).unwrap();
    assert_eq!(budget.available(), 0);
    assert!(matches!(
        budget.try_reserve(1),
        Err(crate::LtxError::Limit(crate::LimitKind::LocalDiskBytes))
    ));

    first.resize(2).unwrap();
    assert_eq!(budget.available(), 2);
    drop(second);
    assert_eq!(budget.available(), 8);
    drop(first);
    assert_eq!(budget.available(), 10);
}

#[test]
fn prepared_disk_budget_divides_parent_credit_without_double_charging() {
    let parent = DiskBudget::new(10);
    let child = parent.try_reserve(10).unwrap().into_budget();
    assert_eq!(parent.used(), 10);
    assert_eq!(child.capacity(), 10);
    assert_eq!(child.used(), 0);
    let first = child.try_reserve(6).unwrap();
    let second = child.try_reserve(4).unwrap();
    assert_eq!(parent.used(), 10);
    assert_eq!(child.used(), 10);
    assert!(child.try_reserve(1).is_err());
    assert!(parent.try_reserve(1).is_err());
    first.resize(3).unwrap();
    assert_eq!(child.used(), 7);
    assert_eq!(parent.used(), 10);
    drop(second);
    drop(child);
    // The live child token retains the prepared envelope after its caller exits.
    assert_eq!(parent.used(), 10);
    drop(first);
    assert_eq!(parent.used(), 0);
}

#[test]
fn prepared_disk_budget_releases_unused_credit_and_accounts_later_growth() {
    let parent = DiskBudget::new(10);
    let child = parent.try_reserve(10).unwrap().into_budget();
    let file = child.try_reserve(6).unwrap();
    child.finish_preparation().unwrap();
    assert_eq!(parent.used(), 6);
    child.finish_preparation().unwrap();
    let competing = parent.try_reserve(4).unwrap();
    assert!(file.try_grow(1).is_err());
    assert_eq!(file.bytes(), 6);
    assert_eq!(child.used(), 6);
    assert_eq!(parent.used(), 10);
    drop(competing);
    file.try_grow(4).unwrap();
    assert_eq!(parent.used(), 10);
    assert_eq!(child.used(), 10);
    assert!(file.try_grow(1).is_err());
    file.resize(2).unwrap();
    assert_eq!(parent.used(), 2);
    assert_eq!(child.used(), 2);
    drop(file);
    assert_eq!(parent.used(), 0);
    assert_eq!(child.used(), 0);
    assert!(parent.finish_preparation().is_err());
}

#[test]
fn prepared_disk_budget_retains_parent_admission_and_refuses_rebinding() {
    #[derive(Default)]
    struct Recorded(std::sync::atomic::AtomicU64);
    impl DiskBudgetAdmission for Recorded {
        fn reconcile(&self, bytes: u64) -> crate::Result<()> {
            self.0.store(bytes, Ordering::SeqCst);
            Ok(())
        }
    }
    let parent = DiskBudget::new(10);
    let admission = Arc::new(Recorded::default());
    parent.install_admission(admission.clone()).unwrap();
    let child = parent.try_reserve(10).unwrap().into_budget();
    let file = child.try_reserve(3).unwrap();
    assert_eq!(admission.0.load(Ordering::SeqCst), 10);
    assert!(child.install_admission(admission.clone()).is_err());
    child.finish_preparation().unwrap();
    assert_eq!(admission.0.load(Ordering::SeqCst), 3);
    file.resize(7).unwrap();
    assert_eq!(admission.0.load(Ordering::SeqCst), 7);
    drop(file);
    assert_eq!(admission.0.load(Ordering::SeqCst), 0);
}

#[test]
fn prepared_disk_budget_nested_envelopes_keep_each_parent_bound() {
    let parent = DiskBudget::new(12);
    let child = parent.try_reserve(10).unwrap().into_budget();
    let grandchild = child.try_reserve(8).unwrap().into_budget();
    let file = grandchild.try_reserve(4).unwrap();
    grandchild.finish_preparation().unwrap();
    assert_eq!(grandchild.used(), 4);
    assert_eq!(child.used(), 4);
    assert_eq!(parent.used(), 10);
    child.finish_preparation().unwrap();
    assert_eq!(parent.used(), 4);
    file.resize(8).unwrap();
    assert_eq!(parent.used(), 8);
    assert!(file.resize(9).is_err());
    assert_eq!(parent.used(), 8);
    drop(file);
    assert_eq!(parent.used(), 0);
}

#[test]
fn prepared_disk_budget_concurrent_children_keep_exact_parent_accounting() {
    let parent = DiskBudget::new(16);
    let child = parent.try_reserve(16).unwrap().into_budget();
    let admitted = Arc::new(std::sync::Barrier::new(9));
    let release = Arc::new(std::sync::Barrier::new(9));
    std::thread::scope(|threads| {
        for _ in 0..8 {
            let child = child.clone();
            let admitted = admitted.clone();
            let release = release.clone();
            threads.spawn(move || {
                let file = child.try_reserve(1).unwrap();
                admitted.wait();
                release.wait();
                file.resize(2).unwrap();
            });
        }
        admitted.wait();
        assert_eq!(child.used(), 8);
        assert_eq!(parent.used(), 16);
        child.finish_preparation().unwrap();
        assert_eq!(parent.used(), 8);
        release.wait();
    });
    assert_eq!(child.used(), 0);
    assert_eq!(parent.used(), 0);
}

#[test]
fn prepared_disk_budget_zero_credit_and_overflow_fail_without_parent_leaks() {
    let parent = DiskBudget::new(u64::MAX);
    let zero = parent.try_reserve(0).unwrap().into_budget();
    assert!(zero.try_reserve(1).is_err());
    zero.finish_preparation().unwrap();
    assert_eq!(parent.used(), 0);
    let child = parent.try_reserve(u64::MAX).unwrap().into_budget();
    let file = child.try_reserve(u64::MAX).unwrap();
    assert!(file.try_grow(1).is_err());
    assert_eq!(parent.used(), u64::MAX);
    child.finish_preparation().unwrap();
    drop(file);
    assert_eq!(parent.used(), 0);
}

#[test]
fn prepared_disk_budget_failed_parent_hook_preserves_credit_and_source_error() {
    struct Refusing(AtomicBool);
    impl DiskBudgetAdmission for Refusing {
        fn reconcile(&self, _bytes: u64) -> crate::Result<()> {
            if self.0.load(Ordering::SeqCst) {
                Err(crate::LtxError::Io(io::Error::new(
                    io::ErrorKind::StorageFull,
                    "injected parent admission failure",
                )))
            } else {
                Ok(())
            }
        }
    }
    let parent = DiskBudget::new(10);
    let admission = Arc::new(Refusing(AtomicBool::new(false)));
    parent.install_admission(admission.clone()).unwrap();
    let child = parent.try_reserve(10).unwrap().into_budget();
    let file = child.try_reserve(3).unwrap();
    admission.0.store(true, Ordering::SeqCst);
    assert!(matches!(
        child.finish_preparation(),
        Err(crate::LtxError::Io(error)) if error.kind() == io::ErrorKind::StorageFull
    ));
    assert_eq!(parent.used(), 10);
    assert_eq!(child.used(), 3);
    assert_eq!(file.bytes(), 3);
    admission.0.store(false, Ordering::SeqCst);
    child.finish_preparation().unwrap();
    assert_eq!(parent.used(), 3);
    admission.0.store(true, Ordering::SeqCst);
    assert!(matches!(
        file.resize(4),
        Err(crate::LtxError::Io(error)) if error.kind() == io::ErrorKind::StorageFull
    ));
    assert_eq!(parent.used(), 3);
    assert_eq!(child.used(), 3);
    assert_eq!(file.bytes(), 3);
    admission.0.store(false, Ordering::SeqCst);
    drop(file);
    assert_eq!(parent.used(), 0);
}

#[test]
fn prepared_disk_budget_competing_scopes_reconcile_the_latest_parent_usage() {
    #[derive(Default)]
    struct Recorded(std::sync::atomic::AtomicU64);
    impl DiskBudgetAdmission for Recorded {
        fn reconcile(&self, bytes: u64) -> crate::Result<()> {
            self.0.store(bytes, Ordering::SeqCst);
            Ok(())
        }
    }
    let parent = DiskBudget::new(16);
    let admission = Arc::new(Recorded::default());
    parent.install_admission(admission.clone()).unwrap();
    for _ in 0..32 {
        let admitted = Arc::new(std::sync::Barrier::new(9));
        let release = Arc::new(std::sync::Barrier::new(9));
        std::thread::scope(|threads| {
            for _ in 0..8 {
                let parent = parent.clone();
                let admitted = admitted.clone();
                let release = release.clone();
                threads.spawn(move || {
                    let child = parent.try_reserve(2).unwrap().into_budget();
                    let file = child.try_reserve(1).unwrap();
                    child.finish_preparation().unwrap();
                    admitted.wait();
                    release.wait();
                    drop(file);
                });
            }
            admitted.wait();
            assert_eq!(parent.used(), 8);
            assert_eq!(admission.0.load(Ordering::SeqCst), 8);
            release.wait();
        });
        assert_eq!(parent.used(), 0);
        assert_eq!(admission.0.load(Ordering::SeqCst), 0);
    }
}

#[cfg(feature = "replica")]
#[test]
fn directory_cache_survives_restart_and_evicts_by_bytes() {
    let directory = tempfile::TempDir::new().unwrap();
    let root = directory.path().join("directory-cache");
    let filesystem: Arc<dyn FileSystem> = Arc::new(DirectFileSystem);
    let cache = DirectoryCache::new(Arc::clone(&filesystem), root.clone(), 5);
    cache.put("first", b"1234", 64).unwrap();
    assert_eq!(cache.budget.used(), 4);
    drop(cache);

    let cache = DirectoryCache::new(Arc::clone(&filesystem), root, 5);
    assert_eq!(cache.get("first", 64).unwrap(), Some(b"1234".to_vec()));
    assert_eq!(cache.stats().entries(), 1);
    assert_eq!(cache.stats().bytes(), 4);
    cache.put("second", b"abcde", 64).unwrap();
    assert!(cache.get("first", 64).unwrap().is_none());
    assert_eq!(cache.get("second", 64).unwrap(), Some(b"abcde".to_vec()));
    assert_eq!(cache.stats().entries(), 1);
    assert_eq!(cache.budget.used(), 5);
}

#[cfg(feature = "replica")]
#[test]
fn directory_cache_reaccounts_a_file_without_membership() {
    let directory = tempfile::TempDir::new().unwrap();
    let cache = DirectoryCache::new(Arc::new(DirectFileSystem), directory.path().to_owned(), 64);
    // A fill can install its bytes before the membership index is persisted.
    // Reusing that file after restart must still acquire its disk reservation.
    std::fs::write(cache.key_path("orphan"), b"verified").unwrap();
    cache.put("orphan", b"verified", 64).unwrap();
    assert_eq!(cache.budget.used(), 8);
    assert_eq!(cache.get("orphan", 64).unwrap(), Some(b"verified".to_vec()));
}

#[cfg(feature = "replica")]
#[test]
fn directory_cache_reads_do_not_rewrite_unchanged_membership() {
    let directory = tempfile::TempDir::new().unwrap();
    let filesystem = Arc::new(FaultFs::default());
    let cache = DirectoryCache::new(filesystem.clone(), directory.path().to_owned(), 64);
    cache.put("present", b"verified", 64).unwrap();
    filesystem.creates.store(0, Ordering::SeqCst);

    for (key, expected) in [("present", Some(b"verified".to_vec())), ("absent", None)] {
        assert_eq!(cache.get(key, 64).unwrap(), expected);
        assert_eq!(filesystem.creates.load(Ordering::SeqCst), 0, "{key}");
    }
}

#[cfg(feature = "replica")]
#[test]
fn directory_cache_discards_truncated_and_symlink_entries() {
    let directory = tempfile::TempDir::new().unwrap();
    let root = directory.path().join("directory-cache");
    let filesystem: Arc<dyn FileSystem> = Arc::new(DirectFileSystem);
    let cache = DirectoryCache::new(Arc::clone(&filesystem), root.clone(), 64);
    cache.put("entry", b"verified", 64).unwrap();
    let path = cache.key_path("entry");
    std::fs::write(&path, b"short").unwrap();
    assert!(cache.get("entry", 64).unwrap().is_none());

    let target = directory.path().join("outside");
    std::fs::write(&target, b"outside").unwrap();
    let symlink = cache.key_path("symlink");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&target, &symlink).unwrap();
    #[cfg(unix)]
    assert!(cache.get("symlink", 64).unwrap().is_none());
}

#[cfg(feature = "replica")]
#[test]
fn directory_cache_cleans_abandoned_private_temporaries_on_restart() {
    let directory = tempfile::TempDir::new().unwrap();
    let root = directory.path().join("directory-cache");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join(".tmp-old"), b"partial").unwrap();
    std::fs::write(root.join(".index-tmp-old"), b"partial").unwrap();
    std::fs::write(root.join("unrelated"), b"keep").unwrap();
    let filesystem: Arc<dyn FileSystem> = Arc::new(DirectFileSystem);
    let _cache = DirectoryCache::new(filesystem, root.clone(), 64);
    assert!(!root.join(".tmp-old").exists());
    assert!(!root.join(".index-tmp-old").exists());
    assert!(root.join("unrelated").exists());
}

#[cfg(feature = "replica")]
#[test]
fn directory_cache_serializes_concurrent_fills_for_one_key() {
    let directory = tempfile::TempDir::new().unwrap();
    let root = directory.path().join("directory-cache");
    let filesystem: Arc<dyn FileSystem> = Arc::new(DirectFileSystem);
    let cache = Arc::new(DirectoryCache::new(Arc::clone(&filesystem), root, 64));
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            scope.spawn(move || {
                cache.put("same-key", b"verified", 64).unwrap();
            });
        }
    });
    assert_eq!(
        cache.get("same-key", 64).unwrap(),
        Some(b"verified".to_vec())
    );
    assert_eq!(cache.stats().entries(), 1);
    assert_eq!(cache.stats().bytes(), 8);
    assert_eq!(cache.budget.used(), 8);
}

#[cfg(feature = "replica")]
#[test]
fn directory_cache_concurrent_fills_preserve_restart_membership() {
    let directory = tempfile::TempDir::new().unwrap();
    let root = directory.path().join("directory-cache");
    let (entered, blocked) = std::sync::mpsc::channel();
    let (resume, released) = std::sync::mpsc::channel();
    let filesystem = Arc::new(FaultFs::default());
    *filesystem.index_pause.lock().unwrap() = Some(IndexPause { entered, released });
    let cache = Arc::new(DirectoryCache::new(filesystem.clone(), root.clone(), 64));
    std::thread::scope(|scope| {
        let first_cache = cache.clone();
        let first = scope.spawn(move || first_cache.put("first", b"first-node", 64).unwrap());
        blocked.recv_timeout(Duration::from_secs(5)).unwrap();
        // Hold the first membership snapshot immediately before rename. A
        // second key can fill concurrently, but cannot publish its index ahead
        // of that older snapshot and then be lost when the first write resumes.
        let second_cache = cache.clone();
        let (finished, completion) = std::sync::mpsc::channel();
        let second = scope.spawn(move || {
            second_cache.put("second", b"second-node", 64).unwrap();
            finished.send(()).unwrap();
        });
        let overtook = completion.recv_timeout(Duration::from_millis(250)).is_ok();
        resume.send(()).unwrap();
        first.join().unwrap();
        second.join().unwrap();
        drop(cache);
        let restarted = DirectoryCache::new(filesystem, root, 64);
        assert_eq!(
            restarted.stats().entries(),
            2,
            "a later fill disappeared from the persisted index"
        );
        assert_eq!(
            restarted.get("first", 64).unwrap(),
            Some(b"first-node".to_vec())
        );
        assert_eq!(
            restarted.get("second", 64).unwrap(),
            Some(b"second-node".to_vec())
        );
        assert!(!overtook, "a newer index overtook a paused older snapshot");
    });
}

#[cfg(feature = "replica")]
struct IndexPause {
    entered: std::sync::mpsc::Sender<()>,
    released: std::sync::mpsc::Receiver<()>,
}

struct TestClock;
impl Clock for TestClock {
    fn unix_millis(&self) -> i64 {
        123456789
    }
    fn file_age(&self, _: &Path) -> io::Result<Duration> {
        Ok(Duration::ZERO)
    }
}

#[derive(Default)]
struct FaultFs {
    reject_create: AtomicBool,
    panic_create: AtomicBool,
    creates: AtomicUsize,
    #[cfg(feature = "replica")]
    index_pause: std::sync::Mutex<Option<IndexPause>>,
}
impl FileSystem for FaultFs {
    fn open(&self, path: &Path) -> io::Result<Box<dyn FileIo>> {
        DirectFileSystem.open(path)
    }
    fn open_rw(&self, path: &Path) -> io::Result<Box<dyn FileIo>> {
        DirectFileSystem.open_rw(path)
    }
    fn create(&self, path: &Path) -> io::Result<Box<dyn FileIo>> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.panic_create.load(Ordering::SeqCst),
            "injected cache panic"
        );
        if self.reject_create.load(Ordering::SeqCst) {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "injected artifact failure",
            ));
        }
        DirectFileSystem.create(path)
    }
    fn file_len(&self, path: &Path) -> io::Result<u64> {
        DirectFileSystem.file_len(path)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.create_dir_all(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        #[cfg(feature = "replica")]
        if to.file_name().is_some_and(|name| name == "index-v1.json") {
            let pause = self.index_pause.lock().unwrap().take();
            if let Some(pause) = pause {
                pause.entered.send(()).unwrap();
                pause.released.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        }
        DirectFileSystem.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.remove_file(path)
    }
    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        DirectFileSystem.canonicalize(path)
    }
    fn exists(&self, path: &Path) -> io::Result<bool> {
        DirectFileSystem.exists(path)
    }
    fn create_dir(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.create_dir(path)
    }
    fn sync_parent(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.sync_parent(path)
    }
    fn persist_new(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        DirectFileSystem.persist_new(path, bytes)
    }
    fn persist_file_new(&self, source: &Path, destination: &Path) -> io::Result<()> {
        DirectFileSystem.persist_file_new(source, destination)
    }
}

#[test]
fn injected_clock_and_capture_filesystem_reach_real_sqlite_transactions() {
    let directory = tempfile::TempDir::new().unwrap();
    let filesystem = Arc::new(FaultFs::default());
    let host = Host::default()
        .with_clock(Arc::new(TestClock))
        .with_filesystem(filesystem.clone());
    let mut db = crate::Db::open_with_host(
        &directory.path().join("db.sqlite"),
        crate::Limits::default(),
        host,
    )
    .unwrap();
    db.transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(1)"))
        .unwrap();
    let batch = db.capture().unwrap();
    let bytes = std::fs::read(batch.segments[0].path()).unwrap();
    assert_eq!(
        crate::ltx::Header::parse(&bytes).unwrap().timestamp,
        123456789
    );
    db.transaction(|tx| tx.execute_batch("INSERT INTO t VALUES(2)"))
        .unwrap();
    filesystem.reject_create.store(true, Ordering::SeqCst);
    assert!(
        matches!(db.capture(), Err(crate::LtxError::Io(error)) if error.kind() == io::ErrorKind::StorageFull)
    );
    assert!(matches!(
        db.transaction(|_| Ok(())),
        Err(crate::LtxError::Fenced)
    ));
}

#[test]
fn synced_scratch_install_never_replaces_a_destination() {
    let directory = tempfile::TempDir::new().unwrap();
    let destination = directory.path().join("database.sqlite");
    let first = directory.path().join("first.scratch");
    let mut file = DirectFileSystem.create(&first).unwrap();
    file.write_all(b"first").unwrap();
    file.sync_all().unwrap();
    drop(file);
    DirectFileSystem
        .persist_file_new(&first, &destination)
        .unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), b"first");
    assert!(!first.exists());

    let second = directory.path().join("second.scratch");
    let mut file = DirectFileSystem.create(&second).unwrap();
    file.write_all(b"second").unwrap();
    file.sync_all().unwrap();
    drop(file);
    assert_eq!(
        DirectFileSystem
            .persist_file_new(&second, &destination)
            .unwrap_err()
            .kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(std::fs::read(destination).unwrap(), b"first");
    assert_eq!(std::fs::read(second).unwrap(), b"second");
}

#[cfg(feature = "replica")]
#[tokio::test]
async fn dropped_executor_jobs_return_errors_without_hanging() {
    struct DroppingExecutor;
    impl Executor for DroppingExecutor {
        fn dispatch(&self, _: Box<dyn FnOnce() + Send>) -> io::Result<()> {
            Ok(())
        }
        fn start_worker(&self, job: Box<dyn FnOnce() + Send>) -> io::Result<Box<dyn Worker>> {
            TokioExecutor.start_worker(job)
        }
    }
    let host = Host::default().with_executor(Arc::new(DroppingExecutor));
    assert!(host.run(|| 42).await.is_err());
    assert_eq!(Host::default().run(|| 42).await.unwrap(), 42);
    let directory = tempfile::TempDir::new().unwrap();
    let cache = Host::default()
        .with_directory_cache(directory.path().join("cache"))
        .await
        .unwrap()
        .with_executor(Arc::new(DroppingExecutor));
    cache
        .directory_cache_put("node".into(), b"verified".to_vec(), 64)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), cache.drain_cache_fills())
        .await
        .unwrap();
    assert_eq!(cache.directory_cache_stats().unwrap().entries(), 0);
}

#[cfg(feature = "replica")]
#[tokio::test]
async fn rejected_cache_dispatch_releases_its_key_and_admission() {
    struct RejectExecutor;
    impl Executor for RejectExecutor {
        fn dispatch(&self, _: Box<dyn FnOnce() + Send>) -> io::Result<()> {
            Err(io::Error::other("injected executor rejection"))
        }
        fn start_worker(&self, job: Box<dyn FnOnce() + Send>) -> io::Result<Box<dyn Worker>> {
            TokioExecutor.start_worker(job)
        }
    }
    let directory = tempfile::TempDir::new().unwrap();
    let jobs = Arc::new(tokio::sync::Semaphore::new(1));
    let host = Host::default()
        .with_job_slots(jobs.clone())
        .with_directory_cache(directory.path().join("cache"))
        .await
        .unwrap();
    assert!(
        host.clone()
            .with_executor(Arc::new(RejectExecutor))
            .directory_cache_put("node".into(), b"verified".to_vec(), 64)
            .is_err()
    );
    assert_eq!(jobs.available_permits(), 1);
    host.directory_cache_put("node".into(), b"verified".to_vec(), 64)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), host.drain_cache_fills())
        .await
        .unwrap();
    assert_eq!(host.directory_cache_stats().unwrap().entries(), 1);
}

#[cfg(feature = "replica")]
#[tokio::test(flavor = "multi_thread")]
async fn cancelled_waiters_do_not_release_running_job_or_recovery_admission() {
    let jobs = Arc::new(tokio::sync::Semaphore::new(1));
    let recovery = Arc::new(tokio::sync::Semaphore::new(1));
    let dirty = Arc::new(tokio::sync::Semaphore::new(1));
    let scratch = Arc::new(tokio::sync::Semaphore::new(1));
    let host = Host::default()
        .with_job_slots(jobs.clone())
        .with_recovery_slots(recovery.clone())
        .with_dirty_slots(dirty.clone())
        .with_scratch_slots(scratch.clone());
    let scope = host
        .for_recovery()
        .await
        .unwrap()
        .for_scratch(1 << 20)
        .await
        .unwrap();
    let (started, entered) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let task = tokio::spawn(async move {
        scope
            .run(move || {
                let _ = started.send(());
                let _ = blocked.recv();
            })
            .await
    });
    entered.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(jobs.available_permits(), 0);
    assert_eq!(recovery.available_permits(), 0);
    assert_eq!(dirty.available_permits(), 0);
    assert_eq!(scratch.available_permits(), 0);
    release.send(()).unwrap();
    let _job = tokio::time::timeout(Duration::from_secs(2), jobs.acquire())
        .await
        .unwrap()
        .unwrap();
    let _recovery = tokio::time::timeout(Duration::from_secs(2), recovery.acquire())
        .await
        .unwrap()
        .unwrap();
    let _dirty = tokio::time::timeout(Duration::from_secs(2), dirty.acquire())
        .await
        .unwrap()
        .unwrap();
    let _scratch = tokio::time::timeout(Duration::from_secs(2), scratch.acquire())
        .await
        .unwrap()
        .unwrap();
}

#[cfg(feature = "replica")]
#[tokio::test]
async fn closed_admission_returns_errors_instead_of_panicking() {
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    slots.close();
    let host = Host::default()
        .with_io_slots(slots.clone())
        .with_job_slots(slots.clone())
        .with_recovery_slots(slots.clone())
        .with_scratch_slots(slots);
    assert!(host.run(|| 1).await.is_err());
    assert!(host.io_permit().await.is_err());
    assert!(host.for_recovery().await.is_err());
    assert!(host.for_scratch(1 << 20).await.is_err());
}

#[cfg(feature = "replica")]
#[tokio::test]
async fn scratch_monitor_rechecks_total_reservation_and_releases_rejection() {
    struct RecordingScratch {
        bytes: AtomicU64,
        reject: AtomicBool,
    }
    impl ScratchMonitor for RecordingScratch {
        fn ensure_available(&self, reserved_bytes: u64) -> io::Result<()> {
            self.bytes.store(reserved_bytes, Ordering::Release);
            if self.reject.load(Ordering::Acquire) {
                return Err(io::Error::from(io::ErrorKind::StorageFull));
            }
            Ok(())
        }
    }

    let slots = Arc::new(tokio::sync::Semaphore::new(3));
    let monitor = Arc::new(RecordingScratch {
        bytes: AtomicU64::new(0),
        reject: AtomicBool::new(false),
    });
    let host = Host::default()
        .with_scratch_slots(slots.clone())
        .with_scratch_monitor(monitor.clone());
    let first = host.for_scratch(1 << 20).await.unwrap();
    assert_eq!(monitor.bytes.load(Ordering::Acquire), 1 << 20);
    let second = host.for_scratch(2 << 20).await.unwrap();
    assert_eq!(monitor.bytes.load(Ordering::Acquire), 3 << 20);
    drop((first, second));

    monitor.reject.store(true, Ordering::Release);

    assert!(matches!(
        host.for_scratch(1 << 20).await,
        Err(crate::LtxError::Io(error)) if error.kind() == io::ErrorKind::StorageFull
    ));
    assert_eq!(monitor.bytes.load(Ordering::Acquire), 1 << 20);
    assert_eq!(slots.available_permits(), 3);
}
