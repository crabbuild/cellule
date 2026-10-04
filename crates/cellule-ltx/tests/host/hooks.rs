#[cfg(feature = "replica")]
use cellule_ltx::environment::{Executor, Worker};
#[cfg(feature = "replica")]
use cellule_ltx::{CellReplica, CellStorageLayout, LocalSegment};
use cellule_ltx::{
    CheckpointMode, Db, Host, Limits, LtxError,
    environment::{DirectFileSystem, FileIo, FileSystem},
};
#[cfg(feature = "replica")]
use cellule_store::Store;
#[cfg(feature = "replica")]
use object_store::throttle::{ThrottleConfig, ThrottledStore};
#[cfg(feature = "replica")]
use object_store::{memory::InMemory, path::Path as ObjectPath};
#[cfg(feature = "replica")]
use std::time::Duration;
use std::{
    collections::BTreeSet,
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

#[cfg(feature = "replica")]
use std::sync::OnceLock;

#[cfg(feature = "replica")]
struct DelayedExecutor {
    delay: Duration,
}

#[cfg(feature = "replica")]
struct TestWorker(std::thread::JoinHandle<()>);

#[cfg(feature = "replica")]
impl Worker for TestWorker {
    fn join(self: Box<Self>) -> io::Result<()> {
        self.0
            .join()
            .map_err(|_| io::Error::other("test worker panicked"))
    }
}

#[cfg(feature = "replica")]
impl Executor for DelayedExecutor {
    fn dispatch(&self, job: Box<dyn FnOnce() + Send>) -> io::Result<()> {
        let delay = self.delay;
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tokio::task::spawn_blocking(job).await;
        });
        Ok(())
    }

    fn start_worker(&self, job: Box<dyn FnOnce() + Send>) -> io::Result<Box<dyn Worker>> {
        Ok(Box::new(TestWorker(std::thread::spawn(job))))
    }
}

struct Pause {
    operation: &'static str,
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    wake: std::sync::Condvar,
}

impl Pause {
    fn wait(&self, operation: &str) {
        if self.operation == operation {
            let mut released = self.released.lock().unwrap();
            self.entered.notify_one();
            while !*released {
                released = self.wake.wait(released).unwrap();
            }
        }
    }
}

struct Release(Arc<Pause>);

impl Drop for Release {
    fn drop(&mut self) {
        *self.0.released.lock().unwrap() = true;
        self.0.wake.notify_all();
    }
}

#[derive(Clone, Default)]
struct Faults {
    failure: Arc<Mutex<Option<&'static str>>>,
    planned: Arc<Mutex<Vec<&'static str>>>,
    calls: Arc<Mutex<BTreeSet<&'static str>>>,
    largest_read: Arc<AtomicUsize>,
    read_calls: Arc<AtomicUsize>,
    checksum_reads: Arc<AtomicUsize>,
    checksum_read_bytes: Arc<AtomicUsize>,
    largest_checksum_read: Arc<AtomicUsize>,
    largest_write: Arc<AtomicUsize>,
    write_calls: Arc<AtomicUsize>,
    file_syncs: Arc<AtomicUsize>,
    parent_syncs: Arc<AtomicUsize>,
    track_all: Arc<AtomicBool>,
    #[cfg(feature = "replica")]
    create_pause: Arc<OnceLock<Arc<InstallPause>>>,
    forbidden_thread: Arc<Mutex<Option<std::thread::ThreadId>>>,
    pause: Arc<Mutex<Option<Arc<Pause>>>>,
    output_sync_started: Arc<AtomicUsize>,
    output_pause: Arc<Mutex<Option<Arc<Pause>>>>,
}

#[cfg(feature = "replica")]
struct InstallPause {
    entered: std::sync::Barrier,
    release: std::sync::Barrier,
}

#[cfg(feature = "replica")]
impl InstallPause {
    fn new() -> Self {
        Self {
            entered: std::sync::Barrier::new(2),
            release: std::sync::Barrier::new(2),
        }
    }
}

impl Faults {
    fn arm(&self, operation: Option<&'static str>) {
        *self.failure.lock().unwrap() = operation;
    }

    /// Arms an ordered list of operations to fail, one injection per match.
    ///
    /// A plan models a sequence of failures across seams — a torn write, then a
    /// failed rename — instead of one armed operation at a time.
    fn plan(&self, operations: impl IntoIterator<Item = &'static str>) {
        *self.planned.lock().unwrap() = operations.into_iter().collect();
    }

    fn check(&self, operation: &'static str) -> io::Result<()> {
        self.calls.lock().unwrap().insert(operation);
        if *self.forbidden_thread.lock().unwrap() == Some(std::thread::current().id()) {
            return Err(io::Error::other("filesystem work on async thread"));
        }
        let pause = self.pause.lock().unwrap().clone();
        if let Some(pause) = pause {
            pause.wait(operation);
        }
        if *self.failure.lock().unwrap() == Some(operation) {
            return Err(io::Error::new(io::ErrorKind::StorageFull, operation));
        }
        let mut planned = self.planned.lock().unwrap();
        if planned.first() == Some(&operation) {
            planned.remove(0);
            return Err(io::Error::new(io::ErrorKind::StorageFull, operation));
        }
        Ok(())
    }
}

struct File {
    inner: Box<dyn FileIo>,
    faults: Faults,
    track: bool,
    checksum: bool,
    output: Option<&'static str>,
}

impl FileIo for File {
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.track {
            self.faults
                .largest_write
                .fetch_max(bytes.len(), Ordering::Relaxed);
            self.faults.write_calls.fetch_add(1, Ordering::Relaxed);
        }
        if let Err(error) = self.faults.check("write_all") {
            self.inner.write_all(&bytes[..bytes.len() / 2])?;
            return Err(error);
        }
        self.inner.write_all(bytes)
    }
    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        if self.track {
            self.faults
                .largest_write
                .fetch_max(bytes.len(), Ordering::Relaxed);
            self.faults.write_calls.fetch_add(1, Ordering::Relaxed);
        }
        if let Err(error) = self.faults.check("write_all_at") {
            self.inner.write_all_at(offset, &bytes[..bytes.len() / 2])?;
            return Err(error);
        }
        self.inner.write_all_at(offset, bytes)
    }
    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        if self.checksum {
            self.faults.checksum_reads.fetch_add(1, Ordering::Relaxed);
            self.faults
                .checksum_read_bytes
                .fetch_add(len, Ordering::Relaxed);
            self.faults
                .largest_checksum_read
                .fetch_max(len, Ordering::Relaxed);
        }
        if self.track {
            self.faults.largest_read.fetch_max(len, Ordering::Relaxed);
            self.faults.read_calls.fetch_add(1, Ordering::Relaxed);
        }
        self.faults.check("read_exact_at")?;
        self.inner.read_exact_at(offset, len)
    }
    fn sync_all(&mut self) -> io::Result<()> {
        if let Some(operation) = self.output {
            self.faults
                .output_sync_started
                .fetch_add(1, Ordering::Relaxed);
            self.faults.check(operation)?;
            let pause = self.faults.output_pause.lock().unwrap().clone();
            if let Some(pause) = pause {
                pause.wait("compaction_output_sync");
            }
        }
        self.faults.check("sync_all")?;
        self.faults.file_syncs.fetch_add(1, Ordering::Relaxed);
        self.inner.sync_all()
    }
    fn file_len(&self) -> io::Result<u64> {
        self.inner.file_len()
    }
    fn set_len(&mut self, len: u64) -> io::Result<()> {
        self.faults.check("set_len")?;
        self.inner.set_len(len)
    }
}

macro_rules! filesystem_operation {
    ($name:ident($($arg:ident: $ty:ty),*) -> $result:ty) => {
        fn $name(&self, $($arg: $ty),*) -> io::Result<$result> {
            self.check(stringify!($name))?;
            DirectFileSystem.$name($($arg),*)
        }
    };
}

impl FileSystem for Faults {
    fn open(&self, path: &Path) -> io::Result<Box<dyn FileIo>> {
        self.check("open")?;
        match is_compaction_output(path) {
            Some("compaction_ltx_sync") => self.check("compaction_ltx_read")?,
            Some("compaction_index_sync") => self.check("compaction_index_read")?,
            _ => {}
        }
        Ok(Box::new(File {
            inner: DirectFileSystem.open(path)?,
            faults: self.clone(),
            checksum: path.to_string_lossy().ends_with(".cellule-ltx-checksums"),
            output: is_compaction_output(path),
            track: self.track_all.load(Ordering::Relaxed)
                || path.to_string_lossy().contains(".ltx"),
        }))
    }
    fn open_rw(&self, path: &Path) -> io::Result<Box<dyn FileIo>> {
        self.check("open_rw")?;
        Ok(Box::new(File {
            inner: DirectFileSystem.open_rw(path)?,
            faults: self.clone(),
            checksum: path.to_string_lossy().ends_with(".cellule-ltx-checksums"),
            output: is_compaction_output(path),
            track: self.track_all.load(Ordering::Relaxed)
                || path.to_string_lossy().contains(".ltx"),
        }))
    }
    fn create(&self, path: &Path) -> io::Result<Box<dyn FileIo>> {
        self.check("create")?;
        let inner = DirectFileSystem.create(path)?;
        #[cfg(feature = "replica")]
        if let Some(pause) = self.create_pause.get() {
            pause.entered.wait();
            pause.release.wait();
        }
        Ok(Box::new(File {
            inner,
            faults: self.clone(),
            checksum: path.to_string_lossy().ends_with(".cellule-ltx-checksums"),
            output: is_compaction_output(path),
            track: self.track_all.load(Ordering::Relaxed)
                || path.to_string_lossy().contains(".ltx"),
        }))
    }
    filesystem_operation!(file_len(path: &Path) -> u64);
    filesystem_operation!(canonicalize(path: &Path) -> PathBuf);
    filesystem_operation!(exists(path: &Path) -> bool);
    filesystem_operation!(create_dir(path: &Path) -> ());
    filesystem_operation!(create_dir_all(path: &Path) -> ());
    filesystem_operation!(remove_file(path: &Path) -> ());
    filesystem_operation!(rename(from: &Path, to: &Path) -> ());
    fn rename_uncommitted(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.check("rename_uncommitted")?;
        DirectFileSystem.rename_uncommitted(from, to)
    }
    fn sync_parent(&self, path: &Path) -> io::Result<()> {
        self.check("sync_parent")?;
        self.parent_syncs.fetch_add(1, Ordering::Relaxed);
        DirectFileSystem.sync_parent(path)
    }
    filesystem_operation!(persist_new(path: &Path, bytes: &[u8]) -> ());
    fn persist_file_new(&self, source: &Path, destination: &Path) -> io::Result<()> {
        self.check("persist_file_new")?;
        DirectFileSystem.persist_file_new(source, destination)?;
        Ok(())
    }
}

fn is_compaction_output(path: &Path) -> Option<&'static str> {
    let name = path.to_string_lossy();
    if name.ends_with("-compacted-ltx") {
        Some("compaction_ltx_sync")
    } else if name.ends_with("-compacted-index") {
        Some("compaction_index_sync")
    } else {
        None
    }
}

fn fixture() -> (tempfile::TempDir, Arc<Faults>, Host, Db) {
    let directory = tempfile::TempDir::new().unwrap();
    let faults = Arc::new(Faults::default());
    let host = Host::default()
        .with_filesystem(faults.clone())
        .with_local_disk_budget(cellule_ltx::DiskBudget::new(1 << 30));
    let mut writer = Db::open_with_host(
        &directory.path().join("source.sqlite"),
        Limits::default(),
        host.clone(),
    )
    .unwrap();
    writer
        .transaction(|tx| {
            tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(randomblob(20000))")
        })
        .unwrap();
    (directory, faults, host, writer)
}

fn injected<T>(result: cellule_ltx::Result<T>) {
    assert!(
        matches!(result, Err(LtxError::Io(error)) if error.kind() == io::ErrorKind::StorageFull)
    );
}

#[cfg(feature = "replica")]
mod activation;
mod capture;
mod compaction;
mod injection;
mod matrix;
mod prepare;
mod restore;
mod volatile;
