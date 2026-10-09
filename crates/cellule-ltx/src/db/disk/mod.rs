//! Per-session admission before SQLite file growth and before committed-cut capture.
//!
//! SQLite connections and open files retain the context. The registration is
//! unregistered only after the managed writer and capture connections close.
use crate::{DiskReservation, LimitKind, LtxError, Result, ltx};
use rusqlite::ffi;
use std::{
    collections::HashMap,
    ffi::{CStr, CString, c_int},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

mod io;

#[derive(Clone, Hash, PartialEq, Eq)]
enum Key {
    Named(Vec<u8>),
    Temporary(u64),
}

struct Tracked {
    bytes: u64,
    // Sparse pages in the inherited extent are charged by the sparse VFS.
    // This wrapper charges growth beyond that extent, plus WAL and SHM.
    unmetered_prefix: u64,
    opens: u32,
    deleted: bool,
}

struct State {
    files: HashMap<Key, Tracked>,
    retained: u64,
    capture_pages: u64,
    capture_credit: u64,
    growth_credit: u64,
    sealing: bool,
    page_size: u32,
    max_file_bytes: u64,
}

impl State {
    fn bytes(&self) -> Result<u64> {
        self.files.values().try_fold(
            self.retained
                .checked_add(self.capture_credit)
                .and_then(|bytes| bytes.checked_add(self.growth_credit))
                .ok_or(LtxError::Limit(LimitKind::LocalDiskBytes))?,
            |total, file| {
                total
                    .checked_add(file.bytes.saturating_sub(file.unmetered_prefix))
                    .ok_or(LtxError::Limit(LimitKind::LocalDiskBytes))
            },
        )
    }

    fn credit(&self, pages: u64) -> Result<u64> {
        // A sync can emit the ordinary cut and a checkpoint boundary image.
        // Both must remain capturable even when the delta exceeds its bound.
        let bound = ltx::cut_upper_bound(self.page_size, pages)?;
        // Each artifact writer also enforces max_file_bytes. A malformed or
        // uncapturable cut still fails and fences; it cannot write past this
        // admitted ceiling merely because its estimated image is larger.
        bound
            .min(self.max_file_bytes)
            .checked_mul(2)
            .ok_or(LtxError::Limit(LimitKind::LocalDiskBytes))
    }
}

struct App {
    base: *mut ffi::sqlite3_vfs,
    reservation: DiskReservation,
    state: Mutex<State>,
    error: Mutex<Option<LtxError>>,
    main: Key,
    sparse_prefix: u64,
    next_temporary: AtomicU64,
}

// SAFETY: the embedding host keeps the base VFS process-live. Its callbacks
// follow SQLite's threading contract; mutable quota state is always locked.
unsafe impl Send for App {}
unsafe impl Sync for App {}

impl App {
    fn state(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| LtxError::InvalidState("SQLite disk state poisoned"))
    }

    fn remember(&self, error: LtxError) -> c_int {
        let rc = if error.classify() == crate::FailureClass::Capacity {
            ffi::SQLITE_FULL
        } else {
            ffi::SQLITE_IOERR
        };
        if let Ok(mut slot) = self.error.lock()
            && slot.is_none()
        {
            *slot = Some(error);
        }
        rc
    }

    fn charge(&self, state: &State) -> Result<()> {
        let bytes = state.bytes()?;
        // Converting reserved growth credit into a file extent leaves this
        // reservation unchanged. Re-enter the node ledger only for a change.
        if bytes == self.reservation.bytes() {
            Ok(())
        } else {
            self.reservation.resize(bytes)
        }
    }

    fn resize_locked(
        &self,
        state: &mut State,
        key: &Key,
        bytes: u64,
        wal_frames: u64,
    ) -> Result<()> {
        let old_pages = state.capture_pages;
        let old_credit = state.capture_credit;
        let old_growth = state.growth_credit;
        let existing = state.files.contains_key(key);
        let old_size = state.files.get(key).map_or(0, |file| file.bytes);
        let prefix = if key == &self.main {
            self.sparse_prefix
        } else {
            0
        };
        let pages = old_pages
            .checked_add(if state.sealing { 0 } else { wal_frames })
            .ok_or(LtxError::Limit(LimitKind::LocalDiskBytes))?;
        let credit = if wal_frames > 0 {
            state.credit(pages)?.max(old_credit)
        } else {
            old_credit
        };
        if let Some(file) = state.files.get_mut(key) {
            file.bytes = bytes;
        } else {
            state.files.insert(
                key.clone(),
                Tracked {
                    bytes,
                    unmetered_prefix: prefix,
                    opens: 0,
                    deleted: false,
                },
            );
        }
        state.capture_pages = pages;
        state.capture_credit = credit;
        let growth = bytes
            .saturating_sub(prefix)
            .saturating_sub(old_size.saturating_sub(prefix));
        state.growth_credit = old_growth.saturating_sub(growth);
        let admission = self.charge(state);
        if admission.is_err() {
            if existing {
                if let Some(file) = state.files.get_mut(key) {
                    file.bytes = old_size;
                }
            } else {
                state.files.remove(key);
            }
            state.capture_pages = old_pages;
            state.capture_credit = old_credit;
            state.growth_credit = old_growth;
        }
        admission
    }

    fn resize_file(&self, key: &Key, bytes: u64, wal_frames: u64) -> Result<()> {
        let mut state = self.state()?;
        self.resize_locked(&mut state, key, bytes, wal_frames)
    }

    fn grow_file(&self, key: &Key, end: u64, wal_frames: u64) -> Result<()> {
        let mut state = self.state()?;
        let bytes = state.files.get(key).map_or(end, |file| file.bytes.max(end));
        self.resize_locked(&mut state, key, bytes, wal_frames)
    }

    fn opened(&self, key: &Key) -> Result<()> {
        let mut state = self.state()?;
        let file = state
            .files
            .get_mut(key)
            .ok_or(LtxError::InvalidState("untracked SQLite file"))?;
        file.opens = file
            .opens
            .checked_add(1)
            .ok_or(LtxError::InvalidState("SQLite file handle overflow"))?;
        Ok(())
    }

    fn closed(&self, key: &Key, deleted: bool) -> Result<()> {
        let mut state = self.state()?;
        let file = state
            .files
            .get_mut(key)
            .ok_or(LtxError::InvalidState("untracked SQLite close"))?;
        file.opens = file
            .opens
            .checked_sub(1)
            .ok_or(LtxError::InvalidState("SQLite file handle underflow"))?;
        file.deleted |= deleted;
        if file.deleted && file.opens == 0 {
            state.files.remove(key);
        }
        self.charge(&state)
    }

    fn forget_file(&self, key: &Key) -> Result<()> {
        let mut state = self.state()?;
        // An unlinked file still consumes bytes while a SQLite handle owns it.
        if let Some(file) = state.files.get_mut(key) {
            file.deleted = true;
            if file.opens == 0 {
                state.files.remove(key);
            }
        }
        self.charge(&state)
    }
}

pub(super) struct Registration {
    vfs: Box<ffi::sqlite3_vfs>,
    name: CString,
    app: Arc<App>,
}

// SAFETY: allocation addresses remain stable when this owner moves. Db retains
// it until all SQLite connections close; registration fields are immutable.
unsafe impl Send for Registration {}

impl Registration {
    pub(super) fn new(
        path: &Path,
        base: Option<&str>,
        reservation: DiskReservation,
        page_size: u32,
        database_bytes: u64,
        sparse: bool,
        max_file_bytes: u64,
    ) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let name = CString::new(format!(
            "cellule-disk-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
        .map_err(|_| LtxError::InvalidState("invalid SQLite disk VFS name"))?;
        let base_name = base
            .map(CString::new)
            .transpose()
            .map_err(|_| LtxError::InvalidState("invalid base VFS name"))?;
        let main = Key::Named(
            path.to_str()
                .ok_or(LtxError::InvalidState("invalid SQLite path"))?
                .as_bytes()
                .to_vec(),
        );
        // SAFETY: SQLite initializes and synchronizes its global registry. The
        // configured base is process-live; every registered pointer below is boxed.
        unsafe {
            let rc = ffi::sqlite3_initialize();
            if rc != ffi::SQLITE_OK {
                return Err(sqlite(rc));
            }
            let base =
                ffi::sqlite3_vfs_find(base_name.as_deref().map_or(std::ptr::null(), CStr::as_ptr));
            if base.is_null() {
                return Err(LtxError::InvalidState("unknown base SQLite VFS"));
            }
            let sparse_prefix = if sparse { database_bytes } else { 0 };
            let app = Arc::new(App {
                base,
                reservation,
                state: Mutex::new(State {
                    files: HashMap::from([(
                        main.clone(),
                        Tracked {
                            bytes: database_bytes,
                            unmetered_prefix: sparse_prefix,
                            opens: 0,
                            deleted: false,
                        },
                    )]),
                    retained: 0,
                    capture_pages: database_bytes.div_ceil(u64::from(page_size)),
                    capture_credit: 0,
                    growth_credit: 0,
                    sealing: false,
                    page_size,
                    max_file_bytes,
                }),
                error: Mutex::new(None),
                main,
                sparse_prefix,
                next_temporary: AtomicU64::new(1),
            });
            let mut vfs = Box::new(io::vfs(base));
            vfs.szOsFile = std::mem::size_of::<io::File>() as c_int;
            vfs.pAppData = Arc::as_ptr(&app).cast_mut().cast();
            vfs.zName = name.as_ptr();
            let rc = ffi::sqlite3_vfs_register(&mut *vfs, 0);
            if rc != ffi::SQLITE_OK {
                return Err(sqlite(rc));
            }
            Ok(Self { vfs, name, app })
        }
    }

    pub(super) fn vfs(&self) -> Result<&str> {
        self.name
            .to_str()
            .map_err(|_| LtxError::InvalidState("invalid SQLite disk VFS name"))
    }

    pub(super) fn admit_capture(&self, pages: u64) -> Result<()> {
        let mut state = self.app.state()?;
        let pages = state.capture_pages.max(pages);
        let credit = state.credit(pages)?.max(state.capture_credit);
        let old_credit = state.capture_credit;
        state.capture_credit = credit;
        let result = self.app.charge(&state);
        if result.is_err() {
            state.capture_credit = old_credit;
        } else {
            state.capture_pages = pages;
            state.sealing = false;
        }
        result
    }

    /// Admit the remaining commit and checkpoint I/O before COMMIT, when a
    /// failed reservation can still be rolled back without ambiguity.
    pub(super) fn seal(&self, pages: u64) -> Result<()> {
        let mut state = self.app.state()?;
        let pages = state.capture_pages.max(pages);
        let old = (
            state.capture_pages,
            state.capture_credit,
            state.growth_credit,
            state.sealing,
        );
        let page_size = u64::from(state.page_size);
        let main_bytes = state.files.get(&self.app.main).map_or(0, |file| file.bytes);
        let database = pages
            .checked_mul(page_size)
            .ok_or(LtxError::Limit(LimitKind::LocalDiskBytes))?;
        // At most one WAL frame per database page remains dirty at COMMIT.
        // The control tables also seal/restart WAL during capture. Keep room
        // for their bounded writes and for backfilling an enlarged main file.
        let growth = pages
            .checked_add(16)
            .and_then(|pages| pages.checked_mul(page_size + crate::WAL_FRAME_HEADER_SIZE as u64))
            .and_then(|bytes| bytes.checked_add(crate::WAL_HEADER_SIZE as u64))
            .and_then(|bytes| bytes.checked_add(database.saturating_sub(main_bytes)))
            .ok_or(LtxError::Limit(LimitKind::LocalDiskBytes))?;
        state.capture_credit = state.credit(
            pages
                .checked_add(16)
                .ok_or(LtxError::Limit(LimitKind::LocalDiskBytes))?,
        )?;
        state.growth_credit = growth;
        state.capture_pages = pages;
        state.sealing = true;
        let result = self.app.charge(&state);
        if result.is_err() {
            (
                state.capture_pages,
                state.capture_credit,
                state.growth_credit,
                state.sealing,
            ) = old;
        }
        result
    }

    /// Only called after complete capture or proven rollback without an older cut.
    pub(super) fn settle(&self, retained: u64, pages: u64, pending: bool) -> Result<()> {
        let mut state = self.app.state()?;
        state.retained = retained;
        if !pending {
            state.capture_pages = pages;
            state.capture_credit = 0;
            state.growth_credit = 0;
            state.sealing = false;
        }
        self.app.charge(&state)
    }

    pub(super) fn take_error(&self) -> Option<LtxError> {
        self.app.error.lock().ok().and_then(|mut slot| slot.take())
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        // SAFETY: Db's writer and capture fields are dropped before this field.
        // Unregistration is synchronized and no connection retains this VFS.
        unsafe {
            ffi::sqlite3_vfs_unregister(&mut *self.vfs);
        }
    }
}

fn sqlite(rc: c_int) -> LtxError {
    rusqlite::Error::SqliteFailure(ffi::Error::new(rc), None).into()
}

/// Reveals the selected VFS file only to the sparse installation seam.
/// The caller must exclusively own the connection and check its activation.
#[cfg(feature = "replica")]
pub(crate) unsafe fn underlying_file(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    // SAFETY: the caller pins an open SQLite file; io verifies its wrapper ABI.
    unsafe { io::underlying_file(file) }
}
