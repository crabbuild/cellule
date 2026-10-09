//! SQLite ABI forwarding with admission before any extending write.
use super::*;
use std::ffi::{c_char, c_void};

struct Handle {
    app: Arc<App>,
    key: Key,
    wal: bool,
    delete_on_close: bool,
}

#[repr(C)]
pub(super) struct File {
    methods: *const ffi::sqlite3_io_methods,
    base: *mut ffi::sqlite3_file,
    handle: *mut Handle,
}

fn guarded(app: &App, operation: impl FnOnce() -> Result<c_int>) -> c_int {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation)) {
        Ok(Ok(rc)) => rc,
        Ok(Err(error)) => app.remember(error),
        Err(_) => ffi::SQLITE_IOERR,
    }
}

unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    // SAFETY: registration pins App; SQLite supplies szOsFile zeroed storage.
    unsafe {
        (*file).pMethods = std::ptr::null();
        let app = &*(*vfs).pAppData.cast::<App>();
        guarded(app, || {
            let base_vfs = app.base;
            let open = (*base_vfs)
                .xOpen
                .ok_or(LtxError::InvalidState("base VFS lacks xOpen"))?;
            let base = ffi::sqlite3_malloc((*base_vfs).szOsFile).cast::<ffi::sqlite3_file>();
            if base.is_null() {
                return Ok(ffi::SQLITE_NOMEM);
            }
            std::ptr::write_bytes(base.cast::<u8>(), 0, (*base_vfs).szOsFile as usize);
            let rc = open(base_vfs, name, base, flags, out);
            let setup = (|| -> Result<Key> {
                if rc != ffi::SQLITE_OK {
                    return Err(sqlite(rc));
                }
                if (*base).pMethods.is_null() {
                    return Err(LtxError::InvalidState("base VFS lacks file methods"));
                }
                let key = if name.is_null() {
                    Key::Temporary(app.next_temporary.fetch_add(1, Ordering::Relaxed))
                } else {
                    Key::Named(CStr::from_ptr(name).to_bytes().to_vec())
                };
                let mut bytes = 0_i64;
                let size = (*(*base).pMethods)
                    .xFileSize
                    .ok_or(LtxError::InvalidState("base VFS lacks xFileSize"))?;
                let rc = size(base, &mut bytes);
                if rc != ffi::SQLITE_OK {
                    return Err(sqlite(rc));
                }
                let bytes = u64::try_from(bytes).map_err(|_| LtxError::LTXCorrupted)?;
                let frames = if flags & ffi::SQLITE_OPEN_WAL != 0 {
                    let page_size = app.state()?.page_size;
                    bytes
                        .saturating_sub(crate::WAL_HEADER_SIZE as u64)
                        .div_ceil(u64::from(page_size) + crate::WAL_FRAME_HEADER_SIZE as u64)
                } else {
                    0
                };
                app.grow_file(&key, bytes, frames)?;
                app.opened(&key)?;
                Ok(key)
            })();
            let key = match setup {
                Ok(key) => key,
                Err(error) => {
                    if !(*base).pMethods.is_null()
                        && let Some(close) = (*(*base).pMethods).xClose
                    {
                        close(base);
                    }
                    ffi::sqlite3_free(base.cast());
                    return if rc == ffi::SQLITE_OK {
                        Err(error)
                    } else {
                        Ok(rc)
                    };
                }
            };
            // Transfer a context reference into every successful open file.
            Arc::increment_strong_count(app);
            let handle = Box::new(Handle {
                app: Arc::from_raw(app),
                key,
                wal: flags & ffi::SQLITE_OPEN_WAL != 0,
                delete_on_close: flags & ffi::SQLITE_OPEN_DELETEONCLOSE != 0,
            });
            let file = file.cast::<File>();
            (*file).base = base;
            (*file).handle = Box::into_raw(handle);
            (*file).methods = &METHODS;
            Ok(ffi::SQLITE_OK)
        })
    }
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    // SAFETY: each successful xOpen transfers one base allocation and Handle;
    // SQLite closes once, including an open whose later setup failed.
    unsafe {
        let file = file.cast::<File>();
        let handle = Box::from_raw((*file).handle);
        let base = (*file).base;
        let rc = (*(*base).pMethods)
            .xClose
            .map_or(ffi::SQLITE_OK, |call| call(base));
        ffi::sqlite3_free(base.cast());
        (*file).methods = std::ptr::null();
        let accounting = guarded(&handle.app, || {
            handle
                .app
                .closed(&handle.key, rc == ffi::SQLITE_OK && handle.delete_on_close)?;
            Ok(rc)
        });
        if rc == ffi::SQLITE_OK { accounting } else { rc }
    }
}

unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    buffer: *const c_void,
    amount: c_int,
    offset: i64,
) -> c_int {
    // SAFETY: SQLite owns the open wrapper and supplies amount initialized bytes.
    unsafe {
        let file = &*file.cast::<File>();
        let handle = &*file.handle;
        guarded(&handle.app, || {
            let amount = u64::try_from(amount).map_err(|_| LtxError::LTXCorrupted)?;
            let offset = u64::try_from(offset).map_err(|_| LtxError::LTXCorrupted)?;
            let end = offset
                .checked_add(amount)
                .ok_or(LtxError::Limit(LimitKind::LocalDiskBytes))?;
            let frames = if handle.wal && amount > 0 && end > crate::WAL_HEADER_SIZE as u64 {
                let frame_size =
                    u64::from(handle.app.state()?.page_size) + crate::WAL_FRAME_HEADER_SIZE as u64;
                // Count every intersected frame, even duplicate/partial writes.
                // A new database page needs a WAL frame before it can commit;
                // this upper bound never parses untrusted partial frame bytes.
                (end - 1 - crate::WAL_HEADER_SIZE as u64) / frame_size
                    - offset.saturating_sub(crate::WAL_HEADER_SIZE as u64) / frame_size
                    + 1
            } else {
                0
            };
            handle.app.grow_file(&handle.key, end, frames)?;
            let write = (*(*file.base).pMethods)
                .xWrite
                .ok_or(LtxError::InvalidState("base VFS lacks xWrite"))?;
            // Keep the admitted extent after partial I/O failure. Releasing it
            // based on the requested write's return code would undercharge residue.
            Ok(write(file.base, buffer, amount as c_int, offset as i64))
        })
    }
}

unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: i64) -> c_int {
    // SAFETY: the wrapper and base file remain live during this callback.
    unsafe {
        let file = &*file.cast::<File>();
        let handle = &*file.handle;
        guarded(&handle.app, || {
            let bytes = u64::try_from(size).map_err(|_| LtxError::LTXCorrupted)?;
            handle.app.grow_file(&handle.key, bytes, 0)?;
            let truncate = (*(*file.base).pMethods)
                .xTruncate
                .ok_or(LtxError::InvalidState("base VFS lacks xTruncate"))?;
            let rc = truncate(file.base, size);
            if rc == ffi::SQLITE_OK {
                handle.app.resize_file(&handle.key, bytes, 0)?;
            }
            Ok(rc)
        })
    }
}

unsafe extern "C" fn x_control(file: *mut ffi::sqlite3_file, op: c_int, arg: *mut c_void) -> c_int {
    // SAFETY: file is open and SQLite's opcode defines arg's ABI.
    unsafe {
        let base = (*file.cast::<File>()).base;
        match op {
            // Advisory preallocation must not extend behind xWrite's admission.
            ffi::SQLITE_FCNTL_SIZE_HINT => ffi::SQLITE_OK,
            ffi::SQLITE_FCNTL_CHUNK_SIZE
            | ffi::SQLITE_FCNTL_BEGIN_ATOMIC_WRITE
            | ffi::SQLITE_FCNTL_COMMIT_ATOMIC_WRITE
            | ffi::SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE => ffi::SQLITE_NOTFOUND,
            _ => (*(*base).pMethods)
                .xFileControl
                .map_or(ffi::SQLITE_NOTFOUND, |call| call(base, op, arg)),
        }
    }
}

unsafe extern "C" fn x_shm_map(
    file: *mut ffi::sqlite3_file,
    page: c_int,
    size: c_int,
    extend: c_int,
    out: *mut *mut c_void,
) -> c_int {
    // SAFETY: SQLite passes an open main file and a writable mapping output.
    unsafe {
        let file = &*file.cast::<File>();
        let handle = &*file.handle;
        guarded(&handle.app, || {
            if extend != 0 {
                let end = u64::try_from(page)
                    .ok()
                    .and_then(|page| page.checked_add(1))
                    .and_then(|page| {
                        u64::try_from(size)
                            .ok()
                            .and_then(|size| page.checked_mul(size))
                    })
                    .ok_or(LtxError::Limit(LimitKind::LocalDiskBytes))?;
                if let Key::Named(name) = &handle.key {
                    let mut name = name.clone();
                    name.extend_from_slice(b"-shm");
                    handle.app.grow_file(&Key::Named(name), end, 0)?;
                }
            }
            let methods = (*file.base).pMethods;
            Ok(if (*methods).iVersion >= 2 {
                (*methods).xShmMap.map_or(ffi::SQLITE_IOERR_SHMMAP, |call| {
                    call(file.base, page, size, extend, out)
                })
            } else {
                ffi::SQLITE_IOERR_SHMMAP
            })
        })
    }
}

macro_rules! forward_io {
    ($name:ident, $field:ident, ($($arg:ident : $ty:ty),*), $failure:expr) => {
        unsafe extern "C" fn $name(file: *mut ffi::sqlite3_file, $($arg:$ty),*) -> c_int {
            // SAFETY: this live wrapper delegates SQLite's unchanged callback ABI.
            unsafe { let base = (*file.cast::<File>()).base;
                (*(*base).pMethods).$field.map_or($failure, |call| call(base, $($arg),*))
            }
        }
    };
}
forward_io!(x_read, xRead, (buffer: *mut c_void, amount: c_int, offset: i64), ffi::SQLITE_IOERR_READ);
forward_io!(x_sync, xSync, (flags: c_int), ffi::SQLITE_IOERR_FSYNC);
forward_io!(x_size, xFileSize, (size: *mut i64), ffi::SQLITE_IOERR_FSTAT);
forward_io!(x_lock, xLock, (lock: c_int), ffi::SQLITE_IOERR_LOCK);
forward_io!(x_unlock, xUnlock, (lock: c_int), ffi::SQLITE_IOERR_UNLOCK);
forward_io!(x_reserved, xCheckReservedLock, (out: *mut c_int), ffi::SQLITE_IOERR_CHECKRESERVEDLOCK);
forward_io!(x_sector, xSectorSize, (), 4096);
unsafe extern "C" fn x_shm_lock(
    file: *mut ffi::sqlite3_file,
    offset: c_int,
    n: c_int,
    flags: c_int,
) -> c_int {
    // SAFETY: check the base version before accessing version-2 callbacks.
    unsafe {
        let base = (*file.cast::<File>()).base;
        let methods = (*base).pMethods;
        if (*methods).iVersion >= 2 {
            (*methods)
                .xShmLock
                .map_or(ffi::SQLITE_IOERR_SHMLOCK, |call| {
                    call(base, offset, n, flags)
                })
        } else {
            ffi::SQLITE_IOERR_SHMLOCK
        }
    }
}
unsafe extern "C" fn x_shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    // SAFETY: check the base version before accessing version-2 callbacks.
    unsafe {
        let base = (*file.cast::<File>()).base;
        let methods = (*base).pMethods;
        if (*methods).iVersion >= 2 {
            (*methods)
                .xShmUnmap
                .map_or(ffi::SQLITE_IOERR_SHMOPEN, |call| call(base, delete))
        } else {
            ffi::SQLITE_IOERR_SHMOPEN
        }
    }
}

unsafe extern "C" fn x_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    // SAFETY: file's base is open. Disable the batch path that bypasses writes.
    unsafe {
        let base = (*file.cast::<File>()).base;
        (*(*base).pMethods)
            .xDeviceCharacteristics
            .map_or(0, |call| call(base))
            & !ffi::SQLITE_IOCAP_BATCH_ATOMIC
    }
}
unsafe extern "C" fn x_shm_barrier(file: *mut ffi::sqlite3_file) {
    // SAFETY: callback storage remains live until xClose.
    unsafe {
        let base = (*file.cast::<File>()).base;
        if (*(*base).pMethods).iVersion >= 2
            && let Some(call) = (*(*base).pMethods).xShmBarrier
        {
            call(base);
        }
    }
}
unsafe extern "C" fn x_fetch(
    file: *mut ffi::sqlite3_file,
    offset: i64,
    amount: c_int,
    out: *mut *mut c_void,
) -> c_int {
    // SAFETY: SQLite provides a writable mapping output. Old VFSes use xRead.
    unsafe {
        let base = (*file.cast::<File>()).base;
        let methods = (*base).pMethods;
        if (*methods).iVersion >= 3
            && let Some(call) = (*methods).xFetch
        {
            return call(base, offset, amount, out);
        }
        *out = std::ptr::null_mut();
        ffi::SQLITE_OK
    }
}
unsafe extern "C" fn x_unfetch(
    file: *mut ffi::sqlite3_file,
    offset: i64,
    pointer: *mut c_void,
) -> c_int {
    // SAFETY: only mappings delegated to the base reach xUnfetch.
    unsafe {
        let base = (*file.cast::<File>()).base;
        let methods = (*base).pMethods;
        if (*methods).iVersion >= 3
            && let Some(call) = (*methods).xUnfetch
        {
            return call(base, offset, pointer);
        }
        ffi::SQLITE_OK
    }
}
static METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 3,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_reserved),
    xFileControl: Some(x_control),
    xSectorSize: Some(x_sector),
    xDeviceCharacteristics: Some(x_characteristics),
    xShmMap: Some(x_shm_map),
    xShmLock: Some(x_shm_lock),
    xShmBarrier: Some(x_shm_barrier),
    xShmUnmap: Some(x_shm_unmap),
    xFetch: Some(x_fetch),
    xUnfetch: Some(x_unfetch),
};

unsafe extern "C" fn x_delete(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    sync: c_int,
) -> c_int {
    // SAFETY: SQLite supplies a valid filename; registration pins App and base.
    unsafe {
        let app = &*(*vfs).pAppData.cast::<App>();
        guarded(app, || {
            let delete = (*app.base)
                .xDelete
                .ok_or(LtxError::InvalidState("base VFS lacks xDelete"))?;
            let rc = delete(app.base, name, sync);
            if rc == ffi::SQLITE_OK {
                app.forget_file(&Key::Named(CStr::from_ptr(name).to_bytes().to_vec()))?;
            }
            Ok(rc)
        })
    }
}
macro_rules! forward_vfs {
    ($name:ident, $field:ident, ($($arg:ident : $ty:ty),*), $ret:ty, $failure:expr) => {
        unsafe extern "C" fn $name(vfs: *mut ffi::sqlite3_vfs, $($arg:$ty),*) -> $ret {
            // SAFETY: registration pins the immutable base; exact callback ABI.
            unsafe { let base = (*(*vfs).pAppData.cast::<App>()).base;
                (*base).$field.map_or($failure, |call| call(base, $($arg),*))
            }
        }
    };
}
forward_vfs!(x_access, xAccess, (name: *const c_char, flags: c_int, out: *mut c_int), c_int, ffi::SQLITE_IOERR_ACCESS);
forward_vfs!(x_full_path, xFullPathname, (name: *const c_char, size: c_int, out: *mut c_char), c_int, ffi::SQLITE_CANTOPEN);
forward_vfs!(x_randomness, xRandomness, (size: c_int, out: *mut c_char), c_int, 0);
forward_vfs!(x_sleep, xSleep, (micros: c_int), c_int, 0);
forward_vfs!(x_current_time, xCurrentTime, (out: *mut f64), c_int, ffi::SQLITE_ERROR);
forward_vfs!(x_current_time64, xCurrentTimeInt64, (out: *mut i64), c_int, ffi::SQLITE_ERROR);
forward_vfs!(x_last_error, xGetLastError, (size: c_int, out: *mut c_char), c_int, 0);
forward_vfs!(x_dl_open, xDlOpen, (name: *const c_char), *mut c_void, std::ptr::null_mut());
forward_vfs!(x_dl_error, xDlError, (size: c_int, out: *mut c_char), (), ());
forward_vfs!(x_dl_close, xDlClose, (handle: *mut c_void), (), ());
type Symbol = Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const c_char)>;
forward_vfs!(x_dl_sym, xDlSym, (handle: *mut c_void, symbol: *const c_char), Symbol, None);
forward_vfs!(x_set_system_call, xSetSystemCall, (name: *const c_char, call: ffi::sqlite3_syscall_ptr), c_int, ffi::SQLITE_NOTFOUND);
forward_vfs!(x_get_system_call, xGetSystemCall, (name: *const c_char), ffi::sqlite3_syscall_ptr, None);
forward_vfs!(x_next_system_call, xNextSystemCall, (name: *const c_char), *const c_char, std::ptr::null());

pub(super) unsafe fn vfs(base: *mut ffi::sqlite3_vfs) -> ffi::sqlite3_vfs {
    // SAFETY: SQLite owns a live base with its declared ABI version. Zeroing
    // initializes optional callbacks absent from older registration versions.
    unsafe {
        let mut vfs: ffi::sqlite3_vfs = std::mem::zeroed();
        vfs.iVersion = (*base).iVersion.min(3);
        vfs.mxPathname = (*base).mxPathname;
        vfs.xOpen = Some(x_open);
        vfs.xDelete = Some(x_delete);
        vfs.xAccess = Some(x_access);
        vfs.xFullPathname = Some(x_full_path);
        vfs.xRandomness = Some(x_randomness);
        vfs.xSleep = Some(x_sleep);
        vfs.xCurrentTime = Some(x_current_time);
        vfs.xGetLastError = Some(x_last_error);
        vfs.xDlOpen = Some(x_dl_open);
        vfs.xDlError = Some(x_dl_error);
        vfs.xDlSym = Some(x_dl_sym);
        vfs.xDlClose = Some(x_dl_close);
        if (*base).iVersion >= 2 && (*base).xCurrentTimeInt64.is_some() {
            vfs.xCurrentTimeInt64 = Some(x_current_time64);
        }
        if (*base).iVersion >= 3 {
            vfs.xSetSystemCall = Some(x_set_system_call);
            vfs.xGetSystemCall = Some(x_get_system_call);
            vfs.xNextSystemCall = Some(x_next_system_call);
        }
        vfs
    }
}

#[cfg(feature = "replica")]
pub(super) unsafe fn underlying_file(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    // SAFETY: the caller retains an open file. A matching methods pointer
    // proves the File layout before reading its base pointer.
    unsafe {
        if !file.is_null() && std::ptr::eq((*file).pMethods, &METHODS) {
            (*file.cast::<File>()).base
        } else {
            file
        }
    }
}
