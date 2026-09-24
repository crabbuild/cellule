//! Startup storage capability probe.
//!
//! The runtime fences Cells with conditional writes and recovers them with
//! ranged reads. A store can accept the conditional headers and ignore them,
//! which fails late and silently: two owners can then hold one Cell. This probe
//! writes one fresh key under the caller's prefix and *reports* every
//! capability rather than trusting the provider, so a service can refuse
//! readiness with the exact failing check.

use std::ops::Range;

use bytes::Bytes;
use object_store::path::Path;
use rand::RngCore;

use crate::{Result, StorageError, Store};

const PROBE_BODY: &[u8] = b"cellule-probe-v1";
const PROBE_UPDATE: &[u8] = b"cellule-probe-v2";
const PROBE_REPLACEMENT: &[u8] = b"cellule-probe-v3";
/// Offset and bytes checked by the ranged read of the updated probe body.
const PROBE_RANGE: Range<u64> = 8..10;
const PROBE_RANGE_BYTES: &[u8] = b"pr";

/// What one capability probe observed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageProbeReport {
    /// Absolute key the probe used; safe to log and safe to delete.
    pub key: String,
    /// A fresh key was created by a conditional create.
    pub create_if_absent: bool,
    /// The same conditional create was refused because the key exists.
    pub reject_existing: bool,
    /// A conditional update with the current ETag succeeded and changed the ETag.
    pub conditional_update: bool,
    /// A conditional update with the previous ETag was refused.
    pub reject_stale_etag: bool,
    /// A ranged read returned the exact requested bytes.
    pub range_read: bool,
    /// A plain read returned the body written by the conditional update.
    pub read_after_write: bool,
}

impl StorageProbeReport {
    /// Reports whether every capability the runtime depends on is present.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.create_if_absent
            && self.reject_existing
            && self.conditional_update
            && self.reject_stale_etag
            && self.range_read
            && self.read_after_write
    }

    /// Names each capability the store failed, in probe order.
    #[must_use]
    pub fn failed_checks(&self) -> Vec<&'static str> {
        let mut failed = Vec::new();
        if !self.create_if_absent {
            failed.push("create-if-absent");
        }
        if !self.reject_existing {
            failed.push("reject-existing");
        }
        if !self.conditional_update {
            failed.push("conditional-update");
        }
        if !self.reject_stale_etag {
            failed.push("reject-stale-etag");
        }
        if !self.range_read {
            failed.push("range-read");
        }
        if !self.read_after_write {
            failed.push("read-after-write");
        }
        failed
    }
}

/// Probes conditional writes and ranged reads with one fresh key under `prefix`.
///
/// The probe deletes its key before returning. A store that silently ignores a
/// condition does not fail this call: the report names the missing capability
/// so the caller can refuse readiness with evidence instead of guessing.
pub async fn probe_storage(
    store: &Store,
    prefix: &Path,
    now_ms: i64,
) -> Result<StorageProbeReport> {
    let mut suffix = [0_u8; 8];
    rand::rng().fill_bytes(&mut suffix);
    let key = prefix
        .clone()
        .join(format!("probe-{now_ms}-{}", encode_hex(&suffix)));

    let create_if_absent = store
        .put_if_absent(&key, Bytes::from_static(PROBE_BODY))
        .await?;
    let reject_existing = !store
        .put_if_absent(&key, Bytes::from_static(PROBE_BODY))
        .await?;
    let (_, first_etag) = store.get_with_etag(&key).await?;
    let next_etag = store
        .update(&key, Bytes::from_static(PROBE_UPDATE), first_etag.clone())
        .await?;
    let conditional_update = next_etag != first_etag;
    let reject_stale_etag = match store
        .update(
            &key,
            Bytes::from_static(PROBE_REPLACEMENT),
            first_etag.clone(),
        )
        .await
    {
        Ok(_) => false,
        Err(StorageError::StateConflict { .. }) => true,
        Err(error) => return Err(error),
    };
    let (updated, _) = store.get_with_etag(&key).await?;
    let read_after_write = updated.as_ref() == PROBE_UPDATE;
    let ranged = store.range_get(&key, PROBE_RANGE).await?;
    let range_read = ranged.as_ref() == PROBE_RANGE_BYTES;
    store.delete(&key).await?;

    Ok(StorageProbeReport {
        key: key.to_string(),
        create_if_absent,
        reject_existing,
        conditional_update,
        reject_stale_etag,
        range_read,
        read_after_write,
    })
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from_digit(u32::from(byte >> 4), 16).unwrap_or('0'));
        encoded.push(char::from_digit(u32::from(byte & 0x0f), 16).unwrap_or('0'));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use super::*;

    fn store() -> Store {
        Store::new(Arc::new(InMemory::new()))
    }

    #[tokio::test]
    async fn probe_reports_every_capability_for_a_conditioned_store() {
        let store = store();
        let prefix = Path::from("cellule-probe");
        let report = probe_storage(&store, &prefix, 1_000).await.unwrap();
        assert!(
            report.passed(),
            "probe failed: {:?}",
            report.failed_checks()
        );
        assert!(report.key.starts_with("cellule-probe/probe-1000-"));
        // The probe cleans up its key.
        let deleted = store.head(&Path::from(report.key.as_str())).await;
        assert!(matches!(deleted, Err(StorageError::NotFound { .. })));
    }

    #[test]
    fn report_names_every_missing_capability() {
        let missing = StorageProbeReport {
            key: "cellule/probe".into(),
            create_if_absent: true,
            reject_existing: false,
            conditional_update: true,
            reject_stale_etag: false,
            range_read: true,
            read_after_write: false,
        };
        assert!(!missing.passed());
        assert_eq!(
            missing.failed_checks(),
            vec!["reject-existing", "reject-stale-etag", "read-after-write"]
        );
    }
}
