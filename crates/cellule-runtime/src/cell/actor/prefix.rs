//! Prefix verification through the runtime's configured origin I/O facilities.
use super::*;
use crate::control::authority::MAX_LINEAGE_ROOTS;

// Conservative transient metadata envelope, independent of database body size:
// 64 descriptor pages (at most 96 decoded descriptors each), bounded concurrent
// 64 KiB root fetch/decode buffers, extents and body/index maps fit the fixed
// 16 MiB portion. Each of at most 10,000 inventory objects and `limit + 1` lineage
// roots receives 1 KiB for maps, vector growth and allocator overhead. Root bodies
// stream through the existing shared I/O host; application Store adapters own
// their stream chunk bounds. The permit spans all awaited verification work.
const ORIGIN_METADATA_BYTES: usize = 16 << 20;
const PREFIX_ENTRY_BYTES: usize = 1024;

impl CellRuntime {
    /// Verifies one exact root prefix through this runtime's shared LTX host.
    ///
    /// Transient lineage/origin metadata is reserved before I/O through the
    /// shared retained-byte ledger, and released on completion or cancellation.
    /// The caller owns the bounded future and authenticates canonical backend
    /// mappings. This grants no authority, serving, root pin or fleet settlement.
    /// Shutdown/admission and exact selected authority still require fresh checks.
    pub async fn verify_root_prefix(
        &self,
        catalog: &CatalogProof,
        authority: &CellAuthority,
        replica: cellule_ltx::CellReplica,
        prefix: cellule_ltx::RootRef,
        root: cellule_ltx::RootRef,
        limit: usize,
    ) -> crate::Result<crate::control::authority::VerifiedRootPrefix> {
        self.ensure_running()?;
        self.check_application_limits(catalog, replica.limits())?;
        if catalog.entry().cell().as_bytes() != &root.cell {
            return Err(Error::Control("Cell prefix catalog and root differ"));
        }
        if limit == 0 || limit > MAX_LINEAGE_ROOTS {
            return Err(Error::Capacity("invalid Cell root lineage traversal bound"));
        }
        let bytes = ORIGIN_METADATA_BYTES + (MAX_LINEAGE_ROOTS + limit + 1) * PREFIX_ENTRY_BYTES;
        let _metadata = self.try_reserve_node_bytes(bytes)?;
        let replica = self.replica_for_read(replica);
        let proof = authority
            .verify_root_prefix(prefix, root, &replica, limit)
            .await?;
        self.ensure_running()?;
        Ok(proof)
    }
}
