//! Complete, bounded traversal through the ordinary verified shard reader.

use super::*;

/// Streaming traversal of all 256 heads captured before the first page read.
///
/// Dropping or failing a traversal supplies no completion receipt. Each page
/// uses the same identity, digest and locator checks as `scan_shard`.
pub struct CatalogScan {
    catalog: CellCatalog,
    shards: Vec<CatalogShardScan>,
    next_shard: usize,
    entry_limit: usize,
    entries: usize,
    failed: bool,
}

/// Complete verified traversal and its original tenant/application head set.
///
/// Heads are observed and rechecked sequentially, not in a global transaction.
/// This is neither a durable pin nor proof of a complete fleet application set,
/// process joining, Cell authority, availability or successor serving. Callers
/// supply those barriers and retain the delivered entries before using this
/// receipt in an operation. Later provisioning can invalidate the observation.
pub struct CatalogScanReceipt {
    catalog: CellCatalog,
    shards: Vec<CatalogShardScan>,
    entries: usize,
}

impl CellCatalog {
    /// Captures every shard head for a bounded complete streaming traversal.
    ///
    /// The nonzero row limit is checked before I/O. Heads retain at most 256
    /// locators each; only one verified page of at most 256 entries is returned
    /// per call. No task, authority mutation or storage listing is started.
    pub async fn scan_all(&self, entry_limit: usize) -> Result<CatalogScan> {
        if entry_limit == 0 || entry_limit > MAX_ENTRIES * 256 {
            return Err(Error::Capacity("invalid complete catalog scan row limit"));
        }
        let mut shards = Vec::with_capacity(256);
        for shard in 0..=u8::MAX {
            shards.push(self.scan_shard(shard).await?);
        }
        Ok(CatalogScan {
            catalog: self.clone(),
            shards,
            next_shard: 0,
            entry_limit,
            entries: 0,
            failed: false,
        })
    }
}

impl CatalogScan {
    /// Reads the next immutable page in global Cell order, skipping empty heads.
    ///
    /// Any read, verification or limit failure permanently prevents completion.
    /// Already delivered pages are partial observations, not a complete set.
    pub async fn next_page(&mut self) -> Result<Option<CatalogScanPage>> {
        if self.failed {
            return Err(Error::Catalog("complete catalog scan previously failed"));
        }
        while let Some(shard) = self.shards.get_mut(self.next_shard) {
            match shard.next_page().await {
                Ok(Some(page)) => {
                    if page.entries.len() > self.entry_limit - self.entries {
                        self.failed = true;
                        return Err(Error::Capacity("complete catalog scan exceeds row limit"));
                    }
                    self.entries += page.entries.len();
                    return Ok(Some(page));
                }
                Ok(None) => self.next_shard += 1,
                Err(error) => {
                    self.failed = true;
                    return Err(error);
                }
            }
        }
        Ok(None)
    }

    /// Consumes an exhausted scan and rechecks every original head, including
    /// absence and ETag. Partial and failed scans cannot yield a receipt.
    pub async fn finish(self) -> Result<CatalogScanReceipt> {
        if self.failed || self.next_shard != self.shards.len() {
            return Err(Error::Catalog("complete catalog scan is not exhausted"));
        }
        let receipt = CatalogScanReceipt {
            catalog: self.catalog,
            shards: self.shards,
            entries: self.entries,
        };
        receipt.revalidate().await?;
        Ok(receipt)
    }
}

impl CatalogScanReceipt {
    /// Returns the original tenant scope.
    #[must_use]
    pub const fn tenant(&self) -> TenantId {
        self.catalog.tenant
    }

    /// Returns the original application scope.
    #[must_use]
    pub const fn application(&self) -> ApplicationId {
        self.catalog.application
    }

    /// Returns the number of entries delivered by the complete traversal.
    #[must_use]
    pub const fn entry_count(&self) -> usize {
        self.entries
    }

    /// Returns the captured revision of one shard; zero records an absent head.
    #[must_use]
    pub fn revision(&self, shard: u8) -> u64 {
        self.shards[usize::from(shard)].revision()
    }

    /// Returns the original ordered immutable page digests of one shard.
    #[must_use]
    pub fn page_digests(&self, shard: u8) -> Vec<Digest> {
        self.shards[usize::from(shard)].page_digests()
    }

    /// Rechecks the captured heads through the same original catalog adapter.
    ///
    /// No retry or refresh substitutes a newer set for the original capture.
    /// Success supplies sequential observations, not an atomic fleet barrier.
    pub async fn revalidate(&self) -> Result<()> {
        for original in &self.shards {
            let current = self.catalog.load_head(original.shard).await?;
            let matches = match current {
                None => original.token.is_none(),
                Some(current) => {
                    original.token.as_ref() == Some(&current.token)
                        && original.revision == current.head.revision
                        && original.pages.len() == current.head.pages.len()
                        && original
                            .pages
                            .iter()
                            .zip(&current.head.pages)
                            .all(|(a, b)| a.digest == b.digest && a.first == b.first)
                }
            };
            if !matches {
                return Err(Error::Catalog("complete catalog scan head changed"));
            }
        }
        Ok(())
    }
}
