use super::*;

/// Verified preparation path and complete origin dependency availability.
///
/// This point observation proves derivation from an exact root through native
/// verified preparations, including compaction. It grants no authority, retention
/// pin, current native serving, original fleet scope or role settlement. Callers
/// must separately authenticate backend mappings and recheck selected authority,
/// boot/operation scope, recovery suffixes and current native work.
#[derive(Debug)]
pub struct VerifiedRootPrefix {
    prefix: RootRef,
    root: RootRef,
    inspected: usize,
    objects: usize,
}
impl VerifiedRootPrefix {
    /// Exact required historical root.
    #[must_use]
    pub const fn prefix(&self) -> RootRef {
        self.prefix
    }
    /// Exact successor whose origin graph was completely verified.
    #[must_use]
    pub const fn root(&self) -> RootRef {
        self.root
    }
    /// Distinct lineage records read while finding the path.
    #[must_use]
    pub const fn inspected_roots(&self) -> usize {
        self.inspected
    }
    /// Complete successor dependency count, including the root.
    #[must_use]
    pub const fn dependency_count(&self) -> usize {
        self.objects
    }
}

impl CellAuthority {
    /// Proves native verified derivation and current origin availability for one
    /// exact successor. Limits all expanded and queued lineage roots;
    /// The origin inventory is capped at 10,000 dependencies and refuses rather
    /// than truncating. The caller owns memory admission and its finite deadline;
    /// use the runtime wrapper to charge the shared node memory ledger.
    pub async fn verify_root_prefix(
        &self,
        prefix: RootRef,
        root: RootRef,
        replica: &CellReplica,
        limit: usize,
    ) -> Result<VerifiedRootPrefix> {
        validate_root(&prefix)?;
        validate_root(&root)?;
        if limit == 0 || limit > MAX_LINEAGE_ROOTS {
            return Err(Error::Capacity("invalid Cell root lineage traversal bound"));
        }
        if prefix.cell != root.cell
            || prefix.incarnation != root.incarnation
            || replica.scope() != (root.cell, root.incarnation)
        {
            return Err(Error::Control("Cell root prefix scope differs"));
        }
        let mut pending = vec![root];
        let mut seen = BTreeMap::from([(root.digest, root)]);
        let mut missing = None;
        let mut inspected = 0;
        let mut reached = false;
        while let Some(candidate) = pending.pop() {
            if candidate == prefix {
                reached = true;
                break;
            }
            if candidate.commit_sequence < prefix.commit_sequence
                || candidate.position.txid < prefix.position.txid
            {
                continue;
            }
            let Some(record) = self.root_lineage(candidate).await? else {
                missing.get_or_insert(candidate);
                continue;
            };
            inspected += 1;
            if record.predecessors.contains(&prefix) {
                reached = true;
                break;
            }
            for parent in record.predecessors {
                match seen.entry(parent.digest) {
                    std::collections::btree_map::Entry::Occupied(entry)
                        if entry.get() != &parent =>
                    {
                        return Err(Error::Control("Cell root lineage digest changed position"));
                    }
                    std::collections::btree_map::Entry::Occupied(_) => {}
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(parent);
                        pending.push(parent);
                    }
                }
                if seen.len() > limit {
                    return Err(Error::Capacity(
                        "Cell root lineage traversal bound exceeded",
                    ));
                }
            }
        }
        if !reached {
            return Err(missing.map_or(
                Error::RootPrefixUnproven {
                    prefix: Box::new(prefix),
                    root: Box::new(root),
                },
                |root| Error::RootLineageIncomplete { root },
            ));
        }
        // Never reuse the metadata cache as availability evidence: authenticate
        // every currently stored byte/extent through the ordinary origin walk.
        let objects = replica
            .reachable_objects_bounded(&root, MAX_LINEAGE_ROOTS)
            .await?
            .len();
        Ok(VerifiedRootPrefix {
            prefix,
            root,
            inspected,
            objects,
        })
    }
}
