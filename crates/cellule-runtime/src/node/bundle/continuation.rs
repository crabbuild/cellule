//! Exact selected-debt continuation across confirmed root checkpoints.
use super::*;

struct PrefixAlias {
    base: cellule_ltx::RootRef,
    locators: usize,
    digest: Digest,
}

/// Process-local witnesses for the prefixes a confirmed root materialized.
/// These grant no coverage or origin availability and retain no frame bodies.
pub(crate) struct MaterializedBundlePrefix {
    root: cellule_ltx::RootRef,
    pin: BundleBindingRef,
    aliases: Vec<PrefixAlias>,
}

impl MaterializedBundlePrefix {
    pub(crate) const fn maximum_retained_bytes() -> usize {
        MAX_LOCATORS * std::mem::size_of::<PrefixAlias>()
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.aliases.capacity() * std::mem::size_of::<PrefixAlias>()
    }
}

fn extend_digest(mut digest: Digest, locators: &[Locator]) -> Result<Digest> {
    for locator in locators {
        let mut hash = blake3::Hasher::new();
        hash.update(digest.as_bytes());
        hash.update(
            locator
                .object
                .ok_or(Error::Control("checkpoint locator is unresolved"))?
                .as_bytes(),
        );
        hash.update(&locator.offset.to_le_bytes());
        hash.update(&locator.bytes.to_le_bytes());
        hash.update(locator.frame_digest.as_bytes());
        digest = Digest::from_bytes(*hash.finalize().as_bytes());
    }
    Ok(digest)
}

fn locator_prefix_digest(locators: &[Locator]) -> Result<Digest> {
    extend_digest(
        Digest::from_bytes(*blake3::hash(b"cellule.confirmed-bundle-prefix.v1").as_bytes()),
        locators,
    )
}

impl BundleCoverageProof {
    pub(crate) fn materialized_prefix(
        &self,
        root: cellule_ltx::RootRef,
        previous: Option<&MaterializedBundlePrefix>,
    ) -> Result<MaterializedBundlePrefix> {
        let base = self.base()?;
        if root.cell != *self.binding.control.cell.as_bytes()
            || root.incarnation != *self.binding.control.incarnation.as_bytes()
            || root.commit_sequence != self.commit_sequence()
            || root.position != self.position()
            || root.commit_sequence <= base.commit_sequence
            || self.binding.locators.is_empty()
            || self.binding.locators.len() > MAX_LOCATORS
        {
            return Err(Error::Control(
                "materialized root differs from selected prefix",
            ));
        }
        let Some(previous) = previous else {
            return Ok(MaterializedBundlePrefix {
                root,
                pin: self.pin,
                aliases: vec![PrefixAlias {
                    base,
                    locators: self.binding.locators.len(),
                    digest: locator_prefix_digest(&self.binding.locators)?,
                }],
            });
        };
        if previous.pin != self.pin
            || root.commit_sequence <= previous.root.commit_sequence
            || root.position.txid <= previous.root.position.txid
        {
            return Err(Error::Control(
                "checkpoint does not advance original writer",
            ));
        }
        let delta = if base == previous.root {
            self.binding.locators.as_slice()
        } else {
            let alias = previous
                .aliases
                .iter()
                .find(|alias| alias.base == base)
                .ok_or(Error::Control("checkpoint lacks original base witness"))?;
            let prefix = self
                .binding
                .locators
                .get(..alias.locators)
                .ok_or(Error::Control("checkpoint omits confirmed prefix"))?;
            if locator_prefix_digest(prefix)? != alias.digest {
                return Err(Error::Control("checkpoint changes confirmed prefix"));
            }
            &self.binding.locators[alias.locators..]
        };
        if delta.is_empty() {
            return Err(Error::Control("checkpoint lacks native advancement"));
        }
        // Every advancing checkpoint adds a native locator. An older base
        // expires once no bounded proof can still contain its entire prefix.
        // Extend hashes, rather than retain O(history) locator arrays.
        let mut aliases = Vec::with_capacity((previous.aliases.len() + 1).min(MAX_LOCATORS));
        for alias in &previous.aliases {
            let count = alias
                .locators
                .checked_add(delta.len())
                .ok_or(Error::Control("checkpoint prefix count overflow"))?;
            if count <= MAX_LOCATORS {
                aliases.push(PrefixAlias {
                    base: alias.base,
                    locators: count,
                    digest: extend_digest(alias.digest, delta)?,
                });
            }
        }
        if aliases.len() >= MAX_LOCATORS {
            return Err(Error::Control("checkpoint alias bound exceeded"));
        }
        aliases.push(PrefixAlias {
            base: previous.root,
            locators: delta.len(),
            digest: locator_prefix_digest(delta)?,
        });
        Ok(MaterializedBundlePrefix {
            root,
            pin: self.pin,
            aliases,
        })
    }

    pub(crate) fn continues_selected_prefix(
        &self,
        previous: &Self,
        checkpoint: Option<&MaterializedBundlePrefix>,
    ) -> Result<()> {
        if self.pin != previous.pin
            || self.session != previous.session
            || self.head.epoch != previous.head.epoch
            || self.binding.application != previous.binding.application
            || self.binding.control.cell != previous.binding.control.cell
            || self.binding.control.incarnation != previous.binding.control.incarnation
            || self.binding.control.epoch != previous.binding.control.epoch
            || self.binding.control.code != previous.binding.control.code
            || self.binding.control.schema != previous.binding.control.schema
            || self.binding.selected_commit < previous.binding.selected_commit
        {
            return Err(Error::Control(
                "selected bundle does not continue root debt",
            ));
        }
        let base = self.base()?;
        let previous_base = previous.base()?;
        let prefix = if base == previous_base {
            previous.binding.locators.as_slice()
        } else {
            let checkpoint = checkpoint.ok_or(Error::Control(
                "selected bundle changes an unconfirmed base",
            ))?;
            if checkpoint.root != base
                || checkpoint.pin != previous.pin
                || checkpoint.root.commit_sequence > previous.commit_sequence()
                || checkpoint.root.position.txid > previous.position().txid
            {
                return Err(Error::Control("selected bundle changes the checkpoint"));
            }
            let alias = checkpoint
                .aliases
                .iter()
                .find(|alias| alias.base == previous_base)
                .ok_or(Error::Control(
                    "selected bundle lacks original base witness",
                ))?;
            let materialized =
                previous
                    .binding
                    .locators
                    .get(..alias.locators)
                    .ok_or(Error::Control(
                        "selected bundle omits materialized locators",
                    ))?;
            if locator_prefix_digest(materialized)? != alias.digest {
                return Err(Error::Control(
                    "selected bundle changes materialized locators",
                ));
            }
            // Only the original confirmed prefix may disappear. Compare the
            // complete remaining suffix, not an endpoint watermark.
            &previous.binding.locators[alias.locators..]
        };
        if !self.binding.locators.starts_with(prefix) {
            return Err(Error::Control(
                "selected bundle does not continue root debt",
            ));
        }
        Ok(())
    }
}
