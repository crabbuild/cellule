use super::*;
use crate::control::ControlState;

/// Complete owner observations for one current Cell incarnation.
///
/// Every closed ownership epoch must have its retained original control. The
/// current owner, if present, is the final row. Missing legacy/restore history
/// fails closed; absence cannot exclude a failed original session. This metadata
/// proves no process joining, complete catalog scope, data availability, current
/// successor serving or operation completion.
pub struct CellOwnerHistory {
    current: Control,
    owners: Vec<Control>,
}

impl CellOwnerHistory {
    /// Exact current control checked before and after history collection.
    #[must_use]
    pub const fn current(&self) -> &Control {
        &self.current
    }

    /// Full original owner observations in strictly increasing epoch order.
    #[must_use]
    pub fn owners(&self) -> &[Control] {
        &self.owners
    }
}

impl CellAuthority {
    /// Loads the full retained owner observation for one exact epoch.
    ///
    /// The version 1 path contains canonical control JSON, bounded by 8 KiB.
    /// A retained observation may come from an unsuccessful departure proposal;
    /// it supplies no proof that its release/takeover/tombstone CAS committed.
    pub async fn owner_observation(
        &self,
        cell: CellId,
        incarnation: IncarnationId,
        epoch: u64,
    ) -> Result<Option<Control>> {
        Ok(self
            .load_owner_observation(cell, incarnation, epoch)
            .await?
            .map(|(control, _)| control))
    }

    /// Collects every original owner epoch without listing objects or changing
    /// authority. The caller bounds retained rows and the enclosing deadline.
    /// Concurrent authority changes refuse this observation rather than return
    /// an incomplete or mixed history. This reads only the current incarnation.
    pub async fn owner_history(&self, cell: CellId, limit: usize) -> Result<CellOwnerHistory> {
        let current = self.load(cell).await?.ok_or(Error::Fenced)?.value;
        let count = match current.state {
            ControlState::Tombstoned => current.epoch - 1,
            _ => current.epoch,
        };
        if limit == 0 || count > limit as u64 {
            return Err(Error::Capacity("Cell owner history exceeds its row limit"));
        }
        let closed = if current.owner.is_some() {
            count - 1
        } else {
            count
        };
        let mut owners = Vec::<Control>::new();
        for epoch in 1..=closed {
            let owner = self
                .owner_observation(cell, current.incarnation, epoch)
                .await?
                .ok_or(Error::OwnerHistoryIncomplete {
                    cell,
                    incarnation: current.incarnation,
                    epoch,
                })?;
            if owner.revision >= current.revision
                || owner.progress >= current.progress
                || owners.last().is_some_and(|previous| {
                    previous.revision >= owner.revision || previous.progress >= owner.progress
                })
            {
                return Err(Error::Control("Cell owner history is not ordered"));
            }
            owners.push(owner);
        }
        if current.owner.is_some() {
            owners.push(current.clone());
        }
        if self
            .load(cell)
            .await?
            .is_none_or(|latest| latest.value != current)
        {
            return Err(Error::Fenced);
        }
        Ok(CellOwnerHistory { current, owners })
    }

    pub(super) async fn retain_owner(&self, observed: &Control) -> Result<()> {
        let path = self.layout.owner_observation_path(
            observed.cell.as_bytes(),
            observed.incarnation.as_bytes(),
            observed.epoch,
        );
        let body = Bytes::from(observed.encode()?);
        let previous = self
            .load_owner_observation(observed.cell, observed.incarnation, observed.epoch)
            .await?;
        if let Some((previous, _)) = &previous
            && retained(previous, observed)?
        {
            return Ok(());
        }
        let written = match previous {
            Some((_, token)) => self.layout.store().update(&path, body, token).await,
            None => {
                self.layout
                    .store()
                    .create_strict_with_etag(&path, body)
                    .await
            }
        };
        match written {
            Ok(_) => Ok(()),
            Err(source) => {
                // Inspect ambiguous publication before allowing authority
                // departure. Absence, older history or an unavailable re-read
                // preserves the original write error. The existing coordinator
                // owns retries and full predicate revalidation; no loop here
                // can indefinitely retain a drained owner's metadata job.
                if let Ok(Some((current, _))) = self
                    .load_owner_observation(observed.cell, observed.incarnation, observed.epoch)
                    .await
                    && retained(&current, observed)?
                {
                    return Ok(());
                }
                Err(source.into())
            }
        }
    }

    async fn load_owner_observation(
        &self,
        cell: CellId,
        incarnation: IncarnationId,
        epoch: u64,
    ) -> Result<Option<(Control, ETag)>> {
        if epoch == 0 {
            return Err(Error::Control("Cell owner history epoch is zero"));
        }
        let path =
            self.layout
                .owner_observation_path(cell.as_bytes(), incarnation.as_bytes(), epoch);
        let (body, token) = match self
            .layout
            .store()
            .get_with_etag_bounded(&path, MAX_CONTROL_BYTES)
            .await
        {
            Ok(value) => value,
            Err(StorageError::NotFound { .. }) => return Ok(None),
            Err(source) => return Err(source.into()),
        };
        let control = Control::decode(&body)?;
        if control.cell != cell
            || control.incarnation != incarnation
            || control.epoch != epoch
            || control.owner.is_none()
        {
            return Err(Error::Control(
                "Cell owner history path differs from its scope",
            ));
        }
        Ok(Some((control, token)))
    }
}

// The same canonical epoch may be proposed for departure at several revisions.
// A delayed proposal cannot replace a later observation or conflate boot owners.
fn retained(previous: &Control, observed: &Control) -> Result<bool> {
    let ordered_progress = if previous.revision >= observed.revision {
        previous.progress.checked_sub(observed.progress)
            == Some(previous.revision - observed.revision)
    } else {
        observed.progress.checked_sub(previous.progress)
            == Some(observed.revision - previous.revision)
    };
    if previous.owner != observed.owner
        || previous.cell != observed.cell
        || previous.incarnation != observed.incarnation
        || previous.epoch != observed.epoch
        || !ordered_progress
        || (previous.revision == observed.revision && previous != observed)
    {
        return Err(Error::Control("Cell owner history observations conflict"));
    }
    Ok(previous.revision >= observed.revision)
}
