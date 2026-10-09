//! One fresh, bounded object read shared by an individual selection operation.
use super::*;

pub(super) struct OriginBundle {
    session: SessionId,
    head: NodeBundleHead,
    body: Bytes,
}

impl OriginBundle {
    pub(super) async fn load(
        layout: &cellule_ltx::CellStorageLayout,
        prepared: &PreparedNodeBundle,
    ) -> Result<Self> {
        let (body, _) = layout
            .store()
            .get_with_etag_bounded(
                &layout.node_coverage_bundle_path(
                    prepared.catalog.session.as_bytes(),
                    prepared.head.epoch,
                    prepared.head.digest.as_bytes(),
                ),
                MAX_BUNDLE_BYTES,
            )
            .await?;
        // Uploaded proposal bytes alone confer no availability. This fresh
        // origin observation must match all bytes, not just the index header.
        if body != prepared.body || index::body_digest(&body)? != prepared.head.digest {
            return Err(Error::Node("bundle proposal differs from canonical origin"));
        }
        Ok(Self {
            session: prepared.catalog.session,
            head: prepared.head,
            // Reuse the proposal's allocation only after this operation's
            // complete fresh read matched it. The original 4-MiB read buffer
            // can then cover bounded historical scratch and checked facts.
            body: prepared.body.clone(),
        })
    }

    pub(super) fn selected_bindings(&self, prepared: &PreparedNodeBundle) -> Result<Vec<Binding>> {
        if self.session != prepared.catalog.session || self.head != prepared.head {
            return Err(Error::Node("bundle metadata origin differs"));
        }
        // Encoding checked these exact private rows and their canonical extents.
        // This operation's complete fresh read matched the encoded bytes before
        // this view can exist. Re-decoding their shards/histories repeats that
        // work; it supplies no additional origin observation. Dependency bodies
        // remain independently verified in the canonical cohort verifier.
        Ok(prepared
            .catalog
            .bindings
            .iter()
            .filter(|binding| {
                binding
                    .locators
                    .iter()
                    .any(|locator| locator.object.is_none())
            })
            .cloned()
            .map(|mut binding| {
                for locator in &mut binding.locators {
                    if locator.object.is_none() {
                        locator.object = Some(self.head.digest);
                    }
                }
                binding
            })
            .collect())
    }

    pub(super) fn range(
        &self,
        session: SessionId,
        epoch: u64,
        object: Digest,
        range: &std::ops::Range<u64>,
    ) -> Result<Option<Bytes>> {
        if session != self.session || epoch != self.head.epoch || object != self.head.digest {
            return Ok(None);
        }
        if range.start > range.end || range.end > self.body.len() as u64 {
            return Err(Error::Node("bundle extent is truncated"));
        }
        let start =
            usize::try_from(range.start).map_err(|_| Error::Node("bundle extent overflow"))?;
        let end = usize::try_from(range.end).map_err(|_| Error::Node("bundle extent overflow"))?;
        Ok(Some(self.body.slice(start..end)))
    }
}

pub(super) async fn read_range(
    layout: &cellule_ltx::CellStorageLayout,
    session: SessionId,
    epoch: u64,
    object: Digest,
    range: std::ops::Range<u64>,
    origin: Option<&OriginBundle>,
) -> Result<Bytes> {
    if let Some(body) = origin
        .map(|origin| origin.range(session, epoch, object, &range))
        .transpose()?
        .flatten()
    {
        return Ok(body);
    }
    Ok(layout
        .store()
        .range_get(
            &layout.node_coverage_bundle_path(session.as_bytes(), epoch, object.as_bytes()),
            range,
        )
        .await?)
}
