use super::*;

impl FleetFailedBootRetirement {
    /// Reuses the immutable original process request after recovery/role closure.
    ///
    /// A fenced request keeps its v2 identity and original collection interval;
    /// a terminal request keeps its v1 identity. This fresh capture still requires
    /// a canonically Retired leader log, every related role settled, complete
    /// physical references and the same original boot/fence. Publication rechecks
    /// the provider and terminal authority independently. Neither an old process
    /// confirmation nor this capture can complete affected-Cell relocation.
    #[allow(clippy::too_many_arguments)]
    pub async fn capture_retained(
        journal: &dyn FleetJournal,
        directory: &NodeDirectory,
        roster: &FleetRoster,
        request: &FleetFailedBootProcessRequest,
        claimant: SessionId,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<Self> {
        let started_at_ms = clock()?;
        if started_at_ms < request.finished_at_ms {
            return Err(Error::Deadline);
        }
        interval(started_at_ms, started_at_ms)?;
        let mut last = started_at_ms;
        let mut clock = || {
            let next = clock()?;
            if next < last {
                return Err(Error::Deadline);
            }
            interval(started_at_ms, next)?;
            last = next;
            Ok(next)
        };
        if roster.snapshot().registry().bootstrap_revision().is_none()
            || roster.snapshot().head().scope() != request.snapshot.head().scope()
            || directory.fleet() != roster.snapshot().head().scope().fleet
        {
            return Err(Error::Fenced);
        }
        roster.confirm(journal, deadline).await?;
        let boot = records::select(roster, &request.boot)?;
        let endpoint = boot.spec().target;
        request
            .confirm_canonical(directory, claimant, clock()?, deadline)
            .await?;
        let canonical = bounded(
            deadline,
            directory.closed_session(endpoint.node, endpoint.session, claimant, clock()?),
        )
        .await?;
        if canonical.fence() != request.fence {
            return Err(Error::Fenced);
        }
        records::references(
            directory,
            roster,
            endpoint.node,
            endpoint.session,
            deadline,
            &mut clock,
        )
        .await?;
        roster.confirm(journal, deadline).await?;
        request
            .confirm_canonical(directory, claimant, clock()?, deadline)
            .await?;
        // Foreign reference collection can suspend. Terminal original authority
        // must agree after it, as well as before it.
        if bounded(
            deadline,
            directory.closed_session(endpoint.node, endpoint.session, claimant, clock()?),
        )
        .await?
            != canonical
        {
            return Err(Error::Fenced);
        }
        let finished_at_ms = clock()?;
        Ok(Self {
            snapshot: roster.snapshot().clone(),
            request: request.clone(),
            canonical,
            started_at_ms,
            finished_at_ms,
        })
    }
}
