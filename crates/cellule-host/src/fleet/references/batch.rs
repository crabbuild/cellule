//! Shared directory reads retain each physical follower's full traversal proof.
use super::*;
use std::collections::HashSet;
use tokio::time::timeout_at;

const PAGE_ENTRIES: usize = 128;

impl FleetFollowerReferences {
    /// Collects up to 128 distinct physical followers in request order.
    /// Every continuation traverses fresh authenticated canonical records;
    /// total page rows across the active windows stay bounded by 128. Complete
    /// per-member inventories retain at most 10,000 rows each, accounted by the
    /// application exactly as for individual collection. Any failure returns no
    /// partial set. This does not confirm the journal or native role barrier.
    pub async fn collect_all(
        directory: &NodeDirectory,
        roster: &FleetRoster,
        members: &[NodeId],
        page_limit: usize,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<Vec<Self>> {
        let mut unique = HashSet::new();
        if members.is_empty()
            || members.len() > PAGE_ENTRIES
            || !(1..=PAGE_ENTRIES).contains(&page_limit)
            || roster.snapshot().registry().bootstrap_revision().is_none()
            || directory.fleet() != roster.snapshot().head().scope().fleet
            || members.iter().any(|member| {
                !unique.insert(*member)
                    || !roster
                        .intents()
                        .iter()
                        .any(|intent| intent.node() == *member)
            })
        {
            return Err(Error::Fenced);
        }
        let mut scans = (0..members.len())
            .map(|_| traversal::Scan::default())
            .collect::<Vec<_>>();
        let mut pending = (0..members.len()).collect::<Vec<_>>();
        while !pending.is_empty() {
            if Instant::now() >= deadline {
                return Err(Error::Deadline);
            }
            let requests = pending
                .iter()
                .map(|index| (members[*index], scans[*index].next))
                .collect::<Vec<_>>();
            let limit = page_limit.min(PAGE_ENTRIES / pending.len());
            let started = clock()?;
            let pages = timeout_at(
                deadline,
                directory.follower_logs_pages(&requests, limit, started),
            )
            .await
            .map_err(|source| Error::Facility {
                name: "fleet-log-inventory-deadline",
                source: Box::new(source),
            })??;
            let finished = clock()?;
            if pages.len() != pending.len() {
                return Err(Error::Node("authoritative follower batch differs"));
            }
            let mut next = Vec::new();
            for (index, page) in pending.into_iter().zip(pages) {
                if !scans[index].accept(members[index], page, started, finished)? {
                    next.push(index);
                }
            }
            pending = next;
        }
        let digest = roster.digest()?;
        scans
            .into_iter()
            .zip(members)
            .map(|(scan, member)| {
                Ok(Self {
                    member: *member,
                    roster: digest,
                    snapshot: roster.snapshot().clone(),
                    topology: scan.topology.ok_or(Error::Fenced)?,
                    started_at_ms: scan.started_at_ms.ok_or(Error::Fenced)?,
                    finished_at_ms: scan.finished_at_ms,
                    collected_at_ms: scan.finished_at_ms,
                    rechecked: None,
                    entries: scan.entries,
                })
            })
            .collect()
    }

    /// Rechecks the whole set after native collection using fresh shared reads.
    /// Starting this call invalidates every previous recheck immediately. All
    /// members must match their original exact rows/topology, full roster and
    /// capture interval before any confirmation advances. A changed member,
    /// deadline, cancellation or source error leaves all members unconfirmed
    /// with their original rows/times intact; retry performs a fresh traversal.
    pub async fn recheck_all(
        original: &mut [Self],
        directory: &NodeDirectory,
        roster: &FleetRoster,
        page_limit: usize,
        deadline: Instant,
        clock: impl FnMut() -> Result<i64>,
    ) -> Result<()> {
        for references in original.iter_mut() {
            references.rechecked = None;
        }
        let digest = roster.digest()?;
        if original.iter().any(|references| {
            roster.snapshot() != &references.snapshot || digest != references.roster
        }) {
            return Err(Error::Fenced);
        }
        let members = original
            .iter()
            .map(|references| references.member)
            .collect::<Vec<_>>();
        let fresh =
            Self::collect_all(directory, roster, &members, page_limit, deadline, clock).await?;
        for (original, fresh) in original.iter().zip(&fresh) {
            if fresh.started_at_ms < original.finished_at_ms
                || fresh.finished_at_ms - original.started_at_ms > 30_000
                || fresh.topology != original.topology
                || fresh.entries != original.entries
            {
                return Err(Error::Node("authoritative follower inventory changed"));
            }
        }
        for (original, fresh) in original.iter_mut().zip(fresh) {
            original.finished_at_ms = fresh.finished_at_ms;
            original.rechecked = Some((fresh.started_at_ms, fresh.finished_at_ms));
        }
        Ok(())
    }
}
