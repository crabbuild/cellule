use super::*;
use cellule_runtime::fleet::operations::EnrollmentEvent;
use futures_util::future::join_all;

impl FleetRecoveredFollowerRetirement {
    /// Publishes each original member's canonical retirement through the existing
    /// journal contract. All dispatched waiters settle, retaining failures separately.
    /// The complete post-publication roster and authority must confirm before a
    /// closure is returned. A lost/cancelled waiter supplies no closure: recapture
    /// a fresh roster and replay the same events. Exact duplicates preserve time.
    /// The adapter retains and joins backend work after a waiter deadline/drop.
    /// Own this future in the application's accepted finite work; it starts no
    /// background task, second action bank, recovery or native retirement path.
    pub async fn publish(
        &self,
        journal: &dyn FleetJournal,
        directory: &NodeDirectory,
        claimant: SessionId,
        deadline: Instant,
        mut clock: impl FnMut() -> Result<i64>,
    ) -> Result<FleetRecoveredFollowerPublication> {
        let now = clock()?;
        interval(self.started_at_ms, now)?;
        if now < self.finished_at_ms {
            return Err(Error::Deadline);
        }
        let snapshot = bounded(deadline, async {
            journal
                .load_snapshot(self.snapshot.head().scope())
                .await
                .map_err(journal_error)
        })
        .await?;
        if snapshot != self.snapshot {
            return Err(Error::Fenced);
        }
        self.confirm_authority(directory, claimant, now, deadline)
            .await?;
        let members = join_all(self.members.iter().zip(&self.evidence).map(
            |(original, digest)| async move {
                let result = bounded(deadline, async {
                    let result = journal
                        .publish_enrollment_result(original, EnrollmentEvent::Retired(*digest), now)
                        .await
                        .map_err(journal_error)?;
                    records::validate_result(original, &result, *digest)?;
                    Ok(result)
                })
                .await
                .map_err(Arc::new);
                FleetRecoveredFollowerMember {
                    original: original.clone(),
                    result,
                }
            },
        ))
        .await;
        // Keep every original response even when a sibling or the final complete
        // barrier fails. Neither a successful write nor an absence read upgrades
        // an ambiguous publication to confirmed ensemble closure.
        let mut last_ms = now;
        let mut advancing_clock = || {
            let next = clock()?;
            if next < last_ms {
                return Err(Error::Deadline);
            }
            last_ms = next;
            Ok(next)
        };
        let closure = self
            .finish(
                journal,
                directory,
                claimant,
                deadline,
                &mut advancing_clock,
                &members,
            )
            .await
            .map_err(Arc::new);
        Ok(FleetRecoveredFollowerPublication { members, closure })
    }

    async fn confirm_authority(
        &self,
        directory: &NodeDirectory,
        claimant: SessionId,
        now: i64,
        deadline: Instant,
    ) -> Result<()> {
        if directory.fleet() != self.snapshot.head().scope().fleet {
            return Err(Error::Fenced);
        }
        let (node, retired) = canonical(directory, &self.retired, claimant, now, deadline).await?;
        if node != self.leader_node || retired != self.retired {
            return Err(Error::Fenced);
        }
        Ok(())
    }

    async fn finish(
        &self,
        journal: &dyn FleetJournal,
        directory: &NodeDirectory,
        claimant: SessionId,
        deadline: Instant,
        clock: &mut impl FnMut() -> Result<i64>,
        members: &[FleetRecoveredFollowerMember],
    ) -> Result<FleetRecoveredFollowerClosure> {
        for member in members {
            if let Err(source) = &member.result {
                return Err(retained(Arc::clone(source)));
            }
        }
        let snapshot = bounded(deadline, async {
            journal
                .load_snapshot(self.snapshot.head().scope())
                .await
                .map_err(journal_error)
        })
        .await?;
        let roster = FleetRoster::collect(journal, &snapshot, deadline).await?;
        let rows = records::select(&roster, self.leader_node, &self.retired)?;
        if rows.len() != members.len() {
            return Err(Error::Fenced);
        }
        for ((row, member), digest) in rows.iter().zip(members).zip(&self.evidence) {
            if member
                .result
                .as_ref()
                .map_err(|source| retained(Arc::clone(source)))?
                != row
            {
                return Err(Error::Control("recovered follower publication changed"));
            }
            records::validate_result(&member.original, row, *digest)?;
        }
        let now = clock()?;
        interval(self.started_at_ms, now)?;
        self.confirm_authority(directory, claimant, now, deadline)
            .await?;
        roster.confirm(journal, deadline).await?;
        let finished_at_ms = clock()?;
        interval(self.started_at_ms, finished_at_ms)?;
        if finished_at_ms < now {
            return Err(Error::Deadline);
        }
        Ok(FleetRecoveredFollowerClosure {
            snapshot,
            leader_node: self.leader_node,
            retired: self.retired.clone(),
            digest: records::closure_digest(self.leader_node, &self.retired, &rows)?,
            members: rows,
            started_at_ms: self.started_at_ms,
            finished_at_ms,
        })
    }
}
