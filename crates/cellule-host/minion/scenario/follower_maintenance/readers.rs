//! Reader obligations on the same physical donor as the foreign follower tail.
use super::*;
use cellule_host::{
    fleet::{
        FleetReaderEvacuationPublication, FleetReaderEvacuationVerifier, FleetReconcileReport,
    },
    read_replicas::{ReadReplicaManager, ReaderEvacuation},
};
use cellule_runtime::{
    client::{CellDescription, CellReadReplica},
    fleet::operations::EnrollmentRecord,
    peer::{PeerReplicaResolver, ReplicaPeerClient},
};

pub(super) struct Readers {
    managers: Vec<ReadReplicaManager>,
    target: CellTarget,
    description: CellDescription,
    peer: ReplicaPeerClient,
    pub(super) verifier: FleetReaderEvacuationVerifier,
    original: EnrollmentRecord,
    reader: CellReadReplica,
}

impl Readers {
    pub(super) async fn initialize(
        journal: &SqliteJournal,
        managers: Vec<ReadReplicaManager>,
        target: CellTarget,
        description: CellDescription,
        peer: ReplicaPeerClient,
        verifier: FleetReaderEvacuationVerifier,
        boots: &[startup::BootOwner],
    ) -> JournalResult<Self> {
        // Both original follower receivers advertise reader capacity. Requiring
        // two readers makes the eventual placement exclude the cordoned donor.
        managers[1]
            .set_target(&target, 0, 2)
            .await?
            .ok_or_else(|| invalid("combined maintenance reader policy is absent"))?;
        let ad = boots[1].refresh_capacity(1, journal, deadline()).await?;
        peer.activate(&target, &boots[0].directory, ad, description)
            .await?;
        let reader = managers[1].resolve(target.clone()).await?;
        let completion = managers[1]
            .enrollment_completion(target.cell_id())
            .await?
            .ok_or_else(|| invalid("combined maintenance reader acceptance is absent"))?;
        let original = journal
            .load_enrollment(scope(), completion.spec.key()?)
            .await?
            .ok_or_else(|| invalid("combined maintenance reader enrollment is absent"))?;
        let inputs = Self {
            managers,
            target,
            description,
            peer,
            verifier,
            original,
            reader,
        };
        inputs.check_original(journal, true).await?;
        Ok(inputs)
    }

    pub(super) async fn check_original(
        &self,
        journal: &SqliteJournal,
        read: bool,
    ) -> JournalResult<()> {
        if self.original.status() != EnrollmentStatus::Established
            || journal
                .load_enrollment(scope(), self.original.spec().key()?)
                .await?
                .as_ref()
                != Some(&self.original)
            || self.reader.lifecycle_observation().await.admission_closed()
        {
            return Err(invalid(
                "combined maintenance prematurely retired its reader",
            ));
        }
        if read
            && self
                .reader
                .query::<application::ReadValue>(None, 0)
                .await?
                .output
                != 29
        {
            return Err(invalid(
                "combined maintenance original reader lost acknowledged state",
            ));
        }
        Ok(())
    }

    pub(super) async fn require_blocked(
        &self,
        report: &FleetReconcileReport,
        journal: &SqliteJournal,
        donor: &CellNode,
    ) -> JournalResult<()> {
        if report
            .snapshot
            .head()
            .maintenance()
            .map(MaintenanceOperation::phase)
            != Some(MaintenancePhase::Evacuating)
            || donor.state() == NodeState::Stopped
            || !donor.is_management_ready()
            || report.blockers.is_empty()
        {
            return Err(invalid(
                "combined maintenance finalized with an unsettled reader",
            ));
        }
        self.check_original(journal, false).await
    }

    pub(super) async fn prepare_replacement(
        &self,
        journal: &SqliteJournal,
        boots: &[startup::BootOwner],
    ) -> JournalResult<()> {
        tokio::time::timeout_at(deadline(), async {
            loop {
                let selected = boots[0]
                    .directory
                    .select_readers(
                        self.target.cell_id(),
                        session(0),
                        self.description.code,
                        2,
                        clock()?,
                        128,
                    )
                    .await?;
                if selected.len() == 2
                    && selected
                        .iter()
                        .all(|ad| ad.session() == session(2) || ad.session() == session(3))
                {
                    return Ok::<_, JournalError>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        let replacement = boots[2].refresh_capacity(2, journal, deadline()).await?;
        self.peer
            .activate(
                &self.target,
                &boots[0].directory,
                replacement,
                self.description,
            )
            .await?;
        let current = journal.load_snapshot(scope()).await?;
        let operation = current
            .head()
            .maintenance()
            .ok_or_else(|| invalid("combined maintenance operation disappeared"))?;
        // Node 3 is live and holds the replacement follower, but has no ready
        // reader. Native evacuation must refuse before closing the old view.
        match self.managers[1]
            .evacuate(&self.original, operation, &self.peer, deadline())
            .await
        {
            Err(cellule_runtime::Error::Control(
                "reader replacement lacks Established enrollment",
            )) => {}
            Err(error) => return Err(Box::new(error)),
            Ok(_) => {
                return Err(invalid(
                    "combined maintenance accepted an absent replacement reader",
                ));
            }
        }
        self.check_original(journal, false).await
    }

    pub(super) async fn complete_replacement(
        &self,
        journal: &SqliteJournal,
        boots: &[startup::BootOwner],
    ) -> JournalResult<ReaderEvacuation> {
        let replacement = boots[3].refresh_capacity(3, journal, deadline()).await?;
        self.peer
            .activate(
                &self.target,
                &boots[0].directory,
                replacement,
                self.description,
            )
            .await?;
        let current = journal.load_snapshot(scope()).await?;
        let operation = current
            .head()
            .maintenance()
            .ok_or_else(|| invalid("combined maintenance operation disappeared"))?;
        let capture = self.managers[1]
            .evacuate(&self.original, operation, &self.peer, deadline())
            .await?;
        let publication = FleetReaderEvacuationPublication::publish(
            &capture,
            journal,
            &self.verifier,
            deadline(),
            clock,
        )
        .await?;
        let record = publication.record()?;
        if record.retired() != capture.retired() || capture.replacements().len() != 2 {
            return Err(invalid(
                "combined maintenance reader publication lost native history",
            ));
        }
        Ok(capture)
    }

    pub(super) async fn readback(
        &self,
        journal: &SqliteJournal,
        capture: &ReaderEvacuation,
    ) -> JournalResult<()> {
        let retired = journal
            .load_enrollment(scope(), self.original.spec().key()?)
            .await?
            .ok_or_else(|| invalid("combined maintenance reader history disappeared"))?;
        let lifecycle = self.reader.lifecycle_observation().await;
        if retired != *capture.retired()
            || retired.status() != EnrollmentStatus::Retired
            || !lifecycle.locally_joined()
        {
            return Err(invalid(
                "combined maintenance original reader was not joined and retired",
            ));
        }
        for index in [2, 3] {
            let reader = self.managers[index].resolve(self.target.clone()).await?;
            if reader
                .query::<application::ReadValue>(Some(capture.minimum()), 0)
                .await?
                .output
                != 29
            {
                return Err(invalid(
                    "combined maintenance replacement reader lost acknowledged state",
                ));
            }
        }
        Ok(())
    }
}
