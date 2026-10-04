//! Exact original request closure through the manager's canonical activation lane.
use super::*;
use cellule_runtime::{
    cell::actor::NodeByteReservation,
    fleet::operations::{EnrollmentRecord, EnrollmentRole, EnrollmentStatus, MAX_RECORD_BYTES},
};
use tokio::time::{Instant, timeout_at};

/// Native joining and confirmed retirement for one exact installed request.
///
/// This local capsule retains the original producer input and final joined prefix.
/// It supplies no replacement policy, writer lineage, fleet roster barrier,
/// process-restart exclusion or maintenance completion. Applications retain the
/// evidence through their existing durable authenticated owner before reporting
/// settlement; losing this capsule cannot make a terminal journal row a join proof.
pub struct ReaderEnrollmentRetirement {
    original: EnrollmentRecord,
    source: ReadReplicaSource,
    retired: EnrollmentRecord,
    receipt: Receipt,
    interval: (i64, i64),
    _memory: NodeByteReservation,
}
impl ReaderEnrollmentRetirement {
    /// Original Established request supplied to the exact native closure.
    #[must_use]
    pub fn original(&self) -> &EnrollmentRecord {
        &self.original
    }
    /// Original canonical catalog, authority and signed source boot input.
    #[must_use]
    pub fn source(&self) -> &ReadReplicaSource {
        &self.source
    }
    /// Confirmed terminal row, retaining original acceptance and establishment.
    #[must_use]
    pub fn retired(&self) -> &EnrollmentRecord {
        &self.retired
    }
    /// Final native prefix after all accepted view work was joined.
    #[must_use]
    pub const fn receipt(&self) -> Receipt {
        self.receipt
    }
    /// Original bounded interval, including activation-lane waiting and publication.
    #[must_use]
    pub const fn interval(&self) -> (i64, i64) {
        self.interval
    }
}

impl ReadReplicaManager {
    /// Joins and retires exactly one installed managed reader request.
    ///
    /// Authenticate both physical endpoints before dispatch. Source maintenance
    /// may use this after establishing the writer successor and current replacement
    /// policy; a receiver on another node can remain Active. The original request,
    /// acceptance, native opening evidence and boot must match under the same lane
    /// as activation and ordinary removal. A delayed request cannot close a newer
    /// reader for the same Cell. Unknown or absent native owners yield no proof.
    ///
    /// No task or admission lane is added. Deadline/cancellation can leave the
    /// original view fenced while its canonical closure or publication is pending;
    /// retry that exact request while the local view and producer remain retained.
    /// A previously discarded owner requires independent durable process evidence.
    /// This grants no fleet settlement/finalization or replacement-policy rights.
    pub async fn remove_enrolled(
        &self,
        original: &EnrollmentRecord,
        deadline: Instant,
    ) -> Result<ReaderEnrollmentRetirement> {
        let captured = Instant::now();
        if captured >= deadline {
            return Err(Error::Deadline);
        }
        let limit = captured
            .checked_add(Duration::from_secs(30))
            .ok_or(Error::Deadline)?;
        let deadline = deadline.min(limit);
        let started = now_ms()?;
        timeout_at(deadline, async {
            // Account dynamic original/source/terminal copies before cloning them.
            let memory = self
                .runtime
                .try_reserve_node_metadata_bytes(4 * MAX_RECORD_BYTES as usize + 4096)?;
            original.to_bytes().map_err(crate::fleet::operation)?;
            let EnrollmentRole::Reader { target, position } = &original.spec().role else {
                return Err(Error::Fenced);
            };
            if original.status() != EnrollmentStatus::Established
                || original.spec().target.session != self.session
                || target.application().as_bytes() != self.layout.application_id()
            {
                return Err(Error::Fenced);
            }
            let _activation = self.activation.lock().await;
            self.ensure_open()?;
            let enrollment = self.bound_enrollment()?.ok_or(Error::Control(
                "exact reader removal requires managed enrollment",
            ))?;
            let (accepted, source) = enrollment.removal_input(original)?;
            enrollment.removal_current(original).await?;
            // The lane pins both indexes. Absence or a different generation is
            // never inferred to be the original joined view from a journal row.
            let reader = self
                .active
                .read()
                .await
                .views
                .get(&target.cell_id())
                .cloned()
                .ok_or(Error::Control(
                    "exact reader removal lacks original native view",
                ))?;
            let before = reader.lifecycle_observation().await.receipt();
            if before.cell != target.cell_id()
                || before.incarnation != position.incarnation
                || before.commit_sequence < position.root.commit_sequence
            {
                return Err(Error::Fenced);
            }
            let (receipt, retired) = self.remove_locked_result(target.cell_id()).await?;
            let receipt = receipt.ok_or(Error::Fenced)?;
            let retired = retired.ok_or(Error::Fenced)?;
            let closed = reader.lifecycle_observation().await;
            if !closed.locally_joined()
                || closed.receipt() != receipt
                || receipt.commit_sequence < before.commit_sequence
                || retired.status() != EnrollmentStatus::Retired
                || retired.spec() != original.spec()
                || retired.accepted_at_ms() != original.accepted_at_ms()
                || retired.established_evidence() != original.established_evidence()
                || retired.settlement_evidence()
                    != Some(ReaderEnrollment::removal_evidence(
                        &accepted, &source, receipt,
                    )?)
            {
                return Err(Error::Fenced);
            }
            let finished = now_ms()?;
            if finished < started || finished - started > 30_000 {
                return Err(Error::Deadline);
            }
            Ok(ReaderEnrollmentRetirement {
                original: original.clone(),
                source,
                retired,
                receipt,
                interval: (started, finished),
                _memory: memory,
            })
        })
        .await
        .map_err(|source| Error::Facility {
            name: "reader-exact-removal-deadline",
            source: Box::new(source),
        })?
    }
}
