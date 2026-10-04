//! Bind exact native removal to the original canonical producer input.
use super::protocol::evidence_input;
use super::*;

impl ReaderEnrollment {
    pub(in crate::read_replicas) fn removal_input(
        &self,
        original: &EnrollmentRecord,
    ) -> Result<(EnrollmentRecord, ReadReplicaSource)> {
        let EnrollmentRole::Reader { target, position } = &original.spec().role else {
            return Err(Error::Fenced);
        };
        let record = self
            .records()?
            .get(&target.cell_id())
            .cloned()
            .ok_or(Error::Fenced)?;
        let progress = data(&record)?;
        let accepted = progress.original.as_ref().ok_or(Error::Fenced)?;
        if original.spec() != &progress.spec
            || original.spec().target.node != self.node
            || original.accepted_at_ms() != accepted.accepted_at_ms()
            || original.status() != EnrollmentStatus::Established
            || !progress.opening_started
            || !progress.opening_joined
            || progress.execution_error.is_some()
            || original.established_evidence()
                != Some(evidence_input(
                    accepted,
                    &progress.source,
                    b"opened",
                    Some(Receipt {
                        cell: target.cell_id(),
                        incarnation: position.incarnation,
                        commit_sequence: position.root.commit_sequence,
                    }),
                )?)
        {
            return Err(Error::Fenced);
        }
        Ok((accepted.clone(), progress.source.clone()))
    }

    pub(in crate::read_replicas) async fn removal_current(
        &self,
        original: &EnrollmentRecord,
    ) -> Result<EnrollmentRecord> {
        let row = self
            .journal
            .load_enrollment(self.scope, original.spec().key().map_err(operation)?)
            .await
            .map_err(journal)?
            .ok_or(Error::Fenced)?;
        if (row.status() == EnrollmentStatus::Established && &row != original)
            || !matches!(
                row.status(),
                EnrollmentStatus::Established | EnrollmentStatus::Retired
            )
            || row.spec() != original.spec()
            || row.accepted_at_ms() != original.accepted_at_ms()
            || row.established_evidence() != original.established_evidence()
            || row.updated_at_ms() < original.updated_at_ms()
        {
            return Err(Error::Fenced);
        }
        Ok(row)
    }

    pub(in crate::read_replicas) fn removal_evidence(
        accepted: &EnrollmentRecord,
        source: &ReadReplicaSource,
        receipt: Receipt,
    ) -> Result<Digest> {
        evidence_input(accepted, source, b"joined-closure", Some(receipt))
    }
}
