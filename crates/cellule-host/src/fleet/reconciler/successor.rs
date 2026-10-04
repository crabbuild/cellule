use super::*;
use cellule_runtime::fleet::operations::{
    ActivationEvidence, EnrollmentRole, EnrollmentStatus, MoveAttempt,
};
use cellule_runtime::identity::{NodeId, SessionId};

impl FleetReconciler {
    pub(super) async fn successor_endpoint(
        &self,
        attempt: &MoveAttempt,
        clock: &PassClock<'_>,
        report: &FleetReconcileReport,
    ) -> Result<Option<(NodeId, SessionId)>> {
        let roster =
            FleetRoster::collect(self.journal.as_ref(), &report.snapshot, clock.deadline).await?;
        let observation = call(
            clock.deadline,
            "fleet-observer",
            self.observer.observe(&roster, clock.now()?, clock.deadline),
        )
        .await?;
        if observation.scope != self.scope || observation.registry != report.snapshot.registry() {
            return Err(operation(OperationError::Conflict));
        }
        roster
            .confirm(self.journal.as_ref(), clock.deadline)
            .await?;
        observation.placements(clock.now()?)?;
        for owned in &observation.cells {
            let row = &owned.observation;
            if row.target != attempt.spec().target || row.incarnation != attempt.spec().incarnation
            {
                continue;
            }
            let Some(position) = &row.position else {
                continue;
            };
            let hint = ActivationEvidence {
                node: owned.node,
                session: owned.session,
                position: position.clone(),
            };
            if attempt.validate_activation(&hint).is_err() {
                continue;
            }
            let enrolled = roster
                .intents()
                .iter()
                .filter(|intent| intent.node() == owned.node && intent.session() == owned.session)
                .any(|intent| {
                    roster.enrollments().iter().any(|record| {
                        record.status() == EnrollmentStatus::Established
                            && matches!(record.spec().role, EnrollmentRole::Node { .. })
                            && crate::fleet::FleetBootObservation::new(
                                intent.clone(),
                                record.clone(),
                            )
                            .is_ok()
                    })
                });
            if !enrolled {
                continue;
            }
            // This bounded, authenticated row selects a read endpoint only.
            // Native authority + actor inspection supplies the serving proof;
            // incomplete role coverage cannot establish absence/finalization.
            return Ok(Some((owned.node, owned.session)));
        }
        Ok(None)
    }
}
