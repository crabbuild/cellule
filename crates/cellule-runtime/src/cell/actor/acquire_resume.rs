//! Original takeover input survives interrupted materialization and activation.
use super::*;
use crate::control::{Control, ControlState};
use crate::recovery::manifest::RecoveryManifestStore;

pub(super) struct TakeoverActivation {
    pub catalog: CatalogProof,
    pub replica: cellule_ltx::CellReplica,
    pub authority: CellAuthority,
    pub input: Control,
    pub claimed: VersionedControl,
    pub recovery_store: RecoveryManifestStore,
    pub destination: PathBuf,
    pub reservation: CellReservation,
    pub node_lease: Option<NodeLeaseGuard>,
    pub observer: Option<Arc<dyn AcquisitionObserver>>,
}

impl CellRuntime {
    /// Resumes this boot's exact interrupted takeover without another ownership
    /// CAS. The original fenced input must match the current claim or its exact
    /// canonical overlay materialization. Reconfirms the durable original input
    /// and result through the recorder before actor admission.
    #[expect(
        clippy::too_many_arguments,
        reason = "resumption binds the original fenced input and exact current claim"
    )]
    pub async fn resume_takeover_restored_observed(
        &self,
        catalog: CatalogProof,
        replica: cellule_ltx::CellReplica,
        authority: CellAuthority,
        original: Control,
        observed: VersionedControl,
        takeover: crate::node::NodeTakeoverProof,
        recovery_store: RecoveryManifestStore,
        destination: PathBuf,
        observer: Arc<dyn AcquisitionObserver>,
    ) -> crate::Result<CellHandle> {
        self.ensure_acquiring()?;
        self.check_application_limits(&catalog, replica.limits())?;
        self.check_application_limits(&catalog, recovery_store.limits())?;
        self.activation_cell(&catalog, &observed)?;
        let owner = observed.value().owner.clone().ok_or(Error::Fenced)?;
        if observed.value().state != ControlState::Recovering
            || original
                .owner
                .as_ref()
                .is_none_or(|owner| owner.session != takeover.session())
            || owner.session != takeover.claimant()
        {
            return Err(Error::Fenced);
        }
        let expected = original.takeover(owner)?;
        if observed.value() != &expected
            && expected
                .validate_transition(observed.value(), Transition::PublishRecovery)
                .is_err()
        {
            return Err(Error::Fenced);
        }
        let current = authority.load(original.cell).await?.ok_or(Error::Fenced)?;
        if current.value() != observed.value() {
            return Err(Error::Fenced);
        }
        let replica = self
            .replica_with_directory_cache(replica, &destination)
            .await?;
        let node_lease = self.inner.node_lease.guard()?;
        let reservation = self.inner.pool.reserve_activation()?;
        observer.before_claim(&original).await?;
        if observed.value() != &expected {
            // PublishRecovery may have committed before its reply or metadata
            // write failed. Derive the exact canonical bytes again; a cleared
            // overlay or matching counters alone cannot establish that result.
            let recovery = original.recovery.as_ref().ok_or(Error::Fenced)?;
            let overlay = recovery_store
                .load_overlay(original.cell, original.incarnation, recovery)
                .await?;
            let prepared = replica
                .prepare_recovered_overlay(&overlay, original.schema)
                .await?;
            let materialized = expected.publish_recovery(&prepared, expected.next_due_ms)?;
            if observed.value() != &materialized {
                return Err(Error::Fenced);
            }
        }
        self.finish_takeover_restored(TakeoverActivation {
            catalog,
            replica,
            authority,
            input: original,
            claimed: observed,
            recovery_store,
            destination,
            reservation,
            node_lease,
            observer: Some(observer),
        })
        .await
    }

    pub(super) async fn finish_takeover_restored(
        &self,
        activation: TakeoverActivation,
    ) -> crate::Result<CellHandle> {
        let TakeoverActivation {
            catalog,
            replica,
            authority,
            input,
            claimed,
            recovery_store,
            destination,
            reservation,
            node_lease,
            observer,
        } = activation;
        let rollback_claim = claimed.clone();
        let materialized = match self
            .publish_attached_recovery(&replica, &authority, claimed, &recovery_store)
            .await
        {
            Ok(materialized) => materialized,
            Err(error) => {
                return match rollback_failed_acquisition(
                    &authority,
                    &rollback_claim,
                    &replica,
                    node_lease,
                )
                .await
                {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(cleanup),
                };
            }
        };
        let rollback_claim = materialized.clone();
        let rollback_authority = authority.clone();
        let rollback_replica = replica.clone();
        let activation = async {
            authority
                .retain_acquisition(&input, materialized.value())
                .await?;
            if let Some(observer) = &observer {
                observer
                    .before_activation(&input, materialized.value())
                    .await?;
            }
            self.activate_restored_reserved(
                catalog,
                replica,
                authority,
                materialized,
                destination,
                reservation,
                None,
            )
            .await
        }
        .await;
        match activation {
            Ok(handle) => Ok(handle),
            Err(error) => match rollback_failed_acquisition(
                &rollback_authority,
                &rollback_claim,
                &rollback_replica,
                node_lease,
            )
            .await
            {
                Ok(()) => Err(error),
                Err(cleanup) => Err(cleanup),
            },
        }
    }
}
