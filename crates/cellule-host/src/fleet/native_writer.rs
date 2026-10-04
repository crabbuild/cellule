//! Shared authentication of an existing managed native writer.
use super::FleetRoster;
use crate::CellNode;
use cellule_runtime::{
    Error, Result,
    cell::{actor::CellServingObservation, catalog::CatalogProof},
    control::authority::CellAuthority,
    identity::{CellTarget, IncarnationId, NodeId},
    node::NodeDirectory,
};

pub(in crate::fleet) struct NativeWriter<'a> {
    pub node: NodeId,
    pub host: &'a CellNode,
    pub catalog: &'a CatalogProof,
    pub authority: &'a CellAuthority,
}

#[allow(clippy::too_many_arguments)]
pub(in crate::fleet) async fn observe(
    target: &CellTarget,
    incarnation: IncarnationId,
    minimum_epoch: u64,
    inputs: NativeWriter<'_>,
    roster: &FleetRoster,
    directory: &NodeDirectory,
    now_ms: i64,
) -> Result<CellServingObservation> {
    if !inputs.host.is_management_ready() {
        return Err(Error::CellDraining);
    }
    let observation = inputs
        .host
        .runtime()
        .observe_serving(inputs.catalog, inputs.authority, incarnation, minimum_epoch)
        .await?;
    if observation.owner().session != inputs.host.session {
        return Err(Error::Fenced);
    }
    let boot = roster.boot(inputs.node, observation.owner().session)?;
    let original_boot = inputs
        .host
        .fleet_startup
        .lock()
        .map_err(|_| Error::Control("CellNode fleet startup lock poisoned"))?
        .as_ref()
        .and_then(|startup| startup.boot.clone())
        .ok_or(Error::Fenced)?;
    if original_boot.spec() != boot.enrollment().spec()
        || original_boot.accepted_at_ms() != boot.enrollment().accepted_at_ms()
        || original_boot.established_evidence() != boot.enrollment().established_evidence()
    {
        return Err(Error::Fenced);
    }
    let signed = directory
        .load_if_live(observation.owner().session, now_ms)
        .await?
        .ok_or(Error::Fenced)?;
    let ad = signed.advertisement();
    if ad.release() != inputs.host.application().registry().release_digest()
        || ad.node() != inputs.node
        || ad.endpoint() != observation.owner().endpoint
        || !inputs.host.application().registry().supports_cell(
            target.namespace(),
            inputs.catalog.entry().role(),
            observation.native().code,
            observation.native().schema,
        )
    {
        return Err(Error::Fenced);
    }
    if !inputs.host.is_management_ready() {
        return Err(Error::CellDraining);
    }
    Ok(observation)
}
