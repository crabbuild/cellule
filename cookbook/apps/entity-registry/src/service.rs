use crate::{
    DIRECTORY, Device, DeviceClient, DeviceKey, Devices, Directory, DirectoryClient,
    EntityRegistry, ProgressError, ProjectionOutcome, ProjectionState,
};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, LocalPeer};
use cellule_runtime::{
    CellTarget, Error as RuntimeError,
    codec::{BoundedDecoder, WireValue},
    peer::{
        EffectPeerClient, PeerAuthorizer, PeerPrincipal, PeerRoundTrip, VerifiedPeerRequest, wire,
    },
    primitives::effects::{EffectRunOutcome, EffectSupervisor, EffectSupervisorError},
};
use prost::Message as _;
use serde::Serialize;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{sync::mpsc, time::sleep};
use tokio_util::sync::CancellationToken;
/// Assembly and delivery failures retain their originating framework evidence.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Node/provider assembly failure.
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
    /// Native contract or transport failure.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    /// Source progress read failure.
    #[error(transparent)]
    Progress(#[from] ProgressError),
    /// Native Effect settlement failure, including uncertain source transitions.
    #[error(transparent)]
    Effect(#[from] EffectSupervisorError),
}
/// Checkpoint emitted after durable directory acceptance, before source acknowledgement.
#[derive(Clone, Debug, Serialize)]
pub struct DeliveryProgress {
    /// Canonical source identity decoded from the signed invocation.
    pub key: DeviceKey,
    /// Revision carried by this Effect; a newer revision may already be projected.
    pub revision: i64,
    /// Stable native Effect identity rendered as lowercase hexadecimal.
    pub effect_id: String,
    /// Source publication sequence, meaningful only within that device Cell.
    pub source_commit_sequence: u64,
    /// Directory publication sequence, meaningful only within the directory Cell.
    pub destination_commit_sequence: u64,
    /// Receiver decision, including harmless acceptance of stale revisions.
    pub outcome: ProjectionOutcome,
}
/// Bounded observations and local fault controls for reproducible recovery journeys.
#[derive(Clone, Default)]
pub struct DeliveryOptions {
    /// Delay first successful receiver reply by at most 10 seconds; drain skips delay.
    pub after_publication: Duration,
    /// Lose first successful receiver reply; the native supervisor resolves its inbox.
    pub drop_reply_once: bool,
    /// Optional bounded nonblocking diagnostic channel; never a durability path.
    pub progress: Option<mpsc::Sender<DeliveryProgress>>,
}
struct Authorizer;
impl PeerAuthorizer for Authorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        let permitted = match request.operation() {
            Some(wire::peer_request::Operation::Read(read)) => matches!(
                read.operation,
                Some(wire::read_request::Operation::Describe(true))
            ),
            Some(wire::peer_request::Operation::DeliverEffect(effect)) => {
                matches!(&effect.operation, Some(wire::effect_request::Operation::CellCommand(c)) if c.command_id==1 && c.codec_version==1)
            }
            Some(wire::peer_request::Operation::ResolveEffect(_)) => true,
            _ => false,
        };
        if permitted
            && request.target().namespace() == DIRECTORY
            && request.permits("cookbook.device.project")
        {
            Ok(())
        } else {
            Err(RuntimeError::PeerAuthorization(
                "device projection permission required",
            ))
        }
    }
}
struct ObservedTransport {
    peer: LocalPeer,
    options: DeliveryOptions,
    used: Arc<AtomicBool>,
    cancel: CancellationToken,
}
impl PeerRoundTrip for ObservedTransport {
    fn send(
        &self,
        target: CellTarget,
        bytes: Vec<u8>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let peer = self.peer.clone();
        let options = self.options.clone();
        let used = self.used.clone();
        let cancel = self.cancel.clone();
        Box::pin(async move {
            let request = peer.verify_request(&bytes)?;
            let invocation = match request.operation() {
                Some(wire::peer_request::Operation::DeliverEffect(effect)) => {
                    let identity = effect
                        .identity
                        .as_ref()
                        .ok_or(RuntimeError::Peer("missing device Effect identity"))?;
                    let Some(wire::effect_request::Operation::CellCommand(command)) =
                        &effect.operation
                    else {
                        return Err(RuntimeError::Peer("unsupported projection operation"));
                    };
                    let mut decoder = BoundedDecoder::new(&command.input, 1024)?;
                    let device = Device::decode(&mut decoder)?;
                    decoder.finish()?;
                    let id: [u8; 32] =
                        identity.effect_id.as_slice().try_into().map_err(|_| {
                            RuntimeError::Peer("invalid projection Effect identity")
                        })?;
                    Some((
                        device,
                        blake3::Hash::from_bytes(id).to_hex().to_string(),
                        identity.source_sequence,
                    ))
                }
                _ => None,
            };
            let reply = peer.send(target, bytes, limit).await?;
            if let Some((device, effect_id, source_commit_sequence)) = invocation {
                let response = wire::PeerReply::decode(reply.as_slice())?;
                if let Some(wire::peer_reply::Outcome::Mutation(mutation)) = response.outcome
                    && let Some(wire::mutation_reply::Outcome::Result(result)) = mutation.outcome
                    && let Some(wire::mutation_result::Result::CommandOutput(bytes)) = result.result
                {
                    let receipt = mutation
                        .receipt
                        .ok_or(RuntimeError::Peer("projection reply has no receipt"))?;
                    let mut decoder = BoundedDecoder::new(&bytes, 16)?;
                    let outcome = ProjectionOutcome::decode(&mut decoder)?;
                    decoder.finish()?;
                    if let Some(sender) = &options.progress {
                        let _ = sender.try_send(DeliveryProgress {
                            key: device.key,
                            revision: device.revision,
                            effect_id,
                            source_commit_sequence,
                            destination_commit_sequence: receipt.commit_sequence,
                            outcome,
                        });
                    }
                    if !used.swap(true, Ordering::SeqCst) {
                        tokio::select! { () = sleep(options.after_publication) => {}, () = cancel.cancelled() => {} }
                        if options.drop_reply_once {
                            return Err(RuntimeError::PeerTransportUnknown {
                                context: "injected lost projection reply after publication",
                                source: Box::new(std::io::Error::new(
                                    std::io::ErrorKind::ConnectionReset,
                                    "reply lost",
                                )),
                            });
                        }
                    }
                }
            }
            Ok(reply)
        })
    }
}
/// Installs owned native delivery workers for an explicit roster of 1..3 source keys.
///
/// Source and directory may be owned by different nodes. The destination handle
/// comes from `directory_node`; the source client is bound to `source_node`.
/// Invoke once before ingress; drain both nodes on startup failure. The local
/// profile bounds resident SQL Cells; larger deployments supply fleet routing.
/// A directory page is never treated as a complete source roster.
pub async fn spawn_delivery(
    source_node: &LocalNode,
    directory_node: &LocalNode,
    source_handle: ApplicationHandle<EntityRegistry>,
    directory_handle: ApplicationHandle<EntityRegistry>,
    keys: &[DeviceKey],
    options: DeliveryOptions,
) -> Result<(), ServiceError> {
    if keys.is_empty()
        || keys.len() > 3
        || keys
            .iter()
            .enumerate()
            .any(|(i, key)| keys[..i].contains(key))
        || options.after_publication > Duration::from_secs(10)
    {
        return Err(RuntimeError::Command(
            "delivery requires 1..3 distinct device keys and delay at most 10 seconds",
        )
        .into());
    }
    let destination = DirectoryClient::new(directory_handle.clone())?;
    // Pin tenant, application, namespace and partition before starting any worker.
    let expected = source_handle.target_for_scope(DIRECTORY, b"directory")?;
    if destination.target() != &expected
        || source_handle.compiled().descriptor_digest()
            != directory_handle.compiled().descriptor_digest()
    {
        return Err(RuntimeError::Identity(
            "projection nodes have different application scopes or descriptors",
        )
        .into());
    }
    let inbox = directory_node
        .open_cell(destination.target(), &Directory)
        .await?;
    let peer = LocalPeer::new(
        source_handle.compiled().registry(),
        destination.target().clone(),
        inbox,
        Arc::new(Authorizer),
    );
    let mut sources = Vec::with_capacity(keys.len());
    for key in keys {
        let client = DeviceClient::new(source_handle.clone(), key.clone())?;
        source_node.open_cell(client.target(), &Devices).await?;
        if client
            .progress(None)
            .await?
            .output
            .is_some_and(|v| v.state == ProjectionState::Failed)
        {
            return Err(RuntimeError::Command("device has a failed projection; investigate and publish a new conditional edit before serving").into());
        }
        let effects = source_handle.effects::<Devices>(client.target().clone())?;
        sources.push((client, effects));
    }
    let used = Arc::new(AtomicBool::new(false));
    for (client, source) in sources {
        let peer = peer.clone();
        let used = used.clone();
        let options = options.clone();
        source_node.spawn_worker(move |cancel| async move {
            let principal = PeerPrincipal {
                issuer: "entity-registry".into(),
                subject: "directory-projector".into(),
                actions: vec![
                    "cell.read".into(),
                    "cell.write".into(),
                    "cookbook.device.project".into(),
                ],
            };
            let transport = ObservedTransport {
                peer: peer.clone(),
                options,
                used,
                cancel: cancel.clone(),
            };
            let supervisor = EffectSupervisor::new(
                source,
                EffectPeerClient::new(peer.signer(), principal, Arc::new(transport)),
                15_000,
            )?;
            while !cancel.is_cancelled() {
                // Native settlement retains both success and business rejection answers.
                match supervisor.run_once().await? {
                    EffectRunOutcome::Failed { .. } => {
                        return Err(ServiceError::Runtime(RuntimeError::Command(
                            "directory projection exhausted its attempts",
                        )));
                    }
                    EffectRunOutcome::Delivered { destination, .. } => {
                        let mut decoder = BoundedDecoder::new(destination.result(), 16)
                            .map_err(RuntimeError::from)?;
                        let result =
                            ProjectionOutcome::decode(&mut decoder).map_err(RuntimeError::from)?;
                        decoder.finish().map_err(RuntimeError::from)?;
                        if matches!(
                            result,
                            ProjectionOutcome::Conflict | ProjectionOutcome::Capacity
                        ) {
                            return Err(ServiceError::Runtime(RuntimeError::Command(
                                "directory rejected the projection; durable source answer retained",
                            )));
                        }
                    }
                    EffectRunOutcome::Idle { .. } => {
                        // Maintenance can expire ready intents before a delivery attempt.
                        // Concurrent source edits can prevent a bounded observation; the
                        // next claim will pick up their intents rather than fail readiness.
                        let progress = match client.progress(None).await {
                            Ok(value) => value.output,
                            Err(ProgressError::Changed) => None,
                            Err(error) => return Err(error.into()),
                        };
                        if progress.is_some_and(|v| v.state == ProjectionState::Failed) {
                            return Err(ServiceError::Runtime(RuntimeError::Command(
                                "device has a terminally failed projection",
                            )));
                        }
                    }
                    _ => {}
                }
                tokio::select! {
                    () = cancel.cancelled() => {},
                    () = sleep(Duration::from_millis(100)) => {},
                }
            }
            Ok::<_, ServiceError>(())
        })?;
    }
    Ok(())
}
