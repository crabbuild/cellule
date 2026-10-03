use crate::{INBOX, Inbox, IntervalScheduler, RecordOutcome, SCHEDULES, ScheduleId, Schedules};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, LocalPeer};
use cellule_runtime::{
    ApplicationId, CellTarget, Error as RuntimeError, TenantId,
    codec::{BoundedDecoder, WireValue},
    partition_for_shard,
    peer::{
        EffectPeerClient, PeerAuthorizer, PeerPrincipal, PeerRoundTrip, VerifiedPeerRequest, wire,
    },
    primitives::{
        cron::CronInvocation,
        effects::{EffectRunOutcome, EffectSupervisor, EffectSupervisorError},
    },
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

/// Service errors retain assembly, transport, and unresolved effect evidence.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Node assembly or provider failure.
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
    /// Framework preparation or peer validation failure.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    /// Effect runner failure, including a pending source transition.
    #[error(transparent)]
    Effect(#[from] EffectSupervisorError),
}
/// Receipt-bearing checkpoint after durable receiver publication, before source ack.
#[derive(Clone, Debug, Serialize)]
pub struct DeliveryProgress {
    /// Stable durable effect identity, rendered as 64 lowercase hexadecimal bytes.
    pub effect_id: String,
    /// Source tick publication that created this intent; scoped to the Cron Cell.
    pub source_commit_sequence: u64,
    /// Exact schedule identity from the typed invocation.
    pub schedule: ScheduleId,
    /// Native source generation.
    pub generation: u64,
    /// Source occurrence counter.
    pub occurrence: u64,
    /// SQL row reused by inbox resolution or redelivery.
    pub row: i64,
    /// Destination publication receipt sequence; scoped to the SQL inbox.
    pub destination_commit_sequence: u64,
}
/// Bounded delivery observations and explicit local fault controls.
#[derive(Clone)]
pub struct DeliveryOptions {
    /// Delay the first successful delivery reply, at most 10 seconds.
    /// A graceful drain skips this delay; SIGKILL at the checkpoint tests recovery.
    pub after_publication: Duration,
    /// Drop the first successful reply after acceptance; the native runner resolves it.
    pub drop_reply_once: bool,
    /// Optional bounded nonblocking observer; it never holds up publication or ack.
    pub progress: Option<mpsc::Sender<DeliveryProgress>>,
}
impl Default for DeliveryOptions {
    fn default() -> Self {
        Self {
            after_publication: Duration::ZERO,
            drop_reply_once: false,
            progress: None,
        }
    }
}
/// Opens the two declared Cron shards and one SQL inbox, using native schema installers.
/// Call during application startup before opening ingress; drain the node on failure.
pub async fn open(
    node: &LocalNode,
    tenant: TenantId,
    application: ApplicationId,
) -> Result<ApplicationHandle<IntervalScheduler>, ServiceError> {
    let handle = node.application_handle::<IntervalScheduler>(tenant)?;
    for shard in 0..2 {
        node.open_cell(
            &CellTarget::new(tenant, application, SCHEDULES, &partition_for_shard(shard))?,
            &Schedules,
        )
        .await?;
    }
    node.open_cell(
        &CellTarget::new(tenant, application, INBOX, &partition_for_shard(0))?,
        &Inbox,
    )
    .await?;
    Ok(handle)
}
struct Authorizer;
impl PeerAuthorizer for Authorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        let allowed = match request.operation() {
            Some(wire::peer_request::Operation::Read(read)) => matches!(
                read.operation,
                Some(wire::read_request::Operation::Describe(true))
            ),
            Some(wire::peer_request::Operation::DeliverEffect(effect)) => {
                matches!(&effect.operation,Some(wire::effect_request::Operation::CellCommand(command)) if command.command_id==1 && command.codec_version==1)
            }
            Some(wire::peer_request::Operation::ResolveEffect(_)) => true,
            _ => false,
        };
        if allowed
            && request.target().namespace() == INBOX
            && request.permits("cookbook.reminder.deliver")
        {
            Ok(())
        } else {
            Err(RuntimeError::PeerAuthorization(
                "reminder delivery scope not permitted",
            ))
        }
    }
}
#[derive(Clone)]
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
        remaining_ms: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let transport = self.clone();
        Box::pin(async move {
            let request = transport.peer.verify_request(&bytes)?;
            let invocation = match request.operation() {
                Some(wire::peer_request::Operation::DeliverEffect(effect)) => {
                    let identity = effect
                        .identity
                        .as_ref()
                        .ok_or(RuntimeError::Peer("missing reminder effect identity"))?;
                    let Some(wire::effect_request::Operation::CellCommand(command)) =
                        &effect.operation
                    else {
                        return Err(RuntimeError::Peer("unsupported reminder effect command"));
                    };
                    let mut decoder = BoundedDecoder::new(&command.input, 2048)?;
                    let invocation = CronInvocation::decode(&mut decoder)?;
                    decoder.finish()?;
                    let id: [u8; 32] = identity.effect_id.as_slice().try_into().map_err(|_| {
                        RuntimeError::Peer("invalid reminder effect identity length")
                    })?;
                    Some((
                        invocation,
                        blake3::Hash::from_bytes(id).to_hex().to_string(),
                        identity.source_sequence,
                    ))
                }
                _ => None,
            };
            let reply = transport.peer.send(target, bytes, remaining_ms).await?;
            if let Some((invocation, effect_id, source_commit_sequence)) = invocation {
                let decoded = wire::PeerReply::decode(reply.as_slice())?;
                if let Some(wire::peer_reply::Outcome::Mutation(mutation)) = decoded.outcome
                    && let Some(wire::mutation_reply::Outcome::Result(result)) = mutation.outcome
                    && let Some(wire::mutation_result::Result::CommandOutput(bytes)) = result.result
                {
                    let receipt = mutation
                        .receipt
                        .ok_or(RuntimeError::Peer("reminder result lacks receipt"))?;
                    let mut decoder = BoundedDecoder::new(&bytes, 16)?;
                    let result = RecordOutcome::decode(&mut decoder)?;
                    decoder.finish()?;
                    if let RecordOutcome::Recorded { row } = result {
                        if let Some(progress) = &transport.options.progress {
                            let _ = progress.try_send(DeliveryProgress {
                                effect_id,
                                source_commit_sequence,
                                schedule: ScheduleId::from_bytes(invocation.schedule_id)?,
                                generation: invocation.generation,
                                occurrence: invocation.occurrence,
                                row,
                                destination_commit_sequence: receipt.commit_sequence,
                            });
                        }
                        if transport
                            .used
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            // Destination publication has finished. Keep the exact reply
                            // intact across graceful cancellation so source settlement
                            // completes before node drain; only the explicit fault drops it.
                            tokio::select! {()=transport.cancel.cancelled()=>{},()=sleep(transport.options.after_publication)=>{}}
                            if transport.options.drop_reply_once {
                                return Err(RuntimeError::PeerTransportUnknown {
                                    context: "injected lost reminder delivery reply",
                                    source: Box::new(std::io::Error::new(
                                        std::io::ErrorKind::ConnectionReset,
                                        "receiver applied the reminder before reply loss",
                                    )),
                                });
                            }
                        }
                    }
                }
            }
            Ok(reply)
        })
    }
}
/// Installs two owned native effect runners, one per source shard.
///
/// Call once during application startup, before ingress. The node already owns
/// the bounded scheduler scanner; this installs delivery, not another Tick loop.
/// Accepted deliveries finish source settlement before cancellation is honored.
/// Any runner failure closes readiness; callers must drain after startup failure.
pub async fn spawn_delivery(
    node: &LocalNode,
    handle: ApplicationHandle<IntervalScheduler>,
    options: DeliveryOptions,
) -> Result<(), ServiceError> {
    if options.after_publication > Duration::from_secs(10) {
        return Err(RuntimeError::Command("delivery checkpoint delay exceeds 10 seconds").into());
    }
    let target = handle.target_for_scope(INBOX, b"reminders")?;
    let inbox = node.open_cell(&target, &Inbox).await?;
    let peer = LocalPeer::new(
        handle.compiled().registry(),
        target.clone(),
        inbox,
        Arc::new(Authorizer),
    );
    let used = Arc::new(AtomicBool::new(false));
    for shard in 0..2 {
        let source = CellTarget::new(
            target.tenant(),
            target.application(),
            SCHEDULES,
            &partition_for_shard(shard),
        )?;
        node.open_cell(&source, &Schedules).await?;
        let source = handle.effects::<Schedules>(source)?;
        let peer = peer.clone();
        let used = used.clone();
        let options = options.clone();
        node.spawn_worker(move |cancel| async move {
            let principal = PeerPrincipal {
                issuer: "interval-scheduler".into(),
                subject: "local-reminders".into(),
                actions: vec![
                    "cell.read".into(),
                    "cell.write".into(),
                    "cookbook.reminder.deliver".into(),
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
                let outcome = supervisor.run_once().await?;
                if matches!(outcome, EffectRunOutcome::Failed { .. }) {
                    return Err(ServiceError::Runtime(RuntimeError::Command(
                        "reminder delivery exhausted its attempts",
                    )));
                }
                tokio::select! {()=cancel.cancelled()=>{},()=sleep(Duration::from_millis(100))=>{}}
            }
            Ok::<_, ServiceError>(())
        })?;
    }
    Ok(())
}
