use crate::{
    DEADLINES, DeadlineTicket, Deadlines, EventKey, Expiration, ExpirationOutcome, ExpireHold,
    INVENTORY, InventoryCells, ReservationClient, Reservations, ScheduleDeadline,
};
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, LocalPeer, SiblingNodeFactory};
use cellule_runtime::{
    CellTarget, Error as RuntimeError,
    codec::{BoundedDecoder, WireValue},
    partition_for_shard,
    peer::{
        EffectPeerClient, PeerAuthorizer, PeerPrincipal, PeerRoundTrip, VerifiedPeerRequest, wire,
    },
    primitives::{
        effects::{
            EffectModule, EffectRunOutcome, EffectSource, EffectState, EffectSupervisor,
            EffectSupervisorError,
        },
        workflow::WorkflowOutcome,
    },
    registry::Command,
};
use prost::Message as _;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Mutex, mpsc},
    time::sleep,
};
use tokio_util::sync::CancellationToken;
/// Owned worker failures preserve source errors and native uncertainty evidence.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Node or provider failure.
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
    /// Framework contract failure.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    /// Native Effect delivery or uncertain source settlement failure.
    #[error(transparent)]
    Effect(#[from] EffectSupervisorError),
    /// Source inventory query failure.
    #[error("inventory supervision query failed: {0}")]
    Query(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Independent deadline observation failure.
    #[error(transparent)]
    Deadline(#[from] crate::DeadlineReadError),
}
/// Bounded diagnostic controls used by the runnable recovery scenario.
#[derive(Clone, Default)]
pub struct DeliveryOptions {
    /// Delay the first expiration reply after destination publication, at most ten seconds.
    /// Graceful cancellation skips the delay and allows source settlement to finish.
    pub after_publication: Duration,
    /// Lose the first expiration reply; the native supervisor resolves its original inbox.
    pub drop_reply_once: bool,
    /// Bounded nonblocking diagnostic channel; never a publication or completion path.
    pub progress: Option<mpsc::Sender<DeliveryProgress>>,
}
/// Checkpoint after expiration destination publication and before source acknowledgement.
#[derive(Clone, Debug, serde::Serialize)]
pub struct DeliveryProgress {
    /// Original conditional expiration capability.
    pub ticket: DeadlineTicket,
    /// Native immutable Effect identity in lowercase hexadecimal.
    pub effect_id: String,
    /// Workflow Cell publication sequence.
    pub source_commit_sequence: u64,
    /// Inventory Cell publication sequence, in a separate receipt domain.
    pub destination_commit_sequence: u64,
    /// Destination decision, including harmless terminal no-ops.
    pub outcome: ExpirationOutcome,
}
struct Authorizer {
    event: EventKey,
    inventory: CellTarget,
    deadlines: Vec<CellTarget>,
}
impl PeerAuthorizer for Authorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        if !request.permits("cookbook.reservation.coordinate") {
            return Err(RuntimeError::PeerAuthorization(
                "reservation coordinator permission required",
            ));
        }
        let target = request.target();
        if target.tenant() != self.inventory.tenant()
            || target.application() != self.inventory.application()
            || !(target.namespace() == INVENTORY || self.deadlines.contains(target))
        {
            return Err(RuntimeError::PeerAuthorization(
                "foreign reservation destination",
            ));
        }
        let allowed = match request.operation() {
            Some(wire::peer_request::Operation::Read(read)) => matches!(
                read.operation,
                Some(wire::read_request::Operation::Describe(true))
            ),
            Some(wire::peer_request::Operation::ResolveEffect(_)) => true,
            Some(wire::peer_request::Operation::DeliverEffect(effect)) => {
                let identity = effect
                    .identity
                    .as_ref()
                    .ok_or(RuntimeError::Peer("missing coordinator Effect identity"))?;
                let Some(wire::effect_request::Operation::CellCommand(command)) = &effect.operation
                else {
                    return Err(RuntimeError::PeerAuthorization(
                        "unsupported reservation delivery",
                    ));
                };
                if command.codec_version != 1 {
                    return Err(RuntimeError::PeerAuthorization(
                        "unsupported coordinator codec",
                    ));
                }
                let mut decoder = BoundedDecoder::new(&command.input, 1024)?;
                if target.namespace() == DEADLINES && command.command_id == ScheduleDeadline::ID {
                    let ticket = DeadlineTicket::decode(&mut decoder)?;
                    decoder.finish()?;
                    ticket.event == self.event
                        && identity.source_cell == self.inventory.cell_id().as_bytes()
                        && *target
                            == CellTarget::new(
                                target.tenant(),
                                target.application(),
                                DEADLINES,
                                &partition_for_shard(cellule_runtime::shard_for_scope(
                                    DEADLINES,
                                    &ticket.workflow_id(),
                                    2,
                                )?),
                            )?
                } else if target.namespace() == INVENTORY && command.command_id == ExpireHold::ID {
                    let expiration = Expiration::decode(&mut decoder)?;
                    decoder.finish()?;
                    let source = CellTarget::new(
                        target.tenant(),
                        target.application(),
                        DEADLINES,
                        &partition_for_shard(cellule_runtime::shard_for_scope(
                            DEADLINES,
                            &expiration.ticket.workflow_id(),
                            2,
                        )?),
                    )?;
                    *target == crate::domain_target(target, &expiration.ticket.event)?
                        && expiration.fired_at_ms >= expiration.ticket.deadline_ms
                        && identity.source_cell == source.cell_id().as_bytes()
                } else {
                    false
                }
            }
            _ => false,
        };
        if allowed {
            Ok(())
        } else {
            Err(RuntimeError::PeerAuthorization(
                "reservation delivery source, target, or operation differs",
            ))
        }
    }
}
#[derive(Clone)]
struct ReceiverRouting {
    primary: CellTarget,
    factory: SiblingNodeFactory,
    gate: Arc<Mutex<()>>,
}
#[derive(Clone)]
struct WorkerOptions {
    delivery: DeliveryOptions,
    used: Arc<AtomicBool>,
    routing: ReceiverRouting,
}
#[derive(Clone)]
struct ObservedTransport {
    peer: LocalPeer,
    options: DeliveryOptions,
    used: Arc<AtomicBool>,
    cancel: CancellationToken,
    routing: ReceiverRouting,
}
impl PeerRoundTrip for ObservedTransport {
    fn send(
        &self,
        target: CellTarget,
        bytes: Vec<u8>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let transport = self.clone();
        Box::pin(async move {
            let request = transport.peer.verify_request(&bytes)?;
            let invocation = match request.operation() {
                Some(wire::peer_request::Operation::DeliverEffect(effect))
                    if target.namespace() == INVENTORY =>
                {
                    let identity = effect
                        .identity
                        .as_ref()
                        .ok_or(RuntimeError::Peer("missing expiration identity"))?;
                    let Some(wire::effect_request::Operation::CellCommand(command)) =
                        &effect.operation
                    else {
                        return Err(RuntimeError::Peer("unsupported expiration operation"));
                    };
                    let expiration: Expiration = crate::definition::decode(&command.input)?;
                    let id: [u8; 32] = identity
                        .effect_id
                        .as_slice()
                        .try_into()
                        .map_err(|_| RuntimeError::Peer("invalid expiration identity"))?;
                    Some((
                        expiration.ticket,
                        blake3::Hash::from_bytes(id).to_hex().to_string(),
                        identity.source_sequence,
                    ))
                }
                _ => None,
            };
            // Authenticate and authorize before acquiring any dynamic Cell.
            // Other event callbacks use one independently leased receiver at a
            // time. Its drain completes inside this accepted transport call.
            transport.peer.authorize_request(&request)?;
            let reply = if target.namespace() == INVENTORY && target != transport.routing.primary {
                let _receiver = transport.routing.gate.lock().await;
                let node = transport.routing.factory.start().await.map_err(|source| {
                    RuntimeError::PeerTransport {
                        context: "start callback receiver",
                        source: Box::new(source),
                    }
                })?;
                let result = async {
                    let handle =
                        node.open_cell(&target, &InventoryCells)
                            .await
                            .map_err(|source| RuntimeError::PeerTransport {
                                context: "acquire callback inventory",
                                source: Box::new(source),
                            })?;
                    transport.peer.dispatch_to(&request, handle).await
                }
                .await;
                let cleanup = node.shutdown().await;
                match (result, cleanup) {
                    (Ok(reply), Ok(())) => reply,
                    (Err(error), cleanup) => {
                        if let Err(cleanup) = cleanup {
                            tracing::error!(%cleanup,"callback receiver drain failed after dispatch error");
                        }
                        return Err(error);
                    }
                    (Ok(_), Err(source)) => {
                        return Err(RuntimeError::PeerTransportUnknown {
                            context: "callback receiver drain failed after dispatch",
                            source: Box::new(source),
                        });
                    }
                }
            } else {
                transport.peer.send(target, bytes, limit).await?
            };
            if let Some((ticket, effect_id, source_commit_sequence)) = invocation {
                let response = wire::PeerReply::decode(reply.as_slice())?;
                if let Some(wire::peer_reply::Outcome::Mutation(mutation)) = response.outcome
                    && let Some(wire::mutation_reply::Outcome::Result(result)) = mutation.outcome
                    && let Some(wire::mutation_result::Result::CommandOutput(bytes)) = result.result
                {
                    let receipt = mutation
                        .receipt
                        .ok_or(RuntimeError::Peer("expiration reply has no receipt"))?;
                    let mut decoder = BoundedDecoder::new(&bytes, 16)?;
                    let outcome = ExpirationOutcome::decode(&mut decoder)?;
                    decoder.finish()?;
                    if outcome != ExpirationOutcome::Invalid {
                        if let Some(sender) = &transport.options.progress {
                            let _ = sender.try_send(DeliveryProgress {
                                ticket,
                                effect_id,
                                source_commit_sequence,
                                destination_commit_sequence: receipt.commit_sequence,
                                outcome,
                            });
                        }
                        if !transport.used.swap(true, Ordering::SeqCst) {
                            tokio::select! {()=sleep(transport.options.after_publication)=>{},()=transport.cancel.cancelled()=>{}}
                            if transport.options.drop_reply_once {
                                return Err(RuntimeError::PeerTransportUnknown {
                                    context: "injected lost expiration reply after publication",
                                    source: Box::new(std::io::Error::new(
                                        std::io::ErrorKind::ConnectionReset,
                                        "reply lost",
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
/// Opens one event and both deadline shards and installs three owned signed Effect workers.
///
/// Invoke once before ingress. This local profile serves exactly one event per
/// process: three resident Cells stay inside its four-worker SQL budget. A
/// serialized same-installation receiver handles callbacks for earlier events
/// with its own lease and bounded node budget, draining before each reply. Native
/// maintenance drives timers; this function owns both delivery directions.
/// The event roster is explicit and never inferred from a history page. Drain
/// the node on any startup failure. Accepted delivery finishes source settlement
/// before honoring cancellation. Timer completion proves intent publication only.
pub async fn spawn_deadlines(
    node: &LocalNode,
    handle: ApplicationHandle<Reservations>,
    event: EventKey,
    options: DeliveryOptions,
) -> Result<(), ServiceError> {
    if options.after_publication > Duration::from_secs(10) {
        return Err(RuntimeError::Command("expiration delay exceeds ten seconds").into());
    }
    let client = ReservationClient::new(handle.clone(), event.clone())?;
    let inventory = client.target().clone();
    let inventory_handle = node.open_cell(&inventory, &InventoryCells).await?;
    let mut cells = vec![(inventory.clone(), inventory_handle)];
    let mut deadlines = Vec::with_capacity(2);
    for shard in 0..2 {
        let target = CellTarget::new(
            inventory.tenant(),
            inventory.application(),
            DEADLINES,
            &partition_for_shard(shard),
        )?;
        cells.push((target.clone(), node.open_cell(&target, &Deadlines).await?));
        deadlines.push(target);
    }
    let peer = LocalPeer::for_destinations(
        handle.compiled().registry(),
        cells,
        Arc::new(Authorizer {
            event,
            inventory: inventory.clone(),
            deadlines: deadlines.clone(),
        }),
    )?;
    let settings = WorkerOptions {
        delivery: options,
        used: Arc::new(AtomicBool::new(false)),
        routing: ReceiverRouting {
            primary: inventory.clone(),
            factory: node.sibling_factory(),
            gate: Arc::new(Mutex::new(())),
        },
    };
    install(
        node,
        handle.effects::<InventoryCells>(inventory)?,
        client.clone(),
        peer.clone(),
        settings.clone(),
        true,
    )?;
    for target in deadlines {
        install(
            node,
            handle.effects::<Deadlines>(target)?,
            client.clone(),
            peer.clone(),
            settings.clone(),
            false,
        )?;
    }
    Ok(())
}
fn install<M: EffectModule + Clone>(
    node: &LocalNode,
    source: EffectSource<M>,
    client: ReservationClient,
    peer: LocalPeer,
    settings: WorkerOptions,
    starting: bool,
) -> Result<(), ServiceError> {
    node.spawn_worker(move |cancel| async move {
        let transport = ObservedTransport {
            peer: peer.clone(),
            options: settings.delivery,
            used: settings.used,
            routing: settings.routing,
            cancel: cancel.clone(),
        };
        let principal = PeerPrincipal {
            issuer: "reservations".into(),
            subject: "deadline-coordinator".into(),
            actions: vec![
                "cell.read".into(),
                "cell.write".into(),
                "cookbook.reservation.coordinate".into(),
            ],
        };
        let supervisor = EffectSupervisor::new(
            source.clone(),
            EffectPeerClient::new(peer.signer(), principal, Arc::new(transport)),
            15_000,
        )?;
        let mut inspect_at = tokio::time::Instant::now();
        while !cancel.is_cancelled() {
            match supervisor.run_once().await? {
                EffectRunOutcome::Failed { .. } => {
                    return Err(ServiceError::Runtime(RuntimeError::Command(
                        "reservation coordination exhausted its attempts",
                    )));
                }
                EffectRunOutcome::Delivered { destination, .. } => {
                    if starting {
                        let mut decoder = BoundedDecoder::new(destination.result(), 64)
                            .map_err(RuntimeError::from)?;
                        let outcome =
                            WorkflowOutcome::decode(&mut decoder).map_err(RuntimeError::from)?;
                        decoder.finish().map_err(RuntimeError::from)?;
                        if !matches!(
                            outcome,
                            WorkflowOutcome::Applied { .. } | WorkflowOutcome::AlreadyExists
                        ) {
                            return Err(ServiceError::Runtime(RuntimeError::Command(
                                "deadline installation was durably rejected",
                            )));
                        }
                    } else {
                        let mut decoder = BoundedDecoder::new(destination.result(), 16)
                            .map_err(RuntimeError::from)?;
                        let outcome =
                            ExpirationOutcome::decode(&mut decoder).map_err(RuntimeError::from)?;
                        decoder.finish().map_err(RuntimeError::from)?;
                        if outcome == ExpirationOutcome::Invalid {
                            return Err(ServiceError::Runtime(RuntimeError::Command(
                                "expiration capability was durably rejected",
                            )));
                        }
                    }
                }
                _ => {}
            }
            // Only currently held seats need live deadline coordination. There
            // are at most 100, independent of the permanent 1024-row history.
            if starting && tokio::time::Instant::now() >= inspect_at {
                let read = client
                    .active()
                    .await
                    .map_err(|e| ServiceError::Query(Box::new(e)))?;
                let now = cellule_cookbook_support::now_ms()?;
                for hold in read.output.holds {
                    let id: [u8; 32] = hold
                        .start_effect
                        .as_slice()
                        .try_into()
                        .map_err(|_| RuntimeError::Command("invalid start effect identity"))?;
                    let failed = source
                        .status(id, None)
                        .await
                        .map_err(|e| ServiceError::Query(Box::new(e)))?
                        .output
                        .is_some_and(|v| v.state == EffectState::Failed);
                    let unavailable = client.deadline(&hold.ticket).await?.is_some_and(|view| {
                        matches!(view.status.as_str(), "failed" | "cancelled" | "paused")
                    });
                    let overdue = hold
                        .ticket
                        .deadline_ms
                        .checked_add(7 * 24 * 60 * 60 * 1000)
                        .is_some_and(|limit| now >= limit);
                    let reason = if failed {
                        Some("active hold has a terminally failed deadline start")
                    } else if unavailable {
                        Some("active hold has an unavailable deadline workflow")
                    } else if overdue {
                        Some("hold deadline exceeded coordination retention; reconcile explicitly")
                    } else {
                        None
                    };
                    if let Some(reason) = reason {
                        // Independent Workflow/source observations may race a valid
                        // settlement. Recheck the exact hold before failing readiness.
                        let current = client
                            .hold(hold.ticket.id, None)
                            .await
                            .map_err(|e| ServiceError::Query(Box::new(e)))?;
                        if current.output.is_some_and(|v| {
                            v.state == crate::HoldState::Held && v.ticket == hold.ticket
                        }) {
                            return Err(ServiceError::Runtime(RuntimeError::Command(reason)));
                        }
                    }
                }
                inspect_at = tokio::time::Instant::now() + Duration::from_secs(1);
            }
            tokio::select! {()=cancel.cancelled()=>{},()=sleep(Duration::from_millis(100))=>{}}
        }
        Ok::<_, ServiceError>(())
    })?;
    Ok(())
}
