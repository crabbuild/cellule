use crate::*;
use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, LocalPeer};
use cellule_runtime::{
    CellTarget, Error, partition_for_shard,
    peer::{
        EffectPeerClient, PeerAuthorizer, PeerPrincipal, PeerRoundTrip, VerifiedPeerRequest,
        wire as peer_wire,
    },
    primitives::{
        effects::{
            EffectModule, EffectRunOutcome, EffectSource, EffectState, EffectSupervisor,
            EffectSupervisorError,
        },
        workflow::{ActivityRunOutcome, ActivitySupervisor, WorkflowOutcome},
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
use tokio::{sync::mpsc, time::sleep};
use tokio_util::sync::CancellationToken;

/// Worker startup and supervision preserve underlying failure sources.
#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    /// Node, storage, or lifecycle error.
    #[error(transparent)]
    Infrastructure(#[from] cellule_cookbook_support::Error),
    /// Original runtime contract error.
    #[error(transparent)]
    Runtime(#[from] Error),
    /// Native outbox/inbox uncertainty or delivery failure.
    #[error(transparent)]
    Effect(#[from] EffectSupervisorError),
    /// Typed source or coordinator read failure.
    #[error("support-desk supervision read failed: {0}")]
    Read(#[source] BoxError),
}
/// Bounded actual-delivery fault controls, shared across native callback workers.
#[derive(Clone, Default)]
pub struct DeliveryOptions {
    /// Optional exact captured generation; unrelated callbacks do not consume the control.
    pub controlled_deadline: Option<Deadline>,
    /// Pause after the native timer intent is ready but before the ticket transaction, at most ten seconds.
    pub before_escalation: Duration,
    /// Pause after actual source publication, at most ten seconds; owned drain skips the delay.
    pub after_publication: Duration,
    /// Lose the selected reply after actual publication; native Inbox resolution preserves the original outcome.
    pub drop_reply_once: bool,
    /// Bounded observation channel; never participates in transaction ordering.
    pub progress: Option<mpsc::Sender<DeliveryProgress>>,
}
/// Actual native callback observation in its separate receipt domains.
#[derive(Clone, Debug, serde::Serialize)]
pub struct DeliveryProgress {
    /// `callback_ready` or `ticket_published`.
    pub event: String,
    /// Original immutable deadline capability.
    pub deadline: Deadline,
    /// Native callback Effect ID.
    pub effect_id: String,
    /// Workflow intent publication sequence.
    pub source_commit_sequence: u64,
    /// Source ticket publication position, only after actual delivery.
    pub ticket_commit_sequence: Option<u64>,
    /// Conditional ticket outcome, absent before dispatch.
    pub outcome: Option<EscalationOutcome>,
}
struct Authorizer {
    tickets: Vec<(TicketKey, CellTarget)>,
    deadline: CellTarget,
    notification: CellTarget,
}
impl PeerAuthorizer for Authorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        let target = request.target();
        if !request.permits("cookbook.support-desk.coordinate")
            || !(target == &self.deadline
                || target == &self.notification
                || self.tickets.iter().any(|(_, t)| t == target))
        {
            return Err(Error::PeerAuthorization("foreign support-desk receiver"));
        }
        let valid = match request.operation() {
            Some(peer_wire::peer_request::Operation::Read(read)) => matches!(
                read.operation,
                Some(peer_wire::read_request::Operation::Describe(true))
            ),
            Some(peer_wire::peer_request::Operation::ResolveEffect(_)) => true,
            Some(peer_wire::peer_request::Operation::DeliverEffect(effect)) => {
                let identity = effect
                    .identity
                    .as_ref()
                    .ok_or(Error::Peer("missing support-desk Effect identity"))?;
                let Some(peer_wire::effect_request::Operation::CellCommand(command)) =
                    &effect.operation
                else {
                    return Err(Error::PeerAuthorization(
                        "unsupported support-desk delivery",
                    ));
                };
                if command.codec_version != 1 {
                    return Err(Error::PeerAuthorization("foreign support-desk codec"));
                }
                if target == &self.deadline && command.command_id == ScheduleDeadline::ID {
                    let input: Deadline = wire::decode(&command.input, 2048)?;
                    input.validate()?;
                    self.tickets.iter().any(|(key, t)| {
                        *key == input.ticket && identity.source_cell == t.cell_id().as_bytes()
                    })
                } else if target == &self.notification
                    && command.command_id == ScheduleNotification::ID
                {
                    let input: Notification = wire::decode(&command.input, 8192)?;
                    input.validate()?;
                    self.tickets.iter().any(|(key, t)| {
                        *key == input.deadline.ticket
                            && input.source_cell == *t.cell_id().as_bytes()
                            && identity.source_cell == input.source_cell
                    })
                } else if target.namespace() == TICKETS && command.command_id == EscalateTicket::ID
                {
                    let input: Escalation = wire::decode(&command.input, 4096)?;
                    input.deadline.validate()?;
                    identity.source_cell == self.deadline.cell_id().as_bytes()
                        && input.fired_at_ms >= input.deadline.due_at_ms
                        && self
                            .tickets
                            .iter()
                            .any(|(key, t)| *key == input.deadline.ticket && t == target)
                } else {
                    false
                }
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(Error::PeerAuthorization(
                "support-desk source, capability, or operation differs",
            ))
        }
    }
}
#[derive(Clone)]
struct Transport {
    peer: LocalPeer,
    options: DeliveryOptions,
    used: Arc<AtomicBool>,
    cancel: CancellationToken,
}
impl PeerRoundTrip for Transport {
    fn send(
        &self,
        target: CellTarget,
        bytes: Vec<u8>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let this = self.clone();
        Box::pin(async move {
            let request = this.peer.verify_request(&bytes)?;
            this.peer.authorize_request(&request)?;
            let invocation = match request.operation() {
                Some(peer_wire::peer_request::Operation::DeliverEffect(effect))
                    if target.namespace() == TICKETS =>
                {
                    let identity = effect
                        .identity
                        .as_ref()
                        .ok_or(Error::Peer("missing escalation identity"))?;
                    let Some(peer_wire::effect_request::Operation::CellCommand(command)) =
                        &effect.operation
                    else {
                        return Err(Error::Peer("invalid escalation operation"));
                    };
                    let input: Escalation = wire::decode(&command.input, 4096)?;
                    let id: [u8; 32] = identity
                        .effect_id
                        .as_slice()
                        .try_into()
                        .map_err(|_| Error::Peer("invalid escalation Effect ID"))?;
                    Some((
                        input.deadline,
                        blake3::Hash::from_bytes(id).to_hex().to_string(),
                        identity.source_sequence,
                    ))
                }
                _ => None,
            };
            let selected = invocation.as_ref().is_some_and(|(deadline, _, _)| {
                this.options
                    .controlled_deadline
                    .as_ref()
                    .is_none_or(|control| control == deadline)
            }) && !this.used.swap(true, Ordering::SeqCst);
            if let Some((deadline, effect_id, source_commit_sequence)) = &invocation {
                if let Some(sender) = &this.options.progress {
                    let _ = sender.try_send(DeliveryProgress {
                        event: "callback_ready".into(),
                        deadline: deadline.clone(),
                        effect_id: effect_id.clone(),
                        source_commit_sequence: *source_commit_sequence,
                        ticket_commit_sequence: None,
                        outcome: None,
                    });
                }
                if selected {
                    tokio::select! { ()=sleep(this.options.before_escalation)=>{}, ()=this.cancel.cancelled()=>{} }
                }
            }
            // Accepted native delivery is never cancelled between receiver publication
            // and source settlement. Only bounded diagnostic pauses observe drain.
            let reply = this.peer.send(target, bytes, limit).await?;
            if let Some((deadline, effect_id, source_commit_sequence)) = invocation {
                let response = peer_wire::PeerReply::decode(reply.as_slice())?;
                if let Some(peer_wire::peer_reply::Outcome::Mutation(mutation)) = response.outcome
                    && let Some(peer_wire::mutation_reply::Outcome::Result(result)) =
                        mutation.outcome
                    && let Some(peer_wire::mutation_result::Result::CommandOutput(output)) =
                        result.result
                {
                    let receipt = mutation
                        .receipt
                        .ok_or(Error::Peer("escalation has no publication receipt"))?;
                    let outcome: EscalationOutcome = wire::decode(&output, 64)?;
                    if let Some(sender) = &this.options.progress {
                        let _ = sender.try_send(DeliveryProgress {
                            event: "ticket_published".into(),
                            deadline,
                            effect_id,
                            source_commit_sequence,
                            ticket_commit_sequence: Some(receipt.commit_sequence),
                            outcome: Some(outcome),
                        });
                    }
                    if selected && outcome != EscalationOutcome::Invalid {
                        tokio::select! { ()=sleep(this.options.after_publication)=>{}, ()=this.cancel.cancelled()=>{} }
                        if this.options.drop_reply_once {
                            return Err(Error::PeerTransportUnknown {
                                context: "lost escalation reply after publication",
                                source: Box::new(std::io::Error::new(
                                    std::io::ErrorKind::ConnectionReset,
                                    "injected reply loss",
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
fn install<M: EffectModule + Clone>(
    node: &LocalNode,
    source: EffectSource<M>,
    peer: LocalPeer,
    options: DeliveryOptions,
    used: Arc<AtomicBool>,
    callback: bool,
) -> Result<(), ServiceError> {
    node.spawn_worker(move |cancel| async move {
        let principal = PeerPrincipal {
            issuer: "support-desk".into(),
            subject: "ticket-coordinator".into(),
            actions: vec![
                "cell.read".into(),
                "cell.write".into(),
                "cookbook.support-desk.coordinate".into(),
            ],
        };
        let supervisor = EffectSupervisor::new(
            source,
            EffectPeerClient::new(
                peer.signer(),
                principal,
                Arc::new(Transport {
                    peer,
                    options,
                    used,
                    cancel: cancel.clone(),
                }),
            ),
            30000,
        )?;
        while !cancel.is_cancelled() {
            match supervisor.run_once().await? {
                EffectRunOutcome::Failed { .. } => {
                    return Err(ServiceError::Runtime(Error::Command(
                        "support coordination exhausted native attempts",
                    )));
                }
                EffectRunOutcome::Delivered { destination, .. } => {
                    if callback {
                        let outcome: EscalationOutcome = wire::decode(destination.result(), 64)?;
                        if outcome == EscalationOutcome::Invalid {
                            return Err(ServiceError::Runtime(Error::Command(
                                "support callback was durably refused",
                            )));
                        }
                    } else {
                        let outcome: WorkflowOutcome = wire::decode(destination.result(), 64)?;
                        if !matches!(
                            outcome,
                            WorkflowOutcome::Applied { .. } | WorkflowOutcome::AlreadyExists
                        ) {
                            return Err(ServiceError::Runtime(Error::Command(
                                "support Workflow start was durably refused",
                            )));
                        }
                    }
                }
                _ => {}
            }
            tokio::select! { ()=cancel.cancelled()=>{}, ()=sleep(Duration::from_millis(100))=>{} }
        }
        Ok::<_, ServiceError>(())
    })?;
    Ok(())
}
/// Opens an explicit 1..2-ticket roster and two coordination Cells, then installs owned runners.
/// All workers belong to `source_node`. Drain it before the independently leased `coordinator_node`.
/// The caller drains both nodes on every startup-error path. Native maintenance owns timer advancement.
pub async fn spawn_coordination(
    source_node: &LocalNode,
    coordinator_node: &LocalNode,
    source: ApplicationHandle<SupportDesk>,
    coordinator: ApplicationHandle<SupportDesk>,
    keys: &[TicketKey],
    options: DeliveryOptions,
) -> Result<(), ServiceError> {
    if !(1..=2).contains(&keys.len())
        || keys
            .iter()
            .enumerate()
            .any(|(i, key)| keys[..i].contains(key))
        || options.before_escalation > Duration::from_secs(10)
        || options.after_publication > Duration::from_secs(10)
    {
        return Err(Error::Command("invalid bounded support-desk roster or delay").into());
    }
    if let Some(control) = &options.controlled_deadline {
        control.validate()?;
        if !keys.contains(&control.ticket) {
            return Err(Error::Identity("controlled deadline is outside support roster").into());
        }
    }
    let mut tickets = vec![];
    let mut cells = vec![];
    for key in keys {
        let client = TicketClient::new(source.clone(), key.clone())?;
        if coordinator.target_for_scope(TICKETS, key.as_bytes())? != *client.target() {
            return Err(Error::Identity("support coordinator scope differs").into());
        }
        cells.push((
            client.target().clone(),
            source_node.open_cell(client.target(), &Tickets).await?,
        ));
        tickets.push((key.clone(), client.target().clone()));
    }
    let deadline = coordinator.target_for_scope(DEADLINES, b"deadlines")?;
    let notification = coordinator.target_for_scope(NOTIFICATIONS, b"notifications")?;
    if deadline.partition() != partition_for_shard(0)
        || notification.partition() != partition_for_shard(0)
    {
        return Err(Error::Identity("support coordination topology differs").into());
    }
    cells.push((
        deadline.clone(),
        coordinator_node.open_cell(&deadline, &Deadlines).await?,
    ));
    cells.push((
        notification.clone(),
        coordinator_node
            .open_cell(&notification, &Notifications)
            .await?,
    ));
    let peer = LocalPeer::for_destinations(
        source.compiled().registry(),
        cells,
        Arc::new(Authorizer {
            tickets: tickets.clone(),
            deadline: deadline.clone(),
            notification,
        }),
    )?;
    let used = Arc::new(AtomicBool::new(false));
    for (_, target) in &tickets {
        install(
            source_node,
            source.effects::<Tickets>(target.clone())?,
            peer.clone(),
            options.clone(),
            used.clone(),
            false,
        )?;
    }
    install(
        source_node,
        coordinator.effects::<Deadlines>(deadline)?,
        peer,
        options,
        used,
        true,
    )?;
    let activity = ActivitySupervisor::new(coordinator.activities::<Notifications>()?, 15000)?;
    source_node.spawn_worker(move |cancel| async move {
        while !cancel.is_cancelled() {
            if matches!(
                activity.run_once(0, None).await?,
                ActivityRunOutcome::IdentityConflict { .. }
            ) {
                return Err(
                    cellule_runtime::primitives::workflow::ActivitySupervisorError::Runtime(
                        Error::Command("support notification completion identity conflict"),
                    ),
                );
            }
            tokio::select! { ()=cancel.cancelled()=>{}, ()=sleep(Duration::from_millis(100))=>{} }
        }
        Ok::<_, cellule_runtime::primitives::workflow::ActivitySupervisorError>(())
    })?;
    source_node.spawn_worker(move |cancel| async move {
        while !cancel.is_cancelled() {
            for (key, target) in &tickets {
                let client = TicketClient::new(source.clone(), key.clone())?;
                let coordinated = CoordinationClient::new(coordinator.clone(), key.clone())?;
                if let Some(ticket) = client
                    .get(None)
                    .await
                    .map_err(|source| ServiceError::Read(Box::new(source)))?
                    .output
                {
                    let effects = source.effects::<Tickets>(target.clone())?;
                    let ids =
                        ticket.notifications.iter().map(|r| r.effect_id).chain(
                            (ticket.status == Status::Open).then_some(ticket.deadline_effect),
                        );
                    for id in ids {
                        if effects
                            .status(id, None)
                            .await
                            .map_err(|source| ServiceError::Read(Box::new(source)))?
                            .output
                            .is_some_and(|status| status.state == EffectState::Failed)
                        {
                            return Err(ServiceError::Runtime(Error::Command(
                                "support ticket retains a failed coordination intent",
                            )));
                        }
                    }
                    if ticket.status == Status::Open {
                        let unavailable = coordinated
                            .deadline(&ticket.deadline)
                            .await
                            .map_err(ServiceError::Read)?
                            .is_some_and(|view| {
                                matches!(view.status.as_str(), "failed" | "cancelled" | "paused")
                            });
                        let now = cellule_cookbook_support::now_ms()?;
                        let overdue = ticket
                            .deadline
                            .due_at_ms
                            .checked_add(MAX_DEADLINE_MS)
                            .is_some_and(|limit| now >= limit);
                        if unavailable || overdue {
                            // Coordination observations can race a valid resolution
                            // or reassignment; only the same still-open generation
                            // justifies withdrawing readiness.
                            let current = client
                                .get(None)
                                .await
                                .map_err(|source| ServiceError::Read(Box::new(source)))?
                                .output;
                            if current.is_some_and(|value| {
                                value.status == Status::Open && value.deadline == ticket.deadline
                            }) {
                                return Err(ServiceError::Runtime(Error::Command(
                                    "active support deadline is unavailable or requires reconciliation",
                                )));
                            }
                        }
                    }
                    for record in &ticket.notifications {
                        if let Some(view) = coordinated
                            .notification(&record.notification)
                            .await
                            .map_err(ServiceError::Read)?
                            && (matches!(view.status.as_str(), "failed" | "cancelled" | "paused")
                                || matches!(
                                    view.state.phase,
                                    NotificationPhase::Failed
                                        | NotificationPhase::Exhausted
                                        | NotificationPhase::Expired
                                ))
                        {
                            return Err(ServiceError::Runtime(Error::Command(
                                "support notification requires external reconciliation",
                            )));
                        }
                    }
                }
            }
            tokio::select! { ()=cancel.cancelled()=>{}, ()=sleep(Duration::from_secs(1))=>{} }
        }
        Ok::<_, ServiceError>(())
    })?;
    Ok(())
}
