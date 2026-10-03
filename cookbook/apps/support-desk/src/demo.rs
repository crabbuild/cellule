//! Retained two-ticket demonstration, including the resolution/deadline race.
use crate::{
    assembly::Service,
    emit,
    files::{self, PlanFile, RequestFile},
    receiver,
};
use cellule_cookbook_support::new_identity;
use cellule_cookbook_support_desk::*;
use cellule_runtime::InvocationError;
use serde::{Deserialize, Serialize};
use std::{path::Path, time::Duration};

const TENANT_PREFIX: &str = "cookbook-demo-";
const FAR_DEADLINE_MS: i64 = 5 * 24 * 60 * 60 * 1000;
const ASSIGNED_DEADLINE_MS: i64 = 4000;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    version: u8,
    tenant: String,
    notification_endpoint: NotificationEndpoint,
    race_ticket: TicketKey,
    notification_ticket: TicketKey,
    race_source_cell: [u8; 32],
    notification_source_cell: [u8; 32],
    race_deadline: Deadline,
    notification_deadline: Deadline,
    notification_key: String,
    requests: [RequestFile; 6],
    attachment: PlanFile,
}
impl Plan {
    fn validate(&self) -> Result<(), BoxError> {
        if self.version != 1
            || !self.tenant.starts_with(TENANT_PREFIX)
            || self.race_ticket == self.notification_ticket
            || self.race_source_cell == [0; 32]
            || self.notification_source_cell == [0; 32]
        {
            return Err("invalid retained support demo plan".into());
        }
        crate::assembly::tenant(&self.tenant)?;
        for request in &self.requests {
            request.validate()?;
            if request.tenant != self.tenant {
                return Err("support demo request scope differs".into());
            }
        }
        let keys = [
            &self.race_ticket,
            &self.race_ticket,
            &self.race_ticket,
            &self.race_ticket,
            &self.notification_ticket,
            &self.notification_ticket,
        ];
        for (request, key) in self.requests.iter().zip(keys) {
            if request.change.ticket != *key {
                return Err("support demo request ticket binding differs".into());
            }
        }
        let [
            Action::Open {
                subject: race_subject,
                requester: race_requester,
                due_at_ms: race_open_due,
                endpoint: race_endpoint,
            },
            Action::Message {
                expected_revision: 1,
                message,
            },
            Action::Assign {
                expected_revision: 3,
                agent: race_agent,
                due_at_ms: race_due,
            },
            Action::Resolve {
                expected_revision: 4,
            },
            Action::Open {
                subject: notify_subject,
                requester: notify_requester,
                due_at_ms: notify_open_due,
                endpoint: notify_endpoint,
            },
            Action::Assign {
                expected_revision: 1,
                agent: notify_agent,
                due_at_ms: notify_due,
            },
        ] = [
            &self.requests[0].change.action,
            &self.requests[1].change.action,
            &self.requests[2].change.action,
            &self.requests[3].change.action,
            &self.requests[4].change.action,
            &self.requests[5].change.action,
        ]
        else {
            return Err("support demo request sequence differs".into());
        };
        if race_subject != "Unable to sign in"
            || notify_subject != "Export is delayed"
            || race_requester != notify_requester
            || race_requester != &Actor::new("demo-requester")?
            || race_agent != notify_agent
            || race_agent != &Actor::new("demo-agent")?
            || race_endpoint != &self.notification_endpoint
            || notify_endpoint != &self.notification_endpoint
            || *race_open_due != self.requests[0].identity.issued_at_ms + FAR_DEADLINE_MS
            || *notify_open_due != self.requests[4].identity.issued_at_ms + FAR_DEADLINE_MS
            || *race_due != self.requests[2].identity.issued_at_ms + ASSIGNED_DEADLINE_MS
            || *notify_due != self.requests[5].identity.issued_at_ms + ASSIGNED_DEADLINE_MS
            || self.race_deadline
                != (Deadline {
                    ticket: self.race_ticket.clone(),
                    generation: 2,
                    due_at_ms: *race_due,
                })
            || self.notification_deadline
                != (Deadline {
                    ticket: self.notification_ticket.clone(),
                    generation: 2,
                    due_at_ms: *notify_due,
                })
            || message.id.as_str()
                != format!(
                    "demo-message-{}",
                    self.race_ticket.as_str().trim_start_matches("demo-race-")
                )
            || message.author != *race_requester
            || message.body != "I have restarted twice; sign-in still fails."
            || self.attachment.tenant != self.tenant
            || self.attachment.plan.descriptor.ticket != self.race_ticket
            || self.attachment.plan.descriptor.id.as_str()
                != format!(
                    "demo-attachment-{}",
                    self.race_ticket.as_str().trim_start_matches("demo-race-")
                )
            || self.attachment.plan.expected_revision != 2
            || self.attachment.plan.bytes
                != b"Support attachment: redacted sign-in diagnostic evidence.\n"
            || self.notification_key
                != notification_key(
                    self.notification_source_cell,
                    &self.notification_deadline,
                    self.notification_endpoint.clone(),
                    Actor::new("demo-agent")?,
                )?
        {
            return Err("retained support demo inputs differ from their declared journey".into());
        }
        self.attachment.validate()?;
        Ok(())
    }
}
fn request(
    tenant: &str,
    ticket: TicketKey,
    action: impl FnOnce(i64) -> Action,
) -> Result<RequestFile, BoxError> {
    let identity: RetainedIdentity = new_identity()?.into();
    Ok(RequestFile {
        version: 1,
        tenant: tenant.into(),
        change: Change {
            ticket,
            action: action(identity.issued_at_ms),
        },
        identity,
    })
}
fn slug() -> Result<String, BoxError> {
    let identity = new_identity()?;
    Ok(
        blake3::Hash::from_bytes(*blake3::hash(identity.request_id.as_bytes()).as_bytes()).to_hex()
            [..24]
            .into(),
    )
}
fn notification_key(
    source_cell: [u8; 32],
    deadline: &Deadline,
    endpoint: NotificationEndpoint,
    agent: Actor,
) -> Result<String, BoxError> {
    Ok(Notification {
        deadline: deadline.clone(),
        source_cell,
        agent: Some(agent),
        endpoint,
        escalated_at_ms: deadline.due_at_ms,
    }
    .key_hex())
}
fn retain_callback_identity(
    state: &Path,
    deadline: &Deadline,
    effect_id: &str,
) -> Result<(), BoxError> {
    crate::effect_hex(effect_id)?;
    let path = state.join(format!(
        "support-desk-demo-callback-{}.json",
        deadline.ticket.as_str()
    ));
    let bytes = serde_json::to_vec(
        &serde_json::json!({"version":1,"deadline":deadline,"effect_id":effect_id}),
    )?;
    if path.exists() {
        if std::fs::read(path)? != bytes {
            return Err("retained support callback identity changed".into());
        }
    } else {
        files::save(&path, &bytes)?;
    }
    Ok(())
}
async fn create(
    service: &Service,
    tenant: &str,
    endpoint: NotificationEndpoint,
) -> Result<Plan, BoxError> {
    let suffix = slug()?;
    let race_ticket = TicketKey::new(format!("demo-race-{suffix}"))?;
    let notification_ticket = TicketKey::new(format!("demo-notify-{suffix}"))?;
    let client = service.ticket(race_ticket.clone()).await?;
    let race_source_cell = *client.target().cell_id().as_bytes();
    let notify_client = service.ticket(notification_ticket.clone()).await?;
    let notification_source_cell = *notify_client.target().cell_id().as_bytes();
    let requester = Actor::new("demo-requester")?;
    let agent = Actor::new("demo-agent")?;
    let open_race = request(tenant, race_ticket.clone(), |issued_at_ms| Action::Open {
        subject: "Unable to sign in".into(),
        requester: requester.clone(),
        due_at_ms: issued_at_ms + FAR_DEADLINE_MS,
        endpoint: endpoint.clone(),
    })?;
    let frozen_message = Message {
        id: MessageId::new(format!("demo-message-{suffix}"))?,
        author: requester.clone(),
        body: "I have restarted twice; sign-in still fails.".into(),
    };
    let message = request(tenant, race_ticket.clone(), |_| Action::Message {
        expected_revision: 1,
        message: frozen_message,
    })?;
    let attachment_bytes = b"Support attachment: redacted sign-in diagnostic evidence.\n".to_vec();
    let descriptor = AttachmentDescriptor::new(
        race_ticket.clone(),
        AttachmentId::new(format!("demo-attachment-{suffix}"))?,
        "sign-in-diagnostic.txt".into(),
        &attachment_bytes,
    )?;
    let plan = AttachmentPlan::new(descriptor, attachment_bytes, 2)?;
    let attachment = PlanFile {
        tenant: tenant.into(),
        plan,
    };
    let race_assign = request(tenant, race_ticket.clone(), |issued_at_ms| Action::Assign {
        expected_revision: 3,
        agent: agent.clone(),
        due_at_ms: issued_at_ms + ASSIGNED_DEADLINE_MS,
    })?;
    let resolve = request(tenant, race_ticket.clone(), |_| Action::Resolve {
        expected_revision: 4,
    })?;
    let open_notification = request(tenant, notification_ticket.clone(), |issued_at_ms| {
        Action::Open {
            subject: "Export is delayed".into(),
            requester,
            due_at_ms: issued_at_ms + FAR_DEADLINE_MS,
            endpoint: endpoint.clone(),
        }
    })?;
    let notify_assign = request(tenant, notification_ticket.clone(), |issued_at_ms| {
        Action::Assign {
            expected_revision: 1,
            agent: agent.clone(),
            due_at_ms: issued_at_ms + ASSIGNED_DEADLINE_MS,
        }
    })?;
    let race_deadline = Deadline {
        ticket: race_ticket.clone(),
        generation: 2,
        due_at_ms: match race_assign.change.action {
            Action::Assign { due_at_ms, .. } => due_at_ms,
            _ => return Err("invalid retained race deadline".into()),
        },
    };
    let notification_deadline = Deadline {
        ticket: notification_ticket.clone(),
        generation: 2,
        due_at_ms: match notify_assign.change.action {
            Action::Assign { due_at_ms, .. } => due_at_ms,
            _ => return Err("invalid retained notification deadline".into()),
        },
    };
    let plan = Plan {
        version: 1,
        tenant: tenant.into(),
        notification_endpoint: endpoint.clone(),
        race_ticket,
        notification_ticket,
        race_source_cell,
        notification_source_cell,
        race_deadline,
        notification_key: notification_key(
            notification_source_cell,
            &notification_deadline,
            endpoint,
            agent,
        )?,
        notification_deadline,
        requests: [
            open_race,
            message,
            race_assign,
            resolve,
            open_notification,
            notify_assign,
        ],
        attachment,
    };
    plan.validate()?;
    Ok(plan)
}
async fn change(service: &Service, request: &RequestFile) -> Result<Outcome, BoxError> {
    let client = service.ticket(request.change.ticket.clone()).await?;
    let result = client
        .change(request.identity.native()?, request.change.action.clone())
        .await;
    match result {
        Ok(result)
            if matches!(
                result.output.decision,
                Decision::Applied | Decision::Duplicate
            ) =>
        {
            Ok(result.output)
        }
        Err(InvocationError::Rejected(result))
            if matches!(
                result.output.decision,
                Decision::Applied | Decision::Duplicate
            ) =>
        {
            Ok(result.output)
        }
        Err(source) => {
            if let InvocationError::Pending(_) = &source {
                emit(
                    &serde_json::json!({"event":"demo_request_unknown","ticket":request.change.ticket,"retain_plan":true}),
                )?;
            }
            Err(source.into())
        }
        Ok(result) => Err(format!(
            "retained support demo command was durably refused: {:?}",
            result.output.decision
        )
        .into()),
    }
}
pub(crate) async fn run(service: &Service, state: &Path, tenant: &str) -> Result<(), BoxError> {
    std::fs::create_dir_all(state)?;
    let active = state.join("support-desk-demo-active.json");
    let local_owned = std::env::var_os("CELLULE_SUPPORT_DESK_NOTIFICATION_ENDPOINT").is_none();
    let endpoint = if local_owned {
        match files::load::<Plan>(&active) {
            Ok(plan) => {
                let port = plan
                    .notification_endpoint
                    .as_str()
                    .parse::<url::Url>()?
                    .port()
                    .ok_or("retained local demo endpoint has no port")?;
                let endpoint = receiver::install_owned(
                    &service.source,
                    state.join("receiver"),
                    port,
                    receiver::Controls {
                        key: None,
                        after_publication_ms: 0,
                        drop_reply_once: true,
                    },
                )
                .await?;
                if endpoint != plan.notification_endpoint {
                    return Err(
                        "restarted local receiver endpoint differs from retained demo plan".into(),
                    );
                }
                plan.notification_endpoint
            }
            Err(source) if active.exists() => {
                return Err(
                    format!("cannot resume retained support demo endpoint: {source}").into(),
                );
            }
            Err(_) => {
                receiver::install_owned(
                    &service.source,
                    state.join("receiver"),
                    std::env::var("CELLULE_SUPPORT_DESK_RECEIVER_PORT")
                        .ok()
                        .map(|v| v.parse())
                        .transpose()?
                        .unwrap_or(0),
                    receiver::Controls {
                        key: None,
                        after_publication_ms: 0,
                        drop_reply_once: true,
                    },
                )
                .await?
            }
        }
    } else {
        NotificationEndpoint::new(std::env::var("CELLULE_SUPPORT_DESK_NOTIFICATION_ENDPOINT")?)?
    };
    let plan = if active.exists() {
        let plan: Plan = files::load(&active)?;
        plan.validate()?;
        if plan.notification_endpoint != endpoint || plan.tenant != tenant {
            return Err("configured support receiver or tenant differs from the pinned retained demo plan; preserve the active plan".into());
        }
        plan
    } else {
        let plan = create(service, tenant, endpoint).await?;
        files::save(&active, &serde_json::to_vec(&plan)?)?;
        plan
    };
    emit(
        &serde_json::json!({"event":"demo_plan","path":active,"race_ticket":plan.race_ticket,"notification_ticket":plan.notification_ticket,"race_deadline":plan.race_deadline,"notification_key":plan.notification_key,"attachment_plan":plan.attachment}),
    )?;
    let (sender, mut progress) = tokio::sync::mpsc::channel(32);
    service
        .workers(
            &[plan.race_ticket.clone(), plan.notification_ticket.clone()],
            DeliveryOptions {
                controlled_deadline: Some(plan.race_deadline.clone()),
                before_escalation: Duration::from_secs(10),
                progress: Some(sender),
                ..Default::default()
            },
        )
        .await?;
    // The full set of permanent requests and attachment identities is synced
    // before this first dispatch; retries replay the exact frozen sequence.
    for request in &plan.requests[..2] {
        change(service, request).await?;
    }
    let attachments = service.attachments().await?;
    let publication = attachments.publish(&plan.attachment.plan).await?;
    emit(
        &serde_json::json!({"event":"attachment_published","publication":publication.publication,"ticket_linked":false}),
    )?;
    let link = attachments.prepare_link(&plan.attachment.plan).await?;
    let link = link.execute().await?;
    if !matches!(
        link.output.decision,
        Decision::Applied | Decision::Duplicate
    ) {
        return Err("support demo attachment link was refused".into());
    }
    emit(
        &serde_json::json!({"event":"attachment_linked","outcome":link.output,"blob_etag":publication.publication.etag}),
    )?;
    for request in [&plan.requests[2], &plan.requests[4], &plan.requests[5]] {
        change(service, request).await?;
    }
    let race = service.ticket(plan.race_ticket.clone()).await?;
    let notify = service.ticket(plan.notification_ticket.clone()).await?;
    let coordinator = service
        .coordinated(plan.notification_ticket.clone())
        .await?;
    let race_coordinator = service.coordinated(plan.race_ticket.clone()).await?;
    let mut race_resolved = false;
    let mut race_callback_settled = false;
    let mut notification_attempts = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        while let Ok(event) = progress.try_recv() {
            emit(&serde_json::json!({"event":"native_callback","progress":event}))?;
            if event.event == "callback_ready" && event.deadline == plan.race_deadline {
                retain_callback_identity(state, &event.deadline, &event.effect_id)?;
                let outcome = change(service, &plan.requests[3]).await?;
                let ticket = outcome
                    .ticket
                    .ok_or("support demo resolution omitted its source state")?;
                if ticket.status != Status::Resolved || ticket.generation != 3 || ticket.escalated {
                    return Err(format!(
                        "support demo resolution differs: status={:?}, ticket_generation={}, deadline_generation={}, escalated={}",
                        ticket.status,
                        ticket.generation,
                        ticket.deadline.generation,
                        ticket.escalated
                    )
                    .into());
                }
                emit(
                    &serde_json::json!({"event":"demo_race_resolved","ticket":ticket,"decision":outcome.decision}),
                )?;
                race_resolved = true;
            } else if event.event == "ticket_published" && event.deadline == plan.race_deadline {
                retain_callback_identity(state, &event.deadline, &event.effect_id)?;
                if event.outcome != Some(EscalationOutcome::Unchanged) {
                    return Err("obsolete support callback changed the resolved ticket".into());
                }
                race_callback_settled = true;
            }
        }
        let actual_race = race.get(None).await?.output;
        if actual_race
            .as_ref()
            .is_some_and(|ticket| ticket.status == Status::Resolved)
            && !race_resolved
        {
            let outcome = change(service, &plan.requests[3]).await?;
            if outcome
                .ticket
                .as_ref()
                .is_none_or(|ticket| ticket.status != Status::Resolved)
            {
                return Err("original demo resolution evidence differs".into());
            }
            emit(
                &serde_json::json!({"event":"demo_race_resolution_replayed","ticket":outcome.ticket,"decision":outcome.decision}),
            )?;
            race_resolved = true;
        }
        let ticket = notify.get(None).await?.output;
        let race_deadline = race_coordinator.deadline(&plan.race_deadline).await?;
        let race_callback_fired = race_deadline.as_ref().is_some_and(|view| {
            view.status == "completed"
                && view.state.ticket == plan.race_deadline
                && view.state.fired_at_ms.is_some()
        });
        if let Some(ticket) = ticket
            && ticket.status == Status::Open
            && ticket.escalated
        {
            let record = ticket
                .notifications
                .last()
                .ok_or("escalated support demo ticket omitted notification")?;
            if record.notification.key_hex() != plan.notification_key {
                return Err("demo notification generation or destination differs".into());
            }
            if let Some(native) = coordinator.notification(&record.notification).await? {
                if native.state.attempts.len() > notification_attempts {
                    notification_attempts = native.state.attempts.len();
                    emit(
                        &serde_json::json!({"event":"demo_notification_attempt","ticket":record.notification.deadline.ticket,"notification":native}),
                    )?;
                }
                if matches!(
                    native.state.phase,
                    NotificationPhase::Failed
                        | NotificationPhase::Exhausted
                        | NotificationPhase::Expired
                ) || native.status == "failed"
                {
                    return Err("support demo notification requires reconciliation; retain plan and receiver record".into());
                }
                if native.state.phase == NotificationPhase::Delivered
                    && race_resolved
                    && (race_callback_settled || race_callback_fired)
                {
                    let verified = attachments
                        .read(plan.attachment.plan.descriptor.key(), None)
                        .await?
                        .output
                        .ok_or("support demo attachment disappeared")?;
                    if verified.bytes != plan.attachment.plan.bytes
                        || verified.publication.descriptor != plan.attachment.plan.descriptor
                    {
                        return Err("support demo complete attachment restore differs".into());
                    }
                    let race_messages = race
                        .messages(PageRequest { after: 0, limit: 4 }, None)
                        .await?
                        .output;
                    if race_messages.ticket.as_ref().is_none_or(|ticket| {
                        ticket.status != Status::Resolved
                            || ticket.attachments.len() != 1
                            || ticket.message_count != 1
                            || ticket.escalated
                            || !race_messages.messages.iter().any(|row| {
                                row.message.id.as_str()
                                    == format!(
                                        "demo-message-{}",
                                        race.key().as_str().trim_start_matches("demo-race-")
                                    )
                            })
                    }) {
                        return Err(
                            "support demo resolved conversation or immutable-link evidence differs"
                                .into(),
                        );
                    }
                    let completed = completed_path(state, &plan);
                    emit(
                        &serde_json::json!({"event":"demo_complete","race_ticket":race.key(),"ticket":ticket,"notification":native,"callback_outcome":"unchanged","retained_plan":completed,"receiver_record":plan.notification_key}),
                    )?;
                    return Ok(());
                }
            }
        }
        if !service.source.is_ready() || !service.coordinator.is_ready() {
            return Err(
                "support demo worker lost readiness; active plan and receipts remain".into(),
            );
        }
        if tokio::time::Instant::now() >= deadline {
            return Err("support demo convergence exceeded 90 seconds; retain its original plan and notification record".into());
        }
        // Keep the separate deadline Workflow visible even before ticket escalation.
        let _ = coordinator.deadline(&plan.notification_deadline).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn completed_path(state: &Path, plan: &Plan) -> std::path::PathBuf {
    state.join(format!(
        "support-desk-demo-completed-{}.json",
        plan.race_ticket.as_str()
    ))
}

/// Publishes completion only after the owning executable has drained both nodes.
pub(crate) fn finalize(state: &Path) -> Result<(), BoxError> {
    let active = state.join("support-desk-demo-active.json");
    let plan: Plan = files::load(&active)?;
    plan.validate()?;
    let completed = completed_path(state, &plan);
    let active_bytes = std::fs::read(&active)?;
    if completed.exists() {
        if std::fs::read(&completed)? != active_bytes {
            return Err("completed support demo plan differs from its active plan".into());
        }
    } else {
        files::save(&completed, &active_bytes)?;
    }
    std::fs::remove_file(&active)?;
    std::fs::File::open(state)?.sync_all()?;
    Ok(())
}

pub(crate) fn tenant_for_state(state: &Path) -> Result<String, BoxError> {
    let active = state.join("support-desk-demo-active.json");
    if active.exists() {
        let plan: Plan = files::load(&active)?;
        plan.validate()?;
        return Ok(plan.tenant);
    }
    Ok(format!("{TENANT_PREFIX}{}", slug()?))
}
