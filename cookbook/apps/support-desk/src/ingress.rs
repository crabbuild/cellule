//! Embedding-owned participant authorization and bounded typed ingress.
use crate::{
    assembly::Service,
    files::{PlanFile, RequestFile, Roster},
    http::{self, Backend, HttpResponse, Reply, RouteError},
};
use cellule_cookbook_support_desk::*;
use cellule_runtime::{
    Committed, InvocationError, Resolution,
    codec::{BoundedDecoder, WireValue},
};
use hyper::{Method, StatusCode};
use std::net::SocketAddr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Customer,
    Agent,
}
enum ActionRequest {
    Ready,
    Get,
    Messages(PageRequest),
    Progress,
    Change(RequestFile),
    Resolve(RequestFile),
    Publish(PlanFile),
    Link(PlanFile),
    ResolveLink(PlanFile),
    Attachment(AttachmentId),
}
struct Operation {
    role: Role,
    key: Option<TicketKey>,
    request: ActionRequest,
}
struct Desk {
    service: Service,
    roster: Roster,
    customer: String,
    agent: String,
}

fn forbidden(message: &'static str) -> RouteError {
    RouteError {
        status: StatusCode::FORBIDDEN,
        message,
        source: None,
    }
}
fn missing() -> RouteError {
    RouteError {
        status: StatusCode::NOT_FOUND,
        message: "unknown support capability route",
        source: None,
    }
}
fn credential(name: &str, default: &str) -> Result<String, BoxError> {
    let value = match std::env::var(name) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => default.into(),
        Err(source) => return Err(source.into()),
    };
    if value.is_empty() || value.len() > 256 || !value.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err("invalid configured support participant credential".into());
    }
    Ok(value)
}
fn identity(key: Option<&str>, request: &RetainedIdentity) -> Result<(), RouteError> {
    let expected = uuid::Uuid::from_bytes(request.request).to_string();
    if key != Some(expected.as_str()) {
        return Err(RouteError {
            status: StatusCode::BAD_REQUEST,
            message: "idempotency key differs from retained native request",
            source: None,
        });
    }
    Ok(())
}
fn authorize_action(roster: &Roster, role: Role, action: &Action) -> Result<(), RouteError> {
    let actor = match role {
        Role::Customer => &roster.requester,
        Role::Agent => &roster.agent,
    };
    let allowed = match action {
        Action::Open {
            requester,
            endpoint,
            ..
        } => {
            role == Role::Customer
                && requester == actor
                && endpoint == &roster.notification_endpoint
        }
        Action::Message { message, .. } => message.author == *actor,
        Action::Assign { agent, .. } => role == Role::Agent && agent == actor,
        Action::Resolve { .. } => true,
        Action::Reopen { .. } => role == Role::Agent,
    };
    if allowed {
        Ok(())
    } else {
        Err(forbidden(
            "actor, endpoint, or action exceeds support capability",
        ))
    }
}
fn command(
    value: Result<Committed<Outcome>, InvocationError<Outcome>>,
) -> Result<HttpResponse, BoxError> {
    match value {
        Ok(value) => http::json(
            &serde_json::json!({"outcome":value.output,"receipt":crate::receipt(value.receipt)}),
        ),
        Err(InvocationError::Rejected(value)) => {
            let mut response = http::json(
                &serde_json::json!({"outcome":value.output,"receipt":crate::receipt(value.receipt)}),
            )?;
            *response.status_mut() = StatusCode::CONFLICT;
            Ok(response)
        }
        Err(InvocationError::Pending(_)) => {
            let mut response = http::json(
                &serde_json::json!({"resolution":"unknown","retain_original_request":true}),
            )?;
            *response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
            Ok(response)
        }
        Err(InvocationError::InvalidPublishedResult { receipt, source }) => {
            crate::files::log_source(
                source.as_ref(),
                "support published result could not be decoded",
            );
            let mut response = http::json(
                &serde_json::json!({"resolution":"published_result_invalid","receipt":crate::receipt(receipt),"retain_original_request":true}),
            )?;
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            Ok(response)
        }
        Err(source) => Err(source.into()),
    }
}
fn resolved(value: Resolution) -> Result<HttpResponse, BoxError> {
    let (body, status) = match value {
        Resolution::Committed(value) => {
            let mut decoder = BoundedDecoder::new(value.result(), (128 << 10) + 1024)?;
            let outcome = Outcome::decode(&mut decoder)?;
            decoder.finish()?;
            (
                serde_json::json!({"resolution":"committed","outcome":outcome,"commit_sequence":value.commit_sequence()}),
                StatusCode::OK,
            )
        }
        Resolution::Absent => (serde_json::json!({"resolution":"absent"}), StatusCode::OK),
        Resolution::Unknown => (
            serde_json::json!({"resolution":"unknown","retain_original_request":true}),
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        Resolution::Expired => (
            serde_json::json!({"resolution":"expired","proves_absence":false}),
            StatusCode::GONE,
        ),
    };
    let mut response = http::json(&body)?;
    *response.status_mut() = status;
    Ok(response)
}
impl Backend for Desk {
    type Operation = Operation;
    fn ready(&self) -> bool {
        self.service.source.is_ready() && self.service.coordinator.is_ready()
    }
    fn credential(&self, _: &Method, path: &str) -> &str {
        if path.starts_with("/agent/") {
            &self.agent
        } else {
            &self.customer
        }
    }
    fn parse(
        &self,
        method: &Method,
        path: &str,
        id: Option<&str>,
        body: &[u8],
    ) -> Result<Operation, RouteError> {
        let (role, rest) = if let Some(rest) = path.strip_prefix("/customer/") {
            (Role::Customer, rest)
        } else if let Some(rest) = path.strip_prefix("/agent/") {
            (Role::Agent, rest)
        } else {
            return Err(missing());
        };
        if method == Method::GET && rest == "ready" && body.is_empty() {
            return Ok(Operation {
                role,
                key: None,
                request: ActionRequest::Ready,
            });
        }
        let rest = rest.strip_prefix("tickets/").ok_or_else(missing)?;
        let (key, route) = rest.split_once('/').unwrap_or((rest, ""));
        let key = TicketKey::new(key).map_err(|source| RouteError::bad(source.into()))?;
        if !self.roster.tickets.contains(&key) {
            return Err(forbidden("ticket is outside the complete support roster"));
        }
        let request = match (method, route) {
            (&Method::GET, "") if body.is_empty() => ActionRequest::Get,
            (&Method::GET, "progress") if body.is_empty() => ActionRequest::Progress,
            (&Method::GET, "messages") if body.is_empty() => ActionRequest::Messages(PageRequest {
                after: 0,
                limit: 16,
            }),
            (&Method::POST, "messages") => {
                let page: PageRequest = serde_json::from_slice(body)
                    .map_err(|source| RouteError::bad(source.into()))?;
                if page.after > MAX_MESSAGES as u32 || !(1..=16).contains(&page.limit) {
                    return Err(RouteError::bad(
                        "invalid bounded support conversation page".into(),
                    ));
                }
                ActionRequest::Messages(page)
            }
            (&Method::POST, "changes" | "resolve") => {
                let record: RequestFile = serde_json::from_slice(body)
                    .map_err(|source| RouteError::bad(source.into()))?;
                record.validate().map_err(RouteError::bad)?;
                if record.tenant != self.roster.tenant || record.change.ticket != key {
                    return Err(forbidden("retained support request scope differs"));
                }
                identity(id, &record.identity)?;
                authorize_action(&self.roster, role, &record.change.action)?;
                if route == "changes" {
                    ActionRequest::Change(record)
                } else {
                    ActionRequest::Resolve(record)
                }
            }
            (&Method::POST, "publish-attachment" | "link-attachment" | "resolve-attachment") => {
                let record: PlanFile = serde_json::from_slice(body)
                    .map_err(|source| RouteError::bad(source.into()))?;
                record.validate().map_err(RouteError::bad)?;
                if record.tenant != self.roster.tenant || record.plan.descriptor.ticket != key {
                    return Err(forbidden("retained support attachment scope differs"));
                }
                identity(
                    id,
                    &record.plan.identities[if route == "publish-attachment" { 0 } else { 3 }],
                )?;
                match route {
                    "publish-attachment" => ActionRequest::Publish(record),
                    "link-attachment" => ActionRequest::Link(record),
                    _ => ActionRequest::ResolveLink(record),
                }
            }
            (&Method::GET, route) if route.starts_with("attachments/") && body.is_empty() => {
                ActionRequest::Attachment(
                    AttachmentId::new(&route["attachments/".len()..])
                        .map_err(|source| RouteError::bad(source.into()))?,
                )
            }
            _ => return Err(missing()),
        };
        Ok(Operation {
            role,
            key: Some(key),
            request,
        })
    }
    async fn execute(&self, op: Operation) -> Result<Reply, BoxError> {
        if matches!(op.request, ActionRequest::Ready) {
            return Ok(http::json(
                &serde_json::json!({"ready":self.ready(),"application":"support-desk"}),
            )?
            .into());
        }
        let key = op.key.ok_or("support request lacks ticket binding")?;
        let client = self.service.ticket(key.clone()).await?;
        // Resolve identity from the authenticated capability. Read current
        // immutable ownership before any command, Blob publication, or query.
        let metadata = client.get(None).await?;
        if op.role == Role::Customer
            && metadata
                .output
                .as_ref()
                .is_some_and(|ticket| ticket.requester != self.roster.requester)
        {
            return Ok(
                http::error(StatusCode::FORBIDDEN, "ticket belongs to another requester").into(),
            );
        }
        if metadata.output.is_none()
            && !matches!(
                op.request,
                ActionRequest::Get
                    | ActionRequest::Change(RequestFile {
                        change: Change {
                            action: Action::Open { .. },
                            ..
                        },
                        ..
                    })
                    | ActionRequest::Resolve(RequestFile {
                        change: Change {
                            action: Action::Open { .. },
                            ..
                        },
                        ..
                    })
            )
        {
            return Ok(http::error(StatusCode::NOT_FOUND, "support ticket absent").into());
        }
        let response = match op.request {
            ActionRequest::Ready => return Err("unexpected support readiness operation".into()),
            ActionRequest::Get => http::json(
                &serde_json::json!({"ticket":metadata.output,"receipt":crate::receipt(metadata.receipt)}),
            )?,
            ActionRequest::Messages(page) => {
                let value = client.messages(page, None).await?;
                http::json(
                    &serde_json::json!({"page":value.output,"receipt":crate::receipt(value.receipt)}),
                )?
            }
            ActionRequest::Progress => {
                let coordinated = self.service.coordinated(key).await?;
                let deadline = match &metadata.output {
                    Some(ticket) => coordinated.deadline(&ticket.deadline).await?,
                    None => None,
                };
                let notification = match metadata
                    .output
                    .as_ref()
                    .and_then(|ticket| ticket.notifications.last())
                {
                    Some(record) => coordinated.notification(&record.notification).await?,
                    None => None,
                };
                http::json(
                    &serde_json::json!({"ticket":metadata.output,"ticket_receipt":crate::receipt(metadata.receipt),"deadline":deadline,"latest_notification":notification}),
                )?
            }
            ActionRequest::Change(record) => command(
                client
                    .change(record.identity.native()?, record.change.action)
                    .await,
            )?,
            ActionRequest::Resolve(record) => {
                if record.identity.expires_at_ms <= cellule_cookbook_support::now_ms()? {
                    resolved(Resolution::Expired)?
                } else {
                    let prepared = client
                        .prepare(record.identity.native()?, record.change.action)
                        .await?;
                    resolved(client.resolve(prepared.evidence()).await?)?
                }
            }
            ActionRequest::Publish(record) => {
                let value = self
                    .service
                    .attachments()
                    .await?
                    .publish(&record.plan)
                    .await?;
                http::json(
                    &serde_json::json!({"publication":value.publication,"ticket_linked":false}),
                )?
            }
            ActionRequest::Link(record) => {
                let prepared = self
                    .service
                    .attachments()
                    .await?
                    .prepare_link(&record.plan)
                    .await?;
                command(prepared.execute().await)?
            }
            ActionRequest::ResolveLink(record) => resolved(
                self.service
                    .attachments()
                    .await?
                    .resolve_link(&record.plan)
                    .await?,
            )?,
            ActionRequest::Attachment(id) => {
                let publication = metadata.output.as_ref().and_then(|ticket| {
                    ticket
                        .attachments
                        .iter()
                        .find(|value| value.descriptor.id == id)
                });
                let Some(publication) = publication else {
                    return Ok(http::error(
                        StatusCode::NOT_FOUND,
                        "support attachment reference absent",
                    )
                    .into());
                };
                let value = self
                    .service
                    .attachments()
                    .await?
                    .read(publication.descriptor.key(), None)
                    .await?;
                let object = value.output.ok_or("referenced support attachment absent")?;
                if object.publication != *publication {
                    return Err("support immutable reference differs from verified Blob".into());
                }
                // JSON byte arrays would exceed the response bound at 64 KiB.
                // Hex is lossless and keeps the full maximum object below 256 KiB.
                let bytes_hex = object
                    .bytes
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                http::json(
                    &serde_json::json!({"publication":object.publication,"bytes_hex":bytes_hex,"blob_receipt":crate::receipt(value.receipt)}),
                )?
            }
        };
        Ok(response.into())
    }
    fn failure(&self, _: &BoxError) -> HttpResponse {
        http::error(
            StatusCode::SERVICE_UNAVAILABLE,
            "support operation failed; retain original request and inspect its exact outcome",
        )
    }
}
pub(crate) async fn install(service: &Service, roster: Roster) -> Result<SocketAddr, BoxError> {
    roster.validate()?;
    let customer = credential(
        "CELLULE_SUPPORT_DESK_CUSTOMER_TOKEN",
        "cellule-cookbook-local-support-customer",
    )?;
    let agent = credential(
        "CELLULE_SUPPORT_DESK_AGENT_TOKEN",
        "cellule-cookbook-local-support-agent",
    )?;
    if customer == agent {
        return Err("support customer and agent credentials must differ".into());
    }
    // Provision only the explicit roster and private Blob Cell before readiness.
    for key in &roster.tickets {
        service.ticket(key.clone()).await?;
    }
    service.attachments().await?;
    http::install(
        &service.source,
        roster.port,
        Desk {
            service: service.clone(),
            roster,
            customer,
            agent,
        },
    )
    .await
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    fn roster() -> Roster {
        Roster {
            tenant: "acme".into(),
            tickets: vec![TicketKey::new("sample").expect("ticket")],
            port: 0,
            requester: Actor::new("customer").expect("customer"),
            agent: Actor::new("agent").expect("agent"),
            notification_endpoint: NotificationEndpoint::new(
                "http://127.0.0.1:19123/notifications",
            )
            .expect("endpoint"),
        }
    }
    #[test]
    fn participant_capabilities_refuse_forged_authors_assignments_and_destinations() {
        let roster = roster();
        let action = Action::Assign {
            expected_revision: 1,
            agent: roster.agent.clone(),
            due_at_ms: 123,
        };
        assert!(authorize_action(&roster, Role::Customer, &action).is_err());
        assert!(authorize_action(&roster, Role::Agent, &action).is_ok());
        let message = Action::Message {
            expected_revision: 1,
            message: Message {
                id: MessageId::new("first").expect("message"),
                author: roster.agent.clone(),
                body: "Hello".into(),
            },
        };
        assert!(authorize_action(&roster, Role::Customer, &message).is_err());
        assert!(authorize_action(&roster, Role::Agent, &message).is_ok());
        let mut open = Action::Open {
            subject: "Help".into(),
            requester: roster.requester.clone(),
            due_at_ms: 123,
            endpoint: roster.notification_endpoint.clone(),
        };
        assert!(authorize_action(&roster, Role::Customer, &open).is_ok());
        assert!(authorize_action(&roster, Role::Agent, &open).is_err());
        if let Action::Open { endpoint, .. } = &mut open {
            *endpoint = NotificationEndpoint::new("http://127.0.0.1:19124/notifications")
                .expect("other endpoint");
        }
        assert!(authorize_action(&roster, Role::Customer, &open).is_err());
    }

    #[tokio::test]
    async fn authenticated_http_preserves_original_receipts_and_resolves_a_fired_timer() {
        use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity, now_ms};
        use cellule_runtime::{ApplicationId, BlobArtifactStore};
        use cellule_store::Store;
        use object_store::{memory::InMemory, path::Path};
        use std::{sync::Arc, time::Duration};
        let directory = tempfile::tempdir().expect("nodes");
        let store = Store::new(Arc::new(InMemory::new()));
        let parts = Store::new(Arc::new(InMemory::new()));
        let compiled = compile().expect("compiled");
        let mut nodes = Vec::new();
        for name in ["source", "coordination"] {
            nodes.push(Arc::new(
                LocalNode::start(
                    compiled.clone(),
                    store.clone(),
                    NodeConfig {
                        state_directory: directory.path().join(name),
                        storage_prefix: Path::from("support-desk-http-tests"),
                        application_id: ApplicationId::from_bytes([0x3a; 16]),
                    },
                )
                .await
                .expect("node"),
            ));
        }
        let tenant = crate::assembly::tenant("acme").expect("tenant");
        let service = Service {
            source: nodes[0].clone(),
            coordinator: nodes[1].clone(),
            handle: nodes[0]
                .application_handle::<SupportDesk>(tenant)
                .expect("source handle")
                .with_blob_artifact_store(BlobArtifactStore::new(parts)),
            coordination: nodes[1]
                .application_handle::<SupportDesk>(tenant)
                .expect("coordination handle"),
        };
        let roster = roster();
        let key = roster.tickets[0].clone();
        let client = service.ticket(key.clone()).await.expect("ticket");
        let due = now_ms().expect("clock") + 2000;
        let deadline = Deadline {
            ticket: key.clone(),
            generation: 1,
            due_at_ms: due,
        };
        let (sender, mut progress) = tokio::sync::mpsc::channel(32);
        service
            .workers(
                &roster.tickets,
                DeliveryOptions {
                    controlled_deadline: Some(deadline.clone()),
                    before_escalation: Duration::from_secs(10),
                    progress: Some(sender),
                    ..Default::default()
                },
            )
            .await
            .expect("workers");
        let address = http::install(
            &service.source,
            0,
            Desk {
                service: service.clone(),
                roster: roster.clone(),
                customer: "test-customer".into(),
                agent: "test-agent".into(),
            },
        )
        .await
        .expect("ingress");
        let http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("HTTP client");
        let route = format!(
            "http://{address}/customer/tickets/{}/changes",
            String::from_utf8_lossy(key.as_bytes())
        );
        let mut open = RequestFile {
            version: 1,
            tenant: roster.tenant.clone(),
            identity: new_identity().expect("identity").into(),
            change: Change {
                ticket: key.clone(),
                action: Action::Open {
                    subject: "Please help".into(),
                    requester: roster.requester.clone(),
                    due_at_ms: due,
                    endpoint: roster.notification_endpoint.clone(),
                },
            },
        };
        crate::files::save(
            &directory.path().join("opening.json"),
            &serde_json::to_vec(&open).expect("original request"),
        )
        .expect("retain before dispatch");
        let id = uuid::Uuid::from_bytes(open.identity.request).to_string();
        let unauthorized = http
            .post(&route)
            .bearer_auth("test-agent")
            .header("Idempotency-Key", &id)
            .json(&open)
            .send()
            .await
            .expect("auth response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        let bad_key = http
            .post(&route)
            .bearer_auth("test-customer")
            .header("Idempotency-Key", "different")
            .json(&open)
            .send()
            .await
            .expect("key response");
        assert_eq!(bad_key.status(), StatusCode::BAD_REQUEST);
        open.tenant = "other".into();
        assert_eq!(
            http.post(&route)
                .bearer_auth("test-customer")
                .header("Idempotency-Key", &id)
                .json(&open)
                .send()
                .await
                .expect("scope response")
                .status(),
            StatusCode::FORBIDDEN
        );
        assert!(
            client
                .get(None)
                .await
                .expect("no dispatch")
                .output
                .is_none()
        );
        open.tenant = roster.tenant.clone();
        let response = http
            .post(&route)
            .bearer_auth("test-customer")
            .header("Idempotency-Key", &id)
            .json(&open)
            .send()
            .await
            .expect("opening response");
        assert_eq!(response.status(), StatusCode::OK);
        let original: serde_json::Value = response.json().await.expect("original answer");
        assert_eq!(original["outcome"]["ticket"]["revision"], 1);
        let mut message = RequestFile {
            version: 1,
            tenant: roster.tenant.clone(),
            identity: new_identity().expect("message identity").into(),
            change: Change {
                ticket: key.clone(),
                action: Action::Message {
                    expected_revision: 1,
                    message: Message {
                        id: MessageId::new("first").expect("message id"),
                        author: roster.agent.clone(),
                        body: "Conversation".into(),
                    },
                },
            },
        };
        let message_id = uuid::Uuid::from_bytes(message.identity.request).to_string();
        assert_eq!(
            http.post(&route)
                .bearer_auth("test-customer")
                .header("Idempotency-Key", &message_id)
                .json(&message)
                .send()
                .await
                .expect("forged author")
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            client
                .get(None)
                .await
                .expect("unchanged source")
                .output
                .expect("ticket")
                .revision,
            1
        );
        if let Action::Message { message, .. } = &mut message.change.action {
            message.author = roster.requester.clone();
        }
        assert_eq!(
            http.post(&route)
                .bearer_auth("test-customer")
                .header("Idempotency-Key", &message_id)
                .json(&message)
                .send()
                .await
                .expect("message response")
                .status(),
            StatusCode::OK
        );
        let replay: serde_json::Value = http
            .post(&route)
            .bearer_auth("test-customer")
            .header("Idempotency-Key", &id)
            .json(&open)
            .send()
            .await
            .expect("replay")
            .json()
            .await
            .expect("replay answer");
        assert_eq!(replay, original);
        let resolution: serde_json::Value = http
            .post(route.trim_end_matches("changes").to_owned() + "resolve")
            .bearer_auth("test-customer")
            .header("Idempotency-Key", &id)
            .json(&open)
            .send()
            .await
            .expect("resolution")
            .json()
            .await
            .expect("resolution answer");
        assert_eq!(resolution["outcome"], original["outcome"]);
        assert_eq!(
            resolution["commit_sequence"],
            original["receipt"]["commit_sequence"]
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = progress.recv().await.expect("callback event");
                if event.event == "callback_ready" {
                    assert_eq!(event.deadline, deadline);
                    break;
                }
            }
        })
        .await
        .expect("real timer callback");
        let resolution = RequestFile {
            version: 1,
            tenant: roster.tenant,
            identity: new_identity().expect("resolution identity").into(),
            change: Change {
                ticket: key,
                action: Action::Resolve {
                    expected_revision: 2,
                },
            },
        };
        let result = http
            .post(&route)
            .bearer_auth("test-customer")
            .header(
                "Idempotency-Key",
                uuid::Uuid::from_bytes(resolution.identity.request).to_string(),
            )
            .json(&resolution)
            .send()
            .await
            .expect("resolve race");
        assert_eq!(result.status(), StatusCode::OK);
        let value: serde_json::Value = result.json().await.expect("resolved ticket");
        assert_eq!(value["outcome"]["ticket"]["status"], "resolved");
        // Owned HTTP and signed callback work drain before releasing either host.
        service.shutdown().await.expect("drain");
        let mut published = None;
        while let Ok(event) = progress.try_recv() {
            if event.event == "ticket_published" {
                published = Some(event);
            }
        }
        assert_eq!(
            published.expect("accepted callback settled").outcome,
            Some(EscalationOutcome::Unchanged)
        );
    }
}
