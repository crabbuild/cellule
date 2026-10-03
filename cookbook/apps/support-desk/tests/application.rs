//! Public SDK evidence for local invariants, permanent identities, and independent recovery.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity, now_ms};
use cellule_cookbook_support_desk::*;
use cellule_runtime::{
    ApplicationId, InvocationError, Resolution, TenantId,
    codec::{BoundedDecoder, BoundedEncoder, WireValue},
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::sync::Arc;
const APP: ApplicationId = ApplicationId::from_bytes([0x3a; 16]);
const TENANT: TenantId = TenantId::from_bytes([0xfe; 16]);
fn store() -> Store {
    Store::new(Arc::new(InMemory::new()))
}
async fn start(store: Store, directory: &std::path::Path) -> LocalNode {
    LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: directory.into(),
            storage_prefix: Path::from("support-desk-sdk-tests"),
            application_id: APP,
        },
    )
    .await
    .unwrap()
}
fn key() -> TicketKey {
    TicketKey::new("cannot-sign-in").unwrap()
}
fn actor(name: &str) -> Actor {
    Actor::new(name).unwrap()
}
async fn attach(node: &LocalNode) -> TicketClient {
    let value = TicketClient::new(
        node.application_handle::<SupportDesk>(TENANT).unwrap(),
        key(),
    )
    .unwrap();
    node.open_cell(value.target(), &Tickets).await.unwrap();
    value
}
async fn open(client: &TicketClient) -> Ticket {
    client
        .change(
            new_identity().unwrap(),
            Action::Open {
                subject: "Cannot sign in".into(),
                requester: actor("alice"),
                due_at_ms: now_ms().unwrap() + 60000,
                endpoint: NotificationEndpoint::new("http://127.0.0.1:19400/notifications")
                    .unwrap(),
            },
        )
        .await
        .unwrap()
        .output
        .ticket
        .unwrap()
}
fn rejected(error: InvocationError<Outcome>) -> Outcome {
    match error {
        InvocationError::Rejected(result) => result.output,
        value => panic!("unexpected failure: {value}"),
    }
}
fn message(n: u32) -> Message {
    Message {
        id: MessageId::new(format!("message-{n}")).unwrap(),
        author: actor("alice"),
        body: format!("Evidence {n}\nWith complete original bytes"),
    }
}

#[test]
fn canonical_keys_codec_bounds_and_external_scope_are_explicit() {
    for name in ["", "Alice", "1ticket", "a--b", "a-", "../ticket"] {
        assert!(TicketKey::new(name).is_err());
    }
    for endpoint in [
        "http://example.org:19400/notifications",
        "http://127.0.0.1/notifications",
        "http://127.0.0.1:19400/notifications?token=x",
        "http://user:secret@127.0.0.1:19400/notifications",
        "http://127.0.0.1:19400/other",
    ] {
        assert!(NotificationEndpoint::new(endpoint).is_err());
    }
    assert!(
        Message {
            body: "x".repeat(2049),
            ..message(1)
        }
        .validate()
        .is_err()
    );
    let input = Change {
        ticket: key(),
        action: Action::Message {
            expected_revision: 1,
            message: message(1),
        },
    };
    let mut encoder = BoundedEncoder::new(16384).unwrap();
    input.encode(&mut encoder).unwrap();
    let bytes = encoder.finish();
    let mut decoder = BoundedDecoder::new(&bytes, 16384).unwrap();
    assert_eq!(Change::decode(&mut decoder).unwrap(), input);
    decoder.finish().unwrap();
    let mut changed = bytes.clone();
    changed[0] = 2;
    assert!(Change::decode(&mut BoundedDecoder::new(&changed, 16384).unwrap()).is_err());
    assert!(
        Change::decode(&mut BoundedDecoder::new(&bytes[..bytes.len() - 1], 16384).unwrap())
            .is_err()
    );
    assert!(
        serde_json::from_str::<Action>(
            r#"{"type":"resolve","expected_revision":1,"raw_sql":"DELETE FROM ticket"}"#
        )
        .is_err()
    );
}

#[tokio::test]
async fn full_permanent_conversation_preserves_exact_retry_and_original_outcome() {
    let directory = tempfile::tempdir().unwrap();
    let node = start(store(), directory.path()).await;
    let client = attach(&node).await;
    let mut ticket = open(&client).await;
    let prepared = client
        .prepare(
            new_identity().unwrap(),
            Action::Message {
                expected_revision: ticket.revision,
                message: message(1),
            },
        )
        .await
        .unwrap();
    let original = prepared.clone().execute().await.unwrap();
    ticket = original.output.ticket.clone().unwrap();
    for n in 2..=MAX_MESSAGES as u32 {
        ticket = client
            .change(
                new_identity().unwrap(),
                Action::Message {
                    expected_revision: ticket.revision,
                    message: message(n),
                },
            )
            .await
            .unwrap()
            .output
            .ticket
            .unwrap();
    }
    let capacity = rejected(
        client
            .change(
                new_identity().unwrap(),
                Action::Message {
                    expected_revision: ticket.revision,
                    message: message(65),
                },
            )
            .await
            .unwrap_err(),
    );
    assert_eq!(capacity.decision, Decision::Capacity);
    assert_eq!(capacity.ticket, Some(ticket.clone()));
    let mut after = 0;
    let mut all = vec![];
    loop {
        let read = client
            .messages(PageRequest { after, limit: 7 }, None)
            .await
            .unwrap();
        all.extend(read.output.messages);
        match read.output.next {
            Some(next) => after = next,
            None => break,
        }
    }
    assert_eq!(all.len(), MAX_MESSAGES);
    assert_eq!(all.last().unwrap().sequence, 64);
    let duplicate = client
        .change(
            new_identity().unwrap(),
            Action::Message {
                expected_revision: 1,
                message: message(1),
            },
        )
        .await
        .unwrap();
    assert_eq!(duplicate.output.decision, Decision::Duplicate);
    let conflict = rejected(
        client
            .change(
                new_identity().unwrap(),
                Action::Message {
                    expected_revision: ticket.revision,
                    message: Message {
                        body: "Changed bytes".into(),
                        ..message(1)
                    },
                },
            )
            .await
            .unwrap_err(),
    );
    assert_eq!(conflict.decision, Decision::Conflict);
    let resolved = client
        .change(
            new_identity().unwrap(),
            Action::Resolve {
                expected_revision: ticket.revision,
            },
        )
        .await
        .unwrap()
        .output
        .ticket
        .unwrap();
    assert_eq!(resolved.status, Status::Resolved);
    assert_eq!(
        client
            .change(
                new_identity().unwrap(),
                Action::Message {
                    expected_revision: 1,
                    message: message(1)
                }
            )
            .await
            .unwrap()
            .output
            .decision,
        Decision::Duplicate
    );
    let replay = prepared.clone().execute().await.unwrap();
    assert_eq!(replay.output, original.output);
    assert_eq!(replay.receipt, original.receipt);
    assert!(matches!(
        client.resolve(prepared.evidence()).await.unwrap(),
        Resolution::Committed(_)
    ));
    assert_eq!(client.get(None).await.unwrap().output.unwrap(), resolved);
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn resolved_and_reassigned_generations_ignore_obsolete_callbacks() {
    let directory = tempfile::tempdir().unwrap();
    let node = start(store(), directory.path()).await;
    let client = attach(&node).await;
    let mut ticket = open(&client).await;
    let old = ticket.deadline.clone();
    ticket = client
        .change(
            new_identity().unwrap(),
            Action::Assign {
                expected_revision: ticket.revision,
                agent: actor("bob"),
                due_at_ms: now_ms().unwrap() + 60000,
            },
        )
        .await
        .unwrap()
        .output
        .ticket
        .unwrap();
    let handle = node.application_handle::<SupportDesk>(TENANT).unwrap();
    // An obsolete callback can be delayed indefinitely. Its historical due time
    // still must be valid; here the original capability is made due by opening
    // a second ticket with a short deadline rather than forging future time.
    let short =
        TicketClient::new(handle.clone(), TicketKey::new("short-deadline").unwrap()).unwrap();
    node.open_cell(short.target(), &Tickets).await.unwrap();
    let opened = short
        .change(
            new_identity().unwrap(),
            Action::Open {
                subject: "Short deadline".into(),
                requester: actor("alice"),
                due_at_ms: now_ms().unwrap() + 100,
                endpoint: ticket.endpoint.clone(),
            },
        )
        .await
        .unwrap()
        .output
        .ticket
        .unwrap();
    let resolved = short
        .change(
            new_identity().unwrap(),
            Action::Resolve {
                expected_revision: opened.revision,
            },
        )
        .await
        .unwrap()
        .output
        .ticket
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let callback = Escalation {
        deadline: opened.deadline.clone(),
        fired_at_ms: now_ms().unwrap(),
    };
    let expired = handle
        .prepare_command::<EscalateTicket>(
            short.target(),
            new_identity().unwrap(),
            callback.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        expired.execute().await.unwrap().output,
        EscalationOutcome::Unchanged
    );
    assert_eq!(short.get(None).await.unwrap().output.unwrap(), resolved);
    let reopened = short
        .change(
            new_identity().unwrap(),
            Action::Reopen {
                expected_revision: resolved.revision,
                due_at_ms: now_ms().unwrap() + 60000,
            },
        )
        .await
        .unwrap()
        .output
        .ticket
        .unwrap();
    assert_eq!(
        handle
            .prepare_command::<EscalateTicket>(short.target(), new_identity().unwrap(), callback)
            .await
            .unwrap()
            .execute()
            .await
            .unwrap()
            .output,
        EscalationOutcome::Unchanged
    );
    assert_eq!(short.get(None).await.unwrap().output.unwrap(), reopened);
    let stale = rejected(
        client
            .change(
                new_identity().unwrap(),
                Action::Resolve {
                    expected_revision: 1,
                },
            )
            .await
            .unwrap_err(),
    );
    assert_eq!(stale.decision, Decision::Conflict);
    let mut forged = old;
    forged.due_at_ms = now_ms().unwrap() - 100;
    forged.generation = ticket.generation;
    assert!(matches!(
        handle
            .prepare_command::<EscalateTicket>(
                client.target(),
                new_identity().unwrap(),
                Escalation {
                    deadline: forged,
                    fired_at_ms: now_ms().unwrap()
                }
            )
            .await
            .unwrap()
            .execute()
            .await,
        Err(InvocationError::Rejected(_))
    ));
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn publication_before_link_keeps_original_revision_and_restores_exact_bytes() {
    let storage = store();
    let parts = store();
    let first = tempfile::tempdir().unwrap();
    let node = start(storage.clone(), first.path()).await;
    let client = attach(&node).await;
    let opened = open(&client).await;
    let handle = node.application_handle::<SupportDesk>(TENANT).unwrap();
    let blobs = AttachmentClient::new(
        handle.with_blob_artifact_store(cellule_runtime::BlobArtifactStore::new(parts.clone())),
    )
    .unwrap();
    node.open_cell(&blobs.target().unwrap(), &Attachments)
        .await
        .unwrap();
    let bytes = (0..MAX_ATTACHMENT_BYTES)
        .map(|n| (n % 251) as u8)
        .collect::<Vec<_>>();
    let descriptor = AttachmentDescriptor::new(
        key(),
        AttachmentId::new("logfile").unwrap(),
        "sign-in.log".into(),
        &bytes,
    )
    .unwrap();
    let plan = AttachmentPlan::new(descriptor.clone(), bytes.clone(), opened.revision).unwrap();
    assert!(blobs.prepare_link(&plan).await.is_err());
    let object = blobs.publish(&plan).await.unwrap();
    assert_eq!(object.bytes, bytes);
    assert_eq!(
        client
            .get(None)
            .await
            .unwrap()
            .output
            .unwrap()
            .attachments
            .len(),
        0
    );
    let edited = client
        .change(
            new_identity().unwrap(),
            Action::Message {
                expected_revision: opened.revision,
                message: message(1),
            },
        )
        .await
        .unwrap()
        .output
        .ticket
        .unwrap();
    let result = blobs.reconcile(&plan).await.unwrap_err();
    let original = result.downcast_ref::<InvocationError<Outcome>>().unwrap();
    assert!(
        matches!(original,InvocationError::Rejected(v) if v.output.decision==Decision::Conflict)
    );
    assert_eq!(blobs.publish(&plan).await.unwrap(), object);
    // Reconciliation preserves stale evidence; the operator's later edit gets
    // an explicit new plan and revision, while immutable publication stays fixed.
    let replacement = AttachmentPlan::new(descriptor, bytes.clone(), edited.revision).unwrap();
    assert_eq!(
        blobs
            .reconcile(&replacement)
            .await
            .unwrap()
            .output
            .ticket
            .unwrap()
            .attachments
            .len(),
        1
    );
    let current = client.get(None).await.unwrap().output.unwrap();
    node.shutdown().await.unwrap();
    let second = tempfile::tempdir().unwrap();
    let restored = start(storage, second.path()).await;
    let restored_client = attach(&restored).await;
    assert_eq!(
        restored_client.get(None).await.unwrap().output.unwrap(),
        current
    );
    let restored_blobs = AttachmentClient::new(
        restored
            .application_handle::<SupportDesk>(TENANT)
            .unwrap()
            .with_blob_artifact_store(cellule_runtime::BlobArtifactStore::new(parts)),
    )
    .unwrap();
    restored
        .open_cell(&restored_blobs.target().unwrap(), &Attachments)
        .await
        .unwrap();
    assert_eq!(
        restored_blobs
            .read(object.publication.descriptor.key(), None)
            .await
            .unwrap()
            .output
            .unwrap(),
        object
    );
    restored.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_timer_fires_then_resolution_prevents_signed_obsolete_escalation() {
    use cellule_runtime::primitives::effects::EffectState;
    let storage = store();
    let directory = tempfile::tempdir().unwrap();
    let source = start(storage.clone(), &directory.path().join("source")).await;
    let coordinator = start(storage.clone(), &directory.path().join("coordinator")).await;
    let client = attach(&source).await;
    let opened = client
        .change(
            new_identity().unwrap(),
            Action::Open {
                subject: "Resolve during delivery".into(),
                requester: actor("alice"),
                due_at_ms: now_ms().unwrap() + 500,
                endpoint: NotificationEndpoint::new("http://127.0.0.1:19400/notifications")
                    .unwrap(),
            },
        )
        .await
        .unwrap()
        .output
        .ticket
        .unwrap();
    let (sender, mut observations) = tokio::sync::mpsc::channel(8);
    spawn_coordination(
        &source,
        &coordinator,
        source.application_handle::<SupportDesk>(TENANT).unwrap(),
        coordinator
            .application_handle::<SupportDesk>(TENANT)
            .unwrap(),
        &[key()],
        DeliveryOptions {
            controlled_deadline: Some(opened.deadline.clone()),
            before_escalation: std::time::Duration::from_secs(10),
            progress: Some(sender),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let ready = match tokio::time::timeout(std::time::Duration::from_secs(15), observations.recv())
        .await
    {
        Ok(Some(value)) => value,
        other => {
            let effects = source
                .application_handle::<SupportDesk>(TENANT)
                .unwrap()
                .effects::<Tickets>(client.target().clone())
                .unwrap();
            let installed = effects.status(opened.deadline_effect, None).await;
            let handle = coordinator
                .application_handle::<SupportDesk>(TENANT)
                .unwrap();
            let workflow = handle
                .query::<cellule_runtime::primitives::workflow::WorkflowGetQuery<Deadlines>>(
                    &handle.target_for_scope(DEADLINES, b"deadlines").unwrap(),
                    None,
                    cellule_runtime::primitives::workflow::WorkflowGetRequest {
                        workflow_id: opened.deadline.key().to_vec(),
                    },
                )
                .await;
            let source_drain = source.shutdown().await;
            let coordination_drain = coordinator.shutdown().await;
            panic!(
                "no actual callback: {other:?}; start={installed:?}; workflow={workflow:?}; source_drain={source_drain:?}; coordination_drain={coordination_drain:?}"
            );
        }
    };
    assert_eq!(ready.event, "callback_ready");
    assert_eq!(ready.deadline, opened.deadline);
    assert_eq!(ready.outcome, None);
    let resolved = client
        .change(
            new_identity().unwrap(),
            Action::Resolve {
                expected_revision: opened.revision,
            },
        )
        .await
        .unwrap()
        .output
        .ticket
        .unwrap();
    assert_eq!(resolved.status, Status::Resolved);
    // Owned drain cancels only the diagnostic pause, then finishes the accepted
    // callback and its original native Inbox acknowledgment while both Cells live.
    source.shutdown().await.unwrap();
    let published = observations.recv().await.unwrap();
    assert_eq!(published.event, "ticket_published");
    assert_eq!(published.outcome, Some(EscalationOutcome::Unchanged));
    let id = *blake3::Hash::from_hex(&ready.effect_id).unwrap().as_bytes();
    let handle = coordinator
        .application_handle::<SupportDesk>(TENANT)
        .unwrap();
    let effects = handle
        .effects::<Deadlines>(handle.target_for_scope(DEADLINES, b"deadlines").unwrap())
        .unwrap();
    let ledger = effects.status(id, None).await.unwrap().output.unwrap();
    assert_eq!(ledger.state, EffectState::Delivered);
    assert_eq!(ledger.attempt, 1);
    coordinator.shutdown().await.unwrap();
    let cold = tempfile::tempdir().unwrap();
    let restored = start(storage, cold.path()).await;
    let client = attach(&restored).await;
    assert_eq!(client.get(None).await.unwrap().output.unwrap(), resolved);
    assert!(resolved.notifications.is_empty());
    restored.shutdown().await.unwrap();
}

#[tokio::test]
async fn last_deadline_generation_still_allows_resolution_and_permanent_message_retry() {
    let directory = tempfile::tempdir().unwrap();
    let node = start(store(), directory.path()).await;
    let client = attach(&node).await;
    let mut ticket = open(&client).await;
    for _ in 1..MAX_GENERATIONS {
        ticket = client
            .change(
                new_identity().unwrap(),
                Action::Assign {
                    expected_revision: ticket.revision,
                    agent: actor("bob"),
                    due_at_ms: now_ms().unwrap() + 60000,
                },
            )
            .await
            .unwrap()
            .output
            .ticket
            .unwrap();
    }
    assert_eq!(ticket.generation, MAX_GENERATIONS);
    let capacity = rejected(
        client
            .change(
                new_identity().unwrap(),
                Action::Assign {
                    expected_revision: ticket.revision,
                    agent: actor("carol"),
                    due_at_ms: now_ms().unwrap() + 60000,
                },
            )
            .await
            .unwrap_err(),
    );
    assert_eq!(capacity.decision, Decision::Capacity);
    let resolved = client
        .change(
            new_identity().unwrap(),
            Action::Resolve {
                expected_revision: ticket.revision,
            },
        )
        .await
        .unwrap()
        .output
        .ticket
        .unwrap();
    assert_eq!(resolved.generation, MAX_GENERATIONS + 1);
    assert_eq!(resolved.status, Status::Resolved);
    resolved.validate().unwrap();
    assert_eq!(
        rejected(
            client
                .change(
                    new_identity().unwrap(),
                    Action::Reopen {
                        expected_revision: resolved.revision,
                        due_at_ms: now_ms().unwrap() + 60000
                    }
                )
                .await
                .unwrap_err()
        )
        .decision,
        Decision::Capacity
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn signed_escalation_and_native_activity_retry_preserve_one_external_notification() {
    use axum::{
        Json, Router,
        extract::State,
        http::{HeaderMap, StatusCode},
        response::IntoResponse,
        routing::post,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    #[derive(Default)]
    struct Inbox {
        records: tokio::sync::Mutex<Vec<Notification>>,
        failed_once: AtomicBool,
    }
    async fn receive(
        State(inbox): State<Arc<Inbox>>,
        headers: HeaderMap,
        Json(input): Json<Notification>,
    ) -> axum::response::Response {
        let token = format!("Bearer {}", receiver_token().unwrap());
        if headers.get("authorization").and_then(|v| v.to_str().ok()) != Some(token.as_str()) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        if input.validate().is_err()
            || headers.get("idempotency-key").and_then(|v| v.to_str().ok())
                != Some(input.key_hex().as_str())
        {
            return StatusCode::BAD_REQUEST.into_response();
        }
        let mut records = inbox.records.lock().await;
        if let Some(previous) = records.iter().find(|item| item.key() == input.key()) {
            if previous != &input {
                return StatusCode::CONFLICT.into_response();
            }
        } else {
            records.push(input.clone());
        }
        drop(records);
        // A retryable response follows actual external application. Recovery must
        // reuse the immutable business key rather than producing another action.
        if !inbox.failed_once.swap(true, Ordering::SeqCst) {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        Json(Acknowledgement {
            key: input.key_hex(),
            content_digest: input.content_digest().unwrap(),
            applied_count: 1,
        })
        .into_response()
    }
    let inbox = Arc::new(Inbox::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let cancel = tokio_util::sync::CancellationToken::new();
    let done = cancel.clone();
    let app = Router::new()
        .route("/notifications", post(receive))
        .with_state(inbox.clone());
    let receiver = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(done.cancelled_owned())
            .await
    });
    let storage = store();
    let directory = tempfile::tempdir().unwrap();
    let source = start(storage.clone(), &directory.path().join("source")).await;
    let coordinator = start(storage, &directory.path().join("coordinator")).await;
    let client = attach(&source).await;
    let opened = client
        .change(
            new_identity().unwrap(),
            Action::Open {
                subject: "Needs escalation".into(),
                requester: actor("alice"),
                due_at_ms: now_ms().unwrap() + 500,
                endpoint: NotificationEndpoint::new(format!(
                    "http://127.0.0.1:{port}/notifications"
                ))
                .unwrap(),
            },
        )
        .await
        .unwrap()
        .output
        .ticket
        .unwrap();
    let (progress, mut delivered) = tokio::sync::mpsc::channel(8);
    spawn_coordination(
        &source,
        &coordinator,
        source.application_handle::<SupportDesk>(TENANT).unwrap(),
        coordinator
            .application_handle::<SupportDesk>(TENANT)
            .unwrap(),
        &[key()],
        DeliveryOptions {
            controlled_deadline: Some(opened.deadline),
            drop_reply_once: true,
            progress: Some(progress),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let coordinated = CoordinationClient::new(
        coordinator
            .application_handle::<SupportDesk>(TENANT)
            .unwrap(),
        key(),
    )
    .unwrap();
    let until = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    let (ticket, state) = loop {
        assert!(source.is_ready());
        assert!(coordinator.is_ready());
        let ticket = client.get(None).await.unwrap().output.unwrap();
        if let Some(record) = ticket.notifications.first()
            && let Some(view) = coordinated
                .notification(&record.notification)
                .await
                .unwrap()
            && view.status == "completed"
            && view.state.phase == NotificationPhase::Delivered
        {
            break (ticket, view.state);
        }
        assert!(
            tokio::time::Instant::now() < until,
            "notification failed to converge"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    assert!(ticket.escalated);
    assert_eq!(ticket.notifications.len(), 1);
    assert_eq!(state.attempts.len(), 2);
    assert_eq!(state.attempts[0].classification, Classification::Retryable);
    assert!(state.attempts[0].may_have_applied);
    assert_eq!(state.attempts[1].classification, Classification::Delivered);
    assert_eq!(*inbox.records.lock().await, vec![state.ticket]);
    let observed = loop {
        let event = delivered.recv().await.unwrap();
        if event.event == "ticket_published" {
            break event;
        }
    };
    assert_eq!(observed.outcome, Some(EscalationOutcome::Escalated));
    let handle = coordinator
        .application_handle::<SupportDesk>(TENANT)
        .unwrap();
    let effects = handle
        .effects::<Deadlines>(handle.target_for_scope(DEADLINES, b"deadlines").unwrap())
        .unwrap();
    let id = *blake3::Hash::from_hex(&observed.effect_id)
        .unwrap()
        .as_bytes();
    let ledger = loop {
        let value = effects.status(id, None).await.unwrap().output.unwrap();
        if value.state == cellule_runtime::primitives::effects::EffectState::Delivered {
            break value;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "lost-reply callback did not settle"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    assert_eq!(ledger.attempt, 1);
    let mut decoder = BoundedDecoder::new(ledger.result.as_deref().unwrap(), 64).unwrap();
    assert_eq!(
        EscalationOutcome::decode(&mut decoder).unwrap(),
        EscalationOutcome::Escalated
    );
    decoder.finish().unwrap();
    source.shutdown().await.unwrap();
    coordinator.shutdown().await.unwrap();
    cancel.cancel();
    receiver.await.unwrap().unwrap();
}
