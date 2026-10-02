//! Public human-decision, timer, Activity, recovery, and compatibility contracts.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_app::ApplicationHandle;
use cellule_cookbook_approvals::{
    ApprovalApplication, ApprovalClient, ApprovalView, Choice, Control, Employee, HistoryRequest,
    Phase, Purchase, PurchaseId, Reminder, compile, open, read_mail, spawn_reminders,
};
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity, now_ms};
use cellule_runtime::{
    ApplicationId, InvocationError, Resolution, TenantId, primitives::workflow::WorkflowOutcome,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{sync::Arc, time::Duration};
const APP: ApplicationId = ApplicationId::from_bytes([0x95; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x96; 16]);
async fn start(
    store: Store,
    root: &std::path::Path,
) -> (LocalNode, ApplicationHandle<ApprovalApplication>) {
    let node = LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: root.into(),
            storage_prefix: Path::from("test-approvals"),
            application_id: APP,
        },
    )
    .await
    .unwrap();
    let handle = open(&node, TENANT, APP).await.unwrap();
    (node, handle)
}
fn store() -> Store {
    Store::new(Arc::new(InMemory::new()))
}
fn employee(name: &str) -> Employee {
    Employee::parse(name).unwrap()
}
fn client(handle: &ApplicationHandle<ApprovalApplication>, name: &str) -> ApprovalClient {
    ApprovalClient::new(handle.clone(), employee(name))
}
fn purchase(root: &std::path::Path) -> Purchase {
    let now = now_ms().unwrap();
    Purchase {
        id: PurchaseId::parse(&uuid::Uuid::now_v7().to_string()).unwrap(),
        requester: employee("alice"),
        approvers: vec![employee("bob"), employee("carol")],
        title: "Workstations".into(),
        units: 2400,
        deadline_ms: now + 60_000,
        remind_at_ms: now,
        mailbox: root.join("mailbox").to_str().unwrap().into(),
        publication_delay_ms: 0,
    }
}
async fn wait(
    node: &LocalNode,
    client: &ApprovalClient,
    id: PurchaseId,
    predicate: impl Fn(&ApprovalView) -> bool,
) -> ApprovalView {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let view = client.get(id, None).await.unwrap().output.unwrap();
            if predicate(&view) {
                return view;
            }
            assert!(node.is_ready());
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn two_votes_are_immutable_signal_dedup_is_durable_and_history_is_bounded() {
    let root = tempfile::tempdir().unwrap();
    let (node, handle) = start(store(), root.path()).await;
    let alice = client(&handle, "alice");
    let bob = client(&handle, "bob");
    let carol = client(&handle, "carol");
    let purchase = purchase(root.path());
    let id = purchase.id;
    let identity = new_identity().unwrap();
    let prepared = alice
        .prepare_submit(identity, purchase.clone())
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    assert!(matches!(
        alice.resolve(&evidence).await.unwrap(),
        Resolution::Absent
    ));
    let started = prepared.execute().await.unwrap();
    assert_eq!(
        alice
            .prepare_submit(identity, purchase)
            .await
            .unwrap()
            .execute()
            .await
            .unwrap(),
        started
    );
    let view = alice
        .get(id, Some(started.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert!(
        alice
            .prepare_vote(
                new_identity().unwrap(),
                id,
                view.run_id,
                *uuid::Uuid::now_v7().as_bytes(),
                Choice::Approve
            )
            .await
            .is_err()
    );
    let signal = *uuid::Uuid::now_v7().as_bytes();
    let vote_identity = new_identity().unwrap();
    let voted = bob
        .prepare_vote(vote_identity, id, view.run_id, signal, Choice::Approve)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_eq!(
        bob.prepare_vote(vote_identity, id, view.run_id, signal, Choice::Approve)
            .await
            .unwrap()
            .execute()
            .await
            .unwrap(),
        voted
    );
    let duplicate = bob
        .prepare_vote(
            new_identity().unwrap(),
            id,
            view.run_id,
            signal,
            Choice::Approve,
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert!(matches!(
        duplicate.output,
        WorkflowOutcome::Duplicate { .. }
    ));
    assert!(
        matches!(bob.prepare_vote(new_identity().unwrap(),id,view.run_id,signal,Choice::Reject).await.unwrap().execute().await,Err(InvocationError::Rejected(result)) if result.output==WorkflowOutcome::IdentityConflict)
    );
    bob.prepare_vote(
        new_identity().unwrap(),
        id,
        view.run_id,
        *uuid::Uuid::now_v7().as_bytes(),
        Choice::Reject,
    )
    .await
    .unwrap()
    .execute()
    .await
    .unwrap();
    let result = carol
        .prepare_vote(
            new_identity().unwrap(),
            id,
            view.run_id,
            *uuid::Uuid::now_v7().as_bytes(),
            Choice::Approve,
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let done = alice
        .get(id, Some(result.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(done.state.phase, Phase::Approved);
    assert_eq!(done.state.votes[&employee("bob")], Choice::Approve);
    assert_eq!(done.state.votes.len(), 2);
    let mut after = None;
    let mut entries = Vec::new();
    loop {
        let page = alice
            .history(
                HistoryRequest {
                    id,
                    run_id: done.run_id,
                    after,
                    limit: 1,
                },
                None,
            )
            .await
            .unwrap()
            .output;
        entries.extend(page.entries);
        let Some(next) = page.next else {
            break;
        };
        after = Some(next);
    }
    assert_eq!(entries, done.state.audit);
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.event == "approved_by")
            .count(),
        2
    );
    for limit in [0, 11] {
        assert!(
            alice
                .history(
                    HistoryRequest {
                        id,
                        run_id: done.run_id,
                        after: None,
                        limit
                    },
                    None
                )
                .await
                .is_err()
        );
    }
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn owned_timers_and_activity_publish_a_verified_immutable_mail_record() {
    let root = tempfile::tempdir().unwrap();
    let (node, handle) = start(store(), root.path()).await;
    let alice = client(&handle, "alice");
    let purchase = purchase(root.path());
    let id = purchase.id;
    alice
        .prepare_submit(new_identity().unwrap(), purchase.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let queued = wait(&node, &alice, id, |view| {
        matches!(view.state.reminder, Reminder::Queued { .. })
    })
    .await;
    assert!(
        !root.path().join("mailbox").exists(),
        "compilation and timers do not install Activity workers"
    );
    spawn_reminders(&node, handle).unwrap();
    let delivered = wait(&node, &alice, id, |view| {
        matches!(view.state.reminder, Reminder::Delivered { .. })
    })
    .await;
    let Reminder::Delivered { receipt } = &delivered.state.reminder else {
        panic!("missing reminder receipt")
    };
    let mail = read_mail(&root.path().join("mailbox"), receipt).unwrap();
    assert_eq!(mail.run_id, queued.run_id);
    assert_eq!(mail.purchase_id, id);
    assert_eq!(mail.recipients, purchase.approvers);
    assert_eq!(
        std::fs::read_dir(root.path().join("mailbox"))
            .unwrap()
            .count(),
        1
    );
    std::fs::write(
        root.path()
            .join("mailbox")
            .join(format!("{}.json", receipt.key)),
        b"corrupt",
    )
    .unwrap();
    assert!(read_mail(&root.path().join("mailbox"), receipt).is_err());
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn cold_restore_resumes_published_reminder_intent_and_preserves_submit_outcome() {
    let root = tempfile::tempdir().unwrap();
    let provider = store();
    let (node, handle) = start(provider.clone(), root.path()).await;
    let alice = client(&handle, "alice");
    let purchase = purchase(root.path());
    let id = purchase.id;
    let identity = new_identity().unwrap();
    let prepared = alice
        .prepare_submit(identity, purchase.clone())
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let sent = prepared.execute().await.unwrap();
    let queued = wait(&node, &alice, id, |view| {
        matches!(view.state.reminder, Reminder::Queued { .. })
    })
    .await;
    node.shutdown().await.unwrap();
    drop(alice);
    drop(handle);
    drop(node);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    let (node, handle) = start(provider, root.path()).await;
    let alice = client(&handle, "alice");
    assert_eq!(
        alice.get(id, None).await.unwrap().output.unwrap().run_id,
        queued.run_id
    );
    assert!(
        matches!(alice.resolve(&evidence).await.unwrap(),Resolution::Committed(outcome) if outcome.commit_sequence()==sent.receipt.commit_sequence)
    );
    spawn_reminders(&node, handle).unwrap();
    wait(&node, &alice, id, |view| {
        matches!(view.state.reminder, Reminder::Delivered { .. })
    })
    .await;
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn pause_resume_cancel_and_restart_keep_exact_run_scope_and_old_vote_resolution() {
    let root = tempfile::tempdir().unwrap();
    let (node, handle) = start(store(), root.path()).await;
    let alice = client(&handle, "alice");
    let bob = client(&handle, "bob");
    let mut purchase = purchase(root.path());
    purchase.remind_at_ms = purchase.deadline_ms - 1000;
    let id = purchase.id;
    alice
        .prepare_submit(new_identity().unwrap(), purchase.clone())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let run = alice.get(id, None).await.unwrap().output.unwrap().run_id;
    assert!(
        bob.prepare_control(new_identity().unwrap(), id, run, Control::Pause)
            .await
            .is_err()
    );
    alice
        .prepare_control(new_identity().unwrap(), id, run, Control::Pause)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_eq!(
        alice.get(id, None).await.unwrap().output.unwrap().status,
        "paused"
    );
    alice
        .prepare_control(new_identity().unwrap(), id, run, Control::Resume)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let identity = new_identity().unwrap();
    let signal = *uuid::Uuid::now_v7().as_bytes();
    let voted = bob
        .prepare_vote(identity, id, run, signal, Choice::Approve)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let cancellation = new_identity().unwrap();
    alice
        .prepare_cancel(cancellation, id, run, *cancellation.request_id.as_bytes())
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_eq!(
        alice.get(id, None).await.unwrap().output.unwrap().status,
        "cancelled"
    );
    purchase.approvers = vec![employee("carol")];
    purchase.deadline_ms = now_ms().unwrap() + 60_000;
    purchase.remind_at_ms = purchase.deadline_ms - 1000;
    let restarted = alice
        .prepare_control(
            new_identity().unwrap(),
            id,
            run,
            Control::Restart { purchase },
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let fresh = alice
        .get(id, Some(restarted.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_ne!(fresh.run_id, run);
    assert!(
        alice
            .history(
                HistoryRequest {
                    id,
                    run_id: run,
                    after: None,
                    limit: 1
                },
                None
            )
            .await
            .is_err()
    );
    assert!(fresh.state.votes.is_empty());
    assert_eq!(fresh.state.audit.len(), 1);
    assert!(
        matches!(bob.resolve_vote(identity,id,run,signal,Choice::Approve).await.unwrap(),Resolution::Committed(outcome) if outcome.commit_sequence()==voted.receipt.commit_sequence)
    );
    assert!(
        bob.prepare_vote(new_identity().unwrap(), id, run, signal, Choice::Approve)
            .await
            .is_err()
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn deadline_wins_over_late_votes_and_racing_signals_record_one_terminal_decision() {
    let root = tempfile::tempdir().unwrap();
    let (node, handle) = start(store(), root.path()).await;
    let alice = client(&handle, "alice");
    let bob = client(&handle, "bob");
    let mut purchase = purchase(root.path());
    purchase.approvers = vec![employee("bob")];
    purchase.deadline_ms = now_ms().unwrap() + 600;
    purchase.remind_at_ms = purchase.deadline_ms - 100;
    let id = purchase.id;
    alice
        .prepare_submit(new_identity().unwrap(), purchase)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let run = alice.get(id, None).await.unwrap().output.unwrap().run_id;
    tokio::time::sleep(Duration::from_millis(600)).await;
    let identity = new_identity().unwrap();
    let signal = *uuid::Uuid::now_v7().as_bytes();
    let outcome = bob
        .prepare_vote(identity, id, run, signal, Choice::Approve)
        .await
        .unwrap()
        .execute()
        .await;
    assert!(matches!(outcome, Ok(_) | Err(InvocationError::Rejected(_))));
    let done = wait(&node, &alice, id, |view| view.state.phase != Phase::Pending).await;
    assert_eq!(done.state.phase, Phase::TimedOut);
    assert!(done.state.votes.is_empty());
    assert_eq!(
        done.state
            .audit
            .iter()
            .filter(|entry| entry.event == "timed_out")
            .count(),
        1
    );
    let repeated = bob
        .prepare_vote(identity, id, run, signal, Choice::Approve)
        .await
        .unwrap()
        .execute()
        .await;
    match (outcome, repeated) {
        (Ok(first), Ok(second)) => assert_eq!(first, second),
        (Err(InvocationError::Rejected(first)), Err(InvocationError::Rejected(second))) => {
            assert_eq!(first, second)
        }
        other => panic!("terminal outcome changed: {other:?}"),
    }
    // Independently race a near-deadline vote with the owned timer. Either
    // outcome is valid, but there must be one coherent terminal business result.
    let mut purchase = purchase_for_race(root.path());
    purchase.approvers = vec![employee("bob")];
    let id = purchase.id;
    alice
        .prepare_submit(new_identity().unwrap(), purchase)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let run = alice.get(id, None).await.unwrap().output.unwrap().run_id;
    tokio::time::sleep(Duration::from_millis(450)).await;
    let _ = bob
        .prepare_vote(
            new_identity().unwrap(),
            id,
            run,
            *uuid::Uuid::now_v7().as_bytes(),
            Choice::Approve,
        )
        .await
        .unwrap()
        .execute()
        .await;
    let done = wait(&node, &alice, id, |view| view.state.phase != Phase::Pending).await;
    assert!(matches!(
        done.state.phase,
        Phase::Approved | Phase::TimedOut
    ));
    assert_eq!(
        done.state
            .audit
            .iter()
            .filter(|entry| matches!(entry.event.as_str(), "approved_by" | "timed_out"))
            .count(),
        1
    );
    assert_eq!(
        done.state.votes.len(),
        usize::from(done.state.phase == Phase::Approved)
    );
    node.shutdown().await.unwrap();
}
fn purchase_for_race(root: &std::path::Path) -> Purchase {
    let mut value = purchase(root);
    value.deadline_ms = now_ms().unwrap() + 500;
    value.remind_at_ms = value.deadline_ms - 50;
    value
}
#[test]
fn canonical_identities_and_history_request_have_stable_wire_fixtures() {
    use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, WireValue};
    assert!(Employee::parse("Alice").is_err());
    assert!(PurchaseId::parse("00000000-0000-0000-0000-000000000000").is_err());
    let id = PurchaseId::parse("018f7ce0-67d0-7000-8000-000000000001").unwrap();
    let fixture = [
        0, 0, 0, 16, 0x01, 0x8f, 0x7c, 0xe0, 0x67, 0xd0, 0x70, 0, 0x80, 0, 0, 0, 0, 0, 0, 1, 0, 0,
        0, 16, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 2,
    ];
    let request = HistoryRequest {
        id,
        run_id: [1; 16],
        after: None,
        limit: 2,
    };
    let mut encoder = BoundedEncoder::new(64).unwrap();
    request.encode(&mut encoder).unwrap();
    assert_eq!(encoder.finish(), fixture);
    let mut decoder = BoundedDecoder::new(&fixture, 64).unwrap();
    assert_eq!(HistoryRequest::decode(&mut decoder).unwrap(), request);
    decoder.finish().unwrap();
}

#[tokio::test]
async fn graceful_drain_finishes_the_activity_after_external_publication() {
    let root = tempfile::tempdir().unwrap();
    let provider = store();
    let (node, handle) = start(provider.clone(), root.path()).await;
    let alice = client(&handle, "alice");
    let mut purchase = purchase(root.path());
    purchase.publication_delay_ms = 1500;
    let id = purchase.id;
    alice
        .prepare_submit(new_identity().unwrap(), purchase)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    spawn_reminders(&node, handle.clone()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if root.path().join("mailbox").exists()
                && std::fs::read_dir(root.path().join("mailbox"))
                    .unwrap()
                    .any(|entry| {
                        entry
                            .unwrap()
                            .path()
                            .extension()
                            .is_some_and(|value| value == "json")
                    })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), node.shutdown())
        .await
        .unwrap()
        .unwrap();
    drop(alice);
    drop(handle);
    drop(node);
    let (node, handle) = start(provider, root.path()).await;
    let view = client(&handle, "alice")
        .get(id, None)
        .await
        .unwrap()
        .output
        .unwrap();
    let Reminder::Delivered { receipt } = &view.state.reminder else {
        panic!("accepted reminder did not settle during drain: {view:?}")
    };
    read_mail(&root.path().join("mailbox"), receipt).unwrap();
    assert_eq!(
        std::fs::read_dir(root.path().join("mailbox"))
            .unwrap()
            .count(),
        1
    );
    node.shutdown().await.unwrap();
}
