//! Public domain, transport ambiguity, and authoritative recovery scenarios.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity, now_ms};
use cellule_cookbook_taskboard::{
    Change, PageRequest, ProjectKey, TaskOutcome, Taskboard, TaskboardClient, Tasks, compile,
};
use cellule_runtime::{
    ApplicationId, CellClient, CellTarget, Error, InvocationError, Resolution, SessionId, TenantId,
    cell::actor::CellHandle,
    peer::{
        PeerAuthorizer, PeerCellResolver, PeerDispatcher, PeerPrincipal, PeerRoundTrip, PeerSigner,
        PeerVerifier, VerifiedPeerRequest, wire,
    },
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

const APPLICATION: ApplicationId = ApplicationId::from_bytes([0x51; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x61; 16]);

async fn start(store: Store, root: &std::path::Path) -> LocalNode {
    LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: root.into(),
            storage_prefix: Path::from("test-taskboard"),
            application_id: APPLICATION,
        },
    )
    .await
    .unwrap()
}

async fn project(node: &LocalNode, slug: &str) -> TaskboardClient {
    let client = TaskboardClient::new(
        node.application_handle::<Taskboard>(TENANT).unwrap(),
        &ProjectKey::new(slug).unwrap(),
    )
    .unwrap();
    node.open_cell(client.target(), &Tasks).await.unwrap();
    client
}

fn create(id: i64) -> Change {
    Change::Create {
        id,
        title: format!("Task {id}"),
    }
}
fn page(limit: u32) -> PageRequest {
    PageRequest { after: None, limit }
}

#[tokio::test]
async fn journey_replays_one_outcome_and_reads_the_published_revision() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let client = project(&node, "project-one").await;
    let identity = new_identity().unwrap();
    let created = client.change(identity, create(1)).await.unwrap();
    assert_eq!(client.change(identity, create(1)).await.unwrap(), created);
    let assigned = client
        .change(
            new_identity().unwrap(),
            Change::Assign {
                id: 1,
                expected_revision: 1,
                assignee: Some("alice".into()),
            },
        )
        .await
        .unwrap();
    let closed = client
        .change(
            new_identity().unwrap(),
            Change::Close {
                id: 1,
                expected_revision: 2,
            },
        )
        .await
        .unwrap();
    let observed = client.list(Some(closed.receipt), page(20)).await.unwrap();
    assert_eq!(observed.output.tasks.len(), 1);
    let task = &observed.output.tasks[0];
    assert!(task.closed);
    assert_eq!(task.revision, 3);
    assert_eq!(task.assignee.as_deref(), Some("alice"));
    assert!(assigned.receipt.commit_sequence < closed.receipt.commit_sequence);
    assert!(observed.receipt.commit_sequence >= closed.receipt.commit_sequence);
    node.shutdown().await.unwrap();
    assert!(!node.is_ready());
}

#[tokio::test]
async fn two_editors_cannot_overwrite_the_same_revision() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let client = project(&node, "contended").await;
    client
        .change(new_identity().unwrap(), create(1))
        .await
        .unwrap();
    let first_identity = new_identity().unwrap();
    let second_identity = new_identity().unwrap();
    let first = Change::Assign {
        id: 1,
        expected_revision: 1,
        assignee: Some("alice".into()),
    };
    let second = Change::Assign {
        id: 1,
        expected_revision: 1,
        assignee: Some("bob".into()),
    };
    let (first_result, second_result) = tokio::join!(
        client.change(first_identity, first.clone()),
        client.change(second_identity, second.clone())
    );
    let (loser_identity, loser_input, rejected) = match (first_result, second_result) {
        (Ok(_), Err(InvocationError::Rejected(value))) => (second_identity, second, value),
        (Err(InvocationError::Rejected(value)), Ok(_)) => (first_identity, first, value),
        other => panic!("expected one winner and one durable rejection: {other:?}"),
    };
    assert_eq!(rejected.output, TaskOutcome::Conflict);
    let Err(InvocationError::Rejected(replayed)) = client.change(loser_identity, loser_input).await
    else {
        panic!("losing outcome must remain rejected");
    };
    assert_eq!(replayed, rejected);
    assert_eq!(
        client.list(None, page(20)).await.unwrap().output.tasks[0].revision,
        2
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn keyset_pages_are_bounded_ordered_and_project_isolated() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let client = project(&node, "pages").await;
    for id in [9, 3, 7] {
        client
            .change(new_identity().unwrap(), create(id))
            .await
            .unwrap();
    }
    let first = client.list(None, page(2)).await.unwrap().output;
    assert_eq!(
        first.tasks.iter().map(|task| task.id).collect::<Vec<_>>(),
        [3, 7]
    );
    assert_eq!(first.next, Some(7));
    let second = client
        .list(
            None,
            PageRequest {
                after: first.next,
                limit: 2,
            },
        )
        .await
        .unwrap()
        .output;
    assert_eq!(
        second.tasks.iter().map(|task| task.id).collect::<Vec<_>>(),
        [9]
    );
    assert_eq!(second.next, None);
    assert!(client.list(None, page(0)).await.is_err());
    assert!(client.list(None, page(101)).await.is_err());
    let other = project(&node, "other").await;
    assert!(
        other
            .list(None, page(20))
            .await
            .unwrap()
            .output
            .tasks
            .is_empty()
    );
    let receipt = client.list(None, page(20)).await.unwrap().receipt;
    assert!(other.list(Some(receipt), page(20)).await.is_err());
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_mutations_and_terminal_state_have_durable_business_outcomes() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let client = project(&node, "validation").await;
    let invalid = client
        .prepare(
            new_identity().unwrap(),
            Change::Create {
                id: 1,
                title: " padded ".into(),
            },
        )
        .await
        .unwrap();
    let evidence = invalid.evidence().clone();
    assert!(
        matches!(invalid.execute().await, Err(InvocationError::Rejected(value)) if value.output == TaskOutcome::Invalid)
    );
    assert!(matches!(
        client.resolve(&evidence).await.unwrap(),
        Resolution::Committed(cellule_runtime::cell::executor::StoredOutcome::Rejected { .. })
    ));
    assert!(
        client
            .list(None, page(20))
            .await
            .unwrap()
            .output
            .tasks
            .is_empty()
    );
    assert!(
        matches!(client.change(new_identity().unwrap(), Change::Close { id: 1, expected_revision: 1 }).await,
        Err(InvocationError::Rejected(value)) if value.output == TaskOutcome::NotFound)
    );
    client
        .change(new_identity().unwrap(), create(1))
        .await
        .unwrap();
    client
        .change(
            new_identity().unwrap(),
            Change::Close {
                id: 1,
                expected_revision: 1,
            },
        )
        .await
        .unwrap();
    assert!(
        matches!(client.change(new_identity().unwrap(), Change::Assign { id: 1, expected_revision: 2, assignee: Some("bob".into()) }).await,
        Err(InvocationError::Rejected(value)) if value.output == TaskOutcome::Conflict)
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_successor_restores_after_all_local_working_files_are_removed() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = start(store.clone(), root.path()).await;
    let client = project(&node, "recovered").await;
    let prepared = client
        .prepare(new_identity().unwrap(), create(1))
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let committed = prepared.execute().await.unwrap();
    node.shutdown().await.unwrap();
    drop(client);
    drop(node);
    for file in std::fs::read_dir(root.path()).unwrap() {
        let path = file.unwrap().path();
        if path.is_dir() {
            std::fs::remove_dir_all(path).unwrap();
        } else {
            std::fs::remove_file(path).unwrap();
        }
    }
    let successor = start(store, root.path()).await;
    let restored = project(&successor, "recovered").await;
    let observed = restored
        .list(Some(committed.receipt), page(20))
        .await
        .unwrap();
    assert_eq!(observed.output.tasks.len(), 1);
    assert_eq!(observed.output.tasks[0].id, 1);
    assert_eq!(observed.output.tasks[0].revision, 1);
    assert!(matches!(
        restored.resolve(&evidence).await.unwrap(),
        Resolution::Committed(_)
    ));
    successor.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_live_owner_cannot_be_stolen_by_another_node() {
    let first_root = tempfile::tempdir().unwrap();
    let second_root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let first = start(store.clone(), first_root.path()).await;
    let client = project(&first, "owned").await;
    let committed = client
        .change(new_identity().unwrap(), create(1))
        .await
        .unwrap();
    let contender = start(store, second_root.path()).await;
    assert!(contender.open_cell(client.target(), &Tasks).await.is_err());
    assert_eq!(
        client
            .list(Some(committed.receipt), page(20))
            .await
            .unwrap()
            .output
            .tasks[0]
            .revision,
        1
    );
    contender.shutdown().await.unwrap();
    assert!(first.is_ready());
    first.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_uses_fresh_files_and_drain_preserves_other_sessions() {
    let root = tempfile::tempdir().unwrap();
    let retained = root.path().join("interrupted-session");
    std::fs::create_dir(&retained).unwrap();
    std::fs::write(retained.join("evidence.sqlite"), b"retain").unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = start(store.clone(), root.path()).await;
    let client = project(&node, "restart").await;
    let identity = new_identity().unwrap();
    let committed = client.change(identity, create(1)).await.unwrap();
    node.shutdown().await.unwrap();
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    let successor = start(store, root.path()).await;
    let restored = project(&successor, "restart").await;
    assert_eq!(
        restored.change(identity, create(1)).await.unwrap(),
        committed
    );
    successor.shutdown().await.unwrap();
    assert_eq!(
        std::fs::read(retained.join("evidence.sqlite")).unwrap(),
        b"retain"
    );
}

struct Resolver {
    target: CellTarget,
    handle: CellHandle,
}
impl PeerCellResolver for Resolver {
    fn resolve(
        &self,
        target: CellTarget,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<CellHandle>> + Send + 'static>> {
        let allowed = target == self.target;
        let handle = self.handle.clone();
        Box::pin(async move {
            if allowed {
                Ok(handle)
            } else {
                Err(Error::CellNotActive)
            }
        })
    }
}
struct Authorizer;
impl PeerAuthorizer for Authorizer {
    fn authorize(&self, request: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        if request.permits("taskboard.test") {
            Ok(())
        } else {
            Err(Error::PeerAuthorization("test principal is missing"))
        }
    }
}
struct LoseReply {
    verifier: Arc<PeerVerifier>,
    dispatcher: Arc<PeerDispatcher>,
    lost: Arc<AtomicBool>,
}
impl PeerRoundTrip for LoseReply {
    fn send(
        &self,
        target: CellTarget,
        bytes: Vec<u8>,
        _: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let verifier = self.verifier.clone();
        let dispatcher = self.dispatcher.clone();
        let lost = self.lost.clone();
        Box::pin(async move {
            let now = now_ms().unwrap();
            let request = verifier.verify(&bytes, now)?;
            if request.target() != &target {
                return Err(Error::Peer("unexpected target"));
            }
            let mutation = matches!(
                request.operation(),
                Some(wire::peer_request::Operation::Mutate(_))
            );
            let reply = dispatcher.dispatch_bytes(&request, now).await?;
            // Publication completed. Lose exactly the first mutation reply,
            // allowing Describe and Resolve to traverse the same signed route.
            if mutation && !lost.swap(true, Ordering::SeqCst) {
                return Err(Error::PeerTransportUnknown {
                    context: "injected lost reply after publication",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "reply lost",
                    )),
                });
            }
            Ok(reply)
        })
    }
}

#[tokio::test]
async fn actual_reply_loss_resolves_without_executing_a_second_logical_command() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let application = compile().unwrap();
    let local = project(&node, "lost-reply").await;
    let handle = node.open_cell(local.target(), &Tasks).await.unwrap();
    let signer = Arc::new(PeerSigner::new(
        SessionId::from_bytes([0x71; 16]),
        application.registry().release_digest(),
        ed25519_dalek::SigningKey::from_bytes(&[0x81; 32]),
    ));
    let dispatcher = Arc::new(PeerDispatcher::new(
        application.registry(),
        Arc::new(Resolver {
            target: local.target().clone(),
            handle,
        }),
        Arc::new(Authorizer),
    ));
    let transport = Arc::new(LoseReply {
        verifier: Arc::new(PeerVerifier::new(
            SessionId::from_bytes([0x71; 16]),
            application.registry().release_digest(),
            signer.verifying_key(),
        )),
        dispatcher,
        lost: Arc::new(AtomicBool::new(false)),
    });
    let peer = CellClient::peer(
        application.registry(),
        signer,
        PeerPrincipal {
            issuer: "taskboard-tests".into(),
            subject: "editor".into(),
            actions: vec![
                "cell.read".into(),
                "cell.write".into(),
                "taskboard.test".into(),
            ],
        },
        transport,
    );
    let client = TaskboardClient::new(
        ApplicationHandle::<Taskboard>::new(peer, application, TENANT, APPLICATION).unwrap(),
        &ProjectKey::new("lost-reply").unwrap(),
    )
    .unwrap();
    let prepared = client
        .prepare(new_identity().unwrap(), create(1))
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    assert!(matches!(
        prepared.execute().await,
        Err(InvocationError::Pending(_))
    ));
    let Resolution::Committed(outcome) = client.resolve(&evidence).await.unwrap() else {
        panic!("lost reply must resolve");
    };
    let observed = client.list(None, page(20)).await.unwrap();
    assert_eq!(observed.output.tasks.len(), 1);
    assert_eq!(observed.output.tasks[0].revision, 1);
    assert_eq!(observed.receipt.commit_sequence, outcome.commit_sequence());
    node.shutdown().await.unwrap();
}

#[test]
fn canonical_keys_and_wire_fixtures_are_stable() {
    use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, WireValue};
    for invalid in ["", "UPPER", " x", "a/b", "-start", "end-"] {
        assert!(ProjectKey::new(invalid).is_err());
    }
    assert_eq!(
        ProjectKey::new("project-one").unwrap().as_bytes(),
        b"project-one"
    );
    let mut encoder = BoundedEncoder::new(1024).unwrap();
    Change::Create {
        id: 1,
        title: "A".into(),
    }
    .encode(&mut encoder)
    .unwrap();
    let bytes = encoder.finish();
    assert_eq!(bytes, [1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, b'A']);
    let mut decoder = BoundedDecoder::new(&bytes, 1024).unwrap();
    assert_eq!(
        Change::decode(&mut decoder).unwrap(),
        Change::Create {
            id: 1,
            title: "A".into()
        }
    );
    decoder.finish().unwrap();
    let mut decoder = BoundedDecoder::new(&[255], 1024).unwrap();
    assert!(Change::decode(&mut decoder).is_err());
}

#[tokio::test]
async fn worker_failure_closes_readiness_preserves_cause_and_releases_ownership() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = start(store.clone(), root.path()).await;
    let client = project(&node, "worker-failure").await;
    let created = client
        .change(new_identity().unwrap(), create(1))
        .await
        .unwrap();
    node.spawn_worker(|_| async {
        Err::<(), _>(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "deliberate worker refusal",
        ))
    })
    .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while node.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let error = node.shutdown().await.unwrap_err();
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
    let mut found = false;
    while let Some(source) = cause {
        found |= source.to_string().contains("deliberate worker refusal");
        cause = source.source();
    }
    assert!(found, "task failure lost its originating cause: {error:?}");
    let evidence = std::fs::read_dir(root.path()).unwrap().count();
    assert!(
        evidence > 0,
        "failed drain retains its local working evidence"
    );
    drop(client);
    drop(node);
    let successor = start(store, root.path()).await;
    let client = project(&successor, "worker-failure").await;
    let state = client.list(Some(created.receipt), page(10)).await.unwrap();
    assert_eq!(state.output.tasks.len(), 1);
    client
        .change(new_identity().unwrap(), create(2))
        .await
        .unwrap();
    successor.shutdown().await.unwrap();
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), evidence);
}
