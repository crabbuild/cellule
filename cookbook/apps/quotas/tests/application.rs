//! Public accounting, contention, permanent settlement, transport ambiguity, and recovery.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_app::ApplicationHandle;
use cellule_cookbook_quotas::*;
use cellule_cookbook_support::{LocalNode, LocalPeer, NodeConfig, new_identity};
use cellule_runtime::{
    ApplicationId, CellClient, CellTarget, Error, InvocationError, Resolution, TenantId,
    codec::{BoundedDecoder, BoundedEncoder, WireValue},
    peer::{PeerAuthorizer, PeerPrincipal, PeerRoundTrip, VerifiedPeerRequest, wire},
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
const APP: ApplicationId = ApplicationId::from_bytes([0xe1; 16]);
const TENANT: TenantId = TenantId::from_bytes([0xe2; 16]);
async fn start(store: Store, root: &std::path::Path) -> LocalNode {
    LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: root.into(),
            storage_prefix: Path::from("quota-test"),
            application_id: APP,
        },
    )
    .await
    .unwrap()
}
fn key(v: &str) -> CustomerKey {
    CustomerKey::new(v).unwrap()
}
fn id(n: u128) -> ReservationId {
    ReservationId::from_bytes(n.to_be_bytes()).unwrap()
}
async fn client(node: &LocalNode, customer: &str) -> QuotaClient {
    let c = QuotaClient::new(
        node.application_handle::<Quotas>(TENANT).unwrap(),
        key(customer),
    )
    .unwrap();
    node.open_cell(c.target(), &Credits).await.unwrap();
    c
}
async fn open(c: &QuotaClient, allowance: i64) -> Account {
    c.change(new_identity().unwrap(), Action::Open { allowance })
        .await
        .unwrap()
        .output
        .account
        .unwrap()
}
async fn reconcile(c: &QuotaClient) -> Account {
    let mut cursor = None;
    let mut reservations = Vec::new();
    let mut last_account = None;
    loop {
        let page = c
            .list(
                PageRequest {
                    after: cursor,
                    limit: 100,
                },
                None,
            )
            .await
            .unwrap()
            .output;
        reservations.extend(page.reservations);
        last_account = page.account.or(last_account);
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    let account = last_account.unwrap();
    assert_eq!(
        account.allowance,
        account.available + account.consumed + account.reserved
    );
    assert!(account.available >= 0);
    assert_eq!(account.reservation_count, reservations.len() as i64);
    assert_eq!(
        account.reserved,
        reservations
            .iter()
            .filter(|r| r.state == ReservationState::Active)
            .map(|r| r.credits)
            .sum::<i64>()
    );
    assert_eq!(
        account.consumed,
        reservations
            .iter()
            .filter(|r| r.state == ReservationState::Consumed)
            .map(|r| r.credits)
            .sum::<i64>()
    );
    account
}
#[tokio::test]
async fn full_journey_settles_credits_once_and_never_reuses_a_terminal_business_id() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let c = client(&node, "journey").await;
    open(&c, 100).await;
    let identity = new_identity().unwrap();
    let reserve = Action::Reserve {
        id: id(1),
        credits: 70,
    };
    let held = c.change(identity, reserve.clone()).await.unwrap();
    assert_eq!(c.change(identity, reserve).await.unwrap(), held);
    assert_eq!(held.output.account.as_ref().unwrap().available, 30);
    let spent = c
        .change(new_identity().unwrap(), Action::Consume { id: id(1) })
        .await
        .unwrap();
    assert_eq!(spent.output.decision, Decision::Consumed);
    let repeated = c
        .change(new_identity().unwrap(), Action::Consume { id: id(1) })
        .await
        .unwrap();
    assert_eq!(repeated.output.decision, Decision::AlreadyConsumed);
    assert_eq!(repeated.output.account, spent.output.account);
    assert!(
        matches!(c.change(new_identity().unwrap(),Action::Release{id:id(1)}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Closed)
    );
    c.change(
        new_identity().unwrap(),
        Action::Reserve {
            id: id(2),
            credits: 30,
        },
    )
    .await
    .unwrap();
    let identity = new_identity().unwrap();
    let returned = c
        .change(identity, Action::Release { id: id(2) })
        .await
        .unwrap();
    assert_eq!(
        c.change(identity, Action::Release { id: id(2) })
            .await
            .unwrap(),
        returned
    );
    let repeated = c
        .change(new_identity().unwrap(), Action::Release { id: id(2) })
        .await
        .unwrap();
    assert_eq!(repeated.output.decision, Decision::AlreadyReleased);
    assert_eq!(repeated.output.account, returned.output.account);
    let again = c
        .change(
            new_identity().unwrap(),
            Action::Reserve {
                id: id(2),
                credits: 30,
            },
        )
        .await
        .unwrap();
    assert_eq!(again.output.decision, Decision::ExistingReservation);
    assert_eq!(
        again.output.reservation.unwrap().state,
        ReservationState::Released
    );
    assert_eq!(again.output.account, returned.output.account);
    assert!(
        matches!(c.change(new_identity().unwrap(),Action::Reserve{id:id(2),credits:29}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Conflict)
    );
    assert!(
        matches!(c.change(new_identity().unwrap(),Action::Consume{id:id(2)}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Closed)
    );
    let account = reconcile(&c).await;
    assert_eq!(
        (account.consumed, account.reserved, account.available),
        (70, 0, 30)
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn concurrent_reservations_cannot_overcommit_and_original_rejection_stays_rejected() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let c = client(&node, "race").await;
    open(&c, 100).await;
    let identities = [new_identity().unwrap(), new_identity().unwrap()];
    let inputs = [
        Action::Reserve {
            id: id(1),
            credits: 75,
        },
        Action::Reserve {
            id: id(2),
            credits: 75,
        },
    ];
    let (a, b) = tokio::join!(
        c.change(identities[0], inputs[0].clone()),
        c.change(identities[1], inputs[1].clone())
    );
    let (winner, loser, rejected) = match (a, b) {
        (Ok(_), Err(InvocationError::Rejected(v))) => (1, 1, v),
        (Err(InvocationError::Rejected(v)), Ok(_)) => (2, 0, v),
        other => panic!("expected one winner and rejection: {other:?}"),
    };
    assert_eq!(rejected.output.decision, Decision::Insufficient);
    let account = reconcile(&c).await;
    assert_eq!(
        (
            account.reserved,
            account.available,
            account.reservation_count
        ),
        (75, 25, 1)
    );
    c.change(new_identity().unwrap(), Action::Release { id: id(winner) })
        .await
        .unwrap();
    let prepared = c
        .prepare(identities[loser], inputs[loser].clone())
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let Err(InvocationError::Rejected(replayed)) = prepared.execute().await else {
        panic!("rejection must replay")
    };
    assert_eq!(rejected, replayed);
    assert!(matches!(
        c.resolve(&evidence).await.unwrap(),
        Resolution::Committed(_)
    ));
    let retry = c
        .change(new_identity().unwrap(), inputs[loser].clone())
        .await
        .unwrap();
    assert_eq!(retry.output.decision, Decision::Reserved);
    reconcile(&c).await;
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn consumption_racing_release_has_one_terminal_state_and_no_double_credit() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let c = client(&node, "terminal-race").await;
    open(&c, 100).await;
    c.change(
        new_identity().unwrap(),
        Action::Reserve {
            id: id(1),
            credits: 80,
        },
    )
    .await
    .unwrap();
    let consume = new_identity().unwrap();
    let release = new_identity().unwrap();
    let (a, b) = tokio::join!(
        c.change(consume, Action::Consume { id: id(1) }),
        c.change(release, Action::Release { id: id(1) })
    );
    match (a, b) {
        (Ok(v), Err(InvocationError::Rejected(r))) => {
            assert_eq!(v.output.decision, Decision::Consumed);
            assert_eq!(r.output.decision, Decision::Closed);
        }
        (Err(InvocationError::Rejected(r)), Ok(v)) => {
            assert_eq!(v.output.decision, Decision::Released);
            assert_eq!(r.output.decision, Decision::Closed);
        }
        other => panic!("expected one terminal winner: {other:?}"),
    };
    let account = reconcile(&c).await;
    assert_eq!(account.reserved, 0);
    assert!(matches!(
        (account.consumed, account.available),
        (80, 20) | (0, 100)
    ));
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn allowance_changes_preserve_holds_consumption_and_revision_preconditions() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let c = client(&node, "administration").await;
    open(&c, 100).await;
    c.change(
        new_identity().unwrap(),
        Action::Reserve {
            id: id(1),
            credits: 60,
        },
    )
    .await
    .unwrap();
    let rejected = c
        .change(
            new_identity().unwrap(),
            Action::SetAllowance {
                expected_revision: 2,
                allowance: 59,
            },
        )
        .await;
    assert!(
        matches!(rejected,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Overcommitted)
    );
    assert!(
        matches!(c.change(new_identity().unwrap(),Action::SetAllowance{expected_revision:1,allowance:120}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Conflict)
    );
    let current = c
        .change(
            new_identity().unwrap(),
            Action::SetAllowance {
                expected_revision: 2,
                allowance: 120,
            },
        )
        .await
        .unwrap();
    assert_eq!(current.output.account.as_ref().unwrap().revision, 3);
    let unchanged = c
        .change(
            new_identity().unwrap(),
            Action::SetAllowance {
                expected_revision: 3,
                allowance: 120,
            },
        )
        .await
        .unwrap();
    assert_eq!(unchanged.output.decision, Decision::Unchanged);
    assert_eq!(unchanged.output.account, current.output.account);
    c.change(new_identity().unwrap(), Action::Consume { id: id(1) })
        .await
        .unwrap();
    assert!(
        matches!(c.change(new_identity().unwrap(),Action::SetAllowance{expected_revision:4,allowance:59}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Overcommitted)
    );
    assert!(
        matches!(c.change(new_identity().unwrap(),Action::Open{allowance:999}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::AlreadyOpen)
    );
    let account = reconcile(&c).await;
    assert_eq!((account.allowance, account.consumed), (120, 60));
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn bounds_and_target_binding_are_enforced_by_both_client_and_server() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let c = client(&node, "validated").await;
    for action in [
        Action::Open { allowance: -1 },
        Action::Open {
            allowance: MAX_CREDITS + 1,
        },
        Action::Reserve {
            id: id(1),
            credits: 0,
        },
        Action::Reserve {
            id: id(1),
            credits: i64::MAX,
        },
        Action::SetAllowance {
            expected_revision: 0,
            allowance: 10,
        },
    ] {
        assert!(
            c.prepare(new_identity().unwrap(), action.clone())
                .await
                .is_err()
        );
        let input = Change {
            customer: key("validated"),
            action,
        };
        let identity = new_identity().unwrap();
        let Err(InvocationError::Rejected(rejected)) = node
            .application_handle::<Quotas>(TENANT)
            .unwrap()
            .command::<ChangeQuota>(c.target(), identity, input.clone())
            .await
        else {
            panic!("server must reject invalid action")
        };
        assert_eq!(rejected.output.decision, Decision::Invalid);
        let Err(InvocationError::Rejected(replayed)) = node
            .application_handle::<Quotas>(TENANT)
            .unwrap()
            .command::<ChangeQuota>(c.target(), identity, input)
            .await
        else {
            panic!("invalid action must replay")
        };
        assert_eq!(rejected, replayed);
    }
    let wrong = Change {
        customer: key("other"),
        action: Action::Open { allowance: 100 },
    };
    assert!(
        matches!(node.application_handle::<Quotas>(TENANT).unwrap().command::<ChangeQuota>(c.target(),new_identity().unwrap(),wrong).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Invalid)
    );
    assert!(c.account(None).await.unwrap().output.is_none());
    open(&c, MAX_CREDITS).await;
    c.change(
        new_identity().unwrap(),
        Action::Reserve {
            id: id(1),
            credits: MAX_CREDITS,
        },
    )
    .await
    .unwrap();
    c.change(new_identity().unwrap(), Action::Consume { id: id(1) })
        .await
        .unwrap();
    assert!(
        matches!(c.change(new_identity().unwrap(),Action::Reserve{id:id(2),credits:1}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Insufficient)
    );
    assert_eq!(reconcile(&c).await.available, 0);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn pages_are_bounded_ordered_coherent_and_customer_scoped() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let c = client(&node, "pages").await;
    open(&c, 100).await;
    for n in [9, 3, 7] {
        c.change(
            new_identity().unwrap(),
            Action::Reserve {
                id: id(n),
                credits: 10,
            },
        )
        .await
        .unwrap();
    }
    let first = c
        .list(
            PageRequest {
                after: None,
                limit: 2,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        first
            .output
            .reservations
            .iter()
            .map(|v| v.id)
            .collect::<Vec<_>>(),
        [id(3), id(7)]
    );
    assert_eq!(first.output.next, Some(id(7)));
    assert_eq!(first.output.account.as_ref().unwrap().reserved, 30);
    let second = c
        .list(
            PageRequest {
                after: first.output.next,
                limit: 2,
            },
            Some(first.receipt),
        )
        .await
        .unwrap();
    assert_eq!(second.output.reservations[0].id, id(9));
    assert!(second.output.next.is_none());
    for limit in [0, 101, u32::MAX] {
        assert!(
            c.list(PageRequest { after: None, limit }, None)
                .await
                .is_err()
        );
    }
    let other = client(&node, "other").await;
    assert!(other.account(Some(first.receipt)).await.is_err());
    assert!(
        other
            .list(
                PageRequest {
                    after: None,
                    limit: 20
                },
                None
            )
            .await
            .unwrap()
            .output
            .account
            .is_none()
    );
    let prepared = c
        .prepare(new_identity().unwrap(), Action::Release { id: id(3) })
        .await
        .unwrap();
    assert!(other.resolve(prepared.evidence()).await.is_err());
    open(&other, 50).await;
    other
        .change(
            new_identity().unwrap(),
            Action::Reserve {
                id: id(3),
                credits: 40,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        other.account(None).await.unwrap().output.unwrap().reserved,
        40
    );
    assert_eq!(c.account(None).await.unwrap().output.unwrap().reserved, 30);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn cold_successor_retains_settlement_business_ids_and_original_mutation_outcomes() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let first = start(store.clone(), a.path()).await;
    let c = client(&first, "recovered").await;
    open(&c, 100).await;
    c.change(
        new_identity().unwrap(),
        Action::Reserve {
            id: id(1),
            credits: 70,
        },
    )
    .await
    .unwrap();
    let identity = new_identity().unwrap();
    let prepared = c
        .prepare(identity, Action::Release { id: id(1) })
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let committed = prepared.execute().await.unwrap();
    let contender = start(store.clone(), b.path()).await;
    assert!(contender.open_cell(c.target(), &Credits).await.is_err());
    first.shutdown().await.unwrap();
    assert_eq!(std::fs::read_dir(a.path()).unwrap().count(), 0);
    let restored = client(&contender, "recovered").await;
    assert_eq!(restored.target(), c.target());
    assert_eq!(
        restored
            .change(identity, Action::Release { id: id(1) })
            .await
            .unwrap(),
        committed
    );
    assert!(matches!(
        restored.resolve(&evidence).await.unwrap(),
        Resolution::Committed(_)
    ));
    let replay = restored
        .change(new_identity().unwrap(), Action::Release { id: id(1) })
        .await
        .unwrap();
    assert_eq!(replay.output.decision, Decision::AlreadyReleased);
    assert_eq!(replay.output.account, committed.output.account);
    assert_eq!(
        restored
            .account(Some(committed.receipt))
            .await
            .unwrap()
            .output
            .unwrap()
            .available,
        100
    );
    assert!(
        c.change(
            new_identity().unwrap(),
            Action::Reserve {
                id: id(2),
                credits: 1
            }
        )
        .await
        .is_err()
    );
    contender.shutdown().await.unwrap();
}
struct Authorizer;
impl PeerAuthorizer for Authorizer {
    fn authorize(&self, r: &VerifiedPeerRequest) -> cellule_runtime::Result<()> {
        if r.permits("quotas.test") {
            Ok(())
        } else {
            Err(Error::PeerAuthorization("quota test permission required"))
        }
    }
}
struct LoseReply {
    peer: LocalPeer,
    lost: Arc<AtomicBool>,
}
impl PeerRoundTrip for LoseReply {
    fn send(
        &self,
        target: CellTarget,
        bytes: Vec<u8>,
        limit: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let peer = self.peer.clone();
        let lost = self.lost.clone();
        Box::pin(async move {
            let request = peer.verify_request(&bytes)?;
            let mutation = matches!(
                request.operation(),
                Some(wire::peer_request::Operation::Mutate(_))
            );
            let reply = peer.send(target, bytes, limit).await?;
            if mutation && !lost.swap(true, Ordering::SeqCst) {
                return Err(Error::PeerTransportUnknown {
                    context: "injected lost quota reply after durable publication",
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
async fn actual_signed_reply_loss_resolves_the_original_release_without_refunding_twice() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let local = client(&node, "lost-reply").await;
    open(&local, 100).await;
    local
        .change(
            new_identity().unwrap(),
            Action::Reserve {
                id: id(1),
                credits: 80,
            },
        )
        .await
        .unwrap();
    let application = compile().unwrap();
    let peer = LocalPeer::new(
        application.registry(),
        local.target().clone(),
        node.open_cell(local.target(), &Credits).await.unwrap(),
        Arc::new(Authorizer),
    );
    let lost = Arc::new(AtomicBool::new(false));
    let native = CellClient::peer(
        application.registry(),
        peer.signer(),
        PeerPrincipal {
            issuer: "quota-tests".into(),
            subject: "customer".into(),
            actions: vec![
                "cell.read".into(),
                "cell.write".into(),
                "quotas.test".into(),
            ],
        },
        Arc::new(LoseReply {
            peer,
            lost: lost.clone(),
        }),
    );
    let client = QuotaClient::new(
        ApplicationHandle::<Quotas>::new(native, application, TENANT, APP).unwrap(),
        key("lost-reply"),
    )
    .unwrap();
    let prepared = client
        .prepare(new_identity().unwrap(), Action::Release { id: id(1) })
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    assert!(matches!(
        prepared.execute().await,
        Err(InvocationError::Pending(_))
    ));
    assert!(lost.load(Ordering::SeqCst));
    let Resolution::Committed(outcome) = client.resolve(&evidence).await.unwrap() else {
        panic!("lost release reply must resolve")
    };
    let mut decoder = BoundedDecoder::new(outcome.result(), 1024).unwrap();
    let resolved = Outcome::decode(&mut decoder).unwrap();
    decoder.finish().unwrap();
    assert_eq!(resolved.decision, Decision::Released);
    let repeated = client
        .change(new_identity().unwrap(), Action::Release { id: id(1) })
        .await
        .unwrap();
    assert_eq!(repeated.output.decision, Decision::AlreadyReleased);
    assert_eq!(repeated.output.account, resolved.account);
    assert_eq!(reconcile(&local).await.available, 100);
    node.shutdown().await.unwrap();
}
#[test]
fn canonical_keys_ids_and_wire_fixtures_are_persisted_contracts() {
    for invalid in ["", "UPPER", " a", "a/b", "-a", "a-", "é"] {
        assert!(CustomerKey::new(invalid).is_err());
    }
    assert!(CustomerKey::new("a".repeat(65)).is_err());
    for invalid in [
        "00000000-0000-0000-0000-000000000000",
        "00000000000000000000000000000001",
        "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",
    ] {
        assert!(ReservationId::parse(invalid).is_err());
    }
    let action = Action::Release { id: id(1) };
    let mut e = BoundedEncoder::new(128).unwrap();
    action.encode(&mut e).unwrap();
    let bytes = e.finish();
    let mut expected = vec![5, 0, 0, 0, 16];
    expected.extend(1_u128.to_be_bytes());
    assert_eq!(bytes, expected);
    let mut d = BoundedDecoder::new(&bytes, 128).unwrap();
    assert_eq!(Action::decode(&mut d).unwrap(), action);
    d.finish().unwrap();
    let mut d = BoundedDecoder::new(&[255], 128).unwrap();
    assert!(Decision::decode(&mut d).is_err());
    let bytes = [0, 0, 0, 0, 101];
    let mut d = BoundedDecoder::new(&bytes, 128).unwrap();
    assert!(Page::decode(&mut d).is_err());
}
#[tokio::test]
async fn permanent_record_capacity_does_not_erase_idempotency_and_settlement_still_works() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let c = client(&node, "capacity").await;
    open(&c, 2000).await;
    for n in 1..=MAX_RESERVATIONS as u128 {
        c.change(
            new_identity().unwrap(),
            Action::Reserve {
                id: id(n),
                credits: 1,
            },
        )
        .await
        .unwrap();
    }
    assert!(
        matches!(c.change(new_identity().unwrap(),Action::Reserve{id:id(1025),credits:1}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Capacity)
    );
    c.change(new_identity().unwrap(), Action::Release { id: id(1) })
        .await
        .unwrap();
    let again = c
        .change(
            new_identity().unwrap(),
            Action::Reserve {
                id: id(1),
                credits: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(again.output.decision, Decision::ExistingReservation);
    assert_eq!(
        again.output.reservation.unwrap().state,
        ReservationState::Released
    );
    assert!(
        matches!(c.change(new_identity().unwrap(),Action::Reserve{id:id(1025),credits:1}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Capacity)
    );
    c.change(new_identity().unwrap(), Action::Consume { id: id(2) })
        .await
        .unwrap();
    let account = reconcile(&c).await;
    assert_eq!(
        (
            account.reserved,
            account.consumed,
            account.available,
            account.reservation_count
        ),
        (1022, 1, 977, 1024)
    );
    node.shutdown().await.unwrap();
}
