//! Public inventory, generation fences, durable deadline delivery, and cold restore.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_cookbook_reservations::*;
use cellule_cookbook_support::{LocalNode, LocalPeer, NodeConfig, new_identity, now_ms};
use cellule_runtime::{
    ApplicationId, CellTarget, InvocationError, Resolution, TenantId,
    codec::{BoundedDecoder, BoundedEncoder, WireValue},
    primitives::effects::EffectState,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{sync::Arc, time::Duration};
const APP: ApplicationId = ApplicationId::from_bytes([0xfa; 16]);
const TENANT: TenantId = TenantId::from_bytes([0xfb; 16]);
async fn start(store: Store, path: &std::path::Path) -> LocalNode {
    LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: path.into(),
            storage_prefix: Path::from("reservation-tests"),
            application_id: APP,
        },
    )
    .await
    .unwrap()
}
fn store() -> Store {
    Store::new(Arc::new(InMemory::new()))
}
fn key(name: &str) -> EventKey {
    EventKey::new(name).unwrap()
}
fn buyer(name: &str) -> BuyerKey {
    BuyerKey::new(name).unwrap()
}
fn id(n: u128) -> HoldId {
    HoldId::from_bytes(n.to_be_bytes()).unwrap()
}
async fn client(node: &LocalNode, event: &str) -> ReservationClient {
    let client = ReservationClient::new(
        node.application_handle::<Reservations>(TENANT).unwrap(),
        key(event),
    )
    .unwrap();
    node.open_cell(client.target(), &InventoryCells)
        .await
        .unwrap();
    client
}
async fn initialize(c: &ReservationClient, seats: u32) {
    assert_eq!(
        c.change(new_identity().unwrap(), Action::Initialize { seats })
            .await
            .unwrap()
            .output
            .decision,
        Decision::Initialized
    );
}
async fn hold(c: &ReservationClient, id: HoldId, seat: u32, deadline_ms: i64) -> Hold {
    c.change(
        new_identity().unwrap(),
        Action::Hold {
            id,
            seat,
            buyer: buyer("alice"),
            deadline_ms,
        },
    )
    .await
    .unwrap()
    .output
    .hold
    .unwrap()
}
async fn wait_hold(node: &LocalNode, c: &ReservationClient, id: HoldId, state: HoldState) -> Hold {
    let until = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        assert!(node.is_ready());
        let value = c.hold(id, None).await.unwrap().output.unwrap();
        if value.state == state {
            return value;
        }
        assert!(
            tokio::time::Instant::now() < until,
            "hold did not reach {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
async fn reconcile(c: &ReservationClient) -> Inventory {
    let mut cursor = None;
    let mut holds = Vec::new();
    let inventory = loop {
        let page = c
            .list(
                PageRequest {
                    after: cursor,
                    limit: 20,
                },
                None,
            )
            .await
            .unwrap()
            .output;
        holds.extend(page.holds);
        cursor = page.next;
        if cursor.is_none() {
            break page.inventory.unwrap();
        }
    };
    assert_eq!(
        inventory.seats,
        inventory.available + inventory.held + inventory.confirmed
    );
    assert_eq!(inventory.history_count, holds.len() as i64);
    assert_eq!(
        inventory.held,
        holds.iter().filter(|v| v.state == HoldState::Held).count() as u32
    );
    assert_eq!(
        inventory.confirmed,
        holds
            .iter()
            .filter(|v| v.state == HoldState::Confirmed)
            .count() as u32
    );
    let mut occupied = std::collections::BTreeSet::new();
    for h in holds
        .iter()
        .filter(|v| matches!(v.state, HoldState::Held | HoldState::Confirmed))
    {
        assert!(occupied.insert(h.ticket.seat));
    }
    inventory
}
#[tokio::test]
async fn concurrent_claims_have_one_winner_and_durable_rejection_does_not_change_after_release() {
    let root = tempfile::tempdir().unwrap();
    let node = start(store(), root.path()).await;
    let c = client(&node, "race").await;
    initialize(&c, 1).await;
    let deadline_ms = now_ms().unwrap() + 60000;
    let actions = [
        Action::Hold {
            id: id(1),
            seat: 1,
            buyer: buyer("alice"),
            deadline_ms,
        },
        Action::Hold {
            id: id(2),
            seat: 1,
            buyer: buyer("bob"),
            deadline_ms,
        },
    ];
    let identities = [new_identity().unwrap(), new_identity().unwrap()];
    let (a, b) = tokio::join!(
        c.change(identities[0], actions[0].clone()),
        c.change(identities[1], actions[1].clone())
    );
    let (held, rejected, loser) = match (a, b) {
        (Ok(v), Err(InvocationError::Rejected(r))) => (v.output.hold.unwrap(), r, 1),
        (Err(InvocationError::Rejected(r)), Ok(v)) => (v.output.hold.unwrap(), r, 0),
        other => panic!("one winner required: {other:?}"),
    };
    assert_eq!(rejected.output.decision, Decision::Occupied);
    assert_eq!(reconcile(&c).await.held, 1);
    c.change(
        new_identity().unwrap(),
        Action::Cancel {
            id: held.ticket.id,
            generation: held.ticket.generation,
            buyer: held.buyer,
        },
    )
    .await
    .unwrap();
    let prepared = c
        .prepare(identities[loser], actions[loser].clone())
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let Err(InvocationError::Rejected(replayed)) = prepared.execute().await else {
        panic!("original rejection must replay")
    };
    assert_eq!(rejected, replayed);
    assert!(matches!(
        c.resolve(&evidence).await.unwrap(),
        Resolution::Committed(_)
    ));
    let replacement = c
        .change(new_identity().unwrap(), actions[loser].clone())
        .await
        .unwrap()
        .output
        .hold
        .unwrap();
    assert_eq!(replacement.ticket.generation, 2);
    assert_eq!(reconcile(&c).await.available, 0);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn confirmation_and_cancellation_are_permanent_buyer_bound_and_idempotent() {
    let root = tempfile::tempdir().unwrap();
    let node = start(store(), root.path()).await;
    let c = client(&node, "journey").await;
    initialize(&c, 2).await;
    let h = hold(&c, id(1), 1, now_ms().unwrap() + 60000).await;
    let identity = new_identity().unwrap();
    let action = Action::Confirm {
        id: id(1),
        generation: h.ticket.generation,
        buyer: buyer("alice"),
    };
    let committed = c.change(identity, action.clone()).await.unwrap();
    assert_eq!(c.change(identity, action.clone()).await.unwrap(), committed);
    let again = c.change(new_identity().unwrap(), action).await.unwrap();
    assert_eq!(again.output.decision, Decision::AlreadyConfirmed);
    assert_eq!(reconcile(&c).await.revision, 3);
    assert!(
        matches!(c.change(new_identity().unwrap(),Action::Cancel{id:id(1),generation:1,buyer:buyer("alice")}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Closed)
    );
    assert!(
        matches!(c.change(new_identity().unwrap(),Action::Confirm{id:id(1),generation:1,buyer:buyer("bob")}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Forbidden&&v.output.hold.is_none())
    );
    let second = hold(&c, id(2), 2, now_ms().unwrap() + 60000).await;
    let cancel = Action::Cancel {
        id: id(2),
        generation: 1,
        buyer: buyer("alice"),
    };
    c.change(new_identity().unwrap(), cancel.clone())
        .await
        .unwrap();
    assert_eq!(
        c.change(new_identity().unwrap(), cancel)
            .await
            .unwrap()
            .output
            .decision,
        Decision::AlreadyCancelled
    );
    let existing = c
        .change(
            new_identity().unwrap(),
            Action::Hold {
                id: id(2),
                seat: 2,
                buyer: buyer("alice"),
                deadline_ms: second.ticket.deadline_ms,
            },
        )
        .await
        .unwrap()
        .output;
    assert_eq!(existing.decision, Decision::ExistingHold);
    assert_eq!(existing.hold.unwrap().state, HoldState::Cancelled);
    assert!(
        matches!(c.change(new_identity().unwrap(),Action::Hold{id:id(2),seat:2,buyer:buyer("alice"),deadline_ms:second.ticket.deadline_ms+1}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Conflict)
    );
    assert_eq!(reconcile(&c).await.available, 1);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn delayed_timer_does_not_allow_confirmation_after_absolute_deadline() {
    let root = tempfile::tempdir().unwrap();
    let node = start(store(), root.path()).await;
    let c = client(&node, "delayed").await;
    initialize(&c, 1).await;
    let h = hold(&c, id(1), 1, now_ms().unwrap() + 150).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    // No delivery worker is installed: the domain command itself must enforce deadline precedence.
    let identity = new_identity().unwrap();
    let action = Action::Confirm {
        id: id(1),
        generation: 1,
        buyer: buyer("alice"),
    };
    let Err(InvocationError::Rejected(rejected)) = c.change(identity, action.clone()).await else {
        panic!("late confirmation must reject")
    };
    assert_eq!(rejected.output.decision, Decision::Expired);
    assert_eq!(
        rejected.output.hold.as_ref().unwrap().state,
        HoldState::Expired
    );
    let Err(InvocationError::Rejected(replayed)) = c.change(identity, action).await else {
        panic!("late rejection must replay")
    };
    assert_eq!(replayed, rejected);
    let replacement = hold(&c, id(2), 1, now_ms().unwrap() + 60000).await;
    assert_eq!(replacement.ticket.generation, 2);
    let native = node.application_handle::<Reservations>(TENANT).unwrap();
    assert_eq!(
        native
            .command::<ExpireHold>(
                c.target(),
                new_identity().unwrap(),
                Expiration {
                    ticket: h.ticket,
                    fired_at_ms: now_ms().unwrap()
                }
            )
            .await
            .unwrap()
            .output,
        ExpirationOutcome::Unchanged
    );
    assert_eq!(reconcile(&c).await.held, 1);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn native_workflow_expiration_releases_once_and_lost_signed_reply_resolves_original_inbox() {
    let root = tempfile::tempdir().unwrap();
    let node = start(store(), root.path()).await;
    let c = client(&node, "timer").await;
    initialize(&c, 1).await;
    let (sender, mut progress) = tokio::sync::mpsc::channel(8);
    spawn_deadlines(
        &node,
        node.application_handle::<Reservations>(TENANT).unwrap(),
        key("timer"),
        DeliveryOptions {
            drop_reply_once: true,
            progress: Some(sender),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let h = hold(&c, id(1), 1, now_ms().unwrap() + 600).await;
    let checkpoint = tokio::time::timeout(Duration::from_secs(20), progress.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.ticket, h.ticket);
    assert_eq!(checkpoint.outcome, ExpirationOutcome::Expired);
    wait_hold(&node, &c, id(1), HoldState::Expired).await;
    assert_eq!(reconcile(&c).await.revision, 3);
    let bytes = (0..64)
        .step_by(2)
        .map(|i| u8::from_str_radix(&checkpoint.effect_id[i..i + 2], 16).unwrap())
        .collect::<Vec<_>>();
    let effect: [u8; 32] = bytes.try_into().unwrap();
    let handle = node.application_handle::<Reservations>(TENANT).unwrap();
    let target = handle
        .target_for_scope(DEADLINES, &h.ticket.workflow_id())
        .unwrap();
    let source = handle.effects::<Deadlines>(target).unwrap();
    let until = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let status = source.status(effect, None).await.unwrap().output.unwrap();
        if status.state == EffectState::Delivered {
            let mut decoder = BoundedDecoder::new(status.result.as_ref().unwrap(), 16).unwrap();
            assert_eq!(
                ExpirationOutcome::decode(&mut decoder).unwrap(),
                ExpirationOutcome::Expired
            );
            decoder.finish().unwrap();
            break;
        }
        assert!(tokio::time::Instant::now() < until);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let view = c.deadline(&h.ticket).await.unwrap().unwrap();
    assert_eq!(view.status, "completed");
    assert!(view.state.fired_at_ms.unwrap() >= h.ticket.deadline_ms);
    assert_eq!(reconcile(&c).await.available, 1);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn asynchronous_old_timer_cannot_release_a_new_generation_and_confirmation_wins_before_deadline()
 {
    let root = tempfile::tempdir().unwrap();
    let node = start(store(), root.path()).await;
    let c = client(&node, "generations").await;
    initialize(&c, 1).await;
    let old = hold(&c, id(1), 1, now_ms().unwrap() + 800).await;
    c.change(
        new_identity().unwrap(),
        Action::Cancel {
            id: id(1),
            generation: 1,
            buyer: buyer("alice"),
        },
    )
    .await
    .unwrap();
    let newer = hold(&c, id(2), 1, now_ms().unwrap() + 60000).await;
    let (sender, mut progress) = tokio::sync::mpsc::channel(8);
    spawn_deadlines(
        &node,
        node.application_handle::<Reservations>(TENANT).unwrap(),
        key("generations"),
        DeliveryOptions {
            progress: Some(sender),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let checkpoint = tokio::time::timeout(Duration::from_secs(20), progress.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.ticket, old.ticket);
    assert_eq!(checkpoint.outcome, ExpirationOutcome::Unchanged);
    assert_eq!(c.hold(id(2), None).await.unwrap().output.unwrap(), newer);
    c.change(
        new_identity().unwrap(),
        Action::Confirm {
            id: id(2),
            generation: 2,
            buyer: buyer("alice"),
        },
    )
    .await
    .unwrap();
    let native = node.application_handle::<Reservations>(TENANT).unwrap();
    let (confirm, expire) = tokio::join!(
        c.change(
            new_identity().unwrap(),
            Action::Confirm {
                id: id(2),
                generation: 2,
                buyer: buyer("alice")
            }
        ),
        native.command::<ExpireHold>(
            c.target(),
            new_identity().unwrap(),
            Expiration {
                ticket: newer.ticket,
                fired_at_ms: now_ms().unwrap() + 60000
            }
        )
    );
    assert_eq!(confirm.unwrap().output.decision, Decision::AlreadyConfirmed);
    assert_eq!(expire.unwrap().output, ExpirationOutcome::Unchanged);
    assert_eq!(reconcile(&c).await.confirmed, 1);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn confirmation_racing_conditional_expiration_has_exactly_one_terminal_allocation() {
    let root = tempfile::tempdir().unwrap();
    let node = start(store(), root.path()).await;
    let c = client(&node, "terminal").await;
    initialize(&c, 1).await;
    let h = hold(&c, id(1), 1, now_ms().unwrap() + 60000).await;
    // The internal expiration capability carries a trusted Workflow logical
    // deadline. Racing its receiver exercises the same serialized transitions.
    let native = node.application_handle::<Reservations>(TENANT).unwrap();
    let (confirm, expire) = tokio::join!(
        c.change(
            new_identity().unwrap(),
            Action::Confirm {
                id: id(1),
                generation: 1,
                buyer: buyer("alice")
            }
        ),
        native.command::<ExpireHold>(
            c.target(),
            new_identity().unwrap(),
            Expiration {
                ticket: h.ticket.clone(),
                fired_at_ms: h.ticket.deadline_ms
            }
        )
    );
    let expire = expire.unwrap().output;
    match confirm {
        Ok(v) => {
            assert_eq!(v.output.decision, Decision::Confirmed);
            assert_eq!(expire, ExpirationOutcome::Unchanged);
        }
        Err(InvocationError::Rejected(v)) => {
            assert_eq!(v.output.decision, Decision::Expired);
            assert_eq!(expire, ExpirationOutcome::Expired);
        }
        e => panic!("unexpected race: {e:?}"),
    };
    let inventory = reconcile(&c).await;
    assert_eq!(inventory.held, 0);
    assert_eq!(inventory.available + inventory.confirmed, 1);
    assert_eq!(inventory.revision, 3);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn cold_successor_replays_acknowledged_hold_and_installs_its_pending_overdue_deadline() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let storage = store();
    let first = start(storage.clone(), a.path()).await;
    let c = client(&first, "recovered").await;
    initialize(&c, 1).await;
    let identity = new_identity().unwrap();
    let action = Action::Hold {
        id: id(1),
        seat: 1,
        buyer: buyer("alice"),
        deadline_ms: now_ms().unwrap() + 700,
    };
    let prepared = c.prepare(identity, action.clone()).await.unwrap();
    let evidence = prepared.evidence().clone();
    let committed = prepared.execute().await.unwrap();
    first.shutdown().await.unwrap();
    assert_eq!(std::fs::read_dir(a.path()).unwrap().count(), 0);
    tokio::time::sleep(Duration::from_millis(800)).await;
    let second = start(storage, b.path()).await;
    let restored = client(&second, "recovered").await;
    assert_eq!(restored.target(), c.target());
    assert_eq!(restored.change(identity, action).await.unwrap(), committed);
    assert!(matches!(
        restored.resolve(&evidence).await.unwrap(),
        Resolution::Committed(_)
    ));
    spawn_deadlines(
        &second,
        second.application_handle::<Reservations>(TENANT).unwrap(),
        key("recovered"),
        Default::default(),
    )
    .await
    .unwrap();
    wait_hold(&second, &restored, id(1), HoldState::Expired).await;
    assert_eq!(reconcile(&restored).await.available, 1);
    assert!(c.inventory(None).await.is_err());
    second.shutdown().await.unwrap();
}
#[tokio::test]
async fn validation_target_scope_buyer_generation_and_keyset_pages_are_enforced() {
    let root = tempfile::tempdir().unwrap();
    let node = start(store(), root.path()).await;
    let c = client(&node, "pages").await;
    let native = node.application_handle::<Reservations>(TENANT).unwrap();
    for action in [
        Action::Initialize { seats: 0 },
        Action::Initialize { seats: 101 },
        Action::Hold {
            id: id(1),
            seat: 0,
            buyer: buyer("alice"),
            deadline_ms: 1,
        },
    ] {
        assert!(
            c.prepare(new_identity().unwrap(), action.clone())
                .await
                .is_err()
        );
        assert!(
            matches!(native.command::<ChangeReservation>(c.target(),new_identity().unwrap(),Change{event:key("pages"),action}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Invalid)
        );
    }
    assert!(
        matches!(native.command::<ChangeReservation>(c.target(),new_identity().unwrap(),Change{event:key("foreign"),action:Action::Initialize{seats:2}}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Invalid)
    );
    initialize(&c, 3).await;
    for (n, seat) in [(9, 1), (3, 2), (7, 3)] {
        hold(&c, id(n), seat, now_ms().unwrap() + 60000).await;
    }
    assert!(
        matches!(c.change(new_identity().unwrap(),Action::Confirm{id:id(9),generation:2,buyer:buyer("alice")}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Conflict)
    );
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
            .holds
            .iter()
            .map(|v| v.ticket.id)
            .collect::<Vec<_>>(),
        [id(3), id(7)]
    );
    assert_eq!(first.output.next, Some(id(7)));
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
    assert_eq!(second.output.holds[0].ticket.id, id(9));
    assert!(second.output.next.is_none());
    for limit in [0, 101, u32::MAX] {
        assert!(
            c.list(PageRequest { after: None, limit }, None)
                .await
                .is_err()
        );
    }
    let other = client(&node, "other").await;
    assert!(other.inventory(Some(first.receipt)).await.is_err());
    let evidence = c
        .prepare(
            new_identity().unwrap(),
            Action::Cancel {
                id: id(9),
                generation: 1,
                buyer: buyer("alice"),
            },
        )
        .await
        .unwrap();
    assert!(other.resolve(evidence.evidence()).await.is_err());
    assert!(other.deadline(&first.output.holds[0].ticket).await.is_err());
    let ticket = first.output.holds[0].ticket.clone();
    let mut different = ticket.clone();
    different.event = key("other");
    assert_ne!(different.workflow_id(), ticket.workflow_id());
    assert!(
        matches!(native.command::<ExpireHold>(c.target(),new_identity().unwrap(),Expiration{ticket:ticket.clone(),fired_at_ms:ticket.deadline_ms-1}).await,Err(InvocationError::Rejected(v)) if v.output==ExpirationOutcome::Invalid)
    );
    assert_eq!(reconcile(&c).await.held, 3);
    node.shutdown().await.unwrap();
}
#[test]
fn canonical_keys_stable_routing_and_wire_tags_are_compatibility_contracts() {
    for invalid in ["", "UPPER", " a", "a/b", "-a", "a-", "é"] {
        assert!(EventKey::new(invalid).is_err());
        assert!(BuyerKey::new(invalid).is_err());
    }
    assert!(EventKey::new("x".repeat(65)).is_err());
    assert!(HoldId::from_bytes([0; 16]).is_err());
    let action = Action::Confirm {
        id: id(1),
        generation: 2,
        buyer: buyer("alice"),
    };
    let mut encoder = BoundedEncoder::new(128).unwrap();
    action.encode(&mut encoder).unwrap();
    let bytes = encoder.finish();
    let mut expected = vec![3, 0, 0, 0, 16];
    expected.extend(1_u128.to_be_bytes());
    expected.extend(2_i64.to_be_bytes());
    expected.extend([0, 0, 0, 5]);
    expected.extend(b"alice");
    assert_eq!(bytes, expected);
    let mut decoder = BoundedDecoder::new(&bytes, 128).unwrap();
    assert_eq!(Action::decode(&mut decoder).unwrap(), action);
    decoder.finish().unwrap();
    let mut decoder = BoundedDecoder::new(&[255], 128).unwrap();
    assert!(Decision::decode(&mut decoder).is_err());
    let mut decoder = BoundedDecoder::new(&[0, 0, 0, 0, 101], 128).unwrap();
    assert!(Page::decode(&mut decoder).is_err());
}
#[tokio::test]
async fn real_native_timer_races_confirmation_under_documented_deadline_precedence() {
    let root = tempfile::tempdir().unwrap();
    let node = start(store(), root.path()).await;
    let c = client(&node, "logical-time").await;
    initialize(&c, 3).await;
    let (sender, mut progress) = tokio::sync::mpsc::channel(8);
    spawn_deadlines(
        &node,
        node.application_handle::<Reservations>(TENANT).unwrap(),
        key("logical-time"),
        DeliveryOptions {
            progress: Some(sender),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let deadline = now_ms().unwrap() + 1800;
    for n in 1..=3 {
        hold(&c, id(n), n as u32, deadline).await;
    }
    let confirm = |n: u128, bias: i64| {
        let client = c.clone();
        async move {
            let wait = (deadline + bias - now_ms().unwrap()).max(0) as u64;
            tokio::time::sleep(Duration::from_millis(wait)).await;
            client
                .change(
                    new_identity().unwrap(),
                    Action::Confirm {
                        id: id(n),
                        generation: 1,
                        buyer: buyer("alice"),
                    },
                )
                .await
        }
    };
    let (early, racing, late) = tokio::join!(confirm(1, -300), confirm(2, 0), confirm(3, 300));
    assert_eq!(early.unwrap().output.decision, Decision::Confirmed);
    assert!(
        matches!(racing,Ok(ref v) if v.output.decision==Decision::Confirmed)
            || matches!(racing,Err(InvocationError::Rejected(ref v)) if v.output.decision==Decision::Expired)
    );
    assert!(
        matches!(late,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Expired)
    );
    let mut seen = std::collections::BTreeSet::new();
    while seen.len() < 3 {
        let checkpoint = tokio::time::timeout(Duration::from_secs(20), progress.recv())
            .await
            .unwrap()
            .unwrap();
        seen.insert(checkpoint.ticket.id);
    }
    let inventory = reconcile(&c).await;
    assert_eq!(inventory.held, 0);
    assert!(inventory.confirmed >= 1 && inventory.confirmed <= 2);
    assert_eq!(inventory.revision, 7);
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn permanent_history_capacity_preserves_old_ids_and_existing_settlement() {
    let root = tempfile::tempdir().unwrap();
    let node = start(store(), root.path()).await;
    let c = client(&node, "capacity").await;
    initialize(&c, 1).await;
    let mut last = None;
    for n in 1..=MAX_HOLDS as u128 {
        let h = hold(&c, id(n), 1, now_ms().unwrap() + 60000).await;
        if n < MAX_HOLDS as u128 {
            c.change(
                new_identity().unwrap(),
                Action::Cancel {
                    id: id(n),
                    generation: h.ticket.generation,
                    buyer: buyer("alice"),
                },
            )
            .await
            .unwrap();
        }
        last = Some(h);
    }
    let last = last.unwrap();
    c.change(
        new_identity().unwrap(),
        Action::Cancel {
            id: last.ticket.id,
            generation: last.ticket.generation,
            buyer: buyer("alice"),
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(c.change(new_identity().unwrap(),Action::Hold{id:id(1025),seat:1,buyer:buyer("alice"),deadline_ms:now_ms().unwrap()+60000}).await,Err(InvocationError::Rejected(v)) if v.output.decision==Decision::Capacity)
    );
    assert_eq!(
        c.change(
            new_identity().unwrap(),
            Action::Hold {
                id: last.ticket.id,
                seat: 1,
                buyer: buyer("alice"),
                deadline_ms: last.ticket.deadline_ms
            }
        )
        .await
        .unwrap()
        .output
        .hold
        .unwrap()
        .state,
        HoldState::Cancelled
    );
    assert_eq!(
        c.change(
            new_identity().unwrap(),
            Action::Cancel {
                id: last.ticket.id,
                generation: last.ticket.generation,
                buyer: buyer("alice")
            }
        )
        .await
        .unwrap()
        .output
        .decision,
        Decision::AlreadyCancelled
    );
    let inventory = reconcile(&c).await;
    assert_eq!(
        (inventory.history_count, inventory.available, inventory.held),
        (MAX_HOLDS, 1, 0)
    );
    node.shutdown().await.unwrap();
}
struct TestAuthorizer;
impl cellule_runtime::peer::PeerAuthorizer for TestAuthorizer {
    fn authorize(
        &self,
        _: &cellule_runtime::peer::VerifiedPeerRequest,
    ) -> cellule_runtime::Result<()> {
        Ok(())
    }
}
#[tokio::test]
async fn multi_destination_roster_is_explicit_bounded_unique_and_bound_to_real_cell_handles() {
    let root = tempfile::tempdir().unwrap();
    let node = start(store(), root.path()).await;
    let c = client(&node, "roster").await;
    let application = compile().unwrap();
    let sql = node.open_cell(c.target(), &InventoryCells).await.unwrap();
    let authorizer = Arc::new(TestAuthorizer);
    assert!(
        LocalPeer::for_destinations(application.registry(), vec![], authorizer.clone()).is_err()
    );
    assert!(
        LocalPeer::for_destinations(
            application.registry(),
            vec![(c.target().clone(), sql.clone())],
            authorizer.clone()
        )
        .is_ok()
    );
    assert!(
        LocalPeer::for_destinations(
            application.registry(),
            vec![(c.target().clone(), sql.clone()); 2],
            authorizer.clone()
        )
        .is_err()
    );
    let wrong = CellTarget::new(
        TenantId::from_bytes([0xff; 16]),
        APP,
        INVENTORY,
        c.target().partition(),
    )
    .unwrap();
    assert!(
        LocalPeer::for_destinations(application.registry(), vec![(wrong, sql)], authorizer)
            .is_err()
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn shared_deadline_shards_finish_prior_event_callbacks_through_bounded_drained_receivers() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let storage = store();
    let first = start(storage.clone(), a.path()).await;
    let c = client(&first, "previous-event").await;
    initialize(&c, 3).await;
    spawn_deadlines(
        &first,
        first.application_handle::<Reservations>(TENANT).unwrap(),
        key("previous-event"),
        Default::default(),
    )
    .await
    .unwrap();
    // The first boot must drain with all timers still pending. Give setup a
    // bounded window while this suite also fills history concurrently; callback
    // observation retains its separate twenty-second limit after the deadline.
    let deadline = now_ms().unwrap() + 20_000;
    let mut holds = Vec::new();
    for n in 1..=3 {
        holds.push(hold(&c, id(n), n as u32, deadline).await);
    }
    c.change(
        new_identity().unwrap(),
        Action::Confirm {
            id: id(2),
            generation: 1,
            buyer: buyer("alice"),
        },
    )
    .await
    .unwrap();
    c.change(
        new_identity().unwrap(),
        Action::Cancel {
            id: id(3),
            generation: 1,
            buyer: buyer("alice"),
        },
    )
    .await
    .unwrap();
    // Verify each committed source intent reached its independent Workflow
    // before ending the first process. Future native timers survive its drain.
    let handle = first.application_handle::<Reservations>(TENANT).unwrap();
    let source = handle
        .effects::<InventoryCells>(c.target().clone())
        .unwrap();
    for h in &holds {
        let until = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let id: [u8; 32] = h.start_effect.as_slice().try_into().unwrap();
            if source
                .status(id, None)
                .await
                .unwrap()
                .output
                .is_some_and(|v| v.state == EffectState::Delivered)
            {
                break;
            }
            assert!(tokio::time::Instant::now() < until);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    for hold in &holds {
        let view = c.deadline(&hold.ticket).await.unwrap().unwrap();
        assert_eq!(view.status, "running");
        assert!(view.state.timer_id.is_some());
        assert!(view.state.fired_at_ms.is_none());
    }
    first.shutdown().await.unwrap();
    assert!(
        now_ms().unwrap() < deadline,
        "restart fixture must drain with all three native timers still in the future"
    );
    let wait = (deadline - now_ms().unwrap()).max(0) as u64;
    tokio::time::sleep(Duration::from_millis(wait + 100)).await;
    let second = start(storage, b.path()).await;
    let current = client(&second, "current-event").await;
    initialize(&current, 1).await;
    let (sender, mut progress) = tokio::sync::mpsc::channel(8);
    spawn_deadlines(
        &second,
        second.application_handle::<Reservations>(TENANT).unwrap(),
        key("current-event"),
        DeliveryOptions {
            drop_reply_once: true,
            progress: Some(sender),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut outcomes = std::collections::BTreeMap::new();
    while outcomes.len() < 3 {
        let checkpoint = tokio::time::timeout(Duration::from_secs(20), progress.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.ticket.event, key("previous-event"));
        outcomes.insert(checkpoint.ticket.id, checkpoint.outcome);
    }
    assert_eq!(outcomes.get(&id(1)), Some(&ExpirationOutcome::Expired));
    assert_eq!(outcomes.get(&id(2)), Some(&ExpirationOutcome::Unchanged));
    assert_eq!(outcomes.get(&id(3)), Some(&ExpirationOutcome::Unchanged));
    // Receiver shutdown is part of each accepted transport call, before its
    // checkpoint. No child SQLite handle or boot directory remains active.
    let receivers = std::fs::read_dir(b.path())
        .unwrap()
        .map(|entry| entry.unwrap().path().join("receivers"))
        .find(|path| path.is_dir())
        .unwrap();
    assert_eq!(std::fs::read_dir(receivers).unwrap().count(), 0);
    let previous = client(&second, "previous-event").await;
    assert_eq!(reconcile(&previous).await.available, 2);
    assert_eq!(reconcile(&current).await.available, 1);
    assert!(second.is_ready());
    second.shutdown().await.unwrap();
    assert_eq!(std::fs::read_dir(b.path()).unwrap().count(), 0);
}
