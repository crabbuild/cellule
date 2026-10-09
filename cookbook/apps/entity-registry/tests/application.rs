//! Public API journeys across real node assembly and native Effect delivery.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_app::ApplicationHandle;
use cellule_cookbook_entity_registry::*;
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity};
use cellule_runtime::{
    ApplicationId, InvocationError, Resolution, TenantId,
    codec::{BoundedDecoder, BoundedEncoder, WireValue},
    control::authority::CellAuthority,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{sync::Arc, time::Duration};
const APP: ApplicationId = ApplicationId::from_bytes([0xc1; 16]);
const TENANT: TenantId = TenantId::from_bytes([0xc2; 16]);
const PREFIX: &str = "entity-test";
async fn start(store: Store, root: &std::path::Path) -> LocalNode {
    LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: root.into(),
            storage_prefix: Path::from(PREFIX),
            application_id: APP,
        },
    )
    .await
    .unwrap()
}
fn key(v: &str) -> DeviceKey {
    DeviceKey::new(v).unwrap()
}
fn attributes(name: &str) -> Attributes {
    Attributes {
        name: name.into(),
        location: "Lab".into(),
        enabled: true,
    }
}
fn change(v: &str, revision: i64, name: &str) -> Change {
    Change {
        key: key(v),
        expected_revision: revision,
        attributes: attributes(name),
    }
}
fn handle(node: &LocalNode) -> ApplicationHandle<EntityRegistry> {
    node.application_handle(TENANT).unwrap()
}
async fn source(node: &LocalNode, v: &str) -> DeviceClient {
    let c = DeviceClient::new(handle(node), key(v)).unwrap();
    node.open_cell(c.target(), &Devices).await.unwrap();
    c
}
async fn open_directory(node: &LocalNode) -> DirectoryClient {
    let c = DirectoryClient::new(handle(node)).unwrap();
    node.open_cell(c.target(), &Directory).await.unwrap();
    c
}
fn published(outcome: ChangeOutcome) -> PublishedDevice {
    match outcome {
        ChangeOutcome::Applied(v) => v,
        _ => panic!("expected applied"),
    }
}
async fn settled(node: &LocalNode, client: &DeviceClient) -> Progress {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            assert!(node.is_ready());
            let v = client.progress(None).await.unwrap().output.unwrap();
            if v.state == ProjectionState::Delivered {
                return v;
            }
            assert_ne!(v.state, ProjectionState::Failed);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap()
}
async fn authority(store: Store, client: &DeviceClient) -> cellule_runtime::control::Control {
    CellAuthority::new(cellule_ltx::CellStorageLayout::new(
        store,
        Path::from(PREFIX),
        *APP.as_bytes(),
    ))
    .load(client.target().cell_id())
    .await
    .unwrap()
    .unwrap()
    .value()
    .clone()
}
#[tokio::test]
async fn atomic_intents_are_visible_pending_until_signed_delivery_settles() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let client = source(&node, "sensor-a").await;
    let directory = open_directory(&node).await;
    let identity = new_identity().unwrap();
    let input = change("sensor-a", 0, "Sensor");
    let first = client.change(identity, input.clone()).await.unwrap();
    assert_eq!(client.change(identity, input).await.unwrap(), first);
    assert_eq!(published(first.output.clone()).device.revision, 1);
    let progress = client
        .progress(Some(first.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(progress.state, ProjectionState::Pending);
    assert_eq!(progress.attempts, Some(0));
    assert!(
        directory
            .lookup(key("sensor-a"), None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    let second = client
        .change(
            new_identity().unwrap(),
            change("sensor-a", 1, "Renamed sensor"),
        )
        .await
        .unwrap();
    assert_ne!(
        published(second.output.clone()).effect_id,
        published(first.output).effect_id
    );
    spawn_delivery(
        &node,
        &node,
        handle(&node),
        handle(&node),
        &[key("sensor-a")],
        DeliveryOptions {
            drop_reply_once: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let progress = settled(&node, &client).await;
    assert_eq!(progress.published.device.revision, 2);
    let projected = directory
        .lookup(key("sensor-a"), None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(projected, progress.published.device);
    node.shutdown().await.unwrap();
    assert!(!node.is_ready());
}
#[tokio::test]
async fn competing_edits_have_one_durable_winner_and_no_losing_intent() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let client = source(&node, "contended").await;
    client
        .change(new_identity().unwrap(), change("contended", 0, "Original"))
        .await
        .unwrap();
    let left = new_identity().unwrap();
    let right = new_identity().unwrap();
    let (a, b) = tokio::join!(
        client.change(left, change("contended", 1, "Alice")),
        client.change(right, change("contended", 1, "Bob"))
    );
    let (id, input, loser) = match (a, b) {
        (Ok(_), Err(InvocationError::Rejected(v))) => (right, change("contended", 1, "Bob"), v),
        (Err(InvocationError::Rejected(v)), Ok(_)) => (left, change("contended", 1, "Alice"), v),
        other => panic!("unexpected decisions {other:?}"),
    };
    assert_eq!(loser.output, ChangeOutcome::Conflict);
    let prepared = client.prepare(id, input).await.unwrap();
    let evidence = prepared.evidence().clone();
    let Err(InvocationError::Rejected(replayed)) = prepared.execute().await else {
        panic!("expected durable conflict")
    };
    assert_eq!(loser, replayed);
    assert!(matches!(
        client.resolve(&evidence).await.unwrap(),
        Resolution::Committed(_)
    ));
    assert_eq!(
        client
            .get(None)
            .await
            .unwrap()
            .output
            .unwrap()
            .device
            .revision,
        2
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn source_moves_between_nodes_while_directory_keeps_its_owner() {
    let roots = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let store = Store::new(Arc::new(InMemory::new()));
    let first = start(store.clone(), roots[0].path()).await;
    let destination = start(store.clone(), roots[1].path()).await;
    let old = source(&first, "moving").await;
    let directory = open_directory(&destination).await;
    let committed = old
        .change(new_identity().unwrap(), change("moving", 0, "First owner"))
        .await
        .unwrap();
    let owner1 = authority(store.clone(), &old).await;
    let dir_authority = CellAuthority::new(cellule_ltx::CellStorageLayout::new(
        store.clone(),
        Path::from(PREFIX),
        *APP.as_bytes(),
    ));
    let dir_before = dir_authority
        .load(directory.target().cell_id())
        .await
        .unwrap()
        .unwrap()
        .value()
        .owner
        .clone();
    let next = start(store.clone(), roots[2].path()).await;
    assert!(
        next.open_cell(old.target(), &Devices).await.is_err(),
        "live owner must not be stolen"
    );
    first.shutdown().await.unwrap();
    let restored = source(&next, "moving").await;
    assert_eq!(restored.target(), old.target());
    let owner2 = authority(store.clone(), &restored).await;
    assert_ne!(owner1.owner, owner2.owner);
    assert!(owner2.epoch > owner1.epoch);
    assert_eq!(owner1.incarnation, owner2.incarnation);
    assert_eq!(
        restored
            .get(Some(committed.receipt))
            .await
            .unwrap()
            .output
            .unwrap(),
        published(committed.output)
    );
    assert_eq!(
        restored.progress(None).await.unwrap().output.unwrap().state,
        ProjectionState::Pending
    );
    assert!(
        old.change(new_identity().unwrap(), change("moving", 1, "Old writer"))
            .await
            .is_err()
    );
    spawn_delivery(
        &next,
        &destination,
        handle(&next),
        handle(&destination),
        &[key("moving")],
        Default::default(),
    )
    .await
    .unwrap();
    let progress = settled(&next, &restored).await;
    assert_eq!(
        directory.lookup(key("moving"), None).await.unwrap().output,
        Some(progress.published.device)
    );
    assert_eq!(
        dir_authority
            .load(directory.target().cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .owner,
        dir_before
    );
    next.shutdown().await.unwrap();
    destination.shutdown().await.unwrap();
}
#[tokio::test]
async fn monotonic_receiver_accepts_stale_and_duplicate_state_but_rejects_revision_collision() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let directory = open_directory(&node).await;
    let h = handle(&node);
    let device = Device {
        key: key("ordered"),
        attributes: attributes("Latest"),
        revision: 3,
    };
    let first = h
        .command::<ProjectDevice>(directory.target(), new_identity().unwrap(), device.clone())
        .await
        .unwrap();
    assert_eq!(first.output, ProjectionOutcome::Applied);
    assert_eq!(
        h.command::<ProjectDevice>(directory.target(), new_identity().unwrap(), device.clone())
            .await
            .unwrap()
            .output,
        ProjectionOutcome::Unchanged
    );
    let stale = Device {
        revision: 1,
        attributes: attributes("Old"),
        ..device.clone()
    };
    assert_eq!(
        h.command::<ProjectDevice>(directory.target(), new_identity().unwrap(), stale)
            .await
            .unwrap()
            .output,
        ProjectionOutcome::Stale
    );
    let colliding = Device {
        attributes: attributes("Contradiction"),
        ..device.clone()
    };
    let id = new_identity().unwrap();
    let Err(InvocationError::Rejected(rejected)) = h
        .command::<ProjectDevice>(directory.target(), id, colliding.clone())
        .await
    else {
        panic!("revision collision must reject")
    };
    assert_eq!(rejected.output, ProjectionOutcome::Conflict);
    let Err(InvocationError::Rejected(replayed)) = h
        .command::<ProjectDevice>(directory.target(), id, colliding)
        .await
    else {
        panic!("collision replay must reject")
    };
    assert_eq!(replayed, rejected);
    assert_eq!(
        directory
            .lookup(device.key.clone(), Some(rejected.receipt))
            .await
            .unwrap()
            .output,
        Some(device)
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn source_scope_and_directory_receipts_cannot_be_substituted() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let a = source(&node, "source-a").await;
    let b = source(&node, "source-b").await;
    let directory = open_directory(&node).await;
    let committed = a
        .change(new_identity().unwrap(), change("source-a", 0, "A"))
        .await
        .unwrap();
    assert!(b.get(Some(committed.receipt)).await.is_err());
    assert!(
        directory
            .lookup(key("source-a"), Some(committed.receipt))
            .await
            .is_err()
    );
    assert!(
        a.prepare(new_identity().unwrap(), change("source-b", 0, "Wrong Cell"))
            .await
            .is_err()
    );
    let prepared = a
        .prepare(new_identity().unwrap(), change("source-a", 1, "Next"))
        .await
        .unwrap();
    assert!(b.resolve(prepared.evidence()).await.is_err());
    // Bypass the domain client to verify the server's target/key binding, too.
    let id = new_identity().unwrap();
    let wrong = change("source-b", 0, "Wrong Cell");
    let Err(InvocationError::Rejected(rejected)) = handle(&node)
        .command::<ChangeDevice>(a.target(), id, wrong.clone())
        .await
    else {
        panic!("wrong source key must reject")
    };
    assert_eq!(rejected.output, ChangeOutcome::Invalid);
    let Err(InvocationError::Rejected(replayed)) = handle(&node)
        .command::<ChangeDevice>(a.target(), id, wrong)
        .await
    else {
        panic!("wrong source key replay must reject")
    };
    assert_eq!(rejected, replayed);
    assert_eq!(
        a.get(None).await.unwrap().output.unwrap().device.revision,
        1
    );
    assert!(b.get(None).await.unwrap().output.is_none());
    let absent = b
        .change(new_identity().unwrap(), change("source-b", 1, "Absent"))
        .await;
    assert!(
        matches!(absent,Err(InvocationError::Rejected(v)) if v.output==ChangeOutcome::NotFound)
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn directory_pages_are_bounded_key_ordered_and_tenant_isolated() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let directory = open_directory(&node).await;
    for k in ["z", "a", "m"] {
        handle(&node)
            .command::<ProjectDevice>(
                directory.target(),
                new_identity().unwrap(),
                Device {
                    key: key(k),
                    attributes: attributes(k),
                    revision: 1,
                },
            )
            .await
            .unwrap();
    }
    let first = directory
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
            .devices
            .iter()
            .map(|v| v.key.as_str())
            .collect::<Vec<_>>(),
        ["a", "m"]
    );
    assert_eq!(first.output.next, Some(key("m")));
    let second = directory
        .list(
            PageRequest {
                after: first.output.next,
                limit: 2,
            },
            Some(first.receipt),
        )
        .await
        .unwrap();
    assert_eq!(second.output.devices[0].key, key("z"));
    assert!(second.output.next.is_none());
    for n in [0, 101, u32::MAX] {
        assert!(
            directory
                .list(
                    PageRequest {
                        after: None,
                        limit: n
                    },
                    None
                )
                .await
                .is_err()
        );
    }
    let other = DirectoryClient::new(
        node.application_handle::<EntityRegistry>(TenantId::from_bytes([0xc3; 16]))
            .unwrap(),
    )
    .unwrap();
    node.open_cell(other.target(), &Directory).await.unwrap();
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
            .devices
            .is_empty()
    );
    assert!(other.lookup(key("a"), Some(first.receipt)).await.is_err());
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn cold_restore_retains_source_intent_and_directory_state_without_working_files() {
    let root = tempfile::tempdir().unwrap();
    let restored_root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let first = start(store.clone(), root.path()).await;
    let client = source(&first, "cold").await;
    let directory = open_directory(&first).await;
    let identity = new_identity().unwrap();
    let input = change("cold", 0, "Cold restore");
    let committed = client.change(identity, input.clone()).await.unwrap();
    first.shutdown().await.unwrap();
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    drop(directory);
    drop(client);
    drop(first);
    let next = start(store, restored_root.path()).await;
    let client = source(&next, "cold").await;
    let directory = open_directory(&next).await;
    assert_eq!(client.change(identity, input).await.unwrap(), committed);
    assert_eq!(
        client.progress(None).await.unwrap().output.unwrap().state,
        ProjectionState::Pending
    );
    spawn_delivery(
        &next,
        &next,
        handle(&next),
        handle(&next),
        &[key("cold")],
        Default::default(),
    )
    .await
    .unwrap();
    settled(&next, &client).await;
    assert_eq!(
        directory
            .lookup(key("cold"), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .revision,
        1
    );
    next.shutdown().await.unwrap();
}
#[tokio::test]
async fn terminal_projection_failure_closes_readiness_and_keeps_failure_evidence() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let node = start(store.clone(), root.path()).await;
    let source = source(&node, "bad-projection").await;
    let directory = open_directory(&node).await;
    source
        .change(
            new_identity().unwrap(),
            change("bad-projection", 0, "Source"),
        )
        .await
        .unwrap();
    handle(&node)
        .command::<ProjectDevice>(
            directory.target(),
            new_identity().unwrap(),
            Device {
                key: key("bad-projection"),
                attributes: attributes("Contradicting revision"),
                revision: 1,
            },
        )
        .await
        .unwrap();
    spawn_delivery(
        &node,
        &node,
        handle(&node),
        handle(&node),
        &[key("bad-projection")],
        Default::default(),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while node.is_ready() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let err = node.shutdown().await.unwrap_err();
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&err);
    let mut retained_cause = false;
    while let Some(error) = cause {
        retained_cause |= error
            .to_string()
            .contains("directory rejected the projection");
        cause = error.source();
    }
    assert!(retained_cause, "missing originating failure: {err:?}");
    let successor_root = tempfile::tempdir().unwrap();
    let successor = start(store, successor_root.path()).await;
    let restored = source_for_failure(&successor).await;
    assert_eq!(
        restored.progress(None).await.unwrap().output.unwrap().state,
        ProjectionState::Failed
    );
    assert_eq!(
        restored
            .get(None)
            .await
            .unwrap()
            .output
            .unwrap()
            .device
            .attributes
            .name,
        "Source"
    );
    assert!(
        spawn_delivery(
            &successor,
            &successor,
            handle(&successor),
            handle(&successor),
            &[key("bad-projection")],
            Default::default()
        )
        .await
        .is_err()
    );
    restored
        .change(
            new_identity().unwrap(),
            change("bad-projection", 1, "Repaired projection"),
        )
        .await
        .unwrap();
    spawn_delivery(
        &successor,
        &successor,
        handle(&successor),
        handle(&successor),
        &[key("bad-projection")],
        Default::default(),
    )
    .await
    .unwrap();
    let repaired = settled(&successor, &restored).await;
    assert_eq!(repaired.published.device.revision, 2);
    successor.shutdown().await.unwrap();
}
async fn source_for_failure(node: &LocalNode) -> DeviceClient {
    source(node, "bad-projection").await
}
#[test]
fn canonical_keys_wire_fixture_and_collection_bounds_are_contracts() {
    for invalid in ["", "UPPER", " padded", "a/b", "-start", "end-", "é"] {
        assert!(DeviceKey::new(invalid).is_err());
    }
    assert!(DeviceKey::new("a".repeat(65)).is_err());
    let input = change("a", 0, "A");
    let mut e = BoundedEncoder::new(1024).unwrap();
    input.encode(&mut e).unwrap();
    let bytes = e.finish();
    assert_eq!(
        bytes,
        [
            0, 0, 0, 1, b'a', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, b'A', 0, 0, 0, 3, b'L', b'a',
            b'b', 1
        ]
    );
    let mut d = BoundedDecoder::new(&bytes, 1024).unwrap();
    assert_eq!(Change::decode(&mut d).unwrap(), input);
    d.finish().unwrap();
    let mut d = BoundedDecoder::new(&[255], 16).unwrap();
    assert!(ProjectionOutcome::decode(&mut d).is_err());
    let too_many = 101_u32.to_be_bytes();
    let mut d = BoundedDecoder::new(&too_many, 64).unwrap();
    assert!(Page::decode(&mut d).is_err());
}

#[tokio::test]
async fn directory_capacity_is_atomic_under_competition_and_allows_existing_updates() {
    let root = tempfile::tempdir().unwrap();
    let node = start(Store::new(Arc::new(InMemory::new())), root.path()).await;
    let directory = open_directory(&node).await;
    let h = handle(&node);
    for index in 0..1023 {
        h.command::<ProjectDevice>(
            directory.target(),
            new_identity().unwrap(),
            Device {
                key: key(&format!("device-{index:04}")),
                attributes: attributes("Registered"),
                revision: 1,
            },
        )
        .await
        .unwrap();
    }
    let inputs = [
        Device {
            key: key("last-a"),
            attributes: attributes("Last A"),
            revision: 1,
        },
        Device {
            key: key("last-b"),
            attributes: attributes("Last B"),
            revision: 1,
        },
    ];
    let identities = [new_identity().unwrap(), new_identity().unwrap()];
    let (a, b) = tokio::join!(
        h.command::<ProjectDevice>(directory.target(), identities[0], inputs[0].clone()),
        h.command::<ProjectDevice>(directory.target(), identities[1], inputs[1].clone())
    );
    let (losing_index, rejected) = match (a, b) {
        (Ok(winner), Err(InvocationError::Rejected(loser)))
            if winner.output == ProjectionOutcome::Applied =>
        {
            (1, loser)
        }
        (Err(InvocationError::Rejected(loser)), Ok(winner))
            if winner.output == ProjectionOutcome::Applied =>
        {
            (0, loser)
        }
        other => panic!("capacity admitted the wrong decisions: {other:?}"),
    };
    assert_eq!(rejected.output, ProjectionOutcome::Capacity);
    let Err(InvocationError::Rejected(replayed)) = h
        .command::<ProjectDevice>(
            directory.target(),
            identities[losing_index],
            inputs[losing_index].clone(),
        )
        .await
    else {
        panic!("capacity rejection must replay")
    };
    assert_eq!(replayed, rejected);
    h.command::<ProjectDevice>(
        directory.target(),
        new_identity().unwrap(),
        Device {
            key: key("device-0000"),
            attributes: attributes("Updated at capacity"),
            revision: 2,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        directory
            .lookup(key("device-0000"), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .revision,
        2
    );
    assert!(
        directory
            .lookup(inputs[losing_index].key.clone(), None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    node.shutdown().await.unwrap();
}
