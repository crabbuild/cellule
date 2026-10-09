//! Public conditional-edit, fixed-shard, expiry, and restart behavior.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use cellule_cookbook_settings::{
    Edit, Expected, Organization, Preference, Preferences, Settings, SettingsClient, compile,
};
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity, now_ms};
use cellule_runtime::{
    ApplicationId, InvocationError, Resolution, TenantId, primitives::kv::KvAtomicOutcome,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{sync::Arc, time::Duration};

async fn start(store: Store, root: &std::path::Path) -> LocalNode {
    LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: root.into(),
            storage_prefix: Path::from("test-settings"),
            application_id: ApplicationId::from_bytes([0x52; 16]),
        },
    )
    .await
    .unwrap()
}
async fn organization(node: &LocalNode, name: &str) -> SettingsClient {
    let client = SettingsClient::new(
        node.application_handle::<Settings>(TenantId::from_bytes([0x62; 16]))
            .unwrap(),
        &Organization::new(name).unwrap(),
    )
    .unwrap();
    node.open_cell(client.target(), &Preferences).await.unwrap();
    client
}
fn put(key: &str, expected: Expected, value: &str) -> Edit {
    Edit {
        key: key.into(),
        expected,
        value: Some(Preference::Text(value.into())),
        expires_at_ms: None,
    }
}
fn fresh_store() -> Store {
    Store::new(Arc::new(InMemory::new()))
}

#[tokio::test]
async fn competing_editors_replay_one_durable_conflict_and_aba_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let node = start(fresh_store(), root.path()).await;
    let client = organization(&node, "acme").await;
    let committed = client
        .edit(
            new_identity().unwrap(),
            vec![put("theme", Expected::Absent, "dark")],
        )
        .await
        .unwrap();
    let observed = client
        .get("theme", Some(committed.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    let first_id = new_identity().unwrap();
    let second_id = new_identity().unwrap();
    let first = vec![put("theme", Expected::Version(observed.version), "light")];
    let second = vec![put("theme", Expected::Version(observed.version), "system")];
    let (first_result, second_result) = tokio::join!(
        client.edit(first_id, first.clone()),
        client.edit(second_id, second.clone())
    );
    let (loser_id, loser, rejected) = match (first_result, second_result) {
        (Ok(_), Err(InvocationError::Rejected(rejected))) => (second_id, second, rejected),
        (Err(InvocationError::Rejected(rejected)), Ok(_)) => (first_id, first, rejected),
        other => panic!("one editor must win: {other:?}"),
    };
    assert!(matches!(
        rejected.output,
        KvAtomicOutcome::PreconditionFailed { .. }
    ));
    assert!(
        matches!(client.edit(loser_id, loser).await, Err(InvocationError::Rejected(replay)) if replay == rejected)
    );
    let current = client.get("theme", None).await.unwrap().output.unwrap();
    let delete_id = new_identity().unwrap();
    let delete = vec![Edit {
        value: None,
        ..put("theme", Expected::Version(current.version), "ignored")
    }];
    let removed = client.edit(delete_id, delete.clone()).await.unwrap();
    client
        .edit(
            new_identity().unwrap(),
            vec![put("theme", Expected::Absent, "recreated")],
        )
        .await
        .unwrap();
    // Replaying a deletion must not remove the newly created setting.
    assert_eq!(client.edit(delete_id, delete).await.unwrap(), removed);
    assert!(matches!(
        client
            .edit(
                new_identity().unwrap(),
                vec![put("theme", Expected::Version(current.version), "stale")]
            )
            .await,
        Err(InvocationError::Rejected(_))
    ));
    let recreated = client.get("theme", None).await.unwrap().output.unwrap();
    assert_ne!(recreated.version, current.version);
    assert_eq!(recreated.value, Preference::Text("recreated".into()));
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_bundle_leaves_every_member_unchanged_and_resolves_rejection() {
    let root = tempfile::tempdir().unwrap();
    let node = start(fresh_store(), root.path()).await;
    let client = organization(&node, "bundles").await;
    client
        .edit(
            new_identity().unwrap(),
            vec![put("theme", Expected::Absent, "dark")],
        )
        .await
        .unwrap();
    let prepared = client
        .prepare(
            new_identity().unwrap(),
            vec![
                put("feature.beta", Expected::Absent, "enabled"),
                put("theme", Expected::Absent, "light"),
            ],
        )
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    assert!(matches!(
        prepared.execute().await,
        Err(InvocationError::Rejected(_))
    ));
    assert!(matches!(
        client.resolve(&evidence).await.unwrap(),
        Resolution::Committed(_)
    ));
    assert!(
        client
            .get("feature.beta", None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    assert_eq!(
        client
            .get("theme", None)
            .await
            .unwrap()
            .output
            .unwrap()
            .value,
        Preference::Text("dark".into())
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn organizations_on_the_same_shard_have_separate_scopes_and_bounded_pages() {
    let root = tempfile::tempdir().unwrap();
    let node = start(fresh_store(), root.path()).await;
    let first = organization(&node, "org-0").await;
    let handle = node
        .application_handle::<Settings>(TenantId::from_bytes([0x62; 16]))
        .unwrap();
    let second = (1..100)
        .map(|id| {
            SettingsClient::new(
                handle.clone(),
                &Organization::new(format!("org-{id}")).unwrap(),
            )
            .unwrap()
        })
        .find(|client| client.target() == first.target())
        .unwrap();
    node.open_cell(second.target(), &Preferences).await.unwrap();
    first
        .edit(
            new_identity().unwrap(),
            ["feature.a", "feature.b", "feature.c", "theme"]
                .into_iter()
                .map(|key| put(key, Expected::Absent, "on"))
                .collect(),
        )
        .await
        .unwrap();
    assert!(second.get("theme", None).await.unwrap().output.is_none());
    assert!(
        second
            .list("", None, 20, None)
            .await
            .unwrap()
            .output
            .settings
            .is_empty()
    );
    let page = first.list("feature.", None, 2, None).await.unwrap().output;
    assert_eq!(
        page.settings
            .iter()
            .map(|setting| setting.key.as_str())
            .collect::<Vec<_>>(),
        ["feature.a", "feature.b"]
    );
    let next = first
        .list("feature.", page.next.as_deref(), 2, None)
        .await
        .unwrap()
        .output;
    assert_eq!(next.settings.len(), 1);
    assert_eq!(next.settings[0].key, "feature.c");
    assert!(next.next.is_none());
    assert!(first.list("", None, 101, None).await.is_err());
    assert!(
        first
            .edit(
                new_identity().unwrap(),
                vec![
                    put("theme", Expected::Absent, "dark"),
                    put("theme", Expected::Absent, "light")
                ]
            )
            .await
            .is_err_and(|error| matches!(error, InvocationError::NotStarted(_)))
    );
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn expiry_is_logical_and_supervised_cleanup_publishes_without_manual_ticks() {
    let root = tempfile::tempdir().unwrap();
    let node = start(fresh_store(), root.path()).await;
    let client = organization(&node, "expiry").await;
    let committed = client
        .edit(
            new_identity().unwrap(),
            vec![Edit {
                expires_at_ms: Some(now_ms().unwrap() + 100),
                ..put("feature.temporary", Expected::Absent, "enabled")
            }],
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let observed = client
                .get("feature.temporary", Some(committed.receipt))
                .await
                .unwrap();
            if observed.output.is_none()
                && observed.receipt.commit_sequence > committed.receipt.commit_sequence
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(node.is_ready());
    client
        .edit(
            new_identity().unwrap(),
            vec![put("feature.temporary", Expected::Absent, "new")],
        )
        .await
        .unwrap();
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_restores_values_versions_receipts_and_original_outcomes() {
    let root = tempfile::tempdir().unwrap();
    let store = fresh_store();
    let node = start(store.clone(), root.path()).await;
    let client = organization(&node, "restart").await;
    let identity = new_identity().unwrap();
    let edits = vec![put("theme", Expected::Absent, "dark")];
    let committed = client.edit(identity, edits.clone()).await.unwrap();
    let before = client
        .get("theme", Some(committed.receipt))
        .await
        .unwrap()
        .output;
    node.shutdown().await.unwrap();
    let successor = start(store, root.path()).await;
    let restored = organization(&successor, "restart").await;
    assert_eq!(
        restored
            .get("theme", Some(committed.receipt))
            .await
            .unwrap()
            .output,
        before
    );
    assert_eq!(restored.edit(identity, edits).await.unwrap(), committed);
    successor.shutdown().await.unwrap();
}

#[test]
fn domain_json_and_canonical_identity_contracts_are_explicit() {
    assert_eq!(Organization::new("org-42").unwrap().as_bytes(), b"org-42");
    assert!(Organization::new("Org-42").is_err());
    assert!(Organization::new("").is_err());
    let edit: Edit = serde_json::from_str(r#"{"key":"theme","expected":{"kind":"absent"},"value":{"type":"text","value":"dark"},"expires_at_ms":null}"#).unwrap();
    assert_eq!(edit, put("theme", Expected::Absent, "dark"));
    assert_eq!(
        serde_json::to_vec(&Preference::Boolean(true)).unwrap(),
        br#"{"type":"boolean","value":true}"#
    );
    assert!(serde_json::from_str::<Expected>(r#"{"kind":"version","version":"AA"}"#).is_err());
    assert!(
        serde_json::from_str::<Preference>(r#"{"type":"boolean","value":true,"extra":1}"#).is_err()
    );
}
