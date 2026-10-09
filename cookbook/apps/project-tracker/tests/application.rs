//! Public typed aggregate, native Blob, signed delivery, and authority-pinned recovery behavior.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_app::ApplicationHandle;
use cellule_cookbook_project_tracker::*;
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity};
use cellule_runtime::{
    ApplicationId, BlobArtifactStore, Committed, InvocationError, Resolution, TenantId,
    codec::{BoundedDecoder, BoundedEncoder, WireValue},
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{sync::Arc, time::Duration};
const APP: ApplicationId = ApplicationId::from_bytes([0x38; 16]);
const TENANT: TenantId = TenantId::from_bytes([0x48; 16]);
struct Harness {
    node: LocalNode,
    handle: ApplicationHandle<ProjectTracker>,
}
async fn start(store: Store, parts: Store, root: &std::path::Path) -> Harness {
    let node = LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: root.into(),
            storage_prefix: Path::from("tracker-tests"),
            application_id: APP,
        },
    )
    .await
    .unwrap();
    let handle = node
        .application_handle::<ProjectTracker>(TENANT)
        .unwrap()
        .with_blob_artifact_store(BlobArtifactStore::new(parts));
    Harness { node, handle }
}
async fn harness(root: &std::path::Path) -> Harness {
    start(
        Store::new(Arc::new(InMemory::new())),
        Store::new(Arc::new(InMemory::new())),
        root,
    )
    .await
}
fn key(value: &str) -> ProjectKey {
    ProjectKey::new(value).unwrap()
}
fn issue(value: &str) -> IssueId {
    IssueId::new(value).unwrap()
}
fn fields(status: IssueStatus) -> IssueFields {
    IssueFields {
        title: "Typed issue".into(),
        description: "Reusable aggregate behavior".into(),
        assignee: Some(Assignee::new("alice").unwrap()),
        status,
    }
}
async fn project(h: &Harness, value: &str) -> ProjectClient {
    let client = ProjectClient::new(h.handle.clone(), key(value)).unwrap();
    h.node.open_cell(client.target(), &Projects).await.unwrap();
    client
}
async fn dashboard(h: &Harness) -> DashboardClient {
    let client = DashboardClient::new(h.handle.clone()).unwrap();
    h.node.open_cell(client.target(), &Dashboard).await.unwrap();
    client
}
async fn attachments(h: &Harness) -> AttachmentClient {
    let client = AttachmentClient::new(h.handle.clone()).unwrap();
    h.node
        .open_cell(&client.target().unwrap(), &Attachments)
        .await
        .unwrap();
    client
}
async fn change(client: &ProjectClient, mutation: ProjectMutation) -> Committed<ChangeOutcome> {
    let project = client
        .get(None)
        .await
        .unwrap()
        .output
        .map(|v| v.project)
        .unwrap_or_else(|| key("roadmap"));
    client
        .change(new_identity().unwrap(), ProjectChange { project, mutation })
        .await
        .unwrap()
}
async fn create(client: &ProjectClient) {
    change(
        client,
        ProjectMutation::Create {
            name: "Roadmap".into(),
        },
    )
    .await;
    change(
        client,
        ProjectMutation::CreateIssue {
            id: issue("one"),
            fields: fields(IssueStatus::Open),
        },
    )
    .await;
}
fn plan(bytes: Vec<u8>, id: &str, revision: i64) -> AttachmentPlan {
    let descriptor = AttachmentDescriptor::new(
        key("roadmap"),
        issue("one"),
        AttachmentId::new(id).unwrap(),
        "notes.bin".into(),
        &bytes,
    )
    .unwrap();
    AttachmentPlan::new(descriptor, bytes, revision).unwrap()
}
fn rejected(
    source: InvocationError<ChangeOutcome>,
    decision: Decision,
) -> Committed<ChangeOutcome> {
    match source {
        InvocationError::Rejected(value) => {
            assert_eq!(value.output.decision, decision);
            assert!(value.output.version.is_none());
            *value
        }
        other => panic!("expected durable domain refusal: {other}"),
    }
}
fn resolved(value: Resolution) -> ChangeOutcome {
    let Resolution::Committed(value) = value else {
        panic!("expected original committed evidence");
    };
    let mut decoder = BoundedDecoder::new(value.result(), 1024).unwrap();
    let output = ChangeOutcome::decode(&mut decoder).unwrap();
    decoder.finish().unwrap();
    output
}
#[tokio::test]
async fn aggregate_intents_replay_and_revision_conflicts_are_durable() {
    let root = tempfile::tempdir().unwrap();
    let h = harness(root.path()).await;
    let project = project(&h, "roadmap").await;
    let dashboard = dashboard(&h).await;
    let input = ProjectChange {
        project: key("roadmap"),
        mutation: ProjectMutation::Create {
            name: "Roadmap".into(),
        },
    };
    let prepared = project
        .prepare(new_identity().unwrap(), input)
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let first = prepared.execute().await.unwrap();
    assert_eq!(
        resolved(project.resolve(&evidence).await.unwrap()),
        first.output
    );
    let pending = project
        .progress(Some(first.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(pending.state, ProjectionState::Pending);
    assert_eq!(pending.attempts, Some(0));
    assert_eq!(pending.version, first.output.version.clone().unwrap());
    assert!(
        dashboard
            .lookup(key("roadmap"), None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    change(
        &project,
        ProjectMutation::CreateIssue {
            id: issue("one"),
            fields: fields(IssueStatus::Open),
        },
    )
    .await;
    let input = ProjectChange {
        project: key("roadmap"),
        mutation: ProjectMutation::EditIssue {
            id: issue("one"),
            expected_revision: 1,
            fields: fields(IssueStatus::Closed),
        },
    };
    let prepared = project
        .prepare(new_identity().unwrap(), input.clone())
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let first = prepared.execute().await.unwrap();
    assert_eq!(
        project
            .prepare(evidence.identity(), input.clone())
            .await
            .unwrap()
            .execute()
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        resolved(project.resolve(&evidence).await.unwrap()),
        first.output
    );
    let refused = project
        .prepare(new_identity().unwrap(), input)
        .await
        .unwrap();
    let refusal_evidence = refused.evidence().clone();
    let refusal = rejected(refused.execute().await.unwrap_err(), Decision::Conflict);
    assert_eq!(
        resolved(project.resolve(&refusal_evidence).await.unwrap()),
        refusal.output
    );
    let state = project
        .get(Some(refusal.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(state.revision, 3);
    assert_eq!(state.issues[0].revision, 2);
    assert_eq!(state.issues[0].fields.status, IssueStatus::Closed);
    let wrong = ProjectChange {
        project: key("other"),
        mutation: ProjectMutation::Create {
            name: "Other".into(),
        },
    };
    assert!(matches!(
        project.prepare(new_identity().unwrap(), wrong).await,
        Err(InvocationError::NotStarted(_))
    ));
    let dash_receipt = dashboard
        .list(
            DashboardPageRequest {
                after: None,
                limit: 1,
            },
            None,
        )
        .await
        .unwrap()
        .receipt;
    assert!(project.get(Some(dash_receipt)).await.is_err());
    assert!(
        dashboard
            .lookup(key("roadmap"), Some(first.receipt))
            .await
            .is_err()
    );
    h.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn concurrent_issue_edits_have_one_winner_and_independent_project_identity() {
    let root = tempfile::tempdir().unwrap();
    let h = harness(root.path()).await;
    let source = project(&h, "roadmap").await;
    create(&source).await;
    let left = ProjectChange {
        project: key("roadmap"),
        mutation: ProjectMutation::EditIssue {
            id: issue("one"),
            expected_revision: 1,
            fields: fields(IssueStatus::Closed),
        },
    };
    let mut right = left.clone();
    if let ProjectMutation::EditIssue { fields, .. } = &mut right.mutation {
        fields.title = "Other concurrent edit".into();
    }
    let (left, right) = tokio::join!(
        source.change(new_identity().unwrap(), left),
        source.change(new_identity().unwrap(), right)
    );
    assert_ne!(left.is_ok(), right.is_ok());
    let loser = match left {
        Ok(_) => right.unwrap_err(),
        Err(error) => error,
    };
    rejected(loser, Decision::Conflict);
    let state = source.get(None).await.unwrap().output.unwrap();
    assert_eq!(state.revision, 3);
    assert_eq!(state.issues[0].revision, 2);
    let other = project(&h, "operations").await;
    assert_ne!(source.target(), other.target());
    assert!(other.get(None).await.unwrap().output.is_none());
    other
        .change(
            new_identity().unwrap(),
            ProjectChange {
                project: key("operations"),
                mutation: ProjectMutation::Create {
                    name: "Operations".into(),
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(source.get(None).await.unwrap().output.unwrap(), state);
    h.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn receiver_accepts_stale_state_and_rejects_same_revision_different_digest() {
    let root = tempfile::tempdir().unwrap();
    let h = harness(root.path()).await;
    let source = project(&h, "roadmap").await;
    create(&source).await;
    let dashboard = dashboard(&h).await;
    let old = source
        .get(None)
        .await
        .unwrap()
        .output
        .unwrap()
        .summary()
        .unwrap();
    change(
        &source,
        ProjectMutation::EditIssue {
            id: issue("one"),
            expected_revision: 1,
            fields: fields(IssueStatus::Closed),
        },
    )
    .await;
    let current = source
        .get(None)
        .await
        .unwrap()
        .output
        .unwrap()
        .summary()
        .unwrap();
    let publish = |value| {
        h.handle.prepare_command::<ProjectDashboard>(
            dashboard.target(),
            new_identity().unwrap(),
            value,
        )
    };
    assert_eq!(
        publish(current.clone())
            .await
            .unwrap()
            .execute()
            .await
            .unwrap()
            .output,
        ProjectionOutcome::Applied
    );
    assert_eq!(
        publish(old).await.unwrap().execute().await.unwrap().output,
        ProjectionOutcome::Stale
    );
    assert_eq!(
        publish(current.clone())
            .await
            .unwrap()
            .execute()
            .await
            .unwrap()
            .output,
        ProjectionOutcome::Unchanged
    );
    let mut corrupt = current.clone();
    corrupt.digest[0] ^= 1;
    assert!(
        matches!(publish(corrupt).await.unwrap().execute().await,Err(InvocationError::Rejected(value)) if value.output==ProjectionOutcome::Conflict)
    );
    assert_eq!(
        dashboard.lookup(key("roadmap"), None).await.unwrap().output,
        Some(current)
    );
    h.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn real_signed_workers_reorder_summaries_and_resolve_a_lost_reply() {
    let root = tempfile::tempdir().unwrap();
    let h = harness(root.path()).await;
    let source = project(&h, "roadmap").await;
    create(&source).await;
    let controlled_effect = source.get(None).await.unwrap().output.unwrap().effect_id;
    change(
        &source,
        ProjectMutation::EditIssue {
            id: issue("one"),
            expected_revision: 1,
            fields: fields(IssueStatus::Closed),
        },
    )
    .await;
    let dashboard = dashboard(&h).await;
    let (sender, mut observations) = tokio::sync::mpsc::channel(16);
    spawn_delivery(
        &h.node,
        &h.node,
        h.handle.clone(),
        h.handle.clone(),
        &[key("roadmap")],
        DeliveryOptions {
            controlled_effect: Some(controlled_effect),
            before_delivery: Duration::from_secs(2),
            drop_reply_once: true,
            progress: Some(sender),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let observations = tokio::time::timeout(Duration::from_secs(15), async {
        let mut values = vec![];
        loop {
            assert!(h.node.is_ready());
            while let Ok(value) = observations.try_recv() {
                values.push(value);
            }
            let latest = source.progress(None).await.unwrap().output.unwrap();
            assert_ne!(latest.state, ProjectionState::Failed);
            if values.iter().any(|v| v.outcome == ProjectionOutcome::Stale)
                && latest.state == ProjectionState::Delivered
            {
                return values;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        observations
            .windows(2)
            .any(|v| v[0].summary.revision > v[1].summary.revision)
    );
    assert_eq!(
        dashboard
            .lookup(key("roadmap"), None)
            .await
            .unwrap()
            .output
            .unwrap(),
        source
            .get(None)
            .await
            .unwrap()
            .output
            .unwrap()
            .summary()
            .unwrap()
    );
    h.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn supported_delivery_delays_keep_the_original_native_attempt() {
    let root = tempfile::tempdir().unwrap();
    let h = harness(root.path()).await;
    let source = project(&h, "roadmap").await;
    let first = change(
        &source,
        ProjectMutation::Create {
            name: "Roadmap".into(),
        },
    )
    .await;
    let second = change(
        &source,
        ProjectMutation::Rename {
            expected_revision: 1,
            name: "Current roadmap".into(),
        },
    )
    .await;
    let effects = [
        first.output.version.unwrap().effect_id,
        second.output.version.unwrap().effect_id,
    ];
    assert!(
        spawn_delivery(
            &h.node,
            &h.node,
            h.handle.clone(),
            h.handle.clone(),
            &[key("roadmap")],
            DeliveryOptions {
                controlled_effect: Some([0xa7; 32]),
                ..Default::default()
            },
        )
        .await
        .is_err()
    );
    assert!(h.node.is_ready());
    for effect in effects {
        assert_eq!(
            source
                .effect_status(effect, None)
                .await
                .unwrap()
                .output
                .unwrap()
                .state,
            cellule_runtime::primitives::effects::EffectState::Ready,
            "foreign control must not start delivery workers",
        );
    }
    spawn_delivery(
        &h.node,
        &h.node,
        h.handle.clone(),
        h.handle.clone(),
        &[key("roadmap")],
        DeliveryOptions {
            controlled_effect: Some(effects[1]),
            before_delivery: Duration::from_secs(10),
            after_publication: Duration::from_secs(10),
            drop_reply_once: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    // Selecting the later intent must leave the earlier delivery unpaused,
    // regardless of which worker reaches its transport first.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let earlier = source
                .effect_status(effects[0], None)
                .await
                .unwrap()
                .output
                .unwrap();
            let selected = source
                .effect_status(effects[1], None)
                .await
                .unwrap()
                .output
                .unwrap();
            if earlier.state == cellule_runtime::primitives::effects::EffectState::Delivered
                && selected.state == cellule_runtime::primitives::effects::EffectState::Leased
            {
                assert_eq!(earlier.attempt, 1);
                assert_eq!(
                    selected.state,
                    cellule_runtime::primitives::effects::EffectState::Leased
                );
                assert_eq!(selected.attempt, 1);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let settled = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            assert!(h.node.is_ready());
            let mut statuses = Vec::new();
            for effect in effects {
                statuses.push(
                    source
                        .effect_status(effect, None)
                        .await
                        .unwrap()
                        .output
                        .unwrap(),
                );
            }
            if statuses.iter().all(|status| {
                status.state == cellule_runtime::primitives::effects::EffectState::Delivered
            }) {
                return statuses;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    h.node.shutdown().await.unwrap();
    for status in settled.unwrap() {
        assert_eq!(
            status.attempt, 1,
            "supported delays must not expire a live native attempt: {status:?}"
        );
    }
}
#[tokio::test]
async fn staged_bytes_are_invisible_until_completion_and_original_link_replays() {
    let root = tempfile::tempdir().unwrap();
    let h = harness(root.path()).await;
    let source = project(&h, "roadmap").await;
    create(&source).await;
    let blobs = attachments(&h).await;
    let plan = plan(vec![255; MAX_ATTACHMENT_BYTES], "guide", 1);
    let begin = blobs.prepare(&plan, AttachmentPhase::Begin).await.unwrap();
    let begin_evidence = begin.evidence().clone();
    begin.execute().await.unwrap();
    blobs
        .prepare(&plan, AttachmentPhase::Part)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert!(
        blobs
            .read(plan.descriptor.key(), None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    assert!(blobs.prepare_link(&plan).await.is_err());
    let completed = blobs
        .prepare(&plan, AttachmentPhase::Complete)
        .await
        .unwrap();
    let evidence = completed.evidence().clone();
    let committed = completed.execute().await.unwrap();
    assert!(matches!(
        blobs.resolve_phase(&begin_evidence).await.unwrap(),
        Resolution::Committed(_)
    ));
    assert!(
        matches!(blobs.resolve_phase(&evidence).await.unwrap(),Resolution::Committed(value) if value.commit_sequence()==committed.receipt.commit_sequence)
    );
    let object = blobs
        .read(plan.descriptor.key(), Some(committed.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(object.bytes, plan.bytes);
    assert!(
        source
            .issue(&issue("one"), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .attachments
            .is_empty()
    );
    let prepared = blobs.prepare_link(&plan).await.unwrap();
    let link_evidence = prepared.evidence().clone();
    let linked = prepared.execute().await.unwrap();
    assert_eq!(linked.output.decision, Decision::Applied);
    assert_eq!(blobs.reconcile(&plan).await.unwrap(), linked);
    assert_eq!(
        resolved(blobs.resolve_link(&plan).await.unwrap()),
        linked.output
    );
    assert_eq!(
        source.resolve(&link_evidence).await.unwrap(),
        blobs.resolve_link(&plan).await.unwrap()
    );
    let state = source
        .get(Some(linked.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(state.revision, 3);
    assert_eq!(state.issues[0].revision, 2);
    assert_eq!(
        state.issues[0].attachments,
        vec![object.publication.clone()]
    );
    let fresh = AttachmentPlan::new(plan.descriptor.clone(), plan.bytes.clone(), 1).unwrap();
    assert_eq!(blobs.publish(&fresh).await.unwrap(), object);
    assert_eq!(
        blobs.reconcile(&fresh).await.unwrap().output.decision,
        Decision::Unchanged
    );
    assert_eq!(source.get(None).await.unwrap().output.unwrap(), state);
    assert!(
        blobs
            .read(plan.descriptor.key(), Some(linked.receipt))
            .await
            .is_err()
    );
    assert!(source.get(Some(committed.receipt)).await.is_err());
    h.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn publication_before_link_survives_cold_restore_without_refreshing_a_stale_revision() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let parts = Store::new(Arc::new(InMemory::new()));
    let state = root.path().join("source");
    let h = start(store.clone(), parts.clone(), &state).await;
    let source = project(&h, "roadmap").await;
    create(&source).await;
    let blobs = attachments(&h).await;
    let plan = plan(b"retained bytes".to_vec(), "notes", 1);
    let publication = blobs.publish(&plan).await.unwrap();
    change(
        &source,
        ProjectMutation::EditIssue {
            id: issue("one"),
            expected_revision: 1,
            fields: fields(IssueStatus::Closed),
        },
    )
    .await;
    h.node.shutdown().await.unwrap();
    drop(h);
    std::fs::remove_dir_all(&state).unwrap();
    let h = start(store, parts, &state).await;
    let source = project(&h, "roadmap").await;
    let blobs = attachments(&h).await;
    assert_eq!(blobs.publish(&plan).await.unwrap(), publication);
    let prepared = blobs.prepare_link(&plan).await.unwrap();
    let evidence = prepared.evidence().clone();
    let refused = rejected(prepared.execute().await.unwrap_err(), Decision::Conflict);
    assert_eq!(
        resolved(source.resolve(&evidence).await.unwrap()),
        refused.output
    );
    let state = source
        .get(Some(refused.receipt))
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!(state.issues[0].revision, 2);
    assert!(state.issues[0].attachments.is_empty());
    let explicit = AttachmentPlan::new(plan.descriptor.clone(), plan.bytes.clone(), 2).unwrap();
    assert_eq!(
        blobs.reconcile(&explicit).await.unwrap().output.decision,
        Decision::Applied
    );
    assert_eq!(
        resolved(blobs.resolve_link(&plan).await.unwrap()).decision,
        Decision::Conflict
    );
    assert_eq!(
        blobs
            .read(plan.descriptor.key(), None)
            .await
            .unwrap()
            .output
            .unwrap(),
        publication
    );
    h.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn attachment_identity_never_rebinds_content_or_metadata_and_tenant_manifests_are_isolated() {
    let root = tempfile::tempdir().unwrap();
    let h = harness(root.path()).await;
    let source = project(&h, "roadmap").await;
    create(&source).await;
    let blobs = attachments(&h).await;
    let original = plan(b"original".to_vec(), "notes", 1);
    let object = blobs.publish(&original).await.unwrap();
    let changed = plan(b"different".to_vec(), "notes", 1);
    assert_eq!(changed.descriptor.key(), original.descriptor.key());
    assert!(blobs.publish(&changed).await.is_err());
    let mut descriptor = original.descriptor.clone();
    descriptor.name = "renamed.bin".into();
    let renamed = AttachmentPlan::new(descriptor, original.bytes.clone(), 1).unwrap();
    assert!(blobs.publish(&renamed).await.is_err());
    assert_eq!(
        blobs
            .read(original.descriptor.key(), None)
            .await
            .unwrap()
            .output
            .unwrap(),
        object
    );
    let other = h
        .node
        .application_handle::<ProjectTracker>(TenantId::from_bytes([0x49; 16]))
        .unwrap()
        .with_blob_artifact_store(BlobArtifactStore::new(Store::new(
            Arc::new(InMemory::new()),
        )));
    let other = AttachmentClient::new(other).unwrap();
    h.node
        .open_cell(&other.target().unwrap(), &Attachments)
        .await
        .unwrap();
    assert_ne!(other.target().unwrap(), blobs.target().unwrap());
    assert!(
        other
            .read(original.descriptor.key(), None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    assert!(
        source
            .issue(&issue("one"), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .attachments
            .is_empty()
    );
    h.node.shutdown().await.unwrap();
}
#[tokio::test]
async fn issue_and_attachment_capacity_refusals_leave_aggregate_versions_unchanged() {
    let root = tempfile::tempdir().unwrap();
    let h = harness(root.path()).await;
    let source = project(&h, "roadmap").await;
    create(&source).await;
    let blobs = attachments(&h).await;
    for index in 0..MAX_ISSUE_ATTACHMENTS {
        let plan = plan(
            vec![index as u8 + 1],
            &format!("file-{index}"),
            index as i64 + 1,
        );
        blobs.publish(&plan).await.unwrap();
        blobs.reconcile(&plan).await.unwrap();
    }
    let before = source.get(None).await.unwrap().output.unwrap();
    let extra = plan(vec![9], "extra", 9);
    blobs.publish(&extra).await.unwrap();
    let prepared = blobs.prepare_link(&extra).await.unwrap();
    rejected(prepared.execute().await.unwrap_err(), Decision::Capacity);
    assert_eq!(source.get(None).await.unwrap().output.unwrap(), before);
    for index in 1..MAX_ISSUES {
        change(
            &source,
            ProjectMutation::CreateIssue {
                id: issue(&format!("issue-{index}")),
                fields: fields(IssueStatus::Open),
            },
        )
        .await;
    }
    let before = source.get(None).await.unwrap().output.unwrap();
    let extra = ProjectChange {
        project: key("roadmap"),
        mutation: ProjectMutation::CreateIssue {
            id: issue("extra"),
            fields: fields(IssueStatus::Open),
        },
    };
    rejected(
        source
            .change(new_identity().unwrap(), extra)
            .await
            .unwrap_err(),
        Decision::Capacity,
    );
    assert_eq!(source.get(None).await.unwrap().output.unwrap(), before);
    h.node.shutdown().await.unwrap();
}
#[test]
fn canonical_keys_retained_plan_integrity_and_full_state_wire_bounds() {
    for invalid in ["", "A", "-a", "a-", "a--b", "a/b", "é", "1a"] {
        assert!(ProjectKey::new(invalid).is_err());
    }
    let original = plan(vec![1], "notes", 1);
    let mut changed = original.clone();
    changed.bytes[0] ^= 1;
    assert!(changed.validate().is_err());
    let mut changed = original.clone();
    changed.identities[3] = changed.identities[0].clone();
    assert!(changed.validate().is_err());
    let mut state = ProjectState {
        project: key("roadmap"),
        name: "n".repeat(120),
        revision: 1,
        issues: vec![],
        effect_id: [1; 32],
    };
    for index in 0..MAX_ISSUES {
        state.issues.push(Issue {
            id: issue(&format!("issue-{index:02}")),
            revision: 1,
            fields: IssueFields {
                title: "t".repeat(120),
                description: "d".repeat(512),
                assignee: Some(Assignee::new("a".repeat(48)).unwrap()),
                status: IssueStatus::Open,
            },
            attachments: vec![],
        });
    }
    for index in 0..MAX_PROJECT_ATTACHMENTS {
        let issue_index = index / MAX_ISSUE_ATTACHMENTS;
        let descriptor = AttachmentDescriptor::new(
            key("roadmap"),
            state.issues[issue_index].id.clone(),
            AttachmentId::new(format!("file-{:02}", index % MAX_ISSUE_ATTACHMENTS)).unwrap(),
            "f".repeat(120),
            &[1],
        )
        .unwrap();
        state.issues[issue_index]
            .attachments
            .push(AttachmentPublication {
                descriptor,
                etag: [2; 32],
            });
    }
    let summary = state.summary().unwrap();
    assert_eq!(summary.attachments, MAX_PROJECT_ATTACHMENTS as u32);
    let mut encoder = BoundedEncoder::new(128 << 10).unwrap();
    state.encode(&mut encoder).unwrap();
    let bytes = encoder.finish();
    let mut decoder = BoundedDecoder::new(&bytes, 128 << 10).unwrap();
    assert_eq!(ProjectState::decode(&mut decoder).unwrap(), state);
    decoder.finish().unwrap();
}
