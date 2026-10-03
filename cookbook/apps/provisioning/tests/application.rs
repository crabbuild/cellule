//! Public domain, native Workflow, provider identity, cancellation, and recovery contracts.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_app::ApplicationHandle;
use cellule_cookbook_provisioning::*;
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity};
use cellule_runtime::{
    ApplicationId, InvocationError, Resolution, TenantId,
    primitives::workflow::{
        ActivityCompletion, ActivityCompletionOutcome, WorkflowActivityClaimCommand,
        WorkflowActivityClaimRequest, WorkflowActivityCompleteCommand,
    },
    registry::Command,
};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};
use std::{sync::Arc, time::Duration};
fn id(value: u128) -> Id {
    Id::from_bytes(value.to_be_bytes()).unwrap()
}
fn spec(value: u128) -> Spec {
    Spec {
        id: id(value),
        name: "volume".into(),
        capacity_mib: 16,
        provider_endpoint: "http://127.0.0.1:19026/".into(),
    }
}
fn store() -> Store {
    Store::new(Arc::new(InMemory::new()))
}
async fn node(store: Store, path: &std::path::Path) -> LocalNode {
    LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: path.into(),
            storage_prefix: Path::from("provisioning-test"),
            application_id: ApplicationId::from_bytes([0x84; 16]),
        },
    )
    .await
    .unwrap()
}
async fn setup(
    node: &LocalNode,
) -> (
    ApplicationHandle<ProvisioningApplication>,
    ProvisioningClient,
) {
    let handle = node
        .application_handle::<ProvisioningApplication>(TenantId::from_bytes([0x85; 16]))
        .unwrap();
    let client = open(node, &handle).await.unwrap();
    node.open_cell(&client.target(PROVIDER).unwrap(), &Provider)
        .await
        .unwrap();
    (handle, client)
}
async fn command<C: Command>(
    handle: &ApplicationHandle<ProvisioningApplication>,
    client: &ProvisioningClient,
    ns: cellule_runtime::NamespaceId,
    input: C::Input,
) -> C::Output
where
    C::Output: std::fmt::Debug,
{
    handle
        .prepare_command::<C>(&client.target(ns).unwrap(), new_identity().unwrap(), input)
        .await
        .unwrap()
        .execute()
        .await
        .unwrap()
        .output
}
async fn start(
    handle: &ApplicationHandle<ProvisioningApplication>,
    client: &ProvisioningClient,
    value: Spec,
) {
    client
        .prepare(new_identity().unwrap(), Change::Request(value.clone()))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_eq!(
        command::<StartResource>(handle, client, FLOWS, value).await,
        DeliveryOutcome::Applied
    );
}
async fn provider(
    client: &ProvisioningClient,
    value: Spec,
    action: ProviderAction,
) -> ProviderResource {
    client
        .prepare_provider(
            new_identity().unwrap(),
            ProviderWork::new(value.clone(), action),
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    client
        .provider(value.id, None)
        .await
        .unwrap()
        .output
        .unwrap()
}
async fn stable(client: &ProvisioningClient, id: Id, phase: ProviderPhase) -> ProviderResource {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        client
            .prepare_advance(new_identity().unwrap(), Advance { limit: 16 })
            .await
            .unwrap()
            .execute()
            .await
            .unwrap();
        let value = client.provider(id, None).await.unwrap().output.unwrap();
        if value.phase == phase {
            return value;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
async fn complete(
    handle: &ApplicationHandle<ProvisioningApplication>,
    client: &ProvisioningClient,
    observation: Observation,
    failed: bool,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let claim = loop {
        let mut claims = command::<WorkflowActivityClaimCommand<Flows>>(
            handle,
            client,
            FLOWS,
            WorkflowActivityClaimRequest {
                limit: 1,
                lease_ms: 30000,
            },
        )
        .await;
        if let Some(claim) = claims.pop() {
            break claim;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let completion = ActivityCompletion {
        run_id: claim.run_id,
        activity_id: claim.activity_id,
        attempt: claim.attempt,
        lease_token: claim.token,
        completion_token: *uuid::Uuid::now_v7().as_bytes(),
        result: if failed {
            b"outcome unknown after provider application".to_vec()
        } else {
            serde_json::to_vec(&observation).unwrap()
        },
        failed,
        retryable: false,
    };
    assert!(matches!(
        command::<WorkflowActivityCompleteCommand<Flows>>(handle, client, FLOWS, completion).await,
        ActivityCompletionOutcome::Applied(_)
    ));
}
async fn project(
    handle: &ApplicationHandle<ProvisioningApplication>,
    client: &ProvisioningClient,
    id: Id,
) -> Reply {
    let call = client
        .workflow(id, None)
        .await
        .unwrap()
        .output
        .unwrap()
        .state
        .waiting
        .unwrap();
    assert_eq!(
        command::<ProjectResource>(handle, client, DIRECTORY, call.clone()).await,
        DeliveryOutcome::Applied
    );
    let projected = client.resource(id, None).await.unwrap().output.unwrap();
    let value = match &call.projection {
        Projection::Ready(_) => {
            if projected.delete_requested {
                ReplyValue::DeleteRequested
            } else {
                ReplyValue::Published
            }
        }
        Projection::Review(_) => ReplyValue::Reviewed,
        Projection::Deleted(_) => ReplyValue::Deleted,
    };
    let reply = Reply { call, value };
    assert_eq!(
        command::<ReplyResource>(handle, client, FLOWS, reply.clone()).await,
        DeliveryOutcome::Applied
    );
    reply
}
async fn active(
    handle: &ApplicationHandle<ProvisioningApplication>,
    client: &ProvisioningClient,
    value: Spec,
) {
    start(handle, client, value.clone()).await;
    provider(client, value.clone(), ProviderAction::Create).await;
    let resource_id = value.id;
    let value = stable(client, resource_id, ProviderPhase::Ready).await;
    complete(handle, client, Observation::Known(value), false).await;
    project(handle, client, resource_id).await;
}
#[tokio::test]
async fn stable_operation_keys_and_full_request_binding_prevent_duplicate_creation() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (_, client) = setup(&node).await;
    let create = ProviderWork::new(spec(1), ProviderAction::Create);
    let delete = ProviderWork::new(spec(1), ProviderAction::Delete);
    assert_ne!(create.operation_key, delete.operation_key);
    let original = provider(&client, spec(1), ProviderAction::Create).await;
    assert_eq!(
        provider(&client, spec(1), ProviderAction::Create).await,
        original
    );
    let mut changed = spec(1);
    changed.capacity_mib += 1;
    let result = client
        .prepare_provider(
            new_identity().unwrap(),
            ProviderWork::new(changed, ProviderAction::Create),
        )
        .await
        .unwrap()
        .execute()
        .await;
    assert!(
        matches!(result,Err(InvocationError::Rejected(value)) if value.output==DeliveryOutcome::Conflict)
    );
    assert_eq!(
        client
            .provider(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .creates,
        1
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn deletion_before_creation_is_a_permanent_provider_tombstone() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (_, client) = setup(&node).await;
    let deleted = provider(&client, spec(1), ProviderAction::Delete).await;
    assert_eq!(
        (deleted.phase, deleted.creates, deleted.deletes),
        (ProviderPhase::Deleted, 0, 1)
    );
    assert_eq!(
        provider(&client, spec(1), ProviderAction::Create).await,
        deleted
    );
    assert_eq!(
        provider(&client, spec(1), ProviderAction::Delete).await,
        deleted
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn polling_is_read_only_and_provider_advancement_publishes_async_ready_and_deleted() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (_, client) = setup(&node).await;
    let original = provider(&client, spec(1), ProviderAction::Create).await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        client.provider(id(1), None).await.unwrap().output.unwrap(),
        original
    );
    let ready = stable(&client, id(1), ProviderPhase::Ready).await;
    assert_eq!((ready.creates, ready.deletes), (1, 0));
    let deleting = provider(&client, spec(1), ProviderAction::Delete).await;
    assert_eq!(deleting.phase, ProviderPhase::Deleting);
    let deleted = stable(&client, id(1), ProviderPhase::Deleted).await;
    assert_eq!((deleted.creates, deleted.deletes), (1, 1));
    assert_eq!(
        provider(&client, spec(1), ProviderAction::Create).await,
        deleted
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn early_delete_signal_binds_before_start_and_skips_creation() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    client
        .prepare(new_identity().unwrap(), Change::Request(spec(1)))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    client
        .prepare(new_identity().unwrap(), Change::Delete(id(1)))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_eq!(
        command::<DeleteResource>(&handle, &client, FLOWS, spec(1)).await,
        DeliveryOutcome::Applied
    );
    command::<StartResource>(&handle, &client, FLOWS, spec(1)).await;
    let flow = client.workflow(id(1), None).await.unwrap().output.unwrap();
    assert_eq!(
        (flow.state.phase, flow.state.stage),
        (Phase::Deleting, Stage::Delete)
    );
    let deleted = provider(&client, spec(1), ProviderAction::Delete).await;
    complete(&handle, &client, Observation::Known(deleted), false).await;
    project(&handle, &client, id(1)).await;
    assert_eq!(
        client
            .resource(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        ResourceStatus::Deleted
    );
    assert_eq!(
        client
            .provider(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .creates,
        0
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn ready_resource_is_projected_before_active_and_lifetime_stays_running() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    start(&handle, &client, spec(1)).await;
    provider(&client, spec(1), ProviderAction::Create).await;
    let ready = stable(&client, id(1), ProviderPhase::Ready).await;
    complete(&handle, &client, Observation::Known(ready), false).await;
    assert_eq!(
        client
            .resource(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        ResourceStatus::Requested
    );
    let reply = project(&handle, &client, id(1)).await;
    assert_eq!(reply.value, ReplyValue::Published);
    let flow = client.workflow(id(1), None).await.unwrap().output.unwrap();
    assert_eq!(
        (flow.state.phase, flow.status),
        (Phase::Active, "running".into())
    );
    assert_eq!(
        client
            .resource(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        ResourceStatus::Active
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn cancellation_during_accepted_create_reconciles_then_cleans_up() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    start(&handle, &client, spec(1)).await;
    let created = provider(&client, spec(1), ProviderAction::Create).await;
    let pending = client
        .workflow(id(1), None)
        .await
        .unwrap()
        .output
        .unwrap()
        .state
        .activity;
    client
        .prepare(new_identity().unwrap(), Change::Delete(id(1)))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    command::<DeleteResource>(&handle, &client, FLOWS, spec(1)).await;
    assert_eq!(
        client
            .workflow(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .activity,
        pending
    );
    complete(&handle, &client, Observation::Known(created), false).await;
    assert_eq!(
        client
            .workflow(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .phase,
        Phase::Deleting
    );
    provider(&client, spec(1), ProviderAction::Delete).await;
    let deleted = stable(&client, id(1), ProviderPhase::Deleted).await;
    complete(&handle, &client, Observation::Known(deleted), false).await;
    project(&handle, &client, id(1)).await;
    let provider = client.provider(id(1), None).await.unwrap().output.unwrap();
    assert_eq!((provider.creates, provider.deletes), (1, 1));
    assert_eq!(
        client
            .workflow(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .phase,
        Phase::Completed
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn deletion_racing_ready_projection_never_republishes_active_after_cancellation() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    start(&handle, &client, spec(1)).await;
    provider(&client, spec(1), ProviderAction::Create).await;
    let ready = stable(&client, id(1), ProviderPhase::Ready).await;
    complete(&handle, &client, Observation::Known(ready), false).await;
    client
        .prepare(new_identity().unwrap(), Change::Delete(id(1)))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    let reply = project(&handle, &client, id(1)).await;
    assert_eq!(reply.value, ReplyValue::DeleteRequested);
    assert_eq!(
        client
            .resource(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        ResourceStatus::Deleting
    );
    command::<DeleteResource>(&handle, &client, FLOWS, spec(1)).await;
    provider(&client, spec(1), ProviderAction::Delete).await;
    let deleted = stable(&client, id(1), ProviderPhase::Deleted).await;
    complete(&handle, &client, Observation::Known(deleted), false).await;
    project(&handle, &client, id(1)).await;
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn cleanup_can_retry_without_recreating_resource_or_claiming_early_completion() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    active(&handle, &client, spec(1)).await;
    client
        .prepare(new_identity().unwrap(), Change::Delete(id(1)))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    command::<DeleteResource>(&handle, &client, FLOWS, spec(1)).await;
    complete(&handle, &client, Observation::Unknown, true).await;
    assert_eq!(
        client
            .resource(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        ResourceStatus::Deleting
    );
    assert_eq!(
        client
            .workflow(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .cleanup_attempts,
        2
    );
    let deleting = provider(&client, spec(1), ProviderAction::Delete).await;
    complete(&handle, &client, Observation::Known(deleting), false).await;
    assert_eq!(
        client
            .workflow(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .phase,
        Phase::PollingDeletion
    );
    let deleted = stable(&client, id(1), ProviderPhase::Deleted).await;
    complete(&handle, &client, Observation::Known(deleted), false).await;
    assert_ne!(
        client
            .workflow(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        "completed"
    );
    project(&handle, &client, id(1)).await;
    let provider = client.provider(id(1), None).await.unwrap().output.unwrap();
    assert_eq!((provider.creates, provider.deletes), (1, 1));
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn uncertain_creation_exhausts_bounded_work_then_operator_reuses_same_business_keys() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    start(&handle, &client, spec(1)).await;
    let created = provider(&client, spec(1), ProviderAction::Create).await;
    for _ in 0..MAX_STAGE_ATTEMPTS {
        complete(&handle, &client, Observation::Unknown, false).await;
    }
    project(&handle, &client, id(1)).await;
    assert_eq!(
        client
            .workflow(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .phase,
        Phase::NeedsReview
    );
    assert_eq!(
        client
            .resource(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        ResourceStatus::NeedsReview
    );
    let input = Reconcile {
        resource: id(1),
        token: id(2),
    };
    for _ in 0..2 {
        client
            .prepare_reconcile(new_identity().unwrap(), input.clone())
            .await
            .unwrap()
            .execute()
            .await
            .unwrap();
    }
    assert_eq!(
        client
            .workflow(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .reconciliations,
        1
    );
    assert_eq!(
        provider(&client, spec(1), ProviderAction::Create).await,
        created
    );
    let ready = stable(&client, id(1), ProviderPhase::Ready).await;
    complete(&handle, &client, Observation::Known(ready), false).await;
    project(&handle, &client, id(1)).await;
    assert_eq!(
        client
            .provider(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .creates,
        1
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn uncertainty_during_cleanup_remains_visible_and_cancellation_does_not_reset_it() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    active(&handle, &client, spec(1)).await;
    client
        .prepare(new_identity().unwrap(), Change::Delete(id(1)))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    command::<DeleteResource>(&handle, &client, FLOWS, spec(1)).await;
    for _ in 0..MAX_STAGE_ATTEMPTS {
        complete(&handle, &client, Observation::Unknown, false).await;
    }
    project(&handle, &client, id(1)).await;
    assert_eq!(
        client
            .workflow(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .phase,
        Phase::NeedsReview
    );
    command::<DeleteResource>(&handle, &client, FLOWS, spec(1)).await;
    assert_eq!(
        client
            .workflow(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .phase,
        Phase::NeedsReview
    );
    assert_eq!(
        client
            .provider(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .phase,
        ProviderPhase::Ready
    );
    client
        .prepare_reconcile(
            new_identity().unwrap(),
            Reconcile {
                resource: id(1),
                token: id(3),
            },
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    provider(&client, spec(1), ProviderAction::Delete).await;
    let deleted = stable(&client, id(1), ProviderPhase::Deleted).await;
    complete(&handle, &client, Observation::Known(deleted), false).await;
    let callback = project(&handle, &client, id(1)).await;
    assert_eq!(
        command::<ReplyResource>(&handle, &client, FLOWS, callback).await,
        DeliveryOutcome::Applied
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn callback_duplicates_are_permanent_and_changed_or_foreign_steps_cannot_mutate_lifetime() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    start(&handle, &client, spec(1)).await;
    provider(&client, spec(1), ProviderAction::Create).await;
    let ready = stable(&client, id(1), ProviderPhase::Ready).await;
    complete(&handle, &client, Observation::Known(ready), false).await;
    let call = client
        .workflow(id(1), None)
        .await
        .unwrap()
        .output
        .unwrap()
        .state
        .waiting
        .unwrap();
    let mut foreign = call.clone();
    foreign.step = id(900).bytes();
    let result = handle
        .prepare_command::<ReplyResource>(
            &client.target(FLOWS).unwrap(),
            new_identity().unwrap(),
            Reply {
                call: foreign,
                value: ReplyValue::Published,
            },
        )
        .await
        .unwrap()
        .execute()
        .await;
    assert!(result.is_err());
    let reply = project(&handle, &client, id(1)).await;
    assert_eq!(
        command::<ReplyResource>(&handle, &client, FLOWS, reply.clone()).await,
        DeliveryOutcome::Applied
    );
    let changed = Reply {
        value: ReplyValue::DeleteRequested,
        ..reply
    };
    let result = handle
        .prepare_command::<ReplyResource>(
            &client.target(FLOWS).unwrap(),
            new_identity().unwrap(),
            changed,
        )
        .await
        .unwrap()
        .execute()
        .await;
    assert!(
        matches!(result,Err(InvocationError::Rejected(value)) if value.output==DeliveryOutcome::Conflict)
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn cold_restore_retains_provider_application_original_receipt_and_pending_native_action() {
    let root = tempfile::tempdir().unwrap();
    let authoritative = store();
    let first = node(authoritative.clone(), &root.path().join("first")).await;
    let (handle, client) = setup(&first).await;
    let identity = new_identity().unwrap();
    let prepared = client
        .prepare(identity, Change::Request(spec(1)))
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let original = prepared.execute().await.unwrap();
    command::<StartResource>(&handle, &client, FLOWS, spec(1)).await;
    provider(&client, spec(1), ProviderAction::Create).await;
    let action = client
        .workflow(id(1), None)
        .await
        .unwrap()
        .output
        .unwrap()
        .state
        .activity;
    first.shutdown().await.unwrap();
    let restored = node(authoritative, &root.path().join("cold")).await;
    let (handle, client) = setup(&restored).await;
    assert!(
        matches!(client.resolve(&evidence).await.unwrap(),Resolution::Committed(value) if value.commit_sequence()==original.receipt.commit_sequence)
    );
    assert_eq!(
        client
            .workflow(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .activity,
        action
    );
    assert_eq!(
        client
            .provider(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .creates,
        1
    );
    let ready = stable(&client, id(1), ProviderPhase::Ready).await;
    complete(&handle, &client, Observation::Known(ready), false).await;
    project(&handle, &client, id(1)).await;
    restored.shutdown().await.unwrap();
}
#[tokio::test]
async fn resource_capacity_rejection_keeps_original_evidence_and_pages_are_bounded() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (_, client) = setup(&node).await;
    for value in 1..=MAX_RESOURCES as u128 {
        client
            .prepare(new_identity().unwrap(), Change::Request(spec(value)))
            .await
            .unwrap()
            .execute()
            .await
            .unwrap();
    }
    let prepared = client
        .prepare(new_identity().unwrap(), Change::Request(spec(500)))
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let result = prepared.execute().await;
    let rejected = match result {
        Err(InvocationError::Rejected(value)) => value,
        _ => panic!(),
    };
    assert_eq!(rejected.output, Outcome::Capacity);
    assert!(
        matches!(client.resolve(&evidence).await.unwrap(),Resolution::Committed(value) if value.commit_sequence()==rejected.receipt.commit_sequence)
    );
    let first = client
        .resources(
            Page {
                after: 0,
                limit: 100,
            },
            None,
        )
        .await
        .unwrap()
        .output;
    let tail = client
        .resources(
            Page {
                after: first.next.unwrap(),
                limit: 100,
            },
            None,
        )
        .await
        .unwrap()
        .output;
    assert_eq!((first.resources.len(), tail.resources.len()), (100, 28));
    assert!(tail.next.is_none());
    node.shutdown().await.unwrap();
}
#[test]
fn canonical_wire_identity_provider_keys_and_validation_are_contracts() {
    use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, WireValue};
    let value = id(1);
    let mut encoder = BoundedEncoder::new(128).unwrap();
    value.encode(&mut encoder).unwrap();
    let bytes = encoder.finish();
    let mut fixture = vec![1, 0, 0, 0, 38];
    fixture.extend_from_slice(b"\"00000000-0000-0000-0000-000000000001\"");
    assert_eq!(bytes, fixture);
    let mut invalid = bytes.clone();
    invalid[0] = 2;
    assert!(Id::decode(&mut BoundedDecoder::new(&invalid, 128).unwrap()).is_err());
    let mut work = ProviderWork::new(spec(1), ProviderAction::Create);
    work.operation_key[0] ^= 1;
    assert!(work.validate().is_err());
    for endpoint in [
        "http://localhost:19026/",
        "http://127.0.0.1/",
        "http://127.0.0.1:19026/resource",
        "http://127.0.0.1:19026/?x=1",
    ] {
        assert!(validate_endpoint(endpoint).is_err());
    }
    let mut value = spec(1);
    value.name = "UPPER".into();
    assert!(value.validate().is_err());
    assert!(
        Page {
            after: 0,
            limit: 101
        }
        .validate()
        .is_err()
    );
    assert!(validate_provider_token("bad token").is_err());
}
#[tokio::test]
async fn regressing_provider_evidence_requires_review_and_keeps_verified_cleanup_progress() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    active(&handle, &client, spec(1)).await;
    let ready = client.provider(id(1), None).await.unwrap().output.unwrap();
    client
        .prepare(new_identity().unwrap(), Change::Delete(id(1)))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    command::<DeleteResource>(&handle, &client, FLOWS, spec(1)).await;
    let deleting = provider(&client, spec(1), ProviderAction::Delete).await;
    complete(
        &handle,
        &client,
        Observation::Known(deleting.clone()),
        false,
    )
    .await;
    complete(&handle, &client, Observation::Known(ready), false).await;
    let projected = project(&handle, &client, id(1)).await;
    assert_eq!(
        projected.call.projection,
        Projection::Review(Some(deleting.clone()))
    );
    let flow = client.workflow(id(1), None).await.unwrap().output.unwrap();
    assert_eq!(flow.state.phase, Phase::NeedsReview);
    assert_eq!(flow.state.cleanup_attempts, 1);
    assert_eq!(flow.state.observed, Some(deleting));
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn reconciliation_token_budget_exhaustion_preserves_uncertainty_and_original_provider_keys() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    let value = spec(1);
    let original_key = ProviderWork::new(value.clone(), ProviderAction::Create).operation_key;
    start(&handle, &client, value).await;
    for cycle in 0..=8 {
        for _ in 0..8 {
            complete(&handle, &client, Observation::Unknown, false).await;
        }
        project(&handle, &client, id(1)).await;
        let flow = client.workflow(id(1), None).await.unwrap().output.unwrap();
        assert_eq!(flow.state.phase, Phase::NeedsReview);
        assert_eq!(flow.state.reconciliations, cycle);
        assert_eq!(
            ProviderWork::new(flow.state.spec, ProviderAction::Create).operation_key,
            original_key
        );
        if cycle < 8 {
            client
                .prepare_reconcile(
                    new_identity().unwrap(),
                    Reconcile {
                        resource: id(1),
                        token: id(100 + u128::from(cycle)),
                    },
                )
                .await
                .unwrap()
                .execute()
                .await
                .unwrap();
        }
    }
    let before = client.workflow(id(1), None).await.unwrap().output.unwrap();
    let outcome = client
        .prepare_reconcile(
            new_identity().unwrap(),
            Reconcile {
                resource: id(1),
                token: id(999),
            },
        )
        .await
        .unwrap()
        .execute()
        .await;
    assert!(
        matches!(outcome, Err(InvocationError::Rejected(value)) if value.output == DeliveryOutcome::Capacity)
    );
    assert_eq!(
        client
            .workflow(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state,
        before.state
    );
    assert_eq!(
        client
            .resource(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        ResourceStatus::NeedsReview
    );
    assert!(client.provider(id(1), None).await.unwrap().output.is_none());
    node.shutdown().await.unwrap();
}
