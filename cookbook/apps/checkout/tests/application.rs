//! Public domain, native Workflow, permanent identity, and recovery contracts.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use cellule_app::ApplicationHandle;
use cellule_cookbook_checkout::*;
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
use std::sync::Arc;
fn id(value: u128) -> Id {
    Id::from_bytes(value.to_be_bytes()).unwrap()
}
fn spec(value: u128) -> OrderSpec {
    OrderSpec {
        id: id(value),
        sku: "widget".into(),
        quantity: 1,
        amount: 125,
        payment_endpoint: "http://127.0.0.1:19025/".into(),
        payment_policy: PaymentPolicy::Approve,
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
            storage_prefix: Path::from("checkout-test"),
            application_id: ApplicationId::from_bytes([0x75; 16]),
        },
    )
    .await
    .unwrap()
}
async fn setup(node: &LocalNode) -> (ApplicationHandle<CheckoutApplication>, CheckoutClient) {
    let handle = node
        .application_handle::<CheckoutApplication>(TenantId::from_bytes([0x76; 16]))
        .unwrap();
    let client = open(node, &handle).await.unwrap();
    node.open_cell(&client.target(PAYMENTS).unwrap(), &Payments)
        .await
        .unwrap();
    (handle, client)
}
async fn command<C: Command>(
    handle: &ApplicationHandle<CheckoutApplication>,
    client: &CheckoutClient,
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
async fn seed(client: &CheckoutClient, units: u32) {
    assert_eq!(
        client
            .prepare_seed(
                new_identity().unwrap(),
                Seed {
                    sku: "widget".into(),
                    units
                }
            )
            .await
            .unwrap()
            .execute()
            .await
            .unwrap()
            .output,
        DeliveryOutcome::Applied
    );
}
async fn start(
    handle: &ApplicationHandle<CheckoutApplication>,
    client: &CheckoutClient,
    spec: OrderSpec,
) {
    client
        .prepare_order(new_identity().unwrap(), OrderChange::Place(spec.clone()))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    assert_eq!(
        command::<StartSaga>(handle, client, SAGAS, spec).await,
        DeliveryOutcome::Applied
    );
}
fn call(value: u128, step: u128, operation: Operation) -> Call {
    Call {
        spec: spec(value),
        run_id: id(100).bytes(),
        step: id(step).bytes(),
        operation,
    }
}
async fn step(
    handle: &ApplicationHandle<CheckoutApplication>,
    client: &CheckoutClient,
    id: Id,
) -> Reply {
    let saga = client.saga(id, None).await.unwrap().output.unwrap();
    let call = saga.state.waiting.unwrap();
    let stock = matches!(
        call.operation,
        Operation::Reserve | Operation::Commit | Operation::Release
    );
    let applied = if stock {
        command::<StockStep>(handle, client, INVENTORY, call.clone()).await
    } else {
        command::<OrderStep>(handle, client, ORDERS, call.clone()).await
    };
    assert_eq!(applied, DeliveryOutcome::Applied);
    let value = match call.operation {
        Operation::Reserve => match client
            .reservation(id, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status
        {
            ReservationStatus::Held => ReplyValue::Held,
            ReservationStatus::Unavailable => ReplyValue::Unavailable,
            _ => panic!(),
        },
        Operation::Commit => ReplyValue::Committed,
        Operation::Release => ReplyValue::Released,
        Operation::Decide => {
            if client
                .order(id, None)
                .await
                .unwrap()
                .output
                .unwrap()
                .cancel_requested
            {
                ReplyValue::Cancelled
            } else {
                ReplyValue::Accepted
            }
        }
        Operation::Review => ReplyValue::ReviewRecorded,
        Operation::Finish(_) => {
            match client.order(id, None).await.unwrap().output.unwrap().status {
                OrderStatus::Finished(result) => ReplyValue::Finished(result),
                _ => panic!(),
            }
        }
    };
    let reply = Reply { call, value };
    assert_eq!(
        command::<ReplySaga>(handle, client, SAGAS, reply.clone()).await,
        DeliveryOutcome::Applied
    );
    reply
}
async fn complete(
    handle: &ApplicationHandle<CheckoutApplication>,
    client: &CheckoutClient,
    observation: PaymentObservation,
    failed: bool,
) {
    let claims = command::<WorkflowActivityClaimCommand<Sagas>>(
        handle,
        client,
        SAGAS,
        WorkflowActivityClaimRequest {
            limit: 1,
            lease_ms: 30000,
        },
    )
    .await;
    assert_eq!(claims.len(), 1);
    let claim = &claims[0];
    let completion = ActivityCompletion {
        run_id: claim.run_id,
        activity_id: claim.activity_id,
        attempt: claim.attempt,
        lease_token: claim.token,
        completion_token: *uuid::Uuid::now_v7().as_bytes(),
        result: if failed {
            b"uncertain after external operation".to_vec()
        } else {
            serde_json::to_vec(&observation).unwrap()
        },
        failed,
        retryable: false,
    };
    assert!(matches!(
        command::<WorkflowActivityCompleteCommand<Sagas>>(handle, client, SAGAS, completion).await,
        ActivityCompletionOutcome::Applied(_)
    ));
}
async fn pay(client: &CheckoutClient, spec: OrderSpec, action: PaymentAction) -> Payment {
    client
        .prepare_payment(
            new_identity().unwrap(),
            PaymentWork {
                spec: spec.clone(),
                action,
            },
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    client.payment(spec.id, None).await.unwrap().output.unwrap()
}
#[tokio::test]
async fn inventory_conserves_capacity_under_concurrent_reservations_and_seed_replay() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    seed(&client, 1).await;
    let a = call(1, 1, Operation::Reserve);
    let b = call(2, 2, Operation::Reserve);
    let (a, b) = tokio::join!(
        command::<StockStep>(&handle, &client, INVENTORY, a),
        command::<StockStep>(&handle, &client, INVENTORY, b)
    );
    assert_eq!((a, b), (DeliveryOutcome::Applied, DeliveryOutcome::Applied));
    let statuses = [
        client
            .reservation(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        client
            .reservation(id(2), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
    ];
    assert_eq!(
        statuses
            .iter()
            .filter(|x| **x == ReservationStatus::Held)
            .count(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|x| **x == ReservationStatus::Unavailable)
            .count(),
        1
    );
    seed(&client, 1).await;
    let stock = client
        .stock("widget".into(), None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!((stock.available, stock.held, stock.sold), (0, 1, 0));
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn release_and_void_tombstones_prevent_late_reacquisition() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    seed(&client, 1).await;
    assert_eq!(
        command::<StockStep>(&handle, &client, INVENTORY, call(1, 1, Operation::Release)).await,
        DeliveryOutcome::Applied
    );
    let result = handle
        .prepare_command::<StockStep>(
            &client.target(INVENTORY).unwrap(),
            new_identity().unwrap(),
            call(1, 2, Operation::Reserve),
        )
        .await
        .unwrap()
        .execute()
        .await;
    assert!(
        matches!(result,Err(InvocationError::Rejected(value)) if value.output==DeliveryOutcome::InvalidState)
    );
    let void = pay(&client, spec(1), PaymentAction::Void).await;
    let auth = pay(&client, spec(1), PaymentAction::Authorize).await;
    assert_eq!(auth, void);
    assert_eq!((auth.authorizations, auth.voids), (0, 1));
    assert_eq!(
        client
            .stock("widget".into(), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .available,
        1
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn old_reserve_delivery_and_duplicate_release_cannot_apply_twice() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    seed(&client, 3).await;
    let reserve = call(1, 1, Operation::Reserve);
    let release = call(1, 2, Operation::Release);
    for call in [reserve.clone(), release.clone(), release, reserve] {
        assert_eq!(
            command::<StockStep>(&handle, &client, INVENTORY, call).await,
            DeliveryOutcome::Applied
        );
    }
    assert_eq!(
        client
            .stock("widget".into(), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .available,
        3
    );
    assert_eq!(
        client
            .reservation(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        ReservationStatus::Released
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn payment_retries_bind_full_spec_and_apply_authorization_and_void_once() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (_, client) = setup(&node).await;
    let original = pay(&client, spec(1), PaymentAction::Authorize).await;
    assert_eq!(
        pay(&client, spec(1), PaymentAction::Authorize).await,
        original
    );
    let mut changed = spec(1);
    changed.amount += 1;
    let result = client
        .prepare_payment(
            new_identity().unwrap(),
            PaymentWork {
                spec: changed,
                action: PaymentAction::Void,
            },
        )
        .await
        .unwrap()
        .execute()
        .await;
    assert!(
        matches!(result,Err(InvocationError::Rejected(value)) if value.output==DeliveryOutcome::Conflict)
    );
    let void = pay(&client, spec(1), PaymentAction::Void).await;
    assert_eq!(pay(&client, spec(1), PaymentAction::Void).await, void);
    assert_eq!(pay(&client, spec(1), PaymentAction::Authorize).await, void);
    assert_eq!((void.authorizations, void.voids), (1, 1));
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn fulfilled_order_requires_decision_stock_commit_and_terminal_callback() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    seed(&client, 1).await;
    start(&handle, &client, spec(1)).await;
    let reserved = step(&handle, &client, id(1)).await;
    let payment = pay(&client, spec(1), PaymentAction::Authorize).await;
    complete(&handle, &client, PaymentObservation::Known(payment), false).await;
    assert_eq!(
        client
            .order(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        OrderStatus::Pending
    );
    step(&handle, &client, id(1)).await;
    assert_eq!(
        client
            .order(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        OrderStatus::Committing
    );
    let cancel = client
        .prepare_order(new_identity().unwrap(), OrderChange::Cancel(id(1)))
        .await
        .unwrap()
        .execute()
        .await;
    assert!(
        matches!(cancel,Err(InvocationError::Rejected(value)) if value.output==OrderOutcome::TooLate)
    );
    step(&handle, &client, id(1)).await;
    assert_ne!(
        client
            .saga(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        "completed"
    );
    let finished = step(&handle, &client, id(1)).await;
    assert_eq!(finished.value, ReplyValue::Finished(OrderResult::Fulfilled));
    assert_eq!(
        client
            .saga(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .phase,
        Phase::Completed
    );
    assert_eq!(
        client
            .stock("widget".into(), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .sold,
        1
    );
    assert_eq!(
        command::<ReplySaga>(&handle, &client, SAGAS, reserved.clone()).await,
        DeliveryOutcome::Applied
    );
    let changed = Reply {
        value: ReplyValue::Unavailable,
        ..reserved
    };
    let result = handle
        .prepare_command::<ReplySaga>(
            &client.target(SAGAS).unwrap(),
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
async fn cancellation_after_authorization_voids_before_releasing_and_finishing() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    seed(&client, 1).await;
    start(&handle, &client, spec(1)).await;
    step(&handle, &client, id(1)).await;
    let payment = pay(&client, spec(1), PaymentAction::Authorize).await;
    client
        .prepare_order(new_identity().unwrap(), OrderChange::Cancel(id(1)))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    complete(&handle, &client, PaymentObservation::Known(payment), false).await;
    step(&handle, &client, id(1)).await;
    assert_eq!(
        client
            .saga(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .phase,
        Phase::Voiding
    );
    assert_eq!(
        client
            .stock("widget".into(), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .held,
        1
    );
    let void = pay(&client, spec(1), PaymentAction::Void).await;
    complete(&handle, &client, PaymentObservation::Known(void), false).await;
    step(&handle, &client, id(1)).await;
    step(&handle, &client, id(1)).await;
    assert_eq!(
        client
            .order(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        OrderStatus::Finished(OrderResult::Cancelled)
    );
    assert_eq!(
        client
            .stock("widget".into(), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .available,
        1
    );
    assert_eq!(
        client
            .prepare_order(new_identity().unwrap(), OrderChange::Cancel(id(1)))
            .await
            .unwrap()
            .execute()
            .await
            .unwrap()
            .output,
        OrderOutcome::CancellationRequested
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn declined_and_unavailable_orders_settle_without_consuming_inventory() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    seed(&client, 1).await;
    let mut declined = spec(1);
    declined.payment_policy = PaymentPolicy::Decline;
    start(&handle, &client, declined.clone()).await;
    step(&handle, &client, id(1)).await;
    let payment = pay(&client, declined, PaymentAction::Authorize).await;
    complete(&handle, &client, PaymentObservation::Known(payment), false).await;
    step(&handle, &client, id(1)).await;
    step(&handle, &client, id(1)).await;
    assert_eq!(
        client
            .order(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        OrderStatus::Finished(OrderResult::Declined)
    );
    let mut unavailable = spec(2);
    unavailable.quantity = 2;
    start(&handle, &client, unavailable).await;
    step(&handle, &client, id(2)).await;
    step(&handle, &client, id(2)).await;
    assert_eq!(
        client
            .order(id(2), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        OrderStatus::Finished(OrderResult::OutOfStock)
    );
    assert!(client.payment(id(2), None).await.unwrap().output.is_none());
    assert_eq!(
        client
            .stock("widget".into(), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .available,
        1
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn unknown_authorization_holds_stock_and_manual_reconciliation_reuses_payment() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    seed(&client, 1).await;
    start(&handle, &client, spec(1)).await;
    step(&handle, &client, id(1)).await;
    let applied = pay(&client, spec(1), PaymentAction::Authorize).await;
    complete(&handle, &client, PaymentObservation::Unknown, true).await;
    step(&handle, &client, id(1)).await;
    let saga = client.saga(id(1), None).await.unwrap().output.unwrap();
    assert_eq!(
        (saga.state.phase, saga.status),
        (Phase::NeedsReview, "running".into())
    );
    assert_eq!(
        client
            .order(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        OrderStatus::NeedsReview
    );
    assert_eq!(
        client
            .stock("widget".into(), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .held,
        1
    );
    let input = Reconcile {
        order: id(1),
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
            .saga(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .reconciliations,
        1
    );
    assert_eq!(
        pay(&client, spec(1), PaymentAction::Authorize).await,
        applied
    );
    complete(&handle, &client, PaymentObservation::Known(applied), false).await;
    for _ in 0..3 {
        step(&handle, &client, id(1)).await;
    }
    assert_eq!(
        client
            .saga(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .result,
        Some(OrderResult::Fulfilled)
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn unknown_void_cannot_release_stock_until_verified_and_review_attempts_are_bounded() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    seed(&client, 1).await;
    start(&handle, &client, spec(1)).await;
    step(&handle, &client, id(1)).await;
    let payment = pay(&client, spec(1), PaymentAction::Authorize).await;
    client
        .prepare_order(new_identity().unwrap(), OrderChange::Cancel(id(1)))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    complete(
        &handle,
        &client,
        PaymentObservation::Known(payment.clone()),
        false,
    )
    .await;
    step(&handle, &client, id(1)).await;
    complete(&handle, &client, PaymentObservation::Known(payment), false).await;
    step(&handle, &client, id(1)).await;
    for attempt in 0..MAX_RECONCILIATIONS {
        assert_eq!(
            client
                .saga(id(1), None)
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
                .stock("widget".into(), None)
                .await
                .unwrap()
                .output
                .unwrap()
                .held,
            1
        );
        client
            .prepare_reconcile(
                new_identity().unwrap(),
                Reconcile {
                    order: id(1),
                    token: id(u128::from(attempt) + 100),
                },
            )
            .await
            .unwrap()
            .execute()
            .await
            .unwrap();
        complete(&handle, &client, PaymentObservation::Unknown, false).await;
        step(&handle, &client, id(1)).await;
    }
    let result = client
        .prepare_reconcile(
            new_identity().unwrap(),
            Reconcile {
                order: id(1),
                token: id(1000),
            },
        )
        .await
        .unwrap()
        .execute()
        .await;
    assert!(
        matches!(result,Err(InvocationError::Rejected(value)) if value.output==DeliveryOutcome::Capacity)
    );
    assert_eq!(
        client
            .stock("widget".into(), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .held,
        1
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn misplaced_callback_rolls_back_binding_and_can_be_applied_at_its_real_phase() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    seed(&client, 1).await;
    start(&handle, &client, spec(1)).await;
    let pending = client
        .saga(id(1), None)
        .await
        .unwrap()
        .output
        .unwrap()
        .state
        .waiting
        .unwrap();
    let mut foreign = pending.clone();
    foreign.step = id(900).bytes();
    let bad = handle
        .prepare_command::<ReplySaga>(
            &client.target(SAGAS).unwrap(),
            new_identity().unwrap(),
            Reply {
                call: foreign,
                value: ReplyValue::Held,
            },
        )
        .await
        .unwrap()
        .execute()
        .await;
    assert!(bad.is_err());
    assert_eq!(
        client
            .saga(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .waiting,
        Some(pending)
    );
    step(&handle, &client, id(1)).await;
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn cold_restore_preserves_external_binding_held_stock_and_original_placement_receipt() {
    let root = tempfile::tempdir().unwrap();
    let authoritative = store();
    let first = node(authoritative.clone(), &root.path().join("first")).await;
    let (handle, client) = setup(&first).await;
    seed(&client, 2).await;
    let identity = new_identity().unwrap();
    let prepared = client
        .prepare_order(identity, OrderChange::Place(spec(1)))
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let placed = prepared.execute().await.unwrap();
    command::<StartSaga>(&handle, &client, SAGAS, spec(1)).await;
    step(&handle, &client, id(1)).await;
    let applied = pay(&client, spec(1), PaymentAction::Authorize).await;
    first.shutdown().await.unwrap();
    let restored = node(authoritative, &root.path().join("cold")).await;
    let (handle, client) = setup(&restored).await;
    assert!(
        matches!(client.resolve(&evidence).await.unwrap(),Resolution::Committed(value) if value.commit_sequence()==placed.receipt.commit_sequence)
    );
    assert_eq!(
        client.payment(id(1), None).await.unwrap().output,
        Some(applied.clone())
    );
    assert_eq!(
        client
            .stock("widget".into(), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .held,
        1
    );
    complete(&handle, &client, PaymentObservation::Known(applied), false).await;
    for _ in 0..3 {
        step(&handle, &client, id(1)).await;
    }
    assert_eq!(
        client
            .order(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .status,
        OrderStatus::Finished(OrderResult::Fulfilled)
    );
    restored.shutdown().await.unwrap();
}
#[test]
fn validation_rejects_aliases_impossible_payment_counts_and_invalid_causal_replies() {
    for endpoint in [
        "http://localhost:19025/",
        "http://127.0.0.1/",
        "http://127.0.0.1:19025/payment",
        "http://127.0.0.1:19025/?x=1",
    ] {
        assert!(validate_endpoint(endpoint).is_err());
    }
    for sku in ["", "UPPER", "-widget", "widget-", "widget/x"] {
        assert!(validate_sku(sku).is_err());
    }
    assert!(Id::try_from("00000000-0000-0000-0000-000000000000".to_owned()).is_err());
    assert!(
        Page {
            after: 0,
            limit: 101
        }
        .validate()
        .is_err()
    );
    assert!(
        Payment {
            spec: spec(1),
            status: PaymentStatus::Authorized,
            authorizations: 0,
            voids: 0
        }
        .validate()
        .is_err()
    );
    assert!(
        Reply {
            call: call(1, 1, Operation::Reserve),
            value: ReplyValue::Released
        }
        .validate()
        .is_err()
    );
    assert!(
        Stock {
            sku: "widget".into(),
            total: 1,
            available: u32::MAX,
            held: 1,
            sold: 1
        }
        .validate()
        .is_err()
    );
}

#[test]
fn version_one_wire_fixture_rejects_changed_version_and_noncanonical_json() {
    use cellule_runtime::codec::{BoundedDecoder, BoundedEncoder, WireValue};
    let value = id(1);
    let mut encoder = BoundedEncoder::new(128).unwrap();
    value.encode(&mut encoder).unwrap();
    let bytes = encoder.finish();
    let mut fixture = vec![1, 0, 0, 0, 38];
    fixture.extend_from_slice(b"\"00000000-0000-0000-0000-000000000001\"");
    assert_eq!(bytes, fixture);
    let mut decoder = BoundedDecoder::new(&bytes, 128).unwrap();
    assert_eq!(Id::decode(&mut decoder).unwrap(), value);
    decoder.finish().unwrap();
    let mut wrong = bytes.clone();
    wrong[0] = 2;
    assert!(Id::decode(&mut BoundedDecoder::new(&wrong, 128).unwrap()).is_err());
    let mut noncanonical = BoundedEncoder::new(128).unwrap();
    noncanonical.write_u8(1).unwrap();
    noncanonical
        .write_bytes(b" \"00000000-0000-0000-0000-000000000001\"")
        .unwrap();
    assert!(Id::decode(&mut BoundedDecoder::new(&noncanonical.finish(), 128).unwrap()).is_err());
}
#[tokio::test]
async fn permanent_product_capacity_rejections_resolve_and_keyset_pages_are_bounded() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (_, client) = setup(&node).await;
    for value in 1..=MAX_PRODUCTS {
        client
            .prepare_seed(
                new_identity().unwrap(),
                Seed {
                    sku: format!("sku-{value}"),
                    units: 1,
                },
            )
            .await
            .unwrap()
            .execute()
            .await
            .unwrap();
    }
    let identity = new_identity().unwrap();
    let prepared = client
        .prepare_seed(
            identity,
            Seed {
                sku: "overflow".into(),
                units: 1,
            },
        )
        .await
        .unwrap();
    let evidence = prepared.evidence().clone();
    let result = prepared.execute().await;
    let rejected = match result {
        Err(InvocationError::Rejected(value)) => value,
        _ => panic!(),
    };
    assert_eq!(rejected.output, DeliveryOutcome::Capacity);
    assert!(
        matches!(client.resolve(&evidence).await.unwrap(),Resolution::Committed(value) if value.commit_sequence()==rejected.receipt.commit_sequence)
    );
    for value in 1..=3 {
        client
            .prepare_order(new_identity().unwrap(), OrderChange::Place(spec(value)))
            .await
            .unwrap()
            .execute()
            .await
            .unwrap();
    }
    let first = client
        .orders(Page { after: 0, limit: 2 }, None)
        .await
        .unwrap()
        .output;
    assert_eq!(first.orders.len(), 2);
    let second = client
        .orders(
            Page {
                after: first.next.unwrap(),
                limit: 2,
            },
            None,
        )
        .await
        .unwrap()
        .output;
    assert_eq!(second.orders.len(), 1);
    assert!(second.next.is_none());
    assert_eq!(
        [
            first.orders[0].1.spec.id,
            first.orders[1].1.spec.id,
            second.orders[0].1.spec.id
        ],
        [id(1), id(2), id(3)]
    );
    node.shutdown().await.unwrap();
}
#[tokio::test]
async fn cancellation_before_stock_failure_reports_actual_terminal_result_without_payment() {
    let root = tempfile::tempdir().unwrap();
    let node = node(store(), root.path()).await;
    let (handle, client) = setup(&node).await;
    start(&handle, &client, spec(1)).await;
    client
        .prepare_order(new_identity().unwrap(), OrderChange::Cancel(id(1)))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    step(&handle, &client, id(1)).await;
    let terminal = step(&handle, &client, id(1)).await;
    assert_eq!(terminal.value, ReplyValue::Finished(OrderResult::Cancelled));
    assert_eq!(
        client
            .saga(id(1), None)
            .await
            .unwrap()
            .output
            .unwrap()
            .state
            .result,
        Some(OrderResult::Cancelled)
    );
    assert!(client.payment(id(1), None).await.unwrap().output.is_none());
    node.shutdown().await.unwrap();
}
