#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use cellule_store::Store;
use object_store::memory::InMemory;
use std::sync::Arc;
async fn local(store: Store, path: &Path, prefix: &str) -> LocalNode {
    LocalNode::start(
        compile().unwrap(),
        store,
        NodeConfig {
            state_directory: path.into(),
            storage_prefix: object_store::path::Path::from(prefix),
            application_id: APPLICATION,
        },
    )
    .await
    .unwrap()
}
async fn payments(node: &LocalNode, fault: Option<PathBuf>) -> (CheckoutClient, String) {
    let handle = node
        .application_handle::<CheckoutApplication>(TENANT)
        .unwrap();
    let client = CheckoutClient::new(handle);
    node.open_cell(&client.target(PAYMENTS).unwrap(), &Payments)
        .await
        .unwrap();
    let address = payment_server::install(node, client.clone(), 0, fault)
        .await
        .unwrap();
    (client, format!("http://{address}/"))
}
fn spec(endpoint: &str, policy: PaymentPolicy) -> OrderSpec {
    OrderSpec {
        id: Id::from_bytes(*uuid::Uuid::now_v7().as_bytes()).unwrap(),
        sku: "widget".into(),
        quantity: 1,
        amount: 125,
        payment_endpoint: endpoint.into(),
        payment_policy: policy,
    }
}
async fn seed(client: &CheckoutClient) {
    client
        .prepare_seed(
            new_identity().unwrap(),
            Seed {
                sku: "widget".into(),
                units: 10,
            },
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
}
fn token() -> String {
    std::env::var("CELLULE_CHECKOUT_PAYMENT_TOKEN")
        .unwrap_or_else(|_| "cookbook-local-payment".into())
}
#[tokio::test]
async fn real_http_reply_loss_and_signed_sagas_settle_each_business_identity_once() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let source = local(store.clone(), &root.path().join("checkout"), "checkout").await;
    let receiver = local(store, &root.path().join("payment"), "payment").await;
    let fixture = root.path().join("fault");
    write(&fixture, b"drop-authorize-reply\n", false).unwrap();
    let (external, endpoint) = payments(&receiver, Some(fixture)).await;
    let handle = source
        .application_handle::<CheckoutApplication>(TENANT)
        .unwrap();
    let client = open(&source, &handle).await.unwrap();
    seed(&client).await;
    let fulfilled = spec(&endpoint, PaymentPolicy::Approve);
    let cancelled = spec(&endpoint, PaymentPolicy::Approve);
    let declined = spec(&endpoint, PaymentPolicy::Decline);
    let mut unavailable = spec(&endpoint, PaymentPolicy::Approve);
    unavailable.sku = "missing".into();
    for value in [&fulfilled, &cancelled, &declined, &unavailable] {
        client
            .prepare_order(new_identity().unwrap(), OrderChange::Place(value.clone()))
            .await
            .unwrap()
            .execute()
            .await
            .unwrap();
    }
    client
        .prepare_order(new_identity().unwrap(), OrderChange::Cancel(cancelled.id))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    spawn_workers(&source, handle).await.unwrap();
    for (value, result) in [
        (&fulfilled, OrderResult::Fulfilled),
        (&cancelled, OrderResult::Cancelled),
        (&declined, OrderResult::Declined),
        (&unavailable, OrderResult::OutOfStock),
    ] {
        wait(&client, &source, value.id, result).await.unwrap();
    }
    let authorization = external
        .payment(fulfilled.id, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!((authorization.authorizations, authorization.voids), (1, 0));
    let void = external
        .payment(cancelled.id, None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!((void.authorizations, void.voids), (1, 1));
    assert_eq!(void.status, PaymentStatus::Voided);
    assert!(
        external
            .payment(unavailable.id, None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    let stock = client
        .stock("widget".into(), None)
        .await
        .unwrap()
        .output
        .unwrap();
    assert_eq!((stock.available, stock.held, stock.sold), (9, 0, 1));
    source.shutdown().await.unwrap();
    receiver.shutdown().await.unwrap();
}
#[tokio::test]
async fn actual_http_outage_requires_manual_review_before_same_key_reconciliation() {
    let root = tempfile::tempdir().unwrap();
    let store = Store::new(Arc::new(InMemory::new()));
    let source = local(store.clone(), &root.path().join("checkout"), "checkout").await;
    let receiver = local(store, &root.path().join("payment"), "payment").await;
    let fixture = root.path().join("fault");
    write(&fixture, b"down\n", false).unwrap();
    let (external, endpoint) = payments(&receiver, Some(fixture.clone())).await;
    let handle = source
        .application_handle::<CheckoutApplication>(TENANT)
        .unwrap();
    let client = open(&source, &handle).await.unwrap();
    seed(&client).await;
    let value = spec(&endpoint, PaymentPolicy::Approve);
    client
        .prepare_order(new_identity().unwrap(), OrderChange::Place(value.clone()))
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    spawn_workers(&source, handle).await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        if client
            .saga(value.id, None)
            .await
            .unwrap()
            .output
            .is_some_and(|value| value.state.phase == Phase::NeedsReview)
        {
            break;
        }
        assert!(source.is_ready() && tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
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
    assert!(
        external
            .payment(value.id, None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    write(&fixture, b"up\n", true).unwrap();
    client
        .prepare_reconcile(
            new_identity().unwrap(),
            Reconcile {
                order: value.id,
                token: Id::from_bytes(*uuid::Uuid::now_v7().as_bytes()).unwrap(),
            },
        )
        .await
        .unwrap()
        .execute()
        .await
        .unwrap();
    wait(&client, &source, value.id, OrderResult::Fulfilled)
        .await
        .unwrap();
    assert_eq!(
        external
            .payment(value.id, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .authorizations,
        1
    );
    source.shutdown().await.unwrap();
    receiver.shutdown().await.unwrap();
}
#[tokio::test]
async fn payment_ingress_rejects_credentials_foreign_origin_and_changed_business_bytes() {
    let root = tempfile::tempdir().unwrap();
    let receiver = local(
        Store::new(Arc::new(InMemory::new())),
        root.path(),
        "payment",
    )
    .await;
    let (external, endpoint) = payments(&receiver, None).await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let work = PaymentWork {
        spec: spec(&endpoint, PaymentPolicy::Approve),
        action: PaymentAction::Authorize,
    };
    assert_eq!(
        client
            .post(format!("{endpoint}payment"))
            .json(&work)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let mut foreign = work.clone();
    foreign.spec.payment_endpoint = "http://127.0.0.1:19025/".into();
    assert_eq!(
        client
            .post(format!("{endpoint}payment"))
            .bearer_auth(token())
            .json(&foreign)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    assert!(
        external
            .payment(work.spec.id, None)
            .await
            .unwrap()
            .output
            .is_none()
    );
    assert!(
        matches!(observe_payment(&work,&token()).await.unwrap(),PaymentObservation::Known(value) if value.authorizations==1)
    );
    let mut changed = work.clone();
    changed.spec.amount += 1;
    assert_eq!(
        client
            .post(format!("{endpoint}payment"))
            .bearer_auth(token())
            .json(&changed)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::CONFLICT
    );
    let bytes = vec![b'x'; 4097];
    assert_eq!(
        client
            .post(format!("{endpoint}payment"))
            .bearer_auth(token())
            .header("content-type", "application/json")
            .body(bytes)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::PAYLOAD_TOO_LARGE
    );
    assert_eq!(
        external
            .payment(work.spec.id, None)
            .await
            .unwrap()
            .output
            .unwrap()
            .spec,
        work.spec
    );
    receiver.shutdown().await.unwrap();
}
