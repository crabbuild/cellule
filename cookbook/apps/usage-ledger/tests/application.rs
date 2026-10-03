//! Public account identity, close-barrier, reconciliation, and sealing behavior.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use cellule_app::ApplicationHandle;
use cellule_cookbook_support::{LocalNode, NodeConfig, new_identity};
use cellule_cookbook_usage_ledger::*;
use cellule_runtime::{ApplicationId, InvocationError, TenantId};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path as StorePath};
use std::sync::Arc;

const APP: ApplicationId = ApplicationId::from_bytes([0xb1; 16]);
const TENANT: TenantId = TenantId::from_bytes([0xb2; 16]);

async fn start(root: &std::path::Path) -> (LocalNode, ApplicationHandle<UsageLedger>) {
    let node = LocalNode::start(
        compile().unwrap(),
        Store::new(Arc::new(InMemory::new())),
        NodeConfig {
            state_directory: root.join("node"),
            storage_prefix: StorePath::from("usage-ledger/tests"),
            application_id: APP,
        },
    )
    .await
    .unwrap();
    let handle = node.application_handle::<UsageLedger>(TENANT).unwrap();
    (node, handle)
}

fn event(period: [u8; 16], account: AccountKey, id: u8, amount: u64) -> UsageEvent {
    UsageEvent {
        period_id: period,
        id: [id; 16],
        account,
        occurred_at_ms: 1500,
        amount_microcredits: amount,
        category: "compute".into(),
    }
}

#[tokio::test]
async fn permanent_events_and_close_snapshot_make_delayed_projection_reconcilable() {
    let temporary = tempfile::tempdir().unwrap();
    let (node, handle) = start(temporary.path()).await;
    let accounts = vec![
        AccountKey::new("alpha").unwrap(),
        AccountKey::new("beta").unwrap(),
    ];
    let spec = PeriodSpec {
        id: [0xc1; 16],
        start_ms: 1000,
        end_ms: 5000,
        accounts: accounts.clone(),
    };
    let mut clients = Vec::new();
    for key in &accounts {
        let client = AccountClient::new(handle.clone(), key.clone()).unwrap();
        node.open_cell(client.target(), &Accounts).await.unwrap();
        let result = client
            .bind(new_identity().unwrap(), spec.clone())
            .await
            .unwrap();
        assert_eq!(result.output.account, *key);
        clients.push(client);
    }
    let period = PeriodClient::new(handle.clone(), spec.id).unwrap();
    node.open_cell(period.target(), &Periods).await.unwrap();
    assert_eq!(
        period
            .create(new_identity().unwrap(), spec.clone())
            .await
            .unwrap()
            .output,
        PeriodDecision::Created
    );
    for (client, key) in clients.iter().zip(&accounts) {
        assert!(
            !client
                .activate(new_identity().unwrap(), spec.clone())
                .await
                .unwrap()
                .output
                .closed
        );
        period
            .confirm_ready(new_identity().unwrap(), key.clone())
            .await
            .unwrap();
    }
    assert_eq!(
        period.get(None).await.unwrap().output.unwrap().status,
        PeriodStatus::Open
    );

    let source = event(spec.id, accounts[0].clone(), 1, 1250);
    assert_eq!(
        clients[0]
            .record(new_identity().unwrap(), source.clone())
            .await
            .unwrap()
            .output,
        UsageDecision::Accepted
    );
    assert_eq!(
        clients[0]
            .record(new_identity().unwrap(), source.clone())
            .await
            .unwrap()
            .output,
        UsageDecision::Duplicate
    );
    let changed = event(spec.id, accounts[0].clone(), 1, 1251);
    assert!(matches!(
        clients[0].record(new_identity().unwrap(), changed).await,
        Err(InvocationError::Rejected(value)) if value.output == UsageDecision::Conflict
    ));
    assert_eq!(
        period
            .get(None)
            .await
            .unwrap()
            .output
            .unwrap()
            .projected_events,
        0,
        "the source fact remains durable while its independent Effect is delayed"
    );

    assert_eq!(
        period
            .begin_close(new_identity().unwrap())
            .await
            .unwrap()
            .output,
        PeriodDecision::Closing
    );
    let first = clients[0]
        .close(new_identity().unwrap(), spec.id)
        .await
        .unwrap()
        .output;
    let empty = clients[1]
        .close(new_identity().unwrap(), spec.id)
        .await
        .unwrap()
        .output;
    assert_eq!(first.events, vec![source.clone()]);
    assert!(empty.events.is_empty());
    assert_eq!(
        clients[0]
            .close(new_identity().unwrap(), spec.id)
            .await
            .unwrap()
            .output,
        first,
        "account close retries return the exact original source snapshot"
    );
    assert_eq!(
        clients[0]
            .record(new_identity().unwrap(), source.clone())
            .await
            .unwrap()
            .output,
        UsageDecision::Duplicate
    );
    let late_event = clients[0]
        .record(
            new_identity().unwrap(),
            event(spec.id, accounts[0].clone(), 2, 500),
        )
        .await;
    assert!(matches!(
        late_event,
        Err(InvocationError::Rejected(value)) if value.output == UsageDecision::Closed
    ));
    assert!(period.seal(new_identity().unwrap()).await.is_err());

    for snapshot in [first.clone(), empty] {
        assert_eq!(
            period
                .reconcile(new_identity().unwrap(), spec.clone(), snapshot)
                .await
                .unwrap()
                .output,
            PeriodDecision::Reconciled
        );
    }
    let report = period.seal(new_identity().unwrap()).await.unwrap().output;
    report.validate().unwrap();
    assert_eq!(report.events, vec![source]);
    assert_eq!(report.event_count, 1);
    assert_eq!(report.total_microcredits, 1250);
    assert_eq!(
        period.get(None).await.unwrap().output.unwrap().status,
        PeriodStatus::Sealed
    );
    assert_eq!(
        period.seal(new_identity().unwrap()).await.unwrap().output,
        report
    );
    node.shutdown().await.unwrap();
}
