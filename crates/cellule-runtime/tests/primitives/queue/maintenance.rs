//! Exact native Queue completion during sticky foreground quiescence.

use super::*;

#[tokio::test]
async fn maintenance_quiescence_preserves_native_queue_completion_and_validation() {
    let baseline = queue_registry();
    let mut builder = RegistryBuilder::new(BuildDescriptor {
        source_revision: "queue-api-test".into(),
        cargo_lock_digest: Digest::from_bytes([5; 32]),
    });
    builder.register(TestQueue).unwrap();
    assert!(builder.bind_command::<ReplacementLease>().is_err());
    assert!(builder.bind_query::<ReplacementValidation>().is_err());
    let registry = Arc::new(builder.finish().unwrap());
    assert_eq!(registry.release_bytes(), baseline.release_bytes());
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        ApplicationId::from_bytes([3; 16]),
        QUEUE_NAMESPACE,
        &0_u32.to_be_bytes(),
    )
    .unwrap();
    let cell = target.cell_id();
    let incarnation = IncarnationId::from_bytes([2; 16]);
    let store = Store::new(Arc::new(InMemory::new()));
    let layout = CellStorageLayout::new(store, Path::from("runtime"), [3; 16]);
    let replica = CellReplica::new(
        layout.clone(),
        *cell.as_bytes(),
        *incarnation.as_bytes(),
        Limits::default(),
    )
    .unwrap();
    let catalog = CellCatalog::new(layout.clone(), target.tenant());
    let proof = catalog
        .provision(
            CatalogEntry::new(
                &target,
                CatalogRole::Queue,
                registry.module_code(QUEUE_MODULE).unwrap(),
                1,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let authority = CellAuthority::new(layout.clone());
    let first_session = SessionId::from_bytes([4; 16]);
    let observed = authority
        .create_initial(
            &proof,
            incarnation,
            Owner {
                session: first_session,
                endpoint: "https://first.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let directory = tempfile::TempDir::new().unwrap();
    let runtime = CellRuntime::new(
        SqlWorkerPool::new(1, 10).unwrap(),
        16 * 1024 * 1024,
        first_session,
    )
    .unwrap();
    let handle = runtime
        .bootstrap(
            proof.clone(),
            replica.clone(),
            authority.clone(),
            observed,
            directory.path().join("first.sqlite"),
            install_queue_schema,
        )
        .await
        .unwrap();
    let queue = QueueNamespace::<TestQueue>::new(
        CellClient::local(registry.clone(), handle.clone()),
        target.tenant(),
        target.application(),
    )
    .unwrap();
    use cellule_runtime::Error;
    use cellule_runtime::cell::actor::CellInventoryEntry;
    use cellule_runtime::primitives::maintenance_readiness::MaintenanceWorkBlocker;

    let sent = queue
        .send(
            mutation_identity(20),
            QueueSendRequest {
                producer_id: [20; 16],
                payload: b"maintenance-job".to_vec(),
                available_at_ms: now_ms(),
            },
        )
        .await
        .unwrap();
    assert!(matches!(sent.output, QueueSendOutcome::Sent { .. }));
    let claimed = queue
        .claim(
            mutation_identity(21),
            0,
            QueueClaimRequest {
                limit: 1,
                lease_ms: 30_000,
            },
        )
        .await
        .unwrap();
    assert_eq!(claimed.output.len(), 1);
    let page = runtime.fleet_cells_page(None, 128).await.unwrap();
    let CellInventoryEntry::Owned(owner) = &page.entries()[0] else {
        panic!("owner missing");
    };
    let generation = owner.generation;
    let epoch = owner.position.as_ref().unwrap().epoch;
    drop(page);
    for (source, generation, incarnation, epoch) in [
        (
            SessionId::from_bytes([90; 16]),
            generation,
            incarnation,
            epoch,
        ),
        (first_session, generation + 1, incarnation, epoch),
        (
            first_session,
            generation,
            IncarnationId::from_bytes([90; 16]),
            epoch,
        ),
        (first_session, generation, incarnation, epoch + 1),
    ] {
        assert!(matches!(
            runtime
                .quiesce_cell_at(cell, source, generation, incarnation, epoch)
                .await,
            Err(Error::Fenced)
        ));
        assert!(queue.info(0, None).await.is_ok());
    }
    runtime
        .quiesce_cell_at(cell, first_session, generation, incarnation, epoch)
        .await
        .unwrap();
    runtime
        .quiesce_cell_at(cell, first_session, generation, incarnation, epoch)
        .await
        .unwrap();
    let page = runtime.fleet_cells_page(None, 128).await.unwrap();
    let CellInventoryEntry::Owned(owner) = &page.entries()[0] else {
        panic!("owner missing");
    };
    assert!(owner.quiescing);
    drop(page);
    assert!(matches!(
        queue
            .send(
                mutation_identity(22),
                QueueSendRequest {
                    producer_id: [22; 16],
                    payload: vec![1],
                    available_at_ms: now_ms()
                }
            )
            .await,
        Err(InvocationError::NotStarted(Error::CellDraining))
    ));
    assert!(matches!(
        queue
            .claim(
                mutation_identity(23),
                0,
                QueueClaimRequest {
                    limit: 1,
                    lease_ms: 10_000
                }
            )
            .await,
        Err(InvocationError::NotStarted(Error::CellDraining))
    ));
    assert!(matches!(
        queue.info(0, None).await,
        Err(InvocationError::NotStarted(Error::CellDraining))
    ));
    // Raw callbacks cannot assert the native completion capability.
    assert!(matches!(
        handle.query(1, 1, |_| panic!("raw query admitted")).await,
        Err(Error::CellDraining)
    ));
    assert!(
        queue
            .validate_claim(0, claimed.output.clone(), Some(claimed.receipt))
            .await
            .unwrap()
            .output
    );
    let mut incorrect = claimed.output.clone();
    incorrect[0].token = [99; 16];
    assert!(
        !queue
            .validate_claim(0, incorrect, Some(claimed.receipt))
            .await
            .unwrap()
            .output
    );
    // Inventory uses the same serialized connection and never revokes a lease.
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let page = runtime.fleet_cells_page(None, 128).await.unwrap();
            if let CellInventoryEntry::Owned(owner) = &page.entries()[0]
                && owner
                    .maintenance_work
                    .is_some_and(|work| work.has_blocker(MaintenanceWorkBlocker::QueueLease))
            {
                break;
            }
            drop(page);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let acked = queue
        .ack(
            mutation_identity(24),
            0,
            claimed.output[0].message_id,
            claimed.output[0].token,
        )
        .await
        .unwrap();
    assert_eq!(
        acked.output,
        QueueLeaseOutcome::Applied {
            state: QueueState::Acked,
            lease_until_ms: None
        }
    );
    assert!(
        !queue
            .validate_claim(0, claimed.output, Some(acked.receipt))
            .await
            .unwrap()
            .output
    );
    assert!(matches!(
        queue.info(0, None).await,
        Err(InvocationError::NotStarted(Error::CellDraining))
    ));
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let page = runtime.fleet_cells_page(None, 128).await.unwrap();
            if let CellInventoryEntry::Owned(owner) = &page.entries()[0]
                && owner
                    .maintenance_work
                    .is_some_and(|work| work.is_transferable())
            {
                assert!(owner.quiescing);
                break;
            }
            drop(page);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(runtime.stats().retained_bytes(), 0);
}

// Duplicate application bindings cannot replace native handlers while keeping
// their private completion classification, even if the caller ignores Err.
struct ReplacementLease;
impl cellule_runtime::registry::Command for ReplacementLease {
    const MODULE: &'static str = QUEUE_MODULE;
    const ID: u32 = 3;
    const CODEC_VERSION: u32 = 1;
    type Input = cellule_runtime::primitives::queue::QueueLeaseRequest;
    type Output = QueueLeaseOutcome;
    fn execute(
        _: &mut cellule_runtime::registry::CommandContext<'_, '_>,
        _: Self::Input,
    ) -> cellule_runtime::Result<cellule_runtime::registry::CommandResult<Self::Output>> {
        panic!("duplicate lease handler replaced native completion");
    }
}
struct ReplacementValidation;
impl cellule_runtime::registry::Query for ReplacementValidation {
    const MODULE: &'static str = QUEUE_MODULE;
    const ID: u32 = 1;
    const CODEC_VERSION: u32 = 1;
    type Input = cellule_runtime::primitives::queue::QueueValidateRequest;
    type Output = bool;
    fn execute(
        _: &mut cellule_runtime::registry::QueryContext<'_>,
        _: Self::Input,
    ) -> cellule_runtime::Result<Self::Output> {
        panic!("duplicate validation handler replaced native completion");
    }
}
