//! Register every partially constructed owner before fallible startup work.
use super::*;

pub(super) async fn initialize(
    root: &tempfile::TempDir,
    journal: &Arc<SqliteJournal>,
    nodes: &mut Vec<Arc<CellNode>>,
    boots: &mut Vec<startup::BootOwner>,
) -> JournalResult<Inputs> {
    let app = application::compile()?;
    let code = *app
        .registry()
        .module_digests()
        .first()
        .ok_or_else(|| invalid("reader maintenance module is absent"))?;
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        ObjectPath::from("reader-maintenance-cells"),
        [3; 16],
    );
    let directory = NodeDirectory::new(
        layout.clone(),
        scope().fleet,
        Digest::from_bytes([31; 32]),
        app.registry().release_digest(),
    );
    let limits = Limits {
        max_database_bytes: 64 << 20,
        max_capture_bytes: 16 << 20,
        ..Limits::default()
    };
    let target = CellTarget::new(
        TenantId::from_bytes([1; 16]),
        scope().application,
        application::NAMESPACE,
        &[1],
    )?;
    let incarnation = IncarnationId::from_bytes([1; 16]);
    let catalog = CellCatalog::new(layout.clone(), target.tenant())
        .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1)?)
        .await?;
    let authority = CellAuthority::new(layout.clone());
    let initial = authority
        .create_initial(&catalog, incarnation, owner(0))
        .await?;
    let replica = CellReplica::new(
        layout.clone(),
        *target.cell_id().as_bytes(),
        *incarnation.as_bytes(),
        limits,
    )?;
    let records = Arc::new(HashMap::from([(
        target.cell_id(),
        Record {
            target: target.clone(),
            incarnation,
            catalog: catalog.clone(),
            replica: replica.clone(),
            authority: authority.clone(),
        },
    )]));
    let mut managers = Vec::new();
    for index in 0..3 {
        let intent = journal
            .register_initial_intent(&NodeIntent::initial(
                scope(),
                node_id(index),
                session(index),
            )?)
            .await?;
        let node = Arc::new(
            CellNodeBuilder::new(app.clone())
                .with_runtime(
                    SqlWorkerPool::new(2, 8)?.with_native_memory_limit(128 << 20)?,
                    16 << 20,
                )
                .with_replica_host(Host::default().with_local_disk_budget(DiskBudget::new(8 << 30)))
                .with_session(session(index))
                .with_fleet_startup_intent(intent.clone())
                .build()?,
        );
        nodes.push(node.clone());
        node.install_task_group(CancellationToken::new(), CancellationToken::new())?;
        node.install_fleet_actions(
            scope(),
            node_id(index),
            journal.clone(),
            Arc::new(adapters::Cells {
                records: records.clone(),
                local: index,
                root: root.path().into(),
                receiver_directory: None,
            }),
        )?;
        let manager = node.install_read_replicas(
            layout.clone(),
            directory.clone(),
            root.path().join(format!("readers-{index}")),
            limits,
        )?;
        node.install_fleet_reader_enrollment(scope(), node_id(index), journal.clone())?;
        managers.push(manager);
        let ad = startup::advertisement(index, &node, &intent).await?;
        let spec = startup::spec(&intent)?;
        boots.push(startup::BootOwner {
            node: node.clone(),
            directory: directory.clone(),
            spec: spec.clone(),
            advertisement: ad.clone(),
            guard: None,
        });
        let original = startup::enroll(journal, &directory, &spec, ad.clone(), clock()?).await?;
        let guard = NodeLeaseGuard::new(clock()?, ad.expires_at_ms())?;
        node.install_node_lease_for_startup(guard.clone())?;
        boots[index].guard = Some(guard);
        node.confirm_fleet_startup(journal.as_ref(), spec.key()?)
            .await?;
        let observed = directory
            .load(session(index), clock()?)
            .await?
            .ok_or_else(|| invalid("original reader maintenance boot is absent"))?;
        node.install_fleet_boot_withdrawal(directory.clone(), observed, original, journal.clone())?;
        node.start()?;
    }
    let handle = nodes[0]
        .runtime()
        .bootstrap(
            catalog,
            replica,
            authority,
            initial,
            root.path().join("source.sqlite"),
            |tx| {
                tx.execute_batch(
                    "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (17)",
                )?;
                Ok(())
            },
        )
        .await?;
    let now = clock()?;
    let identity = MutationIdentity {
        request_id: RequestId::from_bytes([1; 16]),
        issued_at_ms: now,
        expires_at_ms: now
            .checked_add(120_000)
            .ok_or_else(|| invalid("reader maintenance receipt deadline overflow"))?,
    };
    let digest = Digest::from_bytes([1; 32]);
    let outcome = handle
        .execute(identity, digest, now, 64, 64, |tx| {
            tx.execute_batch("UPDATE counter SET value = 29")?;
            Ok(HandlerOutcome::Success(29i64.to_be_bytes().to_vec()))
        })
        .await?;
    let acknowledged = Acknowledged {
        identity,
        digest,
        outcome,
        value: 29,
        source: handle,
    };
    let peer = ReplicaPeerClient::new(
        app.registry(),
        Arc::new(PeerSigner::new(
            session(0),
            app.registry().release_digest(),
            SigningKey::from_bytes(&[1; 32]),
        )),
        PeerPrincipal {
            issuer: "managed-owner".into(),
            subject: "live-owner".into(),
            actions: vec!["replica-maintenance".into()],
        },
        Arc::new(native_peers::NativePeers::new(
            nodes,
            &managers,
            &layout,
            directory.clone(),
        )),
    );
    let description = CellDescription {
        cell: target.cell_id(),
        incarnation,
        code,
        schema: 1,
    };
    managers[1]
        .set_target(&target, 0, 1)
        .await?
        .ok_or_else(|| invalid("reader policy is absent"))?;
    let initial_reader = boots[1]
        .refresh_capacity(1, journal.as_ref(), deadline())
        .await?;
    peer.activate(&target, &directory, initial_reader, description)
        .await?;
    let version = journal.load_snapshot(scope()).await?.registry();
    let version = journal.bootstrap_registry(version).await?;
    journal.set_scheduling(version, true).await?;
    let verifier = FleetReaderEvacuationVerifier::new(
        directory,
        CellAuthority::new(layout.clone()),
        ReadPolicyStore::new(layout),
        peer.clone(),
    );
    Ok(Inputs {
        records,
        managers,
        target,
        description,
        peer,
        verifier,
        acknowledged,
    })
}
