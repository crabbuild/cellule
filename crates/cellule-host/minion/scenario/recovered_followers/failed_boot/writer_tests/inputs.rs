//! Actual catalog roots and fsynced-tail inputs shared by original-boot cases.
use super::*;

pub(super) struct WriterInputs {
    pub(super) sources: Vec<FleetOriginalCatalogSource>,
    pub(super) layouts: Vec<CellStorageLayout>,
    pub(super) expected: Vec<Control>,
    pub(super) bases: Vec<cellule_runtime::node::log_recovery::RecoveryCell>,
    pub(super) frames: Vec<Bytes>,
    pub(super) suffix_owners: Vec<(CellAuthority, CellId)>,
}
impl WriterInputs {
    pub(super) async fn new(
        originals_per_catalog: u64,
        suffixes: bool,
        takeover: bool,
        leader: usize,
    ) -> Self {
        let compiled = crate::scenario::application::compile().unwrap();
        let code = *compiled.registry().module_digests().first().unwrap();
        let mut sources = Vec::new();
        let mut layouts = Vec::new();
        let mut expected = Vec::new();
        let mut bases = Vec::new();
        let mut frames = Vec::new();
        let mut suffix_owners = Vec::new();
        for index in 0..2 {
            let layout = CellStorageLayout::new(
                Store::new(Arc::new(InMemory::new())),
                ObjectPath::from(format!("original-catalog-{index}")),
                [index + 3; 16],
            );
            let tenant = TenantId::from_bytes([index + 1; 16]);
            let catalog = CellCatalog::new(layout.clone(), tenant);
            let authority = CellAuthority::new(layout.clone());
            // Accepted original metadata commits before process joining. Each
            // original is later removed by the sole authority takeover path.
            for n in 0_u64..originals_per_catalog {
                let target = CellTarget::new(
                    tenant,
                    catalog.application(),
                    crate::scenario::application::NAMESPACE,
                    &n.to_be_bytes(),
                )
                .unwrap();
                let proof = catalog
                    .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1).unwrap())
                    .await
                    .unwrap();
                let mut observed = authority
                    .create_initial(&proof, IncarnationId::from_bytes([10; 16]), owner(leader))
                    .await
                    .unwrap();
                if suffixes && n == 0 {
                    let root = tempfile::tempdir().unwrap();
                    let limits = Limits {
                        max_database_bytes: 64 << 20,
                        max_capture_bytes: 16 << 20,
                        ..Limits::default()
                    };
                    let mut connection =
                        rusqlite::Connection::open(root.path().join("writer.sqlite")).unwrap();
                    cellule_runtime::cell::schema::install_runtime_schema(
                        &mut connection,
                        target.cell_id(),
                        IncarnationId::from_bytes([10; 16]),
                        1,
                    )
                    .unwrap();
                    connection.close().unwrap();
                    let mut db =
                        cellule_runtime::ltx::Db::open(&root.path().join("writer.sqlite"), limits)
                            .unwrap();
                    db.transaction(|tx| {
                        tx.execute_batch(
                            "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(1); UPDATE sys_meta SET commit_sequence=1, logical_time_ms=1",
                        )
                    })
                    .unwrap();
                    let cuts = db.capture().unwrap();
                    let replica = CellReplica::new(
                        layout.clone(),
                        *target.cell_id().as_bytes(),
                        [10; 16],
                        limits,
                    )
                    .unwrap();
                    let prepared = replica.prepare(None, &cuts, 1, 1).await.unwrap();
                    observed = authority
                        .transition(
                            &observed,
                            observed.value().publish_prepared(&prepared, None).unwrap(),
                            Transition::Publish,
                        )
                        .await
                        .unwrap();
                    bases.push(cellule_runtime::node::log_recovery::RecoveryCell {
                        application: target.application(),
                        authority: authority.clone(),
                        observed: observed.clone(),
                    });
                    suffix_owners.push((authority.clone(), target.cell_id()));
                    db.transaction(|tx| tx.execute_batch("UPDATE counter SET value=2; UPDATE sys_meta SET commit_sequence=2, logical_time_ms=2"))
                        .unwrap();
                    let cuts = db.capture().unwrap();
                    let segment = &cuts.segments[0];
                    frames.push(
                        cellule_runtime::ltx::encode_node_frame(
                            cellule_runtime::ltx::NodeFrameScope {
                                leader_session: *session(leader).as_bytes(),
                                log_epoch: 4,
                                node_sequence: frames.len() as u64 + 1,
                                application: *target.application().as_bytes(),
                                cell: *target.cell_id().as_bytes(),
                                incarnation: [10; 16],
                                cell_epoch: 1,
                                commit_sequence: 2,
                            },
                            segment.info().clone(),
                            Bytes::from(std::fs::read(segment.path()).unwrap()),
                            limits,
                        )
                        .unwrap()
                        .encoded()
                        .clone(),
                    );
                    db.close().unwrap();
                }
                expected.push(observed.value().clone());
                if takeover && (!suffixes || n != 0) {
                    authority
                        .transition(
                            &observed,
                            observed.value().takeover(owner(1)).unwrap(),
                            Transition::Takeover,
                        )
                        .await
                        .unwrap();
                }
            }
            // An unused bootstrap entry is part of complete traversal.
            let target = CellTarget::new(
                tenant,
                catalog.application(),
                crate::scenario::application::NAMESPACE,
                b"unused",
            )
            .unwrap();
            catalog
                .provision(CatalogEntry::new(&target, CatalogRole::Sql, code, 1).unwrap())
                .await
                .unwrap();
            layouts.push(layout.clone());
            sources.push(
                FleetOriginalCatalogSource::new(
                    Digest::from_bytes([index + 60; 32]),
                    tenant,
                    layout,
                )
                .unwrap(),
            );
        }
        Self {
            sources,
            layouts,
            expected,
            bases,
            frames,
            suffix_owners,
        }
    }
}
