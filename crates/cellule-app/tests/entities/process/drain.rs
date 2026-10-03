//! Final authority roots must restore every follower-acknowledged write.

use super::*;
use cellule_runtime::control::ControlState;
use std::{
    fs::File,
    io::{BufWriter, Write},
};

pub(super) async fn verify_follower_roots(
    sync: &Path,
    layout: &CellStorageLayout,
    application: &cellule_app::CompiledApplication,
    expected: &[u64],
    identities: &[(u64, IncarnationId)],
) {
    let authority = CellAuthority::new(layout.clone());
    let mut output = BufWriter::new(File::create(sync.join("capacity-final-roots.tsv")).unwrap());
    writeln!(
        output,
        "entity\tcell\towner\tepoch\tincarnation\troot_sequence\troot_digest\tstate\towner_present\trestored_sequence\trestored_count"
    )
    .unwrap();
    assert_eq!(identities.len(), expected.len());
    for (entity, count) in expected.iter().enumerate() {
        // This runs only after every host has drained and withdrawn its live
        // session. Use actual authority, not the last publication observation.
        let target = entity_target(application, entity);
        let observed = authority.load(target.cell_id()).await.unwrap().unwrap();
        let control = observed.value();
        assert_eq!(control.state, ControlState::Idle);
        assert!(control.owner.is_none());
        assert_eq!((control.epoch, control.incarnation), identities[entity]);
        let root = control.ltx_root().unwrap();
        let cell_type = application.cell_types()[0];
        let replica = CellReplica::new(
            layout.clone(),
            *target.cell_id().as_bytes(),
            *control.incarnation.as_bytes(),
            Limits {
                max_database_bytes: cell_type.database_limit_bytes(),
                max_capture_bytes: cell_type.capture_limit_bytes(),
                ..Limits::default()
            },
        )
        .unwrap();
        let verified = replica.open_root(&root).await.unwrap();
        // A fresh replica and directory exclude the owners' SQLite files and
        // page caches. Restore authenticates every page before atomic install.
        let directory = tempfile::TempDir::new().unwrap();
        let path = directory.path().join("restored.sqlite");
        assert_eq!(verified.restore(&path).await.unwrap(), root.position);
        let (sequence, actual) = tokio::task::spawn_blocking(move || {
            let connection = cellule_ltx::rusqlite::Connection::open_with_flags(
                path,
                cellule_ltx::rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .unwrap();
            let sequence: u64 = connection
                .query_row(
                    "SELECT commit_sequence FROM sys_meta WHERE singleton = 1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let actual: u64 = connection
                .query_row("SELECT COUNT(*) FROM invoice_receipts", [], |row| {
                    row.get(0)
                })
                .unwrap();
            (sequence, actual)
        })
        .await
        .unwrap();
        assert_eq!(sequence, root.commit_sequence);
        assert_eq!(actual, *count);
        writeln!(
            output,
            "{entity}\t{:?}\t{}\t{}\t{:?}\t{}\t{:?}\t{:?}\t{}\t{sequence}\t{actual}",
            target.cell_id(),
            entity / ENTITIES_PER_NODE,
            control.epoch,
            control.incarnation,
            root.commit_sequence,
            control.root.as_ref().unwrap().digest,
            control.state,
            control.owner.is_some(),
        )
        .unwrap();
        output.flush().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drained_authority_root_is_read_from_fresh_disk_after_owner_shutdown() {
    let application = compiled_entities();
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        "drained-entity-root".into(),
        *ApplicationId::from_bytes([82; 16]).as_bytes(),
    );
    let owners_disk = tempfile::TempDir::new().unwrap();
    let (host, _, _) = process_node::start(
        0,
        application.clone(),
        &layout,
        owners_disk.path(),
        "https://drained-entity.internal".into(),
    )
    .await;
    let handle = provision_entity(
        &host,
        &layout,
        owners_disk.path(),
        0,
        0,
        "https://drained-entity.internal".into(),
    )
    .await;
    let client = EntityReferenceClient::new(
        ApplicationHandle::new(
            CellClient::local(application.registry(), handle),
            application.clone(),
            TenantId::from_bytes([81; 16]),
            ApplicationId::from_bytes([82; 16]),
        )
        .unwrap(),
    )
    .unwrap();
    let order = client.orders(&entity_key(0)).unwrap();
    let mut sequence = 0;
    for occurrence in 1..=3 {
        let committed = order
            .receive_cron(
                reference_identity(u8::try_from(occurrence).unwrap(), now_ms()),
                CronInvocation {
                    schedule_id: [119; 16],
                    generation: 1,
                    occurrence,
                    scheduled_at_ms: now_ms(),
                    payload: b"restore-after-drain".to_vec(),
                },
            )
            .await
            .unwrap();
        sequence = committed.receipt.commit_sequence;
    }
    let authority = CellAuthority::new(layout.clone());
    let observed = authority
        .load(entity_target(&application, 0).cell_id())
        .await
        .unwrap()
        .unwrap();
    let identities = [(observed.value().epoch, observed.value().incarnation)];
    drop(order);
    drop(client);
    host.shutdown().await.unwrap();
    owners_disk.close().unwrap();
    assert!(
        process_node::directory(&layout, &application.registry())
            .live(now_ms(), 32)
            .await
            .unwrap()
            .is_empty()
    );
    let evidence = tempfile::TempDir::new().unwrap();
    verify_follower_roots(evidence.path(), &layout, &application, &[3], &identities).await;
    let output = std::fs::read_to_string(evidence.path().join("capacity-final-roots.tsv")).unwrap();
    let lines = output.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 2);
    let row = lines[1].split('\t').collect::<Vec<_>>();
    assert_eq!(row[5].parse::<u64>().unwrap(), sequence);
    assert_eq!(&row[7..], &["Idle", "false", &sequence.to_string(), "3"]);
}
