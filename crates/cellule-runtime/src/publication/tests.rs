use std::sync::Arc;

use bytes::Bytes;
use cellule_ltx::{CaptureBatch, CellReplica, CellStorageLayout, Db, Limits};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

use super::{
    CellDurabilitySubmitter, CellPublisher, DurabilitySubmissionOutcome, NodeDurabilitySlot,
};
use crate::Error;
use crate::control::authority::CellAuthority;
use crate::control::{Control, Owner};
use crate::identity::IncarnationId;
use crate::identity::{CellId, Digest, SessionId};

#[tokio::test]
async fn quiet_compaction_publishes_exact_root_after_eight_appends() {
    verify_compaction_append(1).await;
}

#[tokio::test]
async fn schema_migration_combines_foreground_compaction_without_an_intermediate_cas() {
    verify_compaction_append(2).await;
}

#[tokio::test]
async fn pressure_append_avoids_intermediate_root_metadata() {
    for schema in [1, 2] {
        let directory = tempfile::tempdir().unwrap();
        let mut database =
            Db::open(&directory.path().join("writer.sqlite"), Limits::default()).unwrap();
        let counted = Arc::new(cellule_store::test_support::CountingObjectStore::new(
            Arc::new(InMemory::new()),
        ));
        let cell = CellId::from_bytes([111; 32]);
        let incarnation = IncarnationId::from_bytes([112; 16]);
        let layout = CellStorageLayout::new(
            Store::new(counted.clone()),
            Path::from("composed-compaction"),
            [113; 16],
        );
        let replica = CellReplica::new(
            layout.clone(),
            *cell.as_bytes(),
            *incarnation.as_bytes(),
            Limits::default(),
        )
        .unwrap();
        let initial = Control::initial(
            cell,
            incarnation,
            Owner {
                session: SessionId::from_bytes([114; 16]),
                endpoint: "https://owner.internal".into(),
            },
            Digest::from_bytes([115; 32]),
            1,
        )
        .unwrap();
        layout
            .store()
            .create_strict(
                &layout.control_path(cell.as_bytes()),
                Bytes::from(initial.encode().unwrap()),
            )
            .await
            .unwrap();
        let authority = CellAuthority::new(layout);
        let observed = authority.load(cell).await.unwrap().unwrap();
        let mut publisher = CellPublisher::new(
            replica.clone(),
            authority.clone(),
            observed,
            directory.path().to_owned(),
        );
        for sequence in 1..=31 {
            database
                .transaction(|transaction| {
                    if sequence == 1 {
                        transaction.execute_batch("CREATE TABLE events(id INTEGER PRIMARY KEY)")?;
                    }
                    transaction.execute("INSERT INTO events VALUES (?1)", [sequence])?;
                    Ok(())
                })
                .unwrap();
            let cuts = database.capture_deferred().unwrap();
            let prepared = publisher.prepare_append(&cuts, sequence, 1).await.unwrap();
            publisher.publish_prepared(&prepared, None).await.unwrap();
        }
        let before = publisher.control().value().ltx_root().unwrap();
        let revision = publisher.control().value().revision;
        database
            .transaction(|transaction| {
                transaction.execute("INSERT INTO events VALUES (32)", [])?;
                Ok(())
            })
            .unwrap();
        let cuts = database.capture_deferred().unwrap();
        counted.reset();
        let composed = publisher.prepare_append(&cuts, 32, schema).await.unwrap();
        let requests = (counted.put_requests(), counted.counts().heads);
        assert_eq!(composed.predecessor(), Some(before));
        assert_eq!(publisher.lineage_confirmed, Some(composed.preparation()));
        let lineage = authority
            .root_lineage(composed.root())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lineage.predecessors(), &[before]);
        assert_eq!(
            authority.load(cell).await.unwrap().unwrap().value(),
            publisher.control().value()
        );

        // Verify before the reference producer can fill any missing objects.
        replica.reachable_objects(&composed.root()).await.unwrap();
        let restored = directory.path().join("restored.sqlite");
        composed.verified().restore(&restored).await.unwrap();
        let connection = cellule_ltx::rusqlite::Connection::open(restored).unwrap();
        let rows: Vec<u64> = connection
            .prepare("SELECT id FROM events ORDER BY id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<cellule_ltx::rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(rows, (1..=32).collect::<Vec<_>>());
        // The old two-step producer remains the immutable-format reference.
        let compacted = replica
            .prepare_scheduled_compaction(&before, directory.path())
            .await
            .unwrap()
            .unwrap();
        let reference = replica
            .prepare_after_compaction(&compacted, &cuts, 32, schema)
            .await
            .unwrap();
        assert_eq!(composed.root(), reference.root());
        if schema == 1 {
            publisher.publish_prepared(&composed, None).await.unwrap();
        } else {
            publisher
                .publish_migration(&composed, None, Digest::from_bytes([116; 32]), schema)
                .await
                .unwrap();
        }
        assert_eq!(publisher.control().value().revision, revision + 1);
        assert_eq!(publisher.control().value().schema, schema);
        database.close().unwrap();
        assert_eq!(
            requests,
            (8, 1),
            "only final root and lineage metadata is retained"
        );
    }
}

async fn verify_compaction_append(schema: u32) {
    let directory = tempfile::tempdir().unwrap();
    let mut database = Db::open(&directory.path().join("cell.sqlite"), Limits::default()).unwrap();
    let cell = CellId::from_bytes([41; 32]);
    let incarnation = IncarnationId::from_bytes([42; 16]);
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("quiet-compaction"),
        [43; 16],
    );
    let replica = CellReplica::new(
        layout.clone(),
        *cell.as_bytes(),
        *incarnation.as_bytes(),
        Limits::default(),
    )
    .unwrap();
    let control = Control::initial(
        cell,
        incarnation,
        Owner {
            session: SessionId::from_bytes([44; 16]),
            endpoint: "https://node.internal:8081".into(),
        },
        Digest::from_bytes([45; 32]),
        1,
    )
    .unwrap();
    layout
        .store()
        .create_strict(
            &layout.control_path(cell.as_bytes()),
            Bytes::from(control.encode().unwrap()),
        )
        .await
        .unwrap();
    let authority = CellAuthority::new(layout);
    let observed = authority.load(cell).await.unwrap().unwrap();
    let mut publisher = CellPublisher::new(
        replica.clone(),
        authority,
        observed,
        directory.path().to_owned(),
    );
    let mut prefix = None;
    for sequence in 1..=8_u64 {
        database
            .transaction(|transaction| {
                if sequence == 1 {
                    transaction
                        .execute_batch("CREATE TABLE events(sequence INTEGER PRIMARY KEY)")?;
                }
                transaction.execute("INSERT INTO events VALUES (?1)", [sequence])?;
                Ok(())
            })
            .unwrap();
        let cuts = database.capture_deferred().unwrap();
        let prepared = publisher.prepare_append(&cuts, sequence, 1).await.unwrap();
        publisher.publish_prepared(&prepared, None).await.unwrap();
        if sequence == 1 {
            prefix = Some(prepared.root());
        }
    }
    assert!(publisher.compaction_due());
    let before = publisher.control().value().ltx_root().unwrap();
    assert_eq!(replica.open_root(&before).await.unwrap().segment_count(), 8);
    assert_eq!(publisher.compact_one_quiet().await.unwrap(), Some(true));
    let after = publisher.control().value().ltx_root().unwrap();
    assert_eq!(after.position, before.position);
    assert_eq!(after.commit_sequence, before.commit_sequence);
    assert_eq!(replica.open_root(&after).await.unwrap().segment_count(), 1);
    let prefix = prefix.unwrap();
    let proof = publisher
        .authority
        .verify_root_prefix(prefix, after, &replica, 64)
        .await
        .unwrap();
    assert_eq!(proof.prefix(), prefix);
    assert_eq!(proof.root(), after);
    assert!(proof.inspected_roots() >= 8 && proof.dependency_count() > 0);
    assert_eq!(publisher.compact_one_quiet().await.unwrap(), Some(false));
    assert!(!publisher.compaction_due());

    let mut segments = Vec::new();
    let mut position = after.position;
    for sequence in 9..=10_u64 {
        database
            .transaction(|transaction| {
                transaction.execute("INSERT INTO events VALUES (?1)", [sequence])?;
                Ok(())
            })
            .unwrap();
        let captured = database.capture_deferred().unwrap();
        segments.extend(captured.segments);
        position = captured.position;
    }
    let cuts = CaptureBatch {
        segments,
        position,
        timing: Default::default(),
    };
    assert_eq!(cuts.segments.len(), 2);
    let prepared = publisher.prepare_append(&cuts, 9, 1).await.unwrap();
    publisher.publish_prepared(&prepared, None).await.unwrap();
    let extended = publisher.control().value().ltx_root().unwrap();
    assert_eq!(extended.position, position);
    assert_eq!(
        replica.open_root(&extended).await.unwrap().segment_count(),
        3
    );

    for sequence in 10..=37_u64 {
        database
            .transaction(|transaction| {
                transaction.execute("INSERT INTO events VALUES (?1)", [sequence + 1])?;
                Ok(())
            })
            .unwrap();
        let cuts = database.capture_deferred().unwrap();
        let prepared = publisher.prepare_append(&cuts, sequence, 1).await.unwrap();
        publisher.publish_prepared(&prepared, None).await.unwrap();
    }
    let at_ceiling = publisher.control().value().ltx_root().unwrap();
    assert_eq!(
        replica
            .open_root(&at_ceiling)
            .await
            .unwrap()
            .segment_count(),
        31
    );
    database
        .transaction(|transaction| {
            transaction.execute("INSERT INTO events VALUES (39)", [])?;
            Ok(())
        })
        .unwrap();
    let cuts = database.capture_deferred().unwrap();
    let before_revision = publisher.control().value().revision;
    assert!(matches!(
        publisher.prepare_append(&cuts, 37, schema).await,
        Err(Error::Ltx(cellule_ltx::LtxError::InvalidState(_)))
    ));
    assert_eq!(publisher.control().value().revision, before_revision);
    assert_eq!(
        publisher
            .authority
            .load(cell)
            .await
            .unwrap()
            .unwrap()
            .value(),
        publisher.control().value(),
        "a rejected successor must leave its compaction private"
    );
    let prepared = publisher.prepare_append(&cuts, 38, schema).await.unwrap();
    assert_eq!(publisher.control().value().ltx_root(), Some(at_ceiling));
    assert_eq!(
        publisher
            .authority
            .load(cell)
            .await
            .unwrap()
            .unwrap()
            .value()
            .ltx_root(),
        Some(at_ceiling)
    );
    assert_eq!(prepared.predecessor(), Some(at_ceiling));
    if schema == 1 {
        publisher.publish_prepared(&prepared, None).await.unwrap();
    } else {
        publisher
            .publish_migration(&prepared, None, Digest::from_bytes([46; 32]), schema)
            .await
            .unwrap();
    }
    assert_eq!(publisher.control().value().schema, schema);
    assert_eq!(publisher.control().value().revision, before_revision + 1);
    let forced = publisher.control().value().ltx_root().unwrap();
    assert!(replica.open_root(&forced).await.unwrap().segment_count() < 32);
    let proof = publisher
        .authority
        .verify_root_prefix(prefix, forced, &replica, 64)
        .await
        .unwrap();
    assert_eq!(proof.root(), forced);
    assert!(proof.inspected_roots() > 30);
    assert!(matches!(
        publisher
            .authority
            .verify_root_prefix(prefix, forced, &replica, 2)
            .await,
        Err(Error::Capacity(_))
    ));
    database.close().unwrap();
}

#[derive(Default)]
struct RecordingSubmissions {
    outcomes: std::sync::Mutex<Vec<DurabilitySubmissionOutcome>>,
}

impl crate::fleet::telemetry::CellTelemetry for RecordingSubmissions {
    fn durability_submission(&self, outcome: DurabilitySubmissionOutcome) {
        self.outcomes.lock().unwrap().push(outcome);
    }
}

#[tokio::test]
async fn commits_report_when_no_enrolled_lane_can_carry_them() {
    let telemetry = crate::fleet::telemetry::CellTelemetryHandle::default();
    let recording = Arc::new(RecordingSubmissions::default());
    telemetry.install(recording.clone()).unwrap();
    let cuts = CaptureBatch {
        segments: Vec::new(),
        position: Default::default(),
        timing: Default::default(),
    };
    let submitter = CellDurabilitySubmitter {
        cell: CellId::from_bytes([71; 32]),
        incarnation: IncarnationId::from_bytes([72; 16]),
        epoch: 1,
        node_lease: None,
        node_durability: None,
        telemetry: telemetry.clone(),
    };
    assert!(submitter.submit(1, &cuts).await.unwrap().is_none());

    let lane: NodeDurabilitySlot = Arc::new(std::sync::RwLock::new(None));
    let submitter = CellDurabilitySubmitter {
        node_durability: Some(lane),
        telemetry,
        ..submitter
    };
    assert!(submitter.submit(1, &cuts).await.unwrap().is_none());

    assert_eq!(
        *recording.outcomes.lock().unwrap(),
        vec![
            DurabilitySubmissionOutcome::Unsupported,
            DurabilitySubmissionOutcome::Unavailable,
        ]
    );
}

/// Transport that refuses every follower request; the gate is fenced first,
/// so no frame reaches it in this test.
struct RefusingTransport;

impl crate::node::log_transport::NodeLogTransport for RefusingTransport {
    fn append<'a>(
        &'a self,
        _member: crate::identity::NodeId,
        _request: crate::node::log_transport::AppendRequest,
    ) -> futures_util::future::BoxFuture<'a, crate::Result<crate::follower::FollowerReceipt>> {
        Box::pin(async { Err(Error::Node("test transport refuses appends")) })
    }

    fn seal<'a>(
        &'a self,
        _member: crate::identity::NodeId,
        _request: crate::node::log_transport::SealRequest,
    ) -> futures_util::future::BoxFuture<'a, crate::Result<crate::follower::FollowerReceipt>> {
        Box::pin(async { Err(Error::Node("test transport refuses seals")) })
    }

    fn retire<'a>(
        &'a self,
        _member: crate::identity::NodeId,
        _request: crate::node::log_transport::RetireRequest,
    ) -> futures_util::future::BoxFuture<'a, crate::Result<crate::follower::FollowerReceipt>> {
        Box::pin(async { Err(Error::Node("test transport refuses retirements")) })
    }

    fn tail<'a>(
        &'a self,
        _member: crate::identity::NodeId,
        _request: crate::node::log_transport::TailRequest,
    ) -> futures_util::future::BoxFuture<'a, crate::Result<Vec<bytes::Bytes>>> {
        Box::pin(async { Err(Error::Node("test transport refuses tails")) })
    }
}

/// Authority that refuses activation; the fenced gate never asks it anything.
struct RefusingAuthority;

impl crate::node::durability::NodeLogAuthority for RefusingAuthority {
    fn activate<'a>(
        &'a self,
        _log_epoch: u64,
    ) -> futures_util::future::BoxFuture<'a, crate::Result<()>> {
        Box::pin(async { Err(Error::Node("test authority refuses activation")) })
    }

    fn advance_coverage<'a>(
        &'a self,
        _log_epoch: u64,
        _tiered_through: u64,
    ) -> futures_util::future::BoxFuture<'a, crate::Result<()>> {
        Box::pin(async { Err(Error::Node("test authority refuses coverage")) })
    }

    fn close<'a>(
        &'a self,
        _retirement: &'a crate::node::log::NodeLogRetirementObservation,
    ) -> futures_util::future::BoxFuture<'a, crate::Result<()>> {
        Box::pin(async { Err(Error::Node("test authority refuses closing")) })
    }
}

#[tokio::test]
async fn commits_report_a_fenced_lane_instead_of_failing() {
    let telemetry = crate::fleet::telemetry::CellTelemetryHandle::default();
    let recording = Arc::new(RecordingSubmissions::default());
    telemetry.install(recording.clone()).unwrap();

    let gate = crate::node::log::DurabilityGate::new(
        SessionId::from_bytes([81; 16]),
        crate::identity::NodeId::from_bytes([82; 16]),
        9,
        [crate::identity::NodeId::from_bytes([83; 16])],
    )
    .unwrap();
    let transport: Arc<dyn crate::node::log_transport::NodeLogTransport> =
        Arc::new(RefusingTransport);
    let shipper = crate::node::log_shipper::NodeLogShipper::new_with_telemetry(
        gate.clone(),
        Arc::clone(&transport),
        Limits::default(),
        telemetry.clone(),
    )
    .unwrap();
    let lease = crate::node::lease::NodeLeaseGuard::new(0, 60_000).unwrap();
    let durability = Arc::new(crate::node::durability::NodeDurability::new(
        gate.clone(),
        shipper,
        Arc::new(RefusingAuthority),
        transport,
        lease,
    ));
    gate.stop_shipping();

    let directory = tempfile::tempdir().unwrap();
    let mut database = Db::open(&directory.path().join("cell.sqlite"), Limits::default()).unwrap();
    database
        .transaction(|transaction| {
            transaction.execute_batch("CREATE TABLE events(sequence INTEGER PRIMARY KEY)")?;
            transaction.execute("INSERT INTO events VALUES (1)", [])?;
            Ok(())
        })
        .unwrap();
    let cuts = database.capture_deferred().unwrap();

    let submitter = CellDurabilitySubmitter {
        cell: CellId::from_bytes([84; 32]),
        incarnation: IncarnationId::from_bytes([85; 16]),
        epoch: 1,
        node_lease: None,
        node_durability: Some(Arc::new(std::sync::RwLock::new(Some((
            crate::identity::ApplicationId::from_bytes([86; 16]),
            durability,
        ))))),
        telemetry,
    };
    assert!(submitter.submit(1, &cuts).await.unwrap().is_none());
    assert_eq!(
        *recording.outcomes.lock().unwrap(),
        vec![DurabilitySubmissionOutcome::Rejected]
    );
    database.close().unwrap();
}
