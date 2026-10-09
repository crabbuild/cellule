#![cfg(feature = "replica")]

use bytes::Bytes;
use cellule_ltx::{
    Db, Limits, NodeFrameScope, encode_node_frame, encode_node_frame_range, inspect_node_frame,
};

fn scope() -> NodeFrameScope {
    NodeFrameScope {
        leader_session: [1; 16],
        log_epoch: 2,
        node_sequence: 3,
        application: [4; 16],
        cell: [5; 32],
        incarnation: [6; 16],
        cell_epoch: 7,
        commit_sequence: 8,
    }
}

#[test]
fn canonical_frame_roundtrips_verified_ltx() {
    let directory = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&directory.path().join("frame.sqlite"), Limits::default()).unwrap();
    database
        .transaction(|transaction| {
            transaction.execute_batch(
                "CREATE TABLE events(id INTEGER PRIMARY KEY, body TEXT NOT NULL);\
                 INSERT INTO events(body) VALUES ('durable')",
            )
        })
        .unwrap();
    let capture = database.capture().unwrap();
    let segment = capture.segments.first().unwrap();
    let body = Bytes::from(std::fs::read(segment.path()).unwrap());

    let frame = encode_node_frame(
        scope(),
        segment.info().clone(),
        body.clone(),
        Limits::default(),
    )
    .unwrap();
    let decoded = inspect_node_frame(frame.encoded().clone(), Limits::default()).unwrap();

    assert_eq!(decoded.scope(), scope());
    assert_eq!(decoded.segment(), segment.info());
    assert_eq!(decoded.body(), &body);
    assert_eq!(decoded.digest(), frame.digest());
    database.close().unwrap();
}

#[test]
fn assigning_sequence_preserves_canonical_bytes_and_shared_frames() {
    let directory = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&directory.path().join("frame.sqlite"), Limits::default()).unwrap();
    database
        .transaction(|transaction| transaction.execute_batch("CREATE TABLE values_(v)"))
        .unwrap();
    let capture = database.capture().unwrap();
    let segment = capture.segments.first().unwrap();
    let body = Bytes::from(std::fs::read(segment.path()).unwrap());
    for first in [scope().commit_sequence, 5] {
        let frame = encode_node_frame_range(
            scope(),
            first,
            segment.info().clone(),
            body.clone(),
            Limits::default(),
        )
        .unwrap();
        let original = frame.clone();
        let reassigned = frame.with_node_sequence(55).unwrap();
        let mut assigned_scope = scope();
        assigned_scope.node_sequence = 55;
        let fresh = encode_node_frame_range(
            assigned_scope,
            first,
            segment.info().clone(),
            body.clone(),
            Limits::default(),
        )
        .unwrap();
        assert_eq!(reassigned.encoded(), fresh.encoded());
        assert_eq!(original.scope(), scope());
        assert_eq!(original.body(), &body);
        let inspected =
            inspect_node_frame(reassigned.encoded().clone(), Limits::default()).unwrap();
        assert_eq!(inspected.scope(), assigned_scope);
        assert_eq!(inspected.first_commit_sequence(), first);
        assert_eq!(inspected.body(), &body);
        assert!(reassigned.clone().with_node_sequence(0).is_err());

        drop(inspected);
        let pointer = reassigned.encoded().as_ptr();
        let exclusive = reassigned.with_node_sequence(u64::MAX).unwrap();
        assert_eq!(exclusive.encoded().as_ptr(), pointer);
        assert_eq!(exclusive.scope().node_sequence, u64::MAX);
        assert_eq!(exclusive.body(), &body);
    }
    database.close().unwrap();
}

#[test]
fn frame_rejects_corruption_trailing_bytes_and_zero_scope() {
    let directory = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&directory.path().join("frame.sqlite"), Limits::default()).unwrap();
    database
        .transaction(|transaction| transaction.execute_batch("CREATE TABLE values_(v)"))
        .unwrap();
    let capture = database.capture().unwrap();
    let segment = capture.segments.first().unwrap();
    let body = Bytes::from(std::fs::read(segment.path()).unwrap());
    let frame = encode_node_frame(
        scope(),
        segment.info().clone(),
        body.clone(),
        Limits::default(),
    )
    .unwrap();

    let mut corrupted = frame.encoded().to_vec();
    let last = corrupted.len() - 1;
    corrupted[last] ^= 1;
    assert!(inspect_node_frame(Bytes::from(corrupted), Limits::default()).is_err());

    let mut trailing = frame.encoded().to_vec();
    trailing.push(0);
    assert!(inspect_node_frame(Bytes::from(trailing), Limits::default()).is_err());

    let mut invalid_scope = scope();
    invalid_scope.leader_session = [0; 16];
    assert!(
        encode_node_frame(
            invalid_scope,
            segment.info().clone(),
            body,
            Limits::default(),
        )
        .is_err()
    );
    database.close().unwrap();
}

#[test]
fn command_range_roundtrips_and_singleton_preserves_version_one_bytes() {
    let directory = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&directory.path().join("range.sqlite"), Limits::default()).unwrap();
    database
        .transaction(|tx| {
            tx.execute_batch("CREATE TABLE values_(v); INSERT INTO values_ VALUES (4)")
        })
        .unwrap();
    let capture = database.capture().unwrap();
    let segment = &capture.segments[0];
    let body = Bytes::from(std::fs::read(segment.path()).unwrap());
    let singleton = encode_node_frame(
        scope(),
        segment.info().clone(),
        body.clone(),
        Limits::default(),
    )
    .unwrap();
    let same = encode_node_frame_range(
        scope(),
        8,
        segment.info().clone(),
        body.clone(),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(singleton.encoded(), same.encoded());
    assert_eq!(&same.encoded()[4..8], &[1, 0, 240, 0]);
    assert_eq!(same.first_commit_sequence(), 8);

    let grouped = encode_node_frame_range(
        scope(),
        5,
        segment.info().clone(),
        body.clone(),
        Limits::default(),
    )
    .unwrap();
    assert_eq!(&grouped.encoded()[4..8], &[2, 0, 248, 0]);
    assert_eq!(grouped.encoded().len(), singleton.encoded().len() + 8);
    let decoded = inspect_node_frame(grouped.encoded().clone(), Limits::default()).unwrap();
    assert_eq!(decoded.first_commit_sequence(), 5);
    assert_eq!(decoded.scope().commit_sequence, 8);
    assert_eq!(decoded.body(), &body);
    assert_ne!(decoded.digest(), singleton.digest());

    for first in [0_u64, 8, 9] {
        let mut invalid = grouped.encoded().to_vec();
        invalid[240..248].copy_from_slice(&first.to_le_bytes());
        assert!(inspect_node_frame(Bytes::from(invalid), Limits::default()).is_err());
    }
    for (version, header) in [(1_u16, 248_u16), (2, 240), (3, 248)] {
        let mut invalid = grouped.encoded().to_vec();
        invalid[4..6].copy_from_slice(&version.to_le_bytes());
        invalid[6..8].copy_from_slice(&header.to_le_bytes());
        assert!(inspect_node_frame(Bytes::from(invalid), Limits::default()).is_err());
    }
    for end in [240, 247, grouped.encoded().len() - 1] {
        assert!(inspect_node_frame(grouped.encoded().slice(..end), Limits::default()).is_err());
    }
    database.close().unwrap();
}
