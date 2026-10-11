//! Bundle envelope fences: magic, layout, identity, and limit rejects.

use std::path::Path;

use cellule_ltx::bundle::{Bundle, BundleBuilder, BundleEntry, BundleRow};
use cellule_ltx::{Db, Limits, SegmentInfo};

fn segment(directory: &Path, marker: u8) -> (SegmentInfo, Vec<u8>) {
    let database_path = directory.join(format!("database-{marker}"));
    let mut database = Db::open(&database_path, Limits::default()).unwrap();
    database
        .transaction(|transaction| transaction.execute_batch("CREATE TABLE values_(v)"))
        .unwrap();
    database.capture().unwrap();
    database
        .transaction(|transaction| {
            transaction.execute("INSERT INTO values_ VALUES (1)", [])?;
            Ok(())
        })
        .unwrap();
    let capture = database.capture().unwrap();
    let segment = capture.segments.first().unwrap();
    let info = segment.info().clone();
    let bytes = std::fs::read(segment.path()).unwrap();
    database.close().unwrap();
    (info, bytes)
}

fn row_info(min_txid: u64, max_txid: u64, size_bytes: u64) -> SegmentInfo {
    SegmentInfo {
        min_txid,
        max_txid,
        page_size: 4096,
        database_pages: 1,
        pre_checksum: 0,
        post_checksum: 0,
        size_bytes,
        blake3: [0; 32],
    }
}

fn envelope(rows: &[BundleRow], payload: Vec<u8>) -> Vec<u8> {
    let footer = serde_json::to_vec(rows).unwrap();
    let mut bytes = payload;
    bytes.extend_from_slice(&footer);
    bytes.extend_from_slice(&u32::try_from(footer.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(b"CRB1");
    bytes
}

#[test]
fn decode_rejects_a_body_without_the_crb1_magic() {
    assert!(Bundle::decode(vec![0; 16], Limits::default()).is_err());
}

#[test]
fn decode_rejects_an_empty_body() {
    assert!(Bundle::decode(Vec::new(), Limits::default()).is_err());
}

#[test]
fn decode_rejects_rows_that_do_not_cover_the_payload() {
    let rows = [BundleRow {
        repository: "aa".to_string(),
        epoch: "bb".to_string(),
        info: row_info(1, 1, 8),
        offset: 0,
    }];
    let bytes = envelope(&rows, vec![0; 16]);
    assert!(Bundle::decode(bytes, Limits::default()).is_err());
}

#[test]
fn decode_rejects_overlapping_rows() {
    let rows = [
        BundleRow {
            repository: "aa".to_string(),
            epoch: "bb".to_string(),
            info: row_info(1, 1, 8),
            offset: 0,
        },
        BundleRow {
            repository: "aa".to_string(),
            epoch: "bb".to_string(),
            info: row_info(2, 2, 8),
            offset: 4,
        },
    ];
    let bytes = envelope(&rows, vec![0; 16]);
    assert!(Bundle::decode(bytes, Limits::default()).is_err());
}

#[test]
fn encode_rejects_a_duplicate_segment_identity() {
    let directory = tempfile::tempdir().unwrap();
    let (info, bytes) = segment(directory.path(), 1);
    let entry = || BundleEntry::for_cell([1; 32], [1; 16], info.clone(), bytes.clone());
    assert!(Bundle::encode(vec![entry(), entry()], Limits::default()).is_err());
}

#[test]
fn encode_matches_the_complete_canonical_envelope_and_independent_decoder() {
    let directory = tempfile::tempdir().unwrap();
    let (info, body) = segment(directory.path(), 4);
    let entries = (1..=4)
        .map(|id| BundleEntry::for_cell([id; 32], [id; 16], info.clone(), body.clone()))
        .collect::<Vec<_>>();
    let mut payload = Vec::new();
    let rows = entries
        .iter()
        .map(|entry| {
            let row = BundleRow {
                repository: entry.repository.clone(),
                epoch: entry.epoch.clone(),
                info: entry.info.clone(),
                offset: payload.len() as u64,
            };
            payload.extend_from_slice(&entry.bytes);
            row
        })
        .collect::<Vec<_>>();
    let expected = envelope(&rows, payload);
    let encoded = Bundle::encode(entries, Limits::default()).unwrap();
    assert_eq!(encoded.read_all().unwrap().as_ref(), expected);
    assert_eq!(encoded.digest(), *blake3::hash(&expected).as_bytes());
    let decoded = Bundle::decode(expected, Limits::default()).unwrap();
    assert_eq!(decoded.digest(), encoded.digest());
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(decoded.rows()[index].info, row.info);
        assert_eq!(decoded.rows()[index].offset, row.offset);
        assert_eq!(decoded.read_segment(index).unwrap().as_ref(), body);
    }
}

#[test]
fn encode_still_verifies_ltx_after_its_outer_digest_is_rebound() {
    let directory = tempfile::tempdir().unwrap();
    let (mut info, mut body) = segment(directory.path(), 5);
    *body.last_mut().unwrap() ^= 1;
    info.blake3 = *blake3::hash(&body).as_bytes();
    assert!(
        Bundle::encode(
            vec![BundleEntry::for_cell([5; 32], [5; 16], info, body)],
            Limits::default(),
        )
        .is_err()
    );
}

#[test]
fn encode_includes_the_footer_in_the_complete_plan_bound() {
    let directory = tempfile::tempdir().unwrap();
    let (info, body) = segment(directory.path(), 6);
    let limits = Limits {
        max_capture_bytes: info.size_bytes,
        max_file_bytes: info.size_bytes,
        max_plan_bytes: info.size_bytes,
        ..Limits::default()
    };
    assert!(matches!(
        Bundle::encode(
            vec![BundleEntry::for_cell([6; 32], [6; 16], info, body)],
            limits,
        ),
        Err(cellule_ltx::LtxError::Limit(
            cellule_ltx::LimitKind::BundleBytes
        ))
    ));
}

#[test]
fn builder_rejects_more_than_the_segment_limit() {
    let directory = tempfile::tempdir().unwrap();
    let limits = Limits {
        max_segments: 1,
        ..Limits::default()
    };
    let mut builder = BundleBuilder::new_temp(directory.path(), limits).unwrap();
    let (info, bytes) = segment(directory.path(), 2);
    builder
        .push(BundleEntry::for_cell([2; 32], [2; 16], info, bytes))
        .unwrap();
    let (info, bytes) = segment(directory.path(), 3);
    assert!(
        builder
            .push(BundleEntry::for_cell([3; 32], [3; 16], info, bytes))
            .is_err()
    );
}
