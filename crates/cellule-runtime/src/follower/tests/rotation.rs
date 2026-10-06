//! Small live chunks, multi-rotation batches, and historical chunk compatibility.

use super::*;

fn rotation_frames() -> Vec<Bytes> {
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&source.path().join("cell.sqlite"), limits).unwrap();
    database
        .transaction(|transaction| {
            transaction.execute_batch(
                "CREATE TABLE values_(v); INSERT INTO values_ VALUES(randomblob(400000))",
            )
        })
        .unwrap();
    let capture = database.capture().unwrap();
    let frames = (1..=5)
        .map(|sequence| frame(sequence, &capture.segments[0], limits))
        .collect::<Vec<_>>();
    let bytes = (RECORD_HEADER_BYTES + frames[0].len()) as u64;
    assert!(bytes * 2 <= 1 << 20 && bytes * 3 > 1 << 20);
    database.close().unwrap();
    frames
}

#[tokio::test]
async fn batch_rotations_preserve_warm_locations_and_covered_chunk_boundaries() {
    let frames = rotation_frames();
    let limits = cellule_ltx::Limits::default();
    let root = tempfile::TempDir::new().unwrap();
    let leader = SessionId::from_bytes([1; 16]);
    let lane = Lane { leader, epoch: 2 };
    let store = FollowerStore::open(
        root.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    assert_eq!(
        store.append(leader, 2, frames.clone(), 0).await.unwrap(),
        FollowerReceipt {
            base_sequence: 1,
            durable_through: 5,
        }
    );
    let chunks = lane_directory(root.path(), lane).join("chunks");
    let first = chunks.join(format!("{:020}-{:020}.log", 1, 2));
    let second = chunks.join(format!("{:020}-{:020}.log", 3, 4));
    assert!(first.is_file() && second.is_file());
    let original = std::fs::read(&first).unwrap();

    // Duplicate input exercises the live index after multiple rotations. A
    // partially covered immutable chunk stays intact until its last sequence.
    let receipt = store
        .append(leader, 2, vec![frames[4].clone()], 1)
        .await
        .unwrap();
    assert_eq!(receipt.base_sequence, 1);
    assert_eq!(std::fs::read(&first).unwrap(), original);
    store.seal(leader, 2).await.unwrap();
    assert_eq!(store.read_tail(leader, 2, 1).await.unwrap(), frames);
    drop(store);

    let cold = FollowerStore::open(
        root.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    assert_eq!(cold.read_tail(leader, 2, 1).await.unwrap(), frames);
}

#[tokio::test]
async fn complete_chunk_coverage_prunes_only_the_original_prefix() {
    let frames = rotation_frames();
    let limits = cellule_ltx::Limits::default();
    let root = tempfile::TempDir::new().unwrap();
    let leader = SessionId::from_bytes([1; 16]);
    let lane = Lane { leader, epoch: 2 };
    let store = FollowerStore::open(
        root.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    store.append(leader, 2, frames.clone(), 0).await.unwrap();
    let chunks = lane_directory(root.path(), lane).join("chunks");
    let first = chunks.join(format!("{:020}-{:020}.log", 1, 2));
    let second = chunks.join(format!("{:020}-{:020}.log", 3, 4));
    assert!(first.is_file() && second.is_file());
    let receipt = store
        .append(leader, 2, vec![frames[4].clone()], 2)
        .await
        .unwrap();
    assert_eq!(receipt.base_sequence, 3);
    assert!(!first.exists());
    assert!(second.is_file());
    store.seal(leader, 2).await.unwrap();
    assert_eq!(store.read_tail(leader, 2, 3).await.unwrap(), frames[2..]);
    drop(store);
    let cold = FollowerStore::open(
        root.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    assert_eq!(cold.read_tail(leader, 2, 3).await.unwrap(), frames[2..]);
}

#[tokio::test]
async fn interrupted_batch_seals_every_valid_record_across_rotations() {
    let frames = rotation_frames();
    let limits = cellule_ltx::Limits::default();
    let root = tempfile::TempDir::new().unwrap();
    let leader = SessionId::from_bytes([1; 16]);
    let store = FollowerStore::open(
        root.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    let mut interrupted = frames.clone();
    interrupted.push(Bytes::from_static(b"invalid node frame"));
    assert!(store.append(leader, 2, interrupted, 0).await.is_err());
    assert_eq!(store.seal(leader, 2).await.unwrap().durable_through, 5);
    assert_eq!(store.read_tail(leader, 2, 1).await.unwrap(), frames);
    drop(store);
    let cold = FollowerStore::open(
        root.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    assert_eq!(cold.read_tail(leader, 2, 1).await.unwrap(), frames);
}

#[tokio::test]
async fn historical_closed_chunks_larger_than_live_target_remain_readable() {
    let frames = rotation_frames();
    let limits = cellule_ltx::Limits::default();
    let root = tempfile::TempDir::new().unwrap();
    let leader = SessionId::from_bytes([1; 16]);
    let lane = Lane { leader, epoch: 2 };
    ensure_lane_directories(root.path(), lane).unwrap();
    let chunks = lane_directory(root.path(), lane).join("chunks");
    let path = chunks.join(format!("{:020}-{:020}.log", 1, 3));
    let mut file = std::fs::File::create(&path).unwrap();
    for (index, encoded) in frames[..3].iter().enumerate() {
        write_record(
            &mut file,
            index as u64 + 1,
            *blake3::hash(encoded).as_bytes(),
            encoded,
        )
        .unwrap();
    }
    file.sync_all().unwrap();
    drop(file);
    sync_directory(&chunks).unwrap();
    assert!(std::fs::metadata(&path).unwrap().len() > 1 << 20);
    let store = FollowerStore::open(
        root.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    assert_eq!(
        store
            .append(leader, 2, vec![frames[3].clone()], 0)
            .await
            .unwrap()
            .durable_through,
        4
    );
    store.seal(leader, 2).await.unwrap();
    assert_eq!(store.read_tail(leader, 2, 1).await.unwrap(), frames[..4]);
}
