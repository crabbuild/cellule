//! Live disk changes must not let a cached range release an unrecoverable append.

use super::*;

#[tokio::test]
async fn cached_open_lane_mutation_cannot_release_unrecoverable_append() {
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&source.path().join("cell.sqlite"), limits).unwrap();
    database
        .transaction(|transaction| {
            transaction.execute_batch("CREATE TABLE values_(v); INSERT INTO values_ VALUES (1)")
        })
        .unwrap();
    let first = database.capture().unwrap();
    database
        .transaction(|transaction| {
            transaction.execute("INSERT INTO values_ VALUES (2)", [])?;
            Ok(())
        })
        .unwrap();
    let second = database.capture().unwrap();
    let frames = (1..=3)
        .map(|sequence| frame(sequence, &first.segments[0], limits))
        .collect::<Vec<_>>();
    let replacement = frame(2, &second.segments[0], limits);
    let leader = SessionId::from_bytes([1; 16]);

    for mutation in [
        "deleted",
        "shortened",
        "prefix_removed",
        "corrupted",
        "substituted",
    ] {
        let root = tempfile::TempDir::new().unwrap();
        let store = FollowerStore::open(
            root.path().to_owned(),
            limits,
            cellule_ltx::DiskBudget::new(1 << 30),
        )
        .unwrap();
        store
            .append(leader, 2, frames[..2].to_vec(), 0)
            .await
            .unwrap();
        let open = lane_directory(root.path(), Lane { leader, epoch: 2 }).join("chunks/open.log");
        let original = std::fs::read(&open).unwrap();
        let first_end = RECORD_HEADER_BYTES + frames[0].len();
        match mutation {
            "deleted" => std::fs::remove_file(&open).unwrap(),
            "shortened" => std::fs::write(&open, &original[..first_end]).unwrap(),
            "prefix_removed" => std::fs::write(&open, &original[first_end..]).unwrap(),
            "corrupted" => {
                let mut corrupted = original.clone();
                *corrupted.last_mut().unwrap() ^= 1;
                std::fs::write(&open, corrupted).unwrap();
            }
            "substituted" => {
                let mut file = std::fs::OpenOptions::new().write(true).open(&open).unwrap();
                file.set_len(first_end as u64).unwrap();
                file.seek(SeekFrom::Start(first_end as u64)).unwrap();
                write_record(
                    &mut file,
                    2,
                    *blake3::hash(&replacement).as_bytes(),
                    &replacement,
                )
                .unwrap();
                file.sync_data().unwrap();
            }
            _ => unreachable!(),
        }
        for attempt in 0..3 {
            assert!(
                store
                    .append(leader, 2, vec![frames[2].clone()], 0)
                    .await
                    .is_err(),
                "{mutation}, attempt {attempt}: a retry must not release a missing or substituted original tail"
            );
        }

        // Rejection must retain the original proof across retries and release
        // growth admission. Restoring the exact bytes permits an exact retry.
        std::fs::write(&open, &original).unwrap();
        std::fs::File::open(&open).unwrap().sync_data().unwrap();
        assert_eq!(
            store
                .append(leader, 2, vec![frames[1].clone()], 0)
                .await
                .unwrap()
                .durable_through,
            2
        );
        assert_eq!(
            store
                .append(leader, 2, vec![frames[2].clone()], 0)
                .await
                .unwrap()
                .durable_through,
            3
        );
        assert_eq!(store.seal(leader, 2).await.unwrap().durable_through, 3);
        drop(store);
        let cold = FollowerStore::open(
            root.path().to_owned(),
            limits,
            cellule_ltx::DiskBudget::new(1 << 30),
        )
        .unwrap();
        assert_eq!(cold.read_tail(leader, 2, 1).await.unwrap(), frames);
    }
    database.close().unwrap();
}

#[tokio::test]
async fn cached_sealed_tail_substitution_remains_rejected_across_retries() {
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&source.path().join("cell.sqlite"), limits).unwrap();
    database
        .transaction(|transaction| {
            transaction.execute_batch("CREATE TABLE values_(v); INSERT INTO values_ VALUES (1)")
        })
        .unwrap();
    let first = database.capture().unwrap();
    database
        .transaction(|transaction| {
            transaction.execute("INSERT INTO values_ VALUES (2)", [])?;
            Ok(())
        })
        .unwrap();
    let second = database.capture().unwrap();
    let encoded = frame(1, &first.segments[0], limits);
    let replacement = frame(1, &second.segments[0], limits);
    let leader = SessionId::from_bytes([1; 16]);
    for paged in [false, true] {
        let root = tempfile::TempDir::new().unwrap();
        let store = FollowerStore::open(
            root.path().to_owned(),
            limits,
            cellule_ltx::DiskBudget::new(1 << 30),
        )
        .unwrap();
        store
            .append(leader, 2, vec![encoded.clone()], 0)
            .await
            .unwrap();
        store.seal(leader, 2).await.unwrap();
        let open = lane_directory(root.path(), Lane { leader, epoch: 2 }).join("chunks/open.log");
        let original = std::fs::read(&open).unwrap();
        let mut file = std::fs::File::create(&open).unwrap();
        write_record(
            &mut file,
            1,
            *blake3::hash(&replacement).as_bytes(),
            &replacement,
        )
        .unwrap();
        file.sync_data().unwrap();
        drop(file);
        for attempt in 0..3 {
            let result = if paged {
                store
                    .read_tail_page(leader, 2, 1)
                    .await
                    .map(|page| page.frames)
            } else {
                store.read_tail(leader, 2, 1).await
            };
            assert!(
                result.is_err(),
                "paged {paged}, attempt {attempt}: retry must preserve the original sealed-tail witness"
            );
        }
        std::fs::write(&open, original).unwrap();
        std::fs::File::open(&open).unwrap().sync_data().unwrap();
        assert_eq!(
            store.read_tail(leader, 2, 1).await.unwrap(),
            vec![encoded.clone()]
        );
    }
    database.close().unwrap();
}

#[tokio::test]
async fn seal_reconciles_valid_records_left_by_failed_append() {
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&source.path().join("cell.sqlite"), limits).unwrap();
    database
        .transaction(|transaction| transaction.execute_batch("CREATE TABLE values_(v)"))
        .unwrap();
    let capture = database.capture().unwrap();
    let frames = (1..=2)
        .map(|sequence| frame(sequence, &capture.segments[0], limits))
        .collect::<Vec<_>>();
    let root = tempfile::TempDir::new().unwrap();
    let leader = SessionId::from_bytes([1; 16]);
    let store = FollowerStore::open(
        root.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    store
        .append(leader, 2, vec![frames[0].clone()], 0)
        .await
        .unwrap();
    assert!(
        store
            .append(
                leader,
                2,
                vec![frames[1].clone(), Bytes::from_static(b"invalid node frame")],
                0,
            )
            .await
            .is_err()
    );
    assert_eq!(store.seal(leader, 2).await.unwrap().durable_through, 2);
    let index = store.index_used.clone();
    drop(store);
    assert_eq!(*index.lock().unwrap(), 0);
    let cold = FollowerStore::open(
        root.path().to_owned(),
        limits,
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    assert_eq!(cold.read_tail(leader, 2, 1).await.unwrap(), frames);
    database.close().unwrap();
}

#[tokio::test]
async fn authoritative_coverage_releases_rejected_prefix_witnesses() {
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&source.path().join("cell.sqlite"), limits).unwrap();
    database
        .transaction(|transaction| {
            transaction.execute_batch("CREATE TABLE values_(v); INSERT INTO values_ VALUES (1)")
        })
        .unwrap();
    let first = database.capture().unwrap();
    database
        .transaction(|transaction| {
            transaction.execute("INSERT INTO values_ VALUES (2)", [])?;
            Ok(())
        })
        .unwrap();
    let second = database.capture().unwrap();
    let frames = (1..=3)
        .map(|sequence| frame(sequence, &first.segments[0], limits))
        .collect::<Vec<_>>();
    let replacement = frame(2, &second.segments[0], limits);
    assert_ne!(replacement, frames[1]);
    let leader = SessionId::from_bytes([1; 16]);
    for substituted in [false, true] {
        let root = tempfile::TempDir::new().unwrap();
        let store = FollowerStore::open(
            root.path().to_owned(),
            limits,
            cellule_ltx::DiskBudget::new(1 << 30),
        )
        .unwrap();
        store
            .append(leader, 2, frames[..2].to_vec(), 0)
            .await
            .unwrap();
        let open = lane_directory(root.path(), Lane { leader, epoch: 2 }).join("chunks/open.log");
        let original = std::fs::read(&open).unwrap();
        let first_end = RECORD_HEADER_BYTES + frames[0].len();
        if substituted {
            let mut file = std::fs::File::create(&open).unwrap();
            file.write_all(&original[..first_end]).unwrap();
            write_record(
                &mut file,
                2,
                *blake3::hash(&replacement).as_bytes(),
                &replacement,
            )
            .unwrap();
            file.sync_data().unwrap();
        } else {
            std::fs::write(&open, &original[first_end..]).unwrap();
        }
        for _ in 0..2 {
            assert!(
                store
                    .append(leader, 2, vec![frames[2].clone()], 0)
                    .await
                    .is_err()
            );
        }
        // The embedding receiver authorizes coverage before this native call.
        // Only the already object-covered prefix may stop being a witness.
        let receipt = store
            .append(leader, 2, vec![frames[2].clone()], 2)
            .await
            .unwrap();
        assert_eq!((receipt.base_sequence, receipt.durable_through), (3, 3));
        store.seal(leader, 2).await.unwrap();
        drop(store);
        let cold = FollowerStore::open(
            root.path().to_owned(),
            limits,
            cellule_ltx::DiskBudget::new(1 << 30),
        )
        .unwrap();
        assert_eq!(
            cold.read_tail(leader, 2, 3).await.unwrap(),
            vec![frames[2].clone()]
        );
    }
    database.close().unwrap();
}

#[tokio::test]
async fn verified_frame_witnesses_remain_scoped_and_revalidate_changed_bytes() {
    let limits = cellule_ltx::Limits::default();
    let source = tempfile::TempDir::new().unwrap();
    let mut database = Db::open(&source.path().join("cell.sqlite"), limits).unwrap();
    database
        .transaction(|transaction| {
            transaction.execute_batch("CREATE TABLE values_(v); INSERT INTO values_ VALUES (1)")
        })
        .unwrap();
    let captured = database.capture().unwrap();
    let encoded = frame(1, &captured.segments[0], limits);
    let digest = *blake3::hash(&encoded).as_bytes();
    let lane = Lane {
        leader: SessionId::from_bytes([1; 16]),
        epoch: 2,
    };
    let root = tempfile::TempDir::new().unwrap();
    let path = root.path().join("chunk.log");
    let mut file = std::fs::File::create(&path).unwrap();
    write_record(&mut file, 1, digest, &encoded).unwrap();
    file.sync_data().unwrap();
    drop(file);
    let initial = scan_chunk(&path, lane, limits, false, None).unwrap();
    assert_eq!(initial.len(), 1);
    let used = Arc::new(Mutex::new(0));
    let known = lane_memory(
        lane,
        limits,
        BTreeMap::from([(1, initial[0].clone())]),
        &initial,
        &used,
    )
    .unwrap();
    assert_eq!(
        scan_chunk(&path, lane, limits, false, Some(&known))
            .unwrap()
            .len(),
        1
    );
    assert!(
        scan_chunk(
            &path,
            Lane { epoch: 3, ..lane },
            limits,
            false,
            Some(&known),
        )
        .is_err()
    );
    assert!(
        scan_chunk(
            &path,
            lane,
            cellule_ltx::Limits {
                max_database_bytes: 512,
                ..limits
            },
            false,
            Some(&known),
        )
        .is_err()
    );

    // Updating the untrusted record digest cannot bless a damaged LTX body.
    let mut damaged = encoded.to_vec();
    *damaged.last_mut().unwrap() ^= 1;
    let mut file = std::fs::File::create(&path).unwrap();
    write_record(&mut file, 1, *blake3::hash(&damaged).as_bytes(), &damaged).unwrap();
    file.sync_data().unwrap();
    drop(file);
    assert!(scan_chunk(&path, lane, limits, false, Some(&known)).is_err());
    assert!(
        scan_chunk(&path, lane, limits, true, Some(&known))
            .unwrap()
            .is_empty()
    );
    assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    drop(known);
    assert_eq!(*used.lock().unwrap(), 0);
    database.close().unwrap();
}
