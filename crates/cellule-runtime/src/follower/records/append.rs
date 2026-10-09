//! Lane appends, seals, retirements, and chunk rewrites.

use super::*;
use std::collections::BTreeSet;

#[expect(
    clippy::too_many_arguments,
    reason = "append keeps exact scope, admission, cached state, and optional phase evidence explicit"
)]
pub(in crate::follower) fn append_sync(
    root: &Path,
    lane: Lane,
    frames: Vec<Bytes>,
    covered_through: u64,
    limits: cellule_ltx::Limits,
    reservation: &LaneAccounting,
    namespace: &Mutex<()>,
    index_used: &Arc<Mutex<u64>>,
    state: &mut Option<LaneMemory>,
    scan_counter: &ScanCounter,
    observation: &mut AppendObservation,
) -> Result<FollowerReceipt> {
    validate_lane(lane)?;
    let directory = lane_directory(root, lane);
    let chunks = directory.join("chunks");
    if state.is_none() {
        ensure_lane_directories(root, lane, namespace, Some(observation))?;
    }
    if directory.join("retired").exists() {
        return Err(Error::Node("follower lane is retired"));
    }
    if directory.join("sealed").exists() {
        return Err(Error::Node("follower lane is sealed"));
    }
    if state.is_none() {
        reservation.invalidate();
        let retained = scan_lane_counted(&chunks, lane, limits, None, scan_counter)?;
        let open_records = scan_chunk(&chunks.join("open.log"), lane, limits, true, None)?;
        persist_lane_records(&chunks, &retained, &open_records, Some(observation))?;
        *state = Some(lane_memory(
            lane,
            limits,
            retained,
            &open_records,
            index_used,
        )?);
    }
    if let Some(memory) = state.as_mut() {
        // Only the caller's authority-checked object proof releases witnesses.
        memory.covered_through = memory.covered_through.max(covered_through);
    }
    if let Some(memory) = state.as_ref().filter(|memory| !memory.needs_reconciliation) {
        let open_path = chunks.join("open.log");
        let actual = match std::fs::metadata(&open_path) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(error.into()),
        };
        if actual != expected_open_bytes(memory)? {
            return Err(Error::Node("follower open length changed after index"));
        }
    }
    let started = observation.mark();
    let changed = prune_covered(
        &chunks,
        lane,
        covered_through,
        limits,
        state.as_ref(),
        reservation,
        observation,
    )?;
    observation.timing.prune = AppendObservation::elapsed(started);
    let state = state
        .as_mut()
        .ok_or(Error::Node("follower lane state did not initialize"))?;
    if changed || state.needs_reconciliation {
        if state.needs_reconciliation {
            reservation.invalidate();
        }
        reconcile_lane_memory(
            &chunks,
            lane,
            limits,
            state,
            scan_counter,
            Some(observation),
        )?;
    }
    let mut durable_through = state
        .records
        .keys()
        .next_back()
        .copied()
        .unwrap_or(covered_through);
    if durable_through < covered_through {
        durable_through = covered_through;
    }

    let open_path = chunks.join("open.log");
    let mut file = open_append(&open_path, observation)?;
    let mut open_bytes = file.metadata()?.len();
    let expected_open_bytes = expected_open_bytes(state)?;
    if open_bytes != expected_open_bytes {
        return Err(Error::Node("follower open length changed after index"));
    }
    let mut pending: Vec<StoredRecord> = Vec::new();
    let mut pending_digests = HashMap::new();
    let mut open_first = state.open_first;
    let mut open_last = state.open_last;
    let frame_count =
        u64::try_from(frames.len()).map_err(|_| Error::Capacity("follower lane index"))?;
    state.index.grow(
        frame_count
            .checked_mul(INDEX_BYTES_PER_RECORD)
            .ok_or(Error::Capacity("follower lane index"))?,
    )?;
    for encoded in frames {
        let frame = cellule_ltx::inspect_node_frame(encoded.clone(), limits)?;
        let scope = frame.scope();
        if scope.leader_session != *lane.leader.as_bytes() || scope.log_epoch != lane.epoch {
            return Err(Error::Node("follower frame changed lane scope"));
        }
        let sequence = scope.node_sequence;
        let digest = frame.digest();
        if let Some(existing) = state.records.get(&sequence) {
            if existing.digest != digest {
                return Err(Error::Node("conflicting duplicate follower frame"));
            }
            // Cached metadata is not authority for a duplicate proof. Verify
            // this exact stored record even when coverage skipped a prune scan.
            read_indexed_record(root, lane, existing, limits)?;
            continue;
        }
        if let Some(existing) = pending_digests.get(&sequence)
            && *existing != digest
        {
            return Err(Error::Node("conflicting duplicate follower frame"));
        }
        if pending_digests.contains_key(&sequence) {
            continue;
        }
        // Object publication can advance while this frame is still queued for
        // shipping. Its authoritative coverage makes a missing prefix safe to
        // skip; the follower must still persist every uncovered suffix frame.
        if sequence <= covered_through {
            continue;
        }
        if sequence != durable_through.saturating_add(1) {
            return Err(Error::Node("follower append has a sequence gap"));
        }
        let record_bytes = RECORD_HEADER_BYTES as u64 + encoded.len() as u64;
        if open_bytes > 0 && open_bytes.saturating_add(record_bytes) > ROTATE_BYTES {
            observation.sync_data(&file)?;
            drop(file);
            let destination = rotate_open(&chunks, &open_path, open_first, open_last, observation)?;
            relocate_records(
                &mut state.records,
                open_first.ok_or(Error::Node("follower open range is missing"))?,
                open_last.ok_or(Error::Node("follower open range is missing"))?,
                &destination,
            );
            let destination: Arc<Path> = Arc::from(destination.as_path());
            for record in &mut pending {
                // A large admitted batch can rotate more than once. Earlier
                // closed records retain their first destination and offsets.
                if record.path.as_ref() == open_path.as_path() {
                    record.path = Arc::clone(&destination);
                }
            }
            file = open_append(&open_path, observation)?;
            open_first = None;
            open_bytes = 0;
        }
        let offset = open_bytes;
        write_record(&mut file, sequence, digest, &encoded)?;
        reservation.added(record_bytes)?;
        open_bytes = open_bytes
            .checked_add(record_bytes)
            .ok_or(Error::Node("follower open byte count overflow"))?;
        open_first.get_or_insert(sequence);
        open_last = Some(sequence);
        durable_through = sequence;
        pending_digests.insert(sequence, digest);
        pending.push(StoredRecord {
            sequence,
            digest,
            path: Arc::from(open_path.as_path()),
            offset: offset
                .checked_add(RECORD_HEADER_BYTES as u64)
                .ok_or(Error::Node("follower record offset overflow"))?,
            length: encoded.len(),
        });
    }
    if !pending.is_empty() {
        observation.sync_data(&file)?;
        state
            .records
            .extend(pending.into_iter().map(|record| (record.sequence, record)));
        state.open_first = open_first;
        state.open_last = open_last;
    }
    let index_bytes = u64::try_from(state.records.len())
        .map_err(|_| Error::Capacity("follower lane index"))?
        .checked_mul(INDEX_BYTES_PER_RECORD)
        .ok_or(Error::Capacity("follower lane index"))?;
    state.index.shrink_to(index_bytes);
    let base_sequence = state
        .records
        .keys()
        .next()
        .copied()
        .unwrap_or_else(|| durable_through.saturating_add(1));
    Ok(FollowerReceipt {
        base_sequence,
        durable_through,
    })
}

fn expected_open_bytes(state: &LaneMemory) -> Result<u64> {
    match state.open_last {
        Some(sequence) => {
            let last = state
                .records
                .get(&sequence)
                .ok_or(Error::Node("follower open record is missing"))?;
            last.offset
                .checked_add(last.length as u64)
                .ok_or(Error::Node("follower open byte count overflow"))
        }
        None => Ok(0),
    }
}

pub(in crate::follower) fn seal_sync(
    root: &Path,
    lane: Lane,
    limits: cellule_ltx::Limits,
    accounting: &LaneAccounting,
    index_used: &Arc<Mutex<u64>>,
    state: &mut Option<LaneMemory>,
    scan_counter: &ScanCounter,
) -> Result<FollowerReceipt> {
    validate_lane(lane)?;
    let directory = lane_directory(root, lane);
    let chunks = directory.join("chunks");
    let retired = directory.join("retired");
    if retired.exists() {
        let covered_through = read_watermark(&retired, "follower retire marker is invalid")?;
        std::fs::File::open(&retired)?.sync_all()?;
        sync_directory(&directory)?;
        return Ok(FollowerReceipt {
            base_sequence: covered_through.saturating_add(1),
            durable_through: covered_through,
        });
    }
    if state.is_none() {
        accounting.invalidate();
        let retained = scan_lane_counted(&chunks, lane, limits, None, scan_counter)?;
        persist_lane_records(&chunks, &retained, &[], None)?;
        *state = Some(lane_memory(lane, limits, retained, &[], index_used)?);
    }
    let state = state
        .as_mut()
        .ok_or(Error::Node("follower lane state did not initialize"))?;
    if state.needs_reconciliation {
        accounting.invalidate();
        reconcile_lane_memory(&chunks, lane, limits, state, scan_counter, None)?;
    }
    let durable_through = state.records.keys().next_back().copied().unwrap_or(0);
    let base_sequence = state.records.keys().next().copied().unwrap_or(0);
    let marker = directory.join("sealed");
    let file = if marker.exists() {
        let stored = read_watermark(&marker, "follower seal marker is invalid")?;
        if stored != durable_through {
            return Err(Error::Node("follower seal watermark differs"));
        }
        std::fs::File::open(&marker)?
    } else {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker)?;
        file.write_all(&durable_through.to_le_bytes())?;
        accounting.added(8)?;
        file
    };
    // Retrying a marker left by a failed sync must complete the same barriers.
    file.sync_all()?;
    sync_directory(&directory)?;
    Ok(FollowerReceipt {
        base_sequence,
        durable_through,
    })
}
pub(in crate::follower) fn retire_sync(
    root: &Path,
    lane: Lane,
    watermark: RetirementWatermark,
    limits: cellule_ltx::Limits,
    namespace: &Mutex<()>,
    scan_counter: &ScanCounter,
) -> Result<FollowerReceipt> {
    validate_lane(lane)?;
    ensure_lane_directories(root, lane, namespace, None)?;
    let directory = lane_directory(root, lane);
    let chunks = directory.join("chunks");
    let retained = scan_lane_counted(&chunks, lane, limits, None, scan_counter)?;
    let durable_through = retained.keys().next_back().copied().unwrap_or(0);
    let covered_through = match watermark {
        RetirementWatermark::Covered(value) => value,
        RetirementWatermark::Recovered { active } => {
            if !active && durable_through != 0 {
                return Err(Error::Node("follower lane has uncovered records"));
            }
            if directory.join("retired").exists() {
                read_watermark(
                    &directory.join("retired"),
                    "follower retire marker is invalid",
                )?
            } else if directory.join("sealed").exists() {
                let sealed =
                    read_watermark(&directory.join("sealed"), "follower seal marker is invalid")?;
                if sealed != durable_through {
                    return Err(Error::Node("follower seal watermark differs"));
                }
                sealed
            } else if !active {
                // Inactive enrollment cannot acknowledge a tail. The shared
                // coverage check still refuses unexpected native records.
                0
            } else {
                return Err(Error::Node("recovered follower lane has no native seal"));
            }
        }
    };
    if durable_through > covered_through {
        return Err(Error::Node("follower lane has uncovered records"));
    }
    let marker = directory.join("retired");
    let file = if marker.exists() {
        if read_watermark(&marker, "follower retire marker is invalid")? != covered_through {
            return Err(Error::Node("follower retire watermark differs"));
        }
        std::fs::File::open(&marker)?
    } else {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&marker)?;
        file.write_all(&covered_through.to_le_bytes())?;
        file
    };
    file.sync_all()?;
    sync_directory(&directory)?;
    if chunks.exists() {
        std::fs::remove_dir_all(&chunks)?;
    }
    let sealed = directory.join("sealed");
    if sealed.exists() {
        std::fs::remove_file(sealed)?;
    }
    sync_directory(&directory)?;
    Ok(FollowerReceipt {
        base_sequence: covered_through.saturating_add(1),
        durable_through: covered_through,
    })
}
pub(in crate::follower) fn write_record(
    file: &mut std::fs::File,
    sequence: u64,
    digest: [u8; 32],
    encoded: &[u8],
) -> Result<()> {
    file.write_all(RECORD_MAGIC)?;
    file.write_all(&sequence.to_le_bytes())?;
    file.write_all(&(encoded.len() as u64).to_le_bytes())?;
    file.write_all(&digest)?;
    file.write_all(encoded)?;
    Ok(())
}
pub(in crate::follower) fn parse_record_header(
    header: &[u8; RECORD_HEADER_BYTES],
) -> Result<(u64, u64, [u8; 32])> {
    if &header[..4] != RECORD_MAGIC {
        return Err(Error::Node("invalid follower record magic"));
    }
    let sequence = u64::from_le_bytes(
        header[4..12]
            .try_into()
            .map_err(|_| Error::Node("invalid follower record sequence"))?,
    );
    let length = u64::from_le_bytes(
        header[12..20]
            .try_into()
            .map_err(|_| Error::Node("invalid follower record length"))?,
    );
    let digest = header[20..]
        .try_into()
        .map_err(|_| Error::Node("invalid follower record digest"))?;
    if sequence == 0 || length == 0 {
        return Err(Error::Node("invalid follower record header"));
    }
    Ok((sequence, length, digest))
}
pub(in crate::follower) fn open_append(
    path: &Path,
    observation: &mut AppendObservation,
) -> Result<std::fs::File> {
    let existed = match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => true,
        Ok(_) => return Err(Error::Node("follower open chunk is not a regular file")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)?;
    if !existed {
        let parent = path
            .parent()
            .ok_or(Error::Node("follower chunk has no parent"))?;
        observation.sync_directory(parent)?;
    }
    Ok(file)
}
pub(in crate::follower) fn rotate_open(
    chunks: &Path,
    open_path: &Path,
    first: Option<u64>,
    last: Option<u64>,
    observation: &mut AppendObservation,
) -> Result<PathBuf> {
    let (Some(first), Some(last)) = (first, last) else {
        return Err(Error::Node("cannot rotate an empty follower chunk"));
    };
    let destination = chunks.join(format!("{first:020}-{last:020}.log"));
    std::fs::rename(open_path, &destination)?;
    observation.sync_directory(chunks)?;
    Ok(destination)
}
pub(in crate::follower) fn relocate_records(
    records: &mut BTreeMap<u64, StoredRecord>,
    first: u64,
    last: u64,
    destination: &Path,
) {
    let path: Arc<Path> = Arc::from(destination);
    for record in records.range_mut(first..=last).map(|(_, record)| record) {
        record.path = Arc::clone(&path);
    }
}
pub(in crate::follower) fn prune_covered(
    chunks: &Path,
    lane: Lane,
    covered_through: u64,
    limits: cellule_ltx::Limits,
    known: Option<&LaneMemory>,
    reservation: &LaneAccounting,
    observation: &mut AppendObservation,
) -> Result<bool> {
    if !chunks.exists() {
        return Ok(known.is_some_and(|known| !known.records.is_empty()));
    }
    let mut removed = false;
    let mut open_removed = false;
    let mut changed;
    let mut retained_paths = BTreeSet::new();
    for entry in std::fs::read_dir(chunks)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(Error::Node("follower chunk name is not UTF-8"));
        };
        if name == "open.log" {
            continue;
        }
        let Some((_, last)) = parse_chunk_name(name) else {
            return Err(Error::Node("invalid follower chunk name"));
        };
        if last <= covered_through {
            let bytes = entry.metadata()?.len();
            std::fs::remove_file(entry.path())?;
            reservation.removed(bytes)?;
            removed = true;
        } else {
            if !entry.file_type()?.is_file() {
                return Err(Error::Node("follower chunk is not a regular file"));
            }
            retained_paths.insert(entry.path());
        }
    }
    let open_path = chunks.join("open.log");
    if open_path.exists() {
        let original_bytes = std::fs::metadata(&open_path)?.len();
        let records = scan_chunk(&open_path, lane, limits, true, known)?;
        let valid_bytes = records.last().map_or(Ok(0), |record| {
            record
                .offset
                .checked_add(record.length as u64)
                .ok_or(Error::Node("follower open byte count overflow"))
        })?;
        reservation.removed(
            original_bytes
                .checked_sub(valid_bytes)
                .ok_or(Error::Node("follower open grew during prune scan"))?,
        )?;
        changed = known.is_some_and(|known| {
            known.open_first != records.first().map(|record| record.sequence)
                || known.open_last != records.last().map(|record| record.sequence)
                || records.iter().any(|record| {
                    !known.records.get(&record.sequence).is_some_and(|previous| {
                        previous.digest == record.digest
                            && previous.length == record.length
                            && previous.offset == record.offset
                            && previous.path == record.path
                    })
                })
        });
        let mut retained = Vec::with_capacity(records.len());
        for record in records {
            if record.sequence <= covered_through {
                removed = true;
                open_removed = true;
            } else {
                retained.push(record);
            }
        }
        if open_removed {
            rewrite_open_chunk(&open_path, retained, reservation, observation)?;
        }
    } else {
        changed = known.is_some_and(|known| known.open_first.is_some());
    }
    changed |= known.is_some_and(|known| {
        known.records.values().any(|record| {
            record.path.as_ref() != open_path && !retained_paths.contains(record.path.as_ref())
        })
    });
    if removed {
        observation.sync_directory(chunks)?;
    }
    Ok(removed || changed)
}
pub(in crate::follower) fn rewrite_open_chunk(
    path: &Path,
    records: Vec<StoredRecord>,
    reservation: &LaneAccounting,
    observation: &mut AppendObservation,
) -> Result<()> {
    let parent = path
        .parent()
        .ok_or(Error::Node("follower open chunk has no parent"))?;
    let temporary = parent.join("open.log.tmp");
    if temporary.exists() {
        let bytes = std::fs::metadata(&temporary)?.len();
        std::fs::remove_file(&temporary)?;
        reservation.removed(bytes)?;
    }
    let original_bytes = std::fs::metadata(path)?.len();
    if records.is_empty() {
        std::fs::remove_file(path)?;
        reservation.removed(original_bytes)?;
        observation.sync_directory(parent)?;
        return Ok(());
    }

    let scratch = records
        .iter()
        .try_fold(0_u64, |bytes, record| {
            bytes
                .checked_add(RECORD_HEADER_BYTES as u64)
                .and_then(|bytes| bytes.checked_add(record.length as u64))
        })
        .ok_or(Error::Node("follower prune byte count overflow"))?;
    // The old chunk and replacement coexist until rename. Hold this extra
    // charge through the lane's final settlement, including partial-I/O errors;
    // a failed cleanup can leave temporary bytes that remain our obligation.
    reservation.try_grow(scratch, observation)?;

    // Covered prefixes are rewritten through a synced temporary and atomically
    // renamed so a restart sees either the old contiguous lane or the new one.
    let result = (|| {
        let mut source = std::fs::File::open(path)?;
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        for record in records {
            source.seek(SeekFrom::Start(record.offset))?;
            let mut encoded = vec![0; record.length];
            source.read_exact(&mut encoded)?;
            if *blake3::hash(&encoded).as_bytes() != record.digest {
                return Err(Error::Node("stored follower record changed during prune"));
            }
            write_record(&mut output, record.sequence, record.digest, &encoded)?;
            reservation.added(RECORD_HEADER_BYTES as u64 + encoded.len() as u64)?;
        }
        observation.sync_data(&output)?;
        drop(output);
        std::fs::rename(&temporary, path)?;
        reservation.removed(original_bytes)?;
        observation.sync_directory(parent)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}
pub(in crate::follower) fn parse_chunk_name(name: &str) -> Option<(u64, u64)> {
    let value = name.strip_suffix(".log")?;
    let (first, last) = value.split_once('-')?;
    if first.len() != 20 || last.len() != 20 {
        return None;
    }
    Some((first.parse().ok()?, last.parse().ok()?))
}
