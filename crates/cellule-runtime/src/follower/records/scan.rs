//! Lane tails, indexed records, and chunk scans.

use super::*;
use std::collections::BTreeSet;

#[expect(
    clippy::too_many_arguments,
    reason = "keeps tail bounds, cache ownership, and scan instrumentation explicit"
)]
pub(in crate::follower) fn read_tail_sync(
    root: &Path,
    lane: Lane,
    first_sequence: u64,
    limits: cellule_ltx::Limits,
    index_used: &Arc<Mutex<u64>>,
    state: &mut Option<LaneMemory>,
    max_bytes: usize,
    max_frames: usize,
    scan_counter: &ScanCounter,
) -> Result<FollowerTailPage> {
    validate_lane(lane)?;
    let directory = lane_directory(root, lane);
    let marker = directory.join("sealed");
    if !marker.exists() {
        return Err(Error::Node("follower lane is not sealed"));
    }
    if state.is_none() {
        let retained =
            scan_lane_counted(&directory.join("chunks"), lane, limits, None, scan_counter)?;
        let open_records =
            scan_chunk(&directory.join("chunks/open.log"), lane, limits, true, None)?;
        let scan_only = retained.clone();
        match lane_memory(lane, limits, retained, &open_records, index_used) {
            Ok(memory) => *state = Some(memory),
            Err(Error::Capacity(_)) => {
                return read_tail_records(
                    root,
                    lane,
                    first_sequence,
                    limits,
                    &scan_only,
                    max_bytes,
                    max_frames,
                );
            }
            Err(error) => return Err(error),
        }
    }
    let state = state
        .as_mut()
        .ok_or(Error::Node("follower lane state did not initialize"))?;
    if state.needs_reconciliation {
        reconcile_lane_memory(
            &directory.join("chunks"),
            lane,
            limits,
            state,
            scan_counter,
            None,
        )?;
    }
    read_tail_records(
        root,
        lane,
        first_sequence,
        limits,
        &state.records,
        max_bytes,
        max_frames,
    )
}
pub(in crate::follower) fn read_tail_records(
    root: &Path,
    lane: Lane,
    first_sequence: u64,
    limits: cellule_ltx::Limits,
    records: &BTreeMap<u64, StoredRecord>,
    max_bytes: usize,
    max_frames: usize,
) -> Result<FollowerTailPage> {
    let directory = lane_directory(root, lane);
    let marker = directory.join("sealed");
    let durable_through = records.keys().next_back().copied().unwrap_or(0);
    if read_watermark(&marker, "follower seal marker is invalid")? != durable_through {
        return Err(Error::Node("follower seal watermark differs"));
    }
    if records.is_empty() || first_sequence < *records.keys().next().unwrap_or(&u64::MAX) {
        return Err(Error::Node("requested follower tail is not retained"));
    }
    if first_sequence > durable_through.saturating_add(1) {
        return Err(Error::Node(
            "requested follower tail is beyond durable data",
        ));
    }
    let mut frames = Vec::new();
    let mut bytes = 0_usize;
    let mut next_sequence = None;
    for (sequence, record) in records.range(first_sequence..) {
        if frames.len() == max_frames
            || (!frames.is_empty() && bytes.saturating_add(record.length) > max_bytes)
        {
            next_sequence = Some(*sequence);
            break;
        }
        let encoded = read_indexed_record(root, lane, record, limits)?;
        bytes = bytes.saturating_add(record.length);
        frames.push(encoded);
    }
    Ok(FollowerTailPage {
        frames,
        next_sequence,
    })
}
pub(in crate::follower) fn read_indexed_record(
    root: &Path,
    lane: Lane,
    record: &StoredRecord,
    limits: cellule_ltx::Limits,
) -> Result<Bytes> {
    let chunks = lane_directory(root, lane).join("chunks");
    if record.path.parent() != Some(chunks.as_path())
        || !std::fs::symlink_metadata(&record.path)?
            .file_type()
            .is_file()
    {
        return Err(Error::Node("indexed follower record path is invalid"));
    }
    let header_offset = record
        .offset
        .checked_sub(RECORD_HEADER_BYTES as u64)
        .ok_or(Error::Node("indexed follower record offset is invalid"))?;
    let mut file = std::fs::File::open(&record.path)?;
    file.seek(SeekFrom::Start(header_offset))?;
    let mut header = [0_u8; RECORD_HEADER_BYTES];
    file.read_exact(&mut header)?;
    let (sequence, length, digest) = parse_record_header(&header)?;
    if sequence != record.sequence || length != record.length as u64 || digest != record.digest {
        return Err(Error::Node("indexed follower record header changed"));
    }
    let mut encoded = vec![0; record.length];
    file.read_exact(&mut encoded)?;
    if *blake3::hash(&encoded).as_bytes() != record.digest {
        return Err(Error::Node("stored follower record changed after index"));
    }
    let frame = cellule_ltx::inspect_node_frame(Bytes::from(encoded.clone()), limits)?;
    let scope = frame.scope();
    if scope.node_sequence != record.sequence
        || scope.leader_session != *lane.leader.as_bytes()
        || scope.log_epoch != lane.epoch
        || frame.digest() != record.digest
    {
        return Err(Error::Node("indexed follower record scope changed"));
    }
    Ok(Bytes::from(encoded))
}
pub(in crate::follower) fn lane_memory(
    lane: Lane,
    limits: cellule_ltx::Limits,
    records: BTreeMap<u64, StoredRecord>,
    open_records: &[StoredRecord],
    index_used: &Arc<Mutex<u64>>,
) -> Result<LaneMemory> {
    let record_count =
        u64::try_from(records.len()).map_err(|_| Error::Capacity("follower lane index"))?;
    let bytes = record_count
        .checked_mul(INDEX_BYTES_PER_RECORD)
        .ok_or(Error::Capacity("follower lane index"))?;
    Ok(LaneMemory {
        lane,
        limits,
        open_first: open_records.first().map(|record| record.sequence),
        open_last: open_records.last().map(|record| record.sequence),
        index: IndexReservation::new(index_used, bytes)?,
        records,
        needs_reconciliation: false,
        covered_through: 0,
    })
}

pub(in crate::follower) fn reconcile_lane_memory(
    chunks: &Path,
    lane: Lane,
    limits: cellule_ltx::Limits,
    state: &mut LaneMemory,
    scan_counter: &ScanCounter,
    observation: Option<&mut AppendObservation>,
) -> Result<()> {
    let records = scan_lane_counted(chunks, lane, limits, Some(state), scan_counter)?;
    // Only authority-proven object coverage may release an original witness.
    for (sequence, original) in state.records.range((
        std::ops::Bound::Excluded(state.covered_through),
        std::ops::Bound::Unbounded,
    )) {
        if !records.get(sequence).is_some_and(|record| {
            record.digest == original.digest && record.length == original.length
        }) {
            return Err(Error::Node("follower lane lost an uncovered indexed frame"));
        }
    }
    let open_records = scan_chunk(&chunks.join("open.log"), lane, limits, true, Some(state))?;
    if state.needs_reconciliation {
        // A failed append may leave valid, unacknowledged records or a renamed
        // chunk. Persist their bytes and directory before promoting the index.
        persist_lane_records(chunks, &records, &open_records, observation)?;
    }
    let bytes = u64::try_from(records.len())
        .map_err(|_| Error::Capacity("follower lane index"))?
        .checked_mul(INDEX_BYTES_PER_RECORD)
        .ok_or(Error::Capacity("follower lane index"))?;
    state.index.resize_to(bytes)?;
    state.records = records;
    state.open_first = open_records.first().map(|record| record.sequence);
    state.open_last = open_records.last().map(|record| record.sequence);
    state.needs_reconciliation = false;
    Ok(())
}

pub(in crate::follower) fn persist_lane_records(
    chunks: &Path,
    records: &BTreeMap<u64, StoredRecord>,
    open_records: &[StoredRecord],
    mut observation: Option<&mut AppendObservation>,
) -> Result<()> {
    let paths = records
        .values()
        .chain(open_records.iter())
        .map(|record| &record.path)
        .collect::<BTreeSet<_>>();
    for path in paths {
        let file = std::fs::File::open(path)?;
        match observation.as_deref_mut() {
            Some(observation) => observation.sync_data(&file)?,
            None => file.sync_data()?,
        }
    }
    if chunks.exists() {
        match observation {
            Some(observation) => observation.sync_directory(chunks)?,
            None => sync_directory(chunks)?,
        }
    }
    Ok(())
}
pub(in crate::follower) fn scan_lane(
    chunks: &Path,
    lane: Lane,
    limits: cellule_ltx::Limits,
    known: Option<&LaneMemory>,
) -> Result<BTreeMap<u64, StoredRecord>> {
    if !chunks.exists() {
        return Ok(BTreeMap::new());
    }
    let mut paths = std::fs::read_dir(chunks)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    let mut records = BTreeMap::new();
    for path in paths {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(Error::Node("follower chunk name is not UTF-8"))?;
        let open = name == "open.log";
        let expected = if open {
            None
        } else {
            Some(parse_chunk_name(name).ok_or(Error::Node("invalid follower chunk name"))?)
        };
        let chunk = scan_chunk(&path, lane, limits, open, known)?;
        if let Some((first, last)) = expected
            && (chunk.first().map(|record| record.sequence) != Some(first)
                || chunk.last().map(|record| record.sequence) != Some(last))
        {
            return Err(Error::Node("follower chunk name differs from contents"));
        }
        for record in chunk {
            match records.entry(record.sequence) {
                Entry::Vacant(entry) => {
                    entry.insert(record);
                }
                Entry::Occupied(entry) if entry.get().digest == record.digest => {}
                Entry::Occupied(_) => {
                    return Err(Error::Node("conflicting stored follower frame"));
                }
            }
        }
    }
    if !records
        .keys()
        .copied()
        .collect::<Vec<_>>()
        .windows(2)
        .all(|pair| pair[0].checked_add(1) == Some(pair[1]))
    {
        return Err(Error::Node("stored follower lane has a sequence gap"));
    }
    Ok(records)
}
pub(in crate::follower) fn scan_lane_counted(
    chunks: &Path,
    lane: Lane,
    limits: cellule_ltx::Limits,
    known: Option<&LaneMemory>,
    counter: &ScanCounter,
) -> Result<BTreeMap<u64, StoredRecord>> {
    count_scan(counter);
    scan_lane(chunks, lane, limits, known)
}
pub(in crate::follower) fn read_watermark(path: &Path, invalid: &'static str) -> Result<u64> {
    let bytes = std::fs::read(path)?;
    let watermark = bytes
        .as_slice()
        .try_into()
        .map(u64::from_le_bytes)
        .map_err(|_| Error::Node(invalid))?;
    Ok(watermark)
}
pub(in crate::follower) fn scan_chunk(
    path: &Path,
    lane: Lane,
    limits: cellule_ltx::Limits,
    truncate_suffix: bool,
    known: Option<&LaneMemory>,
) -> Result<Vec<StoredRecord>> {
    let mut file = match std::fs::OpenOptions::new()
        .read(true)
        .write(truncate_suffix)
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    let mut records = Vec::new();
    let mut valid_bytes = 0_u64;
    let path: Arc<Path> = Arc::from(path);
    loop {
        let mut header = [0_u8; RECORD_HEADER_BYTES];
        match file.read_exact(&mut header) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return finish_scan(file, records, valid_bytes, truncate_suffix);
            }
            Err(error) => return Err(error.into()),
        }
        let parsed = parse_record_header(&header);
        let (sequence, length, digest) = match parsed {
            Ok(value) => value,
            Err(_error) if truncate_suffix => {
                file.set_len(valid_bytes)?;
                file.sync_data()?;
                return Ok(records);
            }
            Err(error) => return Err(error),
        };
        if length
            > limits
                .max_capture_bytes
                .saturating_add(cellule_ltx::MAX_NODE_FRAME_HEADER_BYTES as u64)
            || length > usize::MAX as u64
        {
            if truncate_suffix {
                file.set_len(valid_bytes)?;
                file.sync_data()?;
                return Ok(records);
            }
            return Err(Error::Node("stored follower frame exceeds limit"));
        }
        let mut encoded = vec![0; length as usize];
        if let Err(error) = file.read_exact(&mut encoded) {
            if truncate_suffix && error.kind() == std::io::ErrorKind::UnexpectedEof {
                file.set_len(valid_bytes)?;
                file.sync_data()?;
                return Ok(records);
            }
            return Err(error.into());
        }
        let encoded = Bytes::from(encoded);
        let previous = verified_record(known, lane, limits, sequence);
        // The indexed digest comes from complete envelope/LTX validation.
        // Reuse requires hashing every current disk byte under the same lane
        // and bounds. Cold scans have no witness and always decode the body.
        let unchanged = previous.is_some_and(|record| {
            record.sequence == sequence
                && record.length == encoded.len()
                && record.digest == digest
                && *blake3::hash(&encoded).as_bytes() == digest
        });
        if !unchanged {
            let frame = match cellule_ltx::inspect_node_frame(encoded.clone(), limits) {
                Ok(frame) if frame.digest() == digest => frame,
                Ok(_) | Err(_) if truncate_suffix => {
                    file.set_len(valid_bytes)?;
                    file.sync_data()?;
                    return Ok(records);
                }
                Ok(_) => return Err(Error::Node("stored follower record digest differs")),
                Err(error) => return Err(error.into()),
            };
            let scope = frame.scope();
            if scope.node_sequence != sequence
                || scope.leader_session != *lane.leader.as_bytes()
                || scope.log_epoch != lane.epoch
            {
                if truncate_suffix {
                    file.set_len(valid_bytes)?;
                    file.sync_data()?;
                    return Ok(records);
                }
                return Err(Error::Node("stored follower record scope differs"));
            }
            if previous.is_some_and(|record| {
                record.sequence != sequence
                    || record.length != encoded.len()
                    || record.digest != digest
            }) && known.is_none_or(|memory| sequence > memory.covered_through)
            {
                return Err(Error::Node("stored follower record changed after index"));
            }
        }
        // Keep only verified locations, not every body in the lane. Restart,
        // seal and paged reads must not allocate the entire retained log.
        records.push(StoredRecord {
            sequence,
            digest,
            path: Arc::clone(&path),
            offset: valid_bytes + RECORD_HEADER_BYTES as u64,
            length: encoded.len(),
        });
        valid_bytes = valid_bytes
            .checked_add(RECORD_HEADER_BYTES as u64 + length)
            .ok_or(Error::Node("follower chunk length overflow"))?;
    }
}

fn verified_record(
    known: Option<&LaneMemory>,
    lane: Lane,
    limits: cellule_ltx::Limits,
    sequence: u64,
) -> Option<&StoredRecord> {
    let known = known?;
    if known.lane != lane || verification_bounds(known.limits) != verification_bounds(limits) {
        return None;
    }
    known.records.get(&sequence)
}

fn verification_bounds(limits: cellule_ltx::Limits) -> (u64, u64, u64, u64, usize) {
    let cellule_ltx::Limits {
        max_database_bytes,
        max_capture_bytes,
        max_file_bytes,
        max_plan_bytes,
        max_segments,
    } = limits;
    (
        max_database_bytes,
        max_capture_bytes,
        max_file_bytes,
        max_plan_bytes,
        max_segments,
    )
}
pub(in crate::follower) fn finish_scan(
    mut file: std::fs::File,
    records: Vec<StoredRecord>,
    valid_bytes: u64,
    truncate_suffix: bool,
) -> Result<Vec<StoredRecord>> {
    if file.seek(SeekFrom::End(0))? != valid_bytes {
        if !truncate_suffix {
            return Err(Error::Node("sealed follower chunk has a torn suffix"));
        }
        file.set_len(valid_bytes)?;
        file.sync_data()?;
    }
    Ok(records)
}
