use super::*;

fn extent(object: u8, start: u64, bytes: u64) -> Locator {
    Locator {
        object: Some(Digest::from_bytes([object; 32])),
        offset: HEADER_BYTES as u64 + start,
        bytes,
        frame_digest: Digest::from_bytes([9; 32]),
    }
}

#[test]
fn sparse_metadata_windows_charge_gaps_once_and_preserve_all_original_indices() {
    let extents = [
        extent(1, 0, 10),
        extent(1, 5, 10),
        extent(1, 25, 10),
        extent(1, 50, 10),
        extent(2, 0, 10),
    ];
    let locate = |index: u16| Ok(&extents[usize::from(index)]);
    let mut indices = [4, 3, 2, 1, 0];
    sort(&mut indices, locate).unwrap();
    let mut next = 0;
    let mut padding = 10;
    let windows = cohort(&indices, &mut next, &mut padding, locate).unwrap();
    assert_eq!(windows.len(), 3);
    assert_eq!(windows[0].indices, 0..3);
    assert_eq!(
        windows[0].range,
        HEADER_BYTES as u64..HEADER_BYTES as u64 + 35
    );
    assert_eq!(windows[1].indices, 3..4);
    assert_eq!(windows[2].object, Digest::from_bytes([2; 32]));
    assert_eq!(padding, 0, "overlap cannot buy gap credit");
    assert_eq!(next, extents.len());
    let wire: u64 = windows.iter().map(|w| w.range.end - w.range.start).sum();
    assert!(wire <= extents.iter().map(|e| e.bytes).sum::<u64>() + 10);
    let body = Bytes::from_static(b"01234567890123456789012345678901234");
    assert_eq!(
        windows[0].slice(&body, &extents[2]).unwrap(),
        body.slice(25..35)
    );
    assert!(windows[0].slice(&body.slice(..10), &extents[2]).is_err());
    assert!(windows[0].slice(&body, &extents[4]).is_err());
}

#[test]
fn eight_metadata_windows_and_compact_protocol_indices_stay_bounded() {
    let extents: Vec<_> = (1..=12).map(|object| extent(object, 0, 10)).collect();
    let locate = |index: u16| Ok(&extents[usize::from(index)]);
    let mut indices: Vec<_> = (0..extents.len()).map(|index| index as u16).collect();
    sort(&mut indices, locate).unwrap();
    let mut next = 0;
    let mut padding = MAX_BUNDLE_BYTES;
    assert_eq!(
        cohort(&indices, &mut next, &mut padding, locate)
            .unwrap()
            .len(),
        8
    );
    assert_eq!(next, 8);
    assert_eq!(
        cohort(&indices, &mut next, &mut padding, locate)
            .unwrap()
            .len(),
        4
    );
    assert_eq!(padding, MAX_BUNDLE_BYTES);
    // Catalog and history phases do not overlap. Bound each phase's retained
    // index vector and eight descriptors, leaving 16 KiB for task overhead.
    let planning = MAX_BINDINGS * std::mem::size_of::<u16>()
        + READ_CONCURRENCY * std::mem::size_of::<Window>();
    assert!(planning + 16 * 1024 <= 32 * 1024);
}

#[test]
fn exhausted_gap_credit_keeps_valid_metadata_as_separate_reads() {
    let extents = [extent(1, 0, 10), extent(1, 11, 10)];
    let locate = |index: u16| Ok(&extents[usize::from(index)]);
    let mut indices = [0, 1];
    sort(&mut indices, locate).unwrap();
    let mut next = 0;
    let mut padding = 0;
    let windows = cohort(&indices, &mut next, &mut padding, locate).unwrap();
    assert_eq!(windows.len(), 2);
    let mut invalid = [extent(1, 0, 1)];
    invalid[0].offset = u64::MAX;
    assert!(sort(&mut [0], |index| Ok(&invalid[usize::from(index)])).is_err());
}
