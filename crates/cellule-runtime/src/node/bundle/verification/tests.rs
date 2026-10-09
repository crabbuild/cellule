use super::*;
use crate::control::Owner;
use crate::identity::{ApplicationId, CellId, IncarnationId};

fn binding(locators: Vec<Locator>) -> Binding {
    Binding {
        application: ApplicationId::from_bytes([9; 16]),
        first_commit: 1,
        control: Control::initial(
            CellId::from_bytes([4; 32]),
            IncarnationId::from_bytes([14; 16]),
            Owner {
                session: SessionId::from_bytes([1; 16]),
                endpoint: "https://test.internal".into(),
            },
            Digest::from_bytes([12; 32]),
            1,
        )
        .unwrap(),
        phase: BindingPhase::Open,
        terminal: None,
        selected_sequence: 1,
        selected_commit: 1,
        selected_position: cellule_ltx::Position {
            txid: 1,
            checksum: 2,
        },
        locators,
    }
}

fn extent(object: u8, offset: u64, bytes: u64) -> Locator {
    Locator {
        object: Some(Digest::from_bytes([object; 32])),
        offset,
        bytes,
        frame_digest: Digest::from_bytes([7; 32]),
    }
}

#[test]
fn sparse_ranges_bound_padding_use_union_bytes_and_preserve_every_locator() {
    let bindings = [binding(vec![
        extent(1, 0, 10),
        extent(1, 5, 10),
        extent(1, 25, 10),
        extent(1, 500, 10),
        extent(2, 0, 10),
    ])];
    let planned = windows(&bindings)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(planned.len(), 3);
    assert_eq!(planned[0].range, 0..35);
    assert_eq!(
        planned[0].useful_bytes, 25,
        "overlap cannot buy extra padding"
    );
    assert_eq!(planned[1].range, 500..510);
    assert_eq!(planned[2].object, Digest::from_bytes([2; 32]));
    assert_eq!(
        planned
            .iter()
            .map(|window| window.reads.len())
            .sum::<usize>(),
        5
    );
    for window in &planned {
        assert!(window.range.end - window.range.start <= 2 * window.useful_bytes);
    }
    let large = [binding(vec![
        extent(1, 0, MAX_BUNDLE_BYTES),
        extent(1, MAX_BUNDLE_BYTES, 1),
    ])];
    assert_eq!(
        windows(&large).unwrap().count(),
        2,
        "a window never exceeds shared scratch"
    );
}

#[test]
fn protocol_maximum_verification_payload_fits_its_admitted_metadata_charge() {
    // Bound eight lazy windows, original compact indices, doubled growing
    // window-read capacities, all frame facts and all returned facts.
    // Leave allocator/task overhead inside the remaining admitted headroom.
    let count = MAX_FRAMES * MAX_LOCATORS;
    let windows = READ_CONCURRENCY * std::mem::size_of::<Window>();
    let indices = 3 * count * std::mem::size_of::<ReadIndex>();
    let facts = count * std::mem::size_of::<Option<proof::FrameStep>>();
    let returned = count * std::mem::size_of::<(ReadIndex, proof::FrameStep)>();
    let total = windows + indices + facts + returned;
    assert!(
        total + 256 * 1024 <= COHORT_VERIFICATION_BYTES as usize,
        "payload {total} leaves less than 256 KiB overhead"
    );
    let invalid = [binding(vec![extent(1, 0, 0)])];
    assert!(matches!(super::windows(&invalid), Err(Error::Capacity(_))));
    let invalid = [binding(vec![extent(1, u64::MAX, 1)])];
    assert!(matches!(
        super::windows(&invalid),
        Err(Error::Node("bundle locator overflow"))
    ));
}
