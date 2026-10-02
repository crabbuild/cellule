use super::*;

#[test]
fn cursor_checks_width_nonzero_and_round_trip() {
    let cursor = FollowerEnrollmentInventoryCursor {
        topology: Digest::from_bytes([7; 32]),
        after: 19,
    };
    assert_eq!(
        FollowerEnrollmentInventoryCursor::from_bytes(&cursor.to_bytes()).unwrap(),
        cursor
    );
    for width in [0, 8, 32, 39, 41, 64] {
        assert!(FollowerEnrollmentInventoryCursor::from_bytes(&vec![1; width]).is_err());
    }
    assert!(FollowerEnrollmentInventoryCursor::from_bytes(&[0; 40]).is_err());
    let mut bytes = cursor.to_bytes();
    bytes[32..].fill(0);
    assert!(FollowerEnrollmentInventoryCursor::from_bytes(&bytes).is_err());
    bytes = cursor.to_bytes();
    bytes[..32].fill(0);
    assert!(FollowerEnrollmentInventoryCursor::from_bytes(&bytes).is_err());
}

#[test]
fn fixed_metadata_leaves_room_for_bounded_signing_scratch() {
    // No signed ensemble/proof Vec or provider token is deep-copied into a page.
    // Each validated member is the fixed Follower role, including its acceptance.
    let rows = MAX_EPOCHS
        * (std::mem::size_of::<FollowerEnrollmentProgress>()
            + MAX_MEMBERS * std::mem::size_of::<FollowerEnrollmentMember>());
    assert!(rows + 4 * (MAX_RECORD_BYTES as usize) < PAGE_BYTES);
}

fn progress() -> Progress {
    Progress {
        members: Vec::new(),
        native_started: false,
        no_effect: false,
        delivered: false,
        enrollment: None,
        refusal: None,
        retirement: None,
        native_closed: false,
        execution_error: None,
        journal_error: None,
        reservation: None,
        cleanup: None,
    }
}
fn digest(progress: &Progress) -> Digest {
    let mut hash = blake3::Hasher::new();
    hash_progress(
        &mut hash,
        1,
        Digest::from_bytes([2; 32]),
        None,
        None,
        progress,
    )
    .unwrap();
    Digest::from_bytes(*hash.finalize().as_bytes())
}

#[test]
fn continuation_changes_for_native_delivery_and_separate_original_errors() {
    let mut progress = progress();
    let original = digest(&progress);
    assert_eq!(original, digest(&progress));
    progress.native_started = true;
    let started = digest(&progress);
    assert_ne!(original, started);
    progress.delivered = true;
    let delivered = digest(&progress);
    assert_ne!(started, delivered);
    progress.execution_error = Some(Arc::new(Error::Node("original native error")));
    let failed = digest(&progress);
    assert_ne!(delivered, failed);
    progress.journal_error = Some(Arc::new(Error::Node("original journal error")));
    assert_ne!(failed, digest(&progress));
    progress.native_closed = true;
    assert_ne!(original, digest(&progress));
}
