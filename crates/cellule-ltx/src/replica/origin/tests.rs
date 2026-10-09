use super::*;

#[test]
fn packed_leaf_working_payload_leaves_bounded_allocator_and_task_headroom() {
    // Account for wire decoding/read copies, two descriptor vectors, extent
    // and inventory node overhead, directory verification and shared rows.
    let metadata = MAX_INLINE_SEGMENTS * (3 * std::mem::size_of::<SegmentDescriptor>() + 256)
        + 32 * 1024
        + shared::SHARED_PUBLICATION_ROWS * 128;
    // Root wire decoding, directory scratch and the packed body occupy
    // separate phases; directory frames do not survive into body reads.
    let root_read = 4 * ROOT_BYTES as usize + metadata;
    let directory = 3 * 32 * 1024 + metadata;
    let body = upload::SINGLE_PUT_BYTES as usize + metadata;
    let payload = root_read.max(directory).max(body);
    assert!(
        payload + 128 * 1024 <= WORKING_BYTES,
        "working payload {payload} leaves less than 128 KiB overhead"
    );
}
