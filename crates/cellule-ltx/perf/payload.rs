//! Repeatable high-entropy benchmark data; never a security primitive.
pub fn high_entropy(seed: usize, bytes: usize) -> Vec<u8> {
    let mut state = (seed as u64).wrapping_add(1);
    let mut payload = vec![0; bytes];
    for chunk in payload.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
    }
    payload
}
