use super::*;

#[test]
fn grant_lifetime_rejects_negative_or_wrapped_times() {
    let mut grant = NodeAppendGrant {
        body: [0; BODY_BYTES],
        signature: [0; 64],
    };
    let cases = [
        (-1_i64, 1_i64, 0_i64, false),
        (0, i64::MIN, 0, false),
        (0, 0, 0, false),
        (0, APPEND_GRANT_LIFETIME_MS + 1, 0, false),
        (10, 20, 9, false),
        (10, 20, 20, false),
        (10, 20, 10, true),
    ];
    for (issued, expires, now, valid) in cases {
        grant.body[360..368].copy_from_slice(&(issued as u64).to_be_bytes());
        grant.body[368..376].copy_from_slice(&(expires as u64).to_be_bytes());
        assert_eq!(grant.valid_at(now), valid, "{issued}..{expires} at {now}");
    }
}
