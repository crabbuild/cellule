use super::*;

#[test]
fn detached_history_authenticates_scope_count_bytes_and_the_complete_bounded_array() {
    let mut binding = history_binding([4; 32], MAX_LOCATORS);
    let session = SessionId::from_bytes([1; 16]);
    let body = history::encode(session, 2, &binding).unwrap();
    assert!(body.len() as u64 <= history::MAX_HISTORY_BYTES);
    let descriptor = history::History {
        extent: Locator {
            object: Some(Digest::from_bytes([7; 32])),
            offset: HEADER_BYTES as u64,
            bytes: body.len() as u64,
            frame_digest: Digest::from_bytes(*blake3::hash(&body).as_bytes()),
        },
        count: MAX_LOCATORS,
        native_bytes: 128 * MAX_LOCATORS as u64,
        loaded: None,
    };
    assert_eq!(
        history::decode(&body, session, 2, &binding, &descriptor).unwrap(),
        binding.locators
    );
    for (session, epoch) in [(SessionId::from_bytes([2; 16]), 2), (session, 3)] {
        assert!(history::decode(&body, session, epoch, &binding, &descriptor).is_err());
    }
    let wrong = history_binding([8; 32], MAX_LOCATORS);
    assert!(history::decode(&body, session, 2, &wrong, &descriptor).is_err());
    let mut altered = descriptor.clone();
    altered.count -= 1;
    assert!(history::decode(&body, session, 2, &binding, &altered).is_err());
    altered = descriptor.clone();
    altered.native_bytes -= 1;
    assert!(history::decode(&body, session, 2, &binding, &altered).is_err());
    let mut corrupt = body.to_vec();
    *corrupt.last_mut().unwrap() ^= 1;
    assert!(history::decode(&Bytes::from(corrupt), session, 2, &binding, &descriptor).is_err());
    assert!(
        history::decode(
            &body.slice(..body.len() - 1),
            session,
            2,
            &binding,
            &descriptor
        )
        .is_err()
    );
    binding.locators.push(binding.locators[0].clone());
    assert!(matches!(
        history::encode(session, 2, &binding),
        Err(Error::Capacity("bundle history count"))
    ));
    binding.locators.pop();
    for locator in &mut binding.locators {
        locator.bytes = 17 << 10;
    }
    let oversized = history::encode(session, 2, &binding).unwrap();
    altered = descriptor;
    altered.native_bytes = (17 << 10) * MAX_LOCATORS as u64;
    altered.extent.frame_digest = Digest::from_bytes(*blake3::hash(&oversized).as_bytes());
    assert!(matches!(
        history::decode(&oversized, session, 2, &binding, &altered),
        Err(Error::Capacity("bundle history native bytes"))
    ));
}
