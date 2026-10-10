//! Adversarial checks for the application-owned signed transport boundary.
use super::*;
use ed25519_dalek::SigningKey;
use prost::Message;

#[test]
fn follower_signature_rejects_modified_payload_wrong_key_and_wrong_direction() {
    let key = SigningKey::from_bytes(&[1; 32]);
    let request = wire::Request {
        sender: vec![1; 16],
        member: vec![2; 16],
        leader: vec![1; 16],
        epoch: 1,
        operation: 1,
        grant: vec![7; 32],
        frames: vec![vec![3; 128]],
        deadline_ms: 2000,
        ..Default::default()
    };
    let encoded = wire::sign(request.encode_to_vec(), &key, wire::REQUEST_DOMAIN);
    assert!(wire::verify(&encoded, &key.verifying_key(), wire::REQUEST_DOMAIN).is_ok());
    assert!(
        wire::verify(
            &encoded,
            &SigningKey::from_bytes(&[2; 32]).verifying_key(),
            wire::REQUEST_DOMAIN
        )
        .is_err()
    );
    assert!(wire::verify(&encoded, &key.verifying_key(), wire::RESPONSE_DOMAIN).is_err());
    let mut tampered = wire::Signed::decode(encoded.as_slice()).unwrap();
    let mut altered = request;
    altered.covered_through = 99;
    tampered.body = altered.encode_to_vec();
    assert!(
        wire::verify(
            &tampered.encode_to_vec(),
            &key.verifying_key(),
            wire::REQUEST_DOMAIN
        )
        .is_err()
    );
}

#[test]
fn stale_or_oversized_append_and_frames_on_retirement_are_refused() {
    let mut request = wire::Request {
        sender: vec![1; 16],
        member: vec![2; 16],
        leader: vec![1; 16],
        epoch: 1,
        operation: 1,
        grant: vec![7; 32],
        frames: vec![vec![3; 128]],
        deadline_ms: 2000,
        ..Default::default()
    };
    assert!(wire::validate(&request, 1000).is_ok());
    assert!(matches!(
        wire::validate(&request, 2000),
        Err(Error::PeerAuthorization("expired capacity log request"))
    ));
    assert!(matches!(
        wire::validate(&request, -9000),
        Err(Error::PeerAuthorization(
            "capacity log request deadline exceeds allowed horizon"
        ))
    ));
    request.frames = vec![vec![3; 128]; 65];
    assert!(wire::validate(&request, 1000).is_err());
    request.frames.truncate(1);
    request.operation = 3;
    assert!(wire::validate(&request, 1000).is_err());
    request.frames.clear();
    request.grant.clear();
    assert!(wire::validate(&request, 1000).is_ok());
}

#[test]
fn directory_verification_clock_rollback_preserves_horizon_without_extending_expiry() {
    let request = wire::Request {
        sender: vec![1; 16],
        member: vec![2; 16],
        leader: vec![1; 16],
        epoch: 1,
        operation: 1,
        grant: vec![7; 32],
        frames: vec![vec![3; 128]],
        deadline_ms: 11_000,
        ..Default::default()
    };
    assert!(wire::validate(&request, 1000).is_ok());
    // Reproduces the follower's first successful check followed by a backward
    // clock step during directory I/O; the previous second check rejects it.
    assert!(wire::validate(&request, 918).is_err());
    let now = wire::request_time(1000, 918, Duration::from_millis(25)).unwrap();
    assert!(wire::validate(&request, now).is_ok());
    let now = wire::request_time(1000, 918, Duration::from_secs(10)).unwrap();
    assert!(matches!(
        wire::validate(&request, now),
        Err(Error::PeerAuthorization("expired capacity log request"))
    ));
    // A forward clock step still expires the signed deadline immediately.
    let now = wire::request_time(1000, 11_000, Duration::from_millis(25)).unwrap();
    assert!(wire::validate(&request, now).is_err());
}

#[test]
fn grant_issuance_and_append_tokens_have_separate_signed_shapes() {
    let mut request = wire::Request {
        sender: vec![1; 16],
        leader: vec![1; 16],
        member: vec![2; 16],
        epoch: 1,
        operation: 5,
        first_sequence: 1,
        deadline_ms: 2000,
        ..Default::default()
    };
    assert!(wire::validate(&request, 1000).is_ok());
    request.grant = vec![7; 32];
    assert!(wire::validate(&request, 1000).is_err());
    request.operation = 1;
    request.frames = vec![vec![3; 128]];
    assert!(wire::validate(&request, 1000).is_ok());
    request.grant.pop();
    assert!(wire::validate(&request, 1000).is_err());
}
