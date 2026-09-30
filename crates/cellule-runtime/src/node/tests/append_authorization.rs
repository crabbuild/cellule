//! One fresh canonical observation for mTLS-bound follower appends.

use super::*;
use cellule_store::test_support::CountingObjectStore;
use object_store::throttle::{ThrottleConfig, ThrottledStore};
use std::time::{Duration, Instant};

struct Fixture {
    directory: NodeDirectory,
    store: Arc<CountingObjectStore>,
    key: SigningKey,
    leader: SessionId,
    member: NodeId,
    path: Path,
}

impl Fixture {
    async fn new() -> Self {
        let store = Arc::new(CountingObjectStore::new(Arc::new(ThrottledStore::new(
            InMemory::new(),
            ThrottleConfig {
                wait_get_per_call: Duration::from_millis(5),
                ..ThrottleConfig::default()
            },
        ))));
        let layout =
            CellStorageLayout::new(Store::new(store.clone()), Path::from("append"), [9; 16]);
        let leader = SessionId::from_bytes([1; 16]);
        let follower = SessionId::from_bytes([2; 16]);
        let path = layout.node_path(leader.as_bytes());
        let directory = NodeDirectory::new(
            layout,
            Digest::from_bytes([2; 32]),
            Digest::from_bytes([4; 32]),
            Digest::from_bytes([5; 32]),
        );
        let key = SigningKey::from_bytes(&[7; 32]);
        let created = directory
            .create(advertisement_for(leader, &key, 1, NOW_MS), NOW_MS)
            .await
            .unwrap();
        directory
            .create(advertisement_for(follower, &key, 1, NOW_MS), NOW_MS)
            .await
            .unwrap();
        let enrolled = directory
            .recruit_log(&created, 4, 1, 2, NOW_MS + 1)
            .await
            .unwrap();
        directory
            .advance_log_coverage(&enrolled, 27, NOW_MS + 2)
            .await
            .unwrap();
        store.reset();
        Self {
            directory,
            store,
            key,
            leader,
            member: node(follower),
            path,
        }
    }

    async fn enrollment(&self, now_ms: i64) -> Result<EnrolledPeerVerifier> {
        self.directory
            .peer_verifier(
                self.leader,
                Digest::from_bytes([3; 32]),
                self.key.verifying_key().to_bytes(),
                now_ms,
            )
            .await
    }
}

#[tokio::test]
async fn authenticated_append_uses_one_canonical_observation_per_request() {
    let f = Fixture::new().await;
    for _ in 0..2 {
        f.store.reset();
        let proof = f.enrollment(NOW_MS + 3).await.unwrap();
        assert_eq!(
            proof
                .authorize_log_append(f.member, 4, 27, NOW_MS + 4)
                .unwrap()
                .tiered_through(),
            27
        );
        let reads = f.store.requests();
        assert_eq!(reads.len(), 1);
        assert_eq!(reads[0].location, f.path.to_string());
    }
    // The existing directory API and new per-request proof use identical checks.
    for (member, epoch, watermark) in [
        (f.member, 4, 0),
        (f.member, 4, 28),
        (f.member, 5, 0),
        (NodeId::from_bytes([9; 16]), 4, 0),
    ] {
        let canonical = f
            .directory
            .authorize_log_append(f.leader, member, epoch, watermark, NOW_MS + 3)
            .await;
        let enrolled = f
            .enrollment(NOW_MS + 3)
            .await
            .unwrap()
            .authorize_log_append(member, epoch, watermark, NOW_MS + 3);
        assert_eq!(canonical.is_ok(), enrolled.is_ok());
        assert_eq!(
            canonical.is_ok(),
            member == f.member && epoch == 4 && watermark <= 27
        );
    }
}

#[tokio::test]
async fn append_proof_rechecks_expiry_after_delayed_enrollment_io() {
    let f = Fixture::new().await;
    let read_at = NOW_MS + 9_999;
    let started = Instant::now();
    let proof = f.enrollment(read_at).await.unwrap();
    // The provider read starts while the lease is valid and crosses expiry.
    let authorize_at = read_at + i64::try_from(started.elapsed().as_millis()).unwrap();
    assert!(authorize_at >= NOW_MS + 10_000);
    assert!(
        proof
            .authorize_log_append(f.member, 4, 0, authorize_at)
            .is_err()
    );
    assert_eq!(f.store.requests().len(), 1);
}

#[tokio::test]
async fn append_enrollment_rejects_wrong_identity_and_scope() {
    let f = Fixture::new().await;
    for (certificate, key) in [
        (
            Digest::from_bytes([99; 32]),
            f.key.verifying_key().to_bytes(),
        ),
        (Digest::from_bytes([3; 32]), [99; 32]),
    ] {
        assert!(
            f.directory
                .peer_verifier(f.leader, certificate, key, NOW_MS + 3)
                .await
                .is_err()
        );
    }
    for (fleet, image, release) in [(99, 4, 5), (2, 99, 5), (2, 4, 99)] {
        let directory = NodeDirectory::new(
            f.directory.layout.clone(),
            Digest::from_bytes([fleet; 32]),
            Digest::from_bytes([image; 32]),
            Digest::from_bytes([release; 32]),
        );
        assert!(
            directory
                .peer_verifier(
                    f.leader,
                    Digest::from_bytes([3; 32]),
                    f.key.verifying_key().to_bytes(),
                    NOW_MS + 3
                )
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn append_enrollment_reloads_tombstones_and_closed_logs() {
    let f = Fixture::new().await;
    let observed = f
        .directory
        .load(f.leader, NOW_MS + 3)
        .await
        .unwrap()
        .unwrap();
    let gate =
        crate::node::log::DurabilityGate::new(f.leader, node(f.leader), 4, [f.member]).unwrap();
    let ticket = gate.issue(27).unwrap();
    gate.prove_object(ticket).unwrap();
    crate::node::log::close_node_log(
        &f.directory,
        Arc::new(UnavailableFollowerTransport),
        &observed,
        &gate,
        NOW_MS + 4,
    )
    .await
    .unwrap();
    assert!(
        f.enrollment(NOW_MS + 5)
            .await
            .unwrap()
            .authorize_log_append(f.member, 4, 0, NOW_MS + 5)
            .is_err()
    );
    let closed = f
        .directory
        .load(f.leader, NOW_MS + 5)
        .await
        .unwrap()
        .unwrap();
    f.directory.withdraw(&closed, NOW_MS + 6).await.unwrap();
    f.store.reset();
    assert!(f.enrollment(NOW_MS + 7).await.is_err());
    assert_eq!(f.store.requests().len(), 1);
    assert_eq!(f.store.requests()[0].location, f.path.to_string());
}
