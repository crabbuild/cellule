use super::*;
use crate::{
    identity::NodeId,
    node::{NODE_LOG_PROTOCOL_VERSION, NodeAdvertisement, NodeCapacity, NodeFailureDomain},
};
use cellule_ltx::{CellStorageLayout, Db, Limits, NodeFrameScope, encode_node_frame};
use cellule_store::{Store, test_support::CountingObjectStore};
use object_store::{memory::InMemory, path::Path as ObjectPath};

const NOW: i64 = 1_000_000;
struct Fixture {
    _root: tempfile::TempDir,
    follower: FollowerStore,
    directory: NodeDirectory,
    backend: Arc<CountingObjectStore>,
    key: SigningKey,
    peer: AppendGrantPeer,
    receiver: NodeAdvertisement,
    lease: crate::NodeLeaseGuard,
    db: Db,
}
impl Fixture {
    async fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let key = SigningKey::from_bytes(&[7; 32]);
        let backend = Arc::new(CountingObjectStore::new(Arc::new(InMemory::new())));
        let directory = NodeDirectory::new(
            CellStorageLayout::new(
                Store::new(backend.clone()),
                ObjectPath::from("grants"),
                [9; 16],
            ),
            Digest::from_bytes([2; 32]),
            Digest::from_bytes([4; 32]),
            Digest::from_bytes([5; 32]),
        );
        let advert = |session| {
            NodeAdvertisement::sign(
                NodeId::from_bytes([session; 16]),
                SessionId::from_bytes([session; 16]),
                "https://node.invalid".into(),
                Digest::from_bytes([2; 32]),
                Digest::from_bytes([3; 32]),
                Digest::from_bytes([4; 32]),
                Digest::from_bytes([5; 32]),
                &key,
                1,
                NOW,
                NOW + 10_000,
                vec![Digest::from_bytes([6; 32])],
                vec![1],
                NodeFailureDomain::default(),
                NodeCapacity {
                    free_memory_bytes: 1 << 20,
                    free_disk_bytes: 1 << 20,
                    follower_free_bytes: 1 << 20,
                    follower_retained_bytes: 0,
                    job_credits: 2,
                    log_protocol: NODE_LOG_PROTOCOL_VERSION,
                },
            )
            .unwrap()
        };
        let leader = directory.create(advert(1), NOW).await.unwrap();
        let receiver = advert(2);
        directory.create(receiver.clone(), NOW).await.unwrap();
        directory.recruit_log(&leader, 2, 1, 2, NOW).await.unwrap();
        let follower = FollowerStore::open(
            root.path().join("followers"),
            Limits::default(),
            cellule_ltx::DiskBudget::new(8 << 20),
        )
        .unwrap()
        .with_append_grant_receiver(receiver.session());
        let mut db = Db::open(&root.path().join("source"), Limits::default()).unwrap();
        db.transaction(|tx| tx.execute_batch("CREATE TABLE t(v); INSERT INTO t VALUES(0)"))
            .unwrap();
        Self {
            _root: root,
            follower,
            directory,
            backend,
            key,
            peer: AppendGrantPeer {
                session: SessionId::from_bytes([1; 16]),
                certificate: Digest::from_bytes([3; 32]),
                public_key: SigningKey::from_bytes(&[7; 32]).verifying_key().to_bytes(),
            },
            receiver,
            lease: crate::NodeLeaseGuard::new(NOW, NOW + 10_000).unwrap(),
            db,
        }
    }
    async fn grant(&self, first: u64) -> Result<NodeAppendGrant> {
        self.follower
            .open_append_grant(
                self.peer,
                2,
                first,
                AppendGrantIssuer {
                    directory: &self.directory,
                    signing_key: &self.key,
                    lease: &self.lease,
                    now_ms: NOW,
                },
            )
            .await
    }
    fn frame(&mut self, sequence: u64) -> Bytes {
        self.db
            .transaction(|tx| tx.execute("UPDATE t SET v=v+1", []).map(|_| ()))
            .unwrap();
        let cuts = self.db.capture().unwrap();
        let segment = &cuts.segments[0];
        encode_node_frame(
            NodeFrameScope {
                leader_session: *self.peer.session.as_bytes(),
                log_epoch: 2,
                node_sequence: sequence,
                application: [9; 16],
                cell: [4; 32],
                incarnation: [5; 16],
                cell_epoch: 1,
                commit_sequence: sequence,
            },
            segment.info().clone(),
            Bytes::from(std::fs::read(segment.path()).unwrap()),
            Limits::default(),
        )
        .unwrap()
        .encoded()
        .clone()
    }
    async fn append(&self, grant: &NodeAppendGrant, frames: Vec<Bytes>) -> Result<FollowerReceipt> {
        self.follower
            .append_granted(
                GrantedFollowerAppend {
                    peer: self.peer,
                    log_epoch: 2,
                    grant: grant.digest(),
                    frames,
                },
                self.lease.clone(),
            )
            .await
    }
}

#[tokio::test]
async fn signed_grant_binds_every_byte_and_amortizes_directory_reads() {
    let mut f = Fixture::new().await;
    let grant = f.grant(1).await.unwrap();
    let encoded = grant.encode();
    let decoded = NodeAppendGrant::verify(&encoded, &f.receiver, NOW).unwrap();
    assert_eq!(decoded.digest(), grant.digest());
    assert_eq!(grant.last_sequence(), 512);
    for index in 0..encoded.len() {
        let mut bad = encoded.clone();
        bad[index] ^= 1;
        assert!(
            NodeAppendGrant::verify(&bad, &f.receiver, NOW).is_err(),
            "byte {index}"
        );
    }
    assert!(NodeAppendGrant::verify(&encoded[..encoded.len() - 1], &f.receiver, NOW).is_err());
    assert!(
        NodeAppendGrant::verify(&encoded, &f.receiver, NOW + APPEND_GRANT_LIFETIME_MS).is_err()
    );
    assert!(
        grant
            .authorize(
                SessionId::from_bytes([8; 16]),
                f.peer.certificate,
                f.peer.public_key,
                2,
                1,
                1
            )
            .is_err()
    );
    assert!(
        grant
            .authorize(
                f.peer.session,
                Digest::from_bytes([8; 32]),
                f.peer.public_key,
                2,
                1,
                1
            )
            .is_err()
    );
    assert!(
        grant
            .authorize(f.peer.session, f.peer.certificate, [8; 32], 2, 1, 1)
            .is_err()
    );
    assert!(
        grant
            .authorize(
                f.peer.session,
                f.peer.certificate,
                f.peer.public_key,
                3,
                1,
                1
            )
            .is_err()
    );
    assert!(
        grant
            .authorize(
                f.peer.session,
                f.peer.certificate,
                f.peer.public_key,
                2,
                1,
                513
            )
            .is_err()
    );
    // The corruption loop consumes real time; renew before checking native I/O.
    let grant = f.grant(1).await.unwrap();
    f.backend.reset();
    for sequence in 1..=32 {
        let frame = f.frame(sequence);
        let receipt = f.append(&grant, vec![frame]).await.unwrap();
        assert_eq!(receipt.durable_through, sequence);
    }
    assert!(f.backend.requests().is_empty());
    assert!(f.follower.retained_bytes() > 0);
}

#[tokio::test]
async fn local_seal_and_retirement_exclude_old_grants_and_replayed_issuance() {
    let mut f = Fixture::new().await;
    let grant = f.grant(1).await.unwrap();
    let first = f.frame(1);
    f.append(&grant, vec![first]).await.unwrap();
    // A lost seal did not execute locally. An uncovered retirement must fail
    // and preserve the active append witness rather than manufacture closure.
    assert!(f.follower.retire(f.peer.session, 2, 0).await.is_err());
    let second = f.frame(2);
    f.append(&grant, vec![second.clone()]).await.unwrap();
    assert_eq!(
        f.follower
            .seal(f.peer.session, 2)
            .await
            .unwrap()
            .durable_through,
        2
    );
    assert!(matches!(
        f.append(&grant, vec![second]).await,
        Err(Error::Fenced)
    ));
    f.backend.reset();
    assert!(matches!(f.grant(3).await, Err(Error::Fenced)));
    assert!(f.backend.requests().is_empty());
    f.follower.retire(f.peer.session, 2, 2).await.unwrap();
    let retired = f.follower.retired_lanes(i64::MAX, 8).await.unwrap();
    assert_eq!(retired.len(), 1);
    assert!(
        f.follower
            .remove_retired(retired[0], i64::MAX)
            .await
            .unwrap()
    );
    let replay = f.frame(3);
    assert!(matches!(
        f.append(&grant, vec![replay]).await,
        Err(Error::Fenced)
    ));
}

#[tokio::test]
async fn restart_with_persisted_key_rejects_the_original_receiver_grant() {
    let mut f = Fixture::new().await;
    let grant = f.grant(1).await.unwrap();
    let frame = f.frame(1);
    f.append(&grant, vec![frame.clone()]).await.unwrap();
    let restarted = FollowerStore::open(
        f._root.path().join("followers"),
        Limits::default(),
        cellule_ltx::DiskBudget::new(8 << 20),
    )
    .unwrap()
    .with_append_grant_receiver(SessionId::from_bytes([3; 16]));
    assert!(matches!(
        restarted
            .append_granted(
                GrantedFollowerAppend {
                    peer: f.peer,
                    log_epoch: 2,
                    grant: grant.digest(),
                    frames: vec![frame]
                },
                f.lease.clone()
            )
            .await,
        Err(Error::Fenced)
    ));
    let receipt = restarted.seal(f.peer.session, 2).await.unwrap();
    assert_eq!(receipt.durable_through, 1);
    assert_eq!(
        restarted
            .read_tail(f.peer.session, 2, 1)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(restarted.grant_slots.available_permits(), MAX_APPEND_GRANTS);
}

#[tokio::test]
async fn expiry_and_receiver_lease_loss_cannot_release_a_proof() {
    let mut f = Fixture::new().await;
    let grant = f.grant(1).await.unwrap();
    let lane = Lane {
        leader: f.peer.session,
        epoch: 2,
    };
    // Deterministically exhaust the local monotonic horizon. Changing a wall
    // sample, or replaying the signature, cannot extend this registry deadline.
    f.follower
        .lane_lock(lane)
        .unwrap()
        .grant
        .lock()
        .await
        .current
        .as_mut()
        .unwrap()
        .expires = Instant::now();
    let frame = f.frame(1);
    assert!(matches!(
        f.append(&grant, vec![frame.clone()]).await,
        Err(Error::Fenced)
    ));
    let grant = f.grant(1).await.unwrap();
    f.lease.fence();
    assert!(matches!(
        f.append(&grant, vec![frame]).await,
        Err(Error::Fenced)
    ));
    assert!(matches!(f.grant(1).await, Err(Error::Fenced)));
}

#[tokio::test]
async fn rejected_issuance_does_not_accumulate_empty_lanes_or_index_charges() {
    let f = Fixture::new().await;
    for byte in 10..40 {
        let mut peer = f.peer;
        peer.session = SessionId::from_bytes([byte; 16]);
        assert!(
            f.follower
                .open_append_grant(
                    peer,
                    2,
                    1,
                    AppendGrantIssuer {
                        directory: &f.directory,
                        signing_key: &f.key,
                        lease: &f.lease,
                        now_ms: NOW
                    }
                )
                .await
                .is_err()
        );
    }
    assert!(f.follower.lanes.lock().unwrap().is_empty());
    assert_eq!(*f.follower.index_used.lock().unwrap(), 0);
    assert_eq!(
        f.follower.grant_slots.available_permits(),
        MAX_APPEND_GRANTS
    );
}

#[tokio::test]
async fn grant_issuance_and_durable_seal_have_one_ordered_endpoint() {
    for issue_first in [false, true] {
        let mut f = Fixture::new().await;
        let original = f.grant(1).await.unwrap();
        let first = f.frame(1);
        f.append(&original, vec![first]).await.unwrap();
        let next = f.frame(2);
        let lane = f
            .follower
            .lane_lock(Lane {
                leader: f.peer.session,
                epoch: 2,
            })
            .unwrap();
        let blocked = lane.grant.lock().await;
        let mut issue = Box::pin(f.grant(2));
        let mut seal = Box::pin(f.follower.seal(f.peer.session, 2));
        f.backend.reset();
        // Poll each real operation into the FIFO gate, without racing timers.
        if issue_first {
            assert!(futures_util::poll!(&mut issue).is_pending());
            assert!(futures_util::poll!(&mut seal).is_pending());
        } else {
            assert!(futures_util::poll!(&mut seal).is_pending());
            assert!(futures_util::poll!(&mut issue).is_pending());
        }
        drop(blocked);
        let (issued, sealed) = tokio::join!(issue, seal);
        assert_eq!(sealed.unwrap().durable_through, 1);
        if issue_first {
            let issued = issued.unwrap();
            assert!(matches!(
                f.append(&issued, vec![next.clone()]).await,
                Err(Error::Fenced)
            ));
        } else {
            assert!(matches!(issued, Err(Error::Fenced)));
            assert!(f.backend.requests().is_empty());
        }
        assert!(matches!(
            f.append(&original, vec![next]).await,
            Err(Error::Fenced)
        ));
        assert_eq!(
            f.follower
                .read_tail(f.peer.session, 2, 1)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            f.follower.grant_slots.available_permits(),
            MAX_APPEND_GRANTS
        );
    }
}

#[tokio::test]
async fn only_fresh_grant_coverage_can_release_native_prefix_records() {
    let mut f = Fixture::new().await;
    let original = f.grant(1).await.unwrap();
    let first = f.frame(1);
    let second = f.frame(2);
    f.append(&original, vec![first, second.clone()])
        .await
        .unwrap();
    let leader = f
        .directory
        .load(f.peer.session, NOW)
        .await
        .unwrap()
        .unwrap();
    f.directory
        .advance_log_coverage(&leader, 1, NOW)
        .await
        .unwrap();
    // Publication elsewhere cannot mutate a previously issued pruning floor.
    assert_eq!(
        f.append(&original, vec![second.clone()])
            .await
            .unwrap()
            .base_sequence,
        1
    );
    let renewed = f.grant(2).await.unwrap();
    assert_eq!(renewed.covered_through(), 1);
    f.backend.reset();
    let third = f.frame(3);
    let expected = vec![second, third];
    assert_eq!(
        f.append(&renewed, expected.clone())
            .await
            .unwrap()
            .base_sequence,
        2
    );
    f.follower.seal(f.peer.session, 2).await.unwrap();
    assert!(f.follower.read_tail(f.peer.session, 2, 1).await.is_err());
    assert_eq!(
        f.follower.read_tail(f.peer.session, 2, 2).await.unwrap(),
        expected
    );
    assert!(f.backend.requests().is_empty());
}

#[tokio::test]
async fn enrollment_coverage_ahead_of_queued_frames_does_not_skip_the_requested_witness() {
    let mut f = Fixture::new().await;
    let leader = f
        .directory
        .load(f.peer.session, NOW)
        .await
        .unwrap()
        .unwrap();
    f.directory
        .advance_log_coverage(&leader, 6, NOW)
        .await
        .unwrap();
    // The source queued sequence 1 while its object floor was still zero.
    // Its exact batch receipt requires that witness even if publication wins
    // the race before grant issuance. Fresh authority permits a lower floor.
    let grant = f.grant(1).await.unwrap();
    assert_eq!(grant.covered_through(), 0);
    let frame = f.frame(1);
    let receipt = f.append(&grant, vec![frame.clone()]).await.unwrap();
    assert_eq!(receipt.base_sequence, 1);
    assert_eq!(receipt.durable_through, 1);
    f.follower.seal(f.peer.session, 2).await.unwrap();
    assert_eq!(
        f.follower.read_tail(f.peer.session, 2, 1).await.unwrap(),
        vec![frame]
    );
}
