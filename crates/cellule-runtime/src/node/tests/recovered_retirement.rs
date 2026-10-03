//! Canonical authorization and native fences; full tail pinning is covered by runtime recovery.
use super::*;
use crate::follower::FollowerStore;
use crate::node::log_recovery::retirement::retire_recovered_members;
use crate::node::log_transport::{
    LocalFollowerTransport, LocalRecoveredFollowerTransport, RecoveredNodeLogTransport,
    RecoveredRetireRequest,
};
use std::sync::atomic::{AtomicBool, Ordering};

async fn enrolled(activate: bool) -> (NodeDirectory, VersionedNodeAdvertisement, SessionId) {
    let directory = directory();
    let key = SigningKey::from_bytes(&[7; 32]);
    let leader = SessionId::from_bytes([1; 16]);
    let original = directory
        .create(advertisement_for(leader, &key, 1, NOW_MS), NOW_MS)
        .await
        .unwrap();
    for byte in [2, 3] {
        let member = SessionId::from_bytes([byte; 16]);
        directory
            .create(
                advertisement_for(member, &key, 1, NOW_MS + 9_000),
                NOW_MS + 9_000,
            )
            .await
            .unwrap();
    }
    let enrolled = directory
        .recruit_log(&original, 4, 1, 3, NOW_MS + 9_001)
        .await
        .unwrap();
    let active = if activate {
        directory
            .activate_log(&enrolled, NOW_MS + 9_002)
            .await
            .unwrap()
    } else {
        enrolled
    };
    (directory, active, SessionId::from_bytes([2; 16]))
}

fn frames() -> Vec<Bytes> {
    let root = tempfile::TempDir::new().unwrap();
    let limits = cellule_ltx::Limits::default();
    let mut db = cellule_ltx::Db::open(&root.path().join("cell.sqlite"), limits).unwrap();
    let mut frames = Vec::new();
    for sequence in 1..=2 {
        db.transaction(|tx| {
            tx.execute_batch(if sequence == 1 {
                "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (1)"
            } else {
                "UPDATE counter SET value = 2"
            })
        })
        .unwrap();
        let capture = db.capture().unwrap();
        let segment = &capture.segments[0];
        frames.push(
            cellule_ltx::encode_node_frame(
                cellule_ltx::NodeFrameScope {
                    leader_session: [1; 16],
                    log_epoch: 4,
                    node_sequence: sequence,
                    application: [9; 16],
                    cell: [6; 32],
                    incarnation: [7; 16],
                    cell_epoch: 1,
                    commit_sequence: sequence,
                },
                segment.info().clone(),
                Bytes::from(std::fs::read(segment.path()).unwrap()),
                limits,
            )
            .unwrap()
            .encoded()
            .clone(),
        );
    }
    frames
}

#[tokio::test]
async fn recovered_retirement_rejects_live_recovering_foreign_and_expired_requests() {
    let (directory, active, claimant) = enrolled(true).await;
    let leader = active.advertisement().session();
    let member = node(claimant);
    assert!(
        directory
            .authorize_recovered_log_retire(claimant, member, leader, 4, None, NOW_MS + 9_003)
            .await
            .is_err()
    );
    let fenced = directory
        .claim_expired(leader, claimant, NOW_MS + 10_001)
        .await
        .unwrap();
    assert!(
        directory
            .authorize_recovered_log_retire(claimant, member, leader, 4, None, NOW_MS + 10_002)
            .await
            .is_err()
    );
    // Authority fixture only: the runtime integration case pins real overlays.
    let manifest = Some(Digest::from_bytes([20; 32]));
    directory
        .seal_recovery(&fenced, manifest, NOW_MS + 10_002)
        .await
        .unwrap();
    for (sender, receiver, epoch, pointer, now) in [
        (leader, member, 4, manifest, NOW_MS + 10_003),
        (claimant, node(leader), 4, manifest, NOW_MS + 10_003),
        (claimant, member, 5, manifest, NOW_MS + 10_003),
        (claimant, member, 4, None, NOW_MS + 10_003),
        (claimant, member, 4, manifest, NOW_MS + 19_000),
    ] {
        assert!(
            directory
                .authorize_recovered_log_retire(sender, receiver, leader, epoch, pointer, now)
                .await
                .is_err()
        );
    }
    let authorization = directory
        .authorize_recovered_log_retire(claimant, member, leader, 4, manifest, NOW_MS + 10_003)
        .await
        .unwrap();
    assert_eq!(authorization.member(), member);
    assert_eq!(authorization.sealed().log().recovery_manifest(), manifest);
    assert!(directory.log_epoch_referenced(leader, 4).await.unwrap());
}

struct Members {
    peers: Vec<(NodeId, LocalRecoveredFollowerTransport)>,
    lose_first: AtomicBool,
    contradict_first: AtomicBool,
}
impl Members {
    fn peer(&self, member: NodeId) -> &LocalRecoveredFollowerTransport {
        &self
            .peers
            .iter()
            .find(|(node, _)| *node == member)
            .unwrap()
            .1
    }
}
impl NodeLogTransport for Members {
    fn append<'a>(
        &'a self,
        member: NodeId,
        request: AppendRequest,
    ) -> BoxFuture<'a, Result<crate::follower::FollowerReceipt>> {
        self.peer(member).append(member, request)
    }
    fn seal<'a>(
        &'a self,
        member: NodeId,
        request: SealRequest,
    ) -> BoxFuture<'a, Result<crate::follower::FollowerReceipt>> {
        self.peer(member).seal(member, request)
    }
    fn retire<'a>(
        &'a self,
        member: NodeId,
        request: RetireRequest,
    ) -> BoxFuture<'a, Result<crate::follower::FollowerReceipt>> {
        self.peer(member).retire(member, request)
    }
    fn tail<'a>(
        &'a self,
        member: NodeId,
        request: TailRequest,
    ) -> BoxFuture<'a, Result<Vec<Bytes>>> {
        self.peer(member).tail(member, request)
    }
}
impl RecoveredNodeLogTransport for Members {
    fn retire_recovered<'a>(
        &'a self,
        member: NodeId,
        request: RecoveredRetireRequest,
    ) -> BoxFuture<'a, Result<crate::follower::FollowerReceipt>> {
        Box::pin(async move {
            let mut receipt = self.peer(member).retire_recovered(member, request).await?;
            if member == self.peers[0].0 {
                if self.lose_first.swap(false, Ordering::AcqRel) {
                    return Err(Error::Node("lost recovered retirement reply"));
                }
                if self.contradict_first.swap(false, Ordering::AcqRel) {
                    receipt.base_sequence = 0;
                }
            }
            Ok(receipt)
        })
    }
}

#[tokio::test]
async fn recovered_retirement_joins_original_members_retains_fences_and_replays_lost_results() {
    let (directory, active, claimant) = enrolled(true).await;
    let leader = active.advertisement().session();
    let fenced = directory
        .claim_expired(leader, claimant, NOW_MS + 10_001)
        .await
        .unwrap();
    let sealed = directory
        .seal_recovery(&fenced, Some(Digest::from_bytes([20; 32])), NOW_MS + 10_002)
        .await
        .unwrap();
    let first = tempfile::TempDir::new().unwrap();
    let second = tempfile::TempDir::new().unwrap();
    let roots = [first, second];
    let mut stores = Vec::new();
    let mut peers = Vec::new();
    let frames = frames();
    for (index, member) in sealed.log().members().iter().copied().enumerate() {
        let store = FollowerStore::open(
            roots[index].path().to_owned(),
            cellule_ltx::Limits::default(),
            cellule_ltx::DiskBudget::new(1 << 30),
        )
        .unwrap();
        store
            .append(leader, 4, frames[..2 - index].to_vec(), 0)
            .await
            .unwrap();
        let peer = LocalRecoveredFollowerTransport::new(
            LocalFollowerTransport::new(member, store.clone()),
            directory.clone(),
            claimant,
            || Ok(NOW_MS + 10_003),
        )
        .unwrap();
        // Canonical completion cannot replace the original receiver's native seal.
        assert!(
            peer.retire_recovered(
                member,
                RecoveredRetireRequest {
                    sealed: sealed.clone()
                }
            )
            .await
            .is_err()
        );
        assert!(store.retained_bytes() > 8);
        store.seal(leader, 4).await.unwrap();
        if index == 0 {
            let marker = roots[index]
                .path()
                .join("followers")
                .join(crate::identity::encode_hex(leader.as_bytes()))
                .join("4/sealed");
            std::fs::write(&marker, 3u64.to_le_bytes()).unwrap();
            assert!(matches!(
                peer.retire_recovered(
                    member,
                    RecoveredRetireRequest {
                        sealed: sealed.clone()
                    }
                )
                .await,
                Err(Error::Node("follower seal watermark differs"))
            ));
            assert!(store.retained_bytes() > 8);
            std::fs::write(marker, 2u64.to_le_bytes()).unwrap();
        }
        stores.push(store);
        peers.push((member, peer));
    }
    let transport = Arc::new(Members {
        peers,
        lose_first: AtomicBool::new(true),
        contradict_first: AtomicBool::new(false),
    });
    let observed = retire_recovered_members(transport.clone(), &sealed)
        .await
        .unwrap();
    assert_eq!(observed.members().len(), 2);
    assert!(observed.members()[0].result().is_err());
    assert!(observed.members()[1].result().is_ok());
    let source = observed.members()[0].result().unwrap_err();
    let failure = observed.confirmed().err().unwrap();
    assert!(std::error::Error::source(&failure).is_some());
    assert!(Arc::ptr_eq(
        &source,
        &observed.members()[0].result().unwrap_err()
    ));
    for store in &stores {
        assert_eq!(store.retained_bytes(), 8);
    }
    assert!(directory.log_epoch_referenced(leader, 4).await.unwrap());
    transport.contradict_first.store(true, Ordering::Release);
    let contradicted = retire_recovered_members(transport.clone(), &sealed)
        .await
        .unwrap();
    assert!(contradicted.confirmed().is_err());
    assert!(contradicted.members()[1].result().is_ok());
    let replay = retire_recovered_members(transport, &sealed)
        .await
        .unwrap()
        .confirmed()
        .unwrap();
    assert_eq!(replay.members()[0].result().unwrap().durable_through, 2);
    assert_eq!(replay.members()[1].result().unwrap().durable_through, 1);
    assert!(
        directory
            .retired_recovered_log(&sealed, claimant, NOW_MS + 10_003)
            .await
            .unwrap()
            .is_none()
    );
    let retired = directory
        .retire_recovered_log(&replay, claimant, NOW_MS + 10_004)
        .await
        .unwrap();
    assert_eq!(retired.log().phase(), NodeLogPhase::Retired);
    assert_eq!(
        retired.log().recovery_manifest(),
        sealed.log().recovery_manifest()
    );
    assert_eq!(
        directory
            .retire_recovered_log(&replay, claimant, NOW_MS + 10_005)
            .await
            .unwrap(),
        retired
    );
    assert!(!directory.log_epoch_referenced(leader, 4).await.unwrap());
    assert!(
        directory
            .takeover_proof(leader, claimant, NOW_MS + 10_005)
            .await
            .unwrap()
            .is_some()
    );
    let resumed = directory
        .seal_recovery(&fenced, sealed.log().recovery_manifest(), NOW_MS + 10_005)
        .await
        .unwrap();
    assert!(NodeTakeoverProof::after_recovery(&fenced, &resumed).is_ok());
    for store in stores {
        assert!(store.append(leader, 4, frames.clone(), 0).await.is_err());
        let candidates = store.retired_lanes(i64::MAX, 2).await.unwrap();
        assert_eq!(candidates.len(), 1);
        // Exact native candidate and canonical closure are both present. This
        // unit supplies the candidate's actual mtime as its chosen grace bound.
        assert!(
            store
                .remove_retired(candidates[0], candidates[0].retired_at_ms())
                .await
                .unwrap()
        );
        assert_eq!(store.retained_bytes(), 0);
    }
    assert_eq!(
        directory
            .retired_recovered_log(&sealed, claimant, NOW_MS + 10_005)
            .await
            .unwrap(),
        Some(retired)
    );
}

#[tokio::test]
async fn recovered_inactive_enrollment_fences_empty_lanes_but_refuses_unexpected_records() {
    let (directory, enrolled, claimant) = enrolled(false).await;
    let leader = enrolled.advertisement().session();
    let fenced = directory
        .claim_expired(leader, claimant, NOW_MS + 10_001)
        .await
        .unwrap();
    let sealed = directory
        .seal_recovery(&fenced, None, NOW_MS + 10_002)
        .await
        .unwrap();
    assert!(!sealed.log().active());
    let root = tempfile::TempDir::new().unwrap();
    let store = FollowerStore::open(
        root.path().to_owned(),
        cellule_ltx::Limits::default(),
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    let member = sealed.log().members()[0];
    let authorization = directory
        .authorize_recovered_log_retire(claimant, member, leader, 4, None, NOW_MS + 10_003)
        .await
        .unwrap();
    assert_eq!(
        store
            .retire_recovered(member, authorization)
            .await
            .unwrap()
            .durable_through,
        0
    );
    assert_eq!(store.retained_bytes(), 8);
    assert!(store.append(leader, 4, frames(), 0).await.is_err());
    let other = tempfile::TempDir::new().unwrap();
    let unexpected = FollowerStore::open(
        other.path().to_owned(),
        cellule_ltx::Limits::default(),
        cellule_ltx::DiskBudget::new(1 << 30),
    )
    .unwrap();
    unexpected.append(leader, 4, frames(), 0).await.unwrap();
    let authorization = directory
        .authorize_recovered_log_retire(claimant, member, leader, 4, None, NOW_MS + 10_003)
        .await
        .unwrap();
    assert!(matches!(
        unexpected.retire_recovered(member, authorization).await,
        Err(Error::Node("follower lane has uncovered records"))
    ));
    assert!(unexpected.retained_bytes() > 8);
    // A native seal cannot turn unexpected records from an inactive canonical
    // enrollment into a recoverable, acknowledged tail.
    unexpected.seal(leader, 4).await.unwrap();
    let authorization = directory
        .authorize_recovered_log_retire(claimant, member, leader, 4, None, NOW_MS + 10_003)
        .await
        .unwrap();
    assert!(matches!(
        unexpected.retire_recovered(member, authorization).await,
        Err(Error::Node("follower lane has uncovered records"))
    ));
    assert!(unexpected.retained_bytes() > 8);
    assert!(directory.log_epoch_referenced(leader, 4).await.unwrap());
}
