use super::*;
use crate::follower::{FollowerReceipt, FollowerStore};
use crate::node::log_recovery::{NodeLogRecovery, RecoveryCell, RecoveryCoordinator};
use crate::node::log_state::NodeLogStatus;
use crate::node::log_transport::{
    AppendRequest, NodeLogTransport, RetireRequest, SealRequest, TailRequest,
};
use crate::recovery::manifest::RecoveryManifestStore;
use futures_util::future::BoxFuture;

struct Followers(Vec<(NodeId, FollowerStore)>);
impl Followers {
    fn get(&self, member: NodeId) -> Result<&FollowerStore> {
        self.0
            .iter()
            .find_map(|(id, store)| (*id == member).then_some(store))
            .ok_or(Error::Node("test follower absent"))
    }
}
impl NodeLogTransport for Followers {
    fn append<'a>(
        &'a self,
        member: NodeId,
        request: AppendRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            self.get(member)?
                .append(
                    request.leader_session,
                    request.log_epoch,
                    request.frames,
                    request.covered_through,
                )
                .await
        })
    }
    fn seal<'a>(
        &'a self,
        member: NodeId,
        request: SealRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            self.get(member)?
                .seal(request.leader_session, request.log_epoch)
                .await
        })
    }
    fn tail<'a>(
        &'a self,
        member: NodeId,
        request: TailRequest,
    ) -> BoxFuture<'a, Result<Vec<Bytes>>> {
        Box::pin(async move {
            self.get(member)?
                .read_tail(
                    request.leader_session,
                    request.log_epoch,
                    request.first_sequence,
                )
                .await
        })
    }
    fn retire<'a>(
        &'a self,
        member: NodeId,
        request: RetireRequest,
    ) -> BoxFuture<'a, Result<FollowerReceipt>> {
        Box::pin(async move {
            self.get(member)?
                .retire(
                    request.leader_session,
                    request.log_epoch,
                    request.covered_through,
                )
                .await
        })
    }
}

async fn enroll(f: &mut Fixture) {
    let mut node = f.node.advertisement().clone();
    node.log = Some(
        NodeLogStatus::open(
            node.node,
            EPOCH,
            vec![NodeId::from_bytes([2; 16]), NodeId::from_bytes([3; 16])],
        )
        .unwrap()
        .activate(node.node)
        .unwrap(),
    );
    node.generation += 1;
    f.node = f
        .directory
        .update_advertisement(&f.node, node, NOW)
        .await
        .unwrap();
}

async fn fence(f: &Fixture) -> crate::node::FencedNodeSession {
    let now = NOW + 31_000;
    let mut claimant = f.node.advertisement().clone();
    claimant.node = NodeId::from_bytes([8; 16]);
    claimant.session = SessionId::from_bytes([8; 16]);
    claimant.log = None;
    claimant.bundle = None;
    claimant.issued_at_ms = now;
    claimant.expires_at_ms = now + 30_000;
    claimant.signature = SigningKey::from_bytes(&[10; 32])
        .sign(&claimant.signing_bytes().unwrap())
        .to_bytes();
    f.directory.create(claimant, now).await.unwrap();
    f.directory
        .claim_expired(
            SessionId::from_bytes([1; 16]),
            SessionId::from_bytes([8; 16]),
            now,
        )
        .await
        .unwrap()
}

enum Scenario {
    Pruned,
    Overlap,
    SelectedOnly,
    Conflict,
    MissingCell,
}

#[tokio::test]
async fn recovery_joins_selected_only_cell_and_pruned_prefix_with_prior_fleet_ack() {
    verify_recovery(Scenario::Pruned).await;
}
#[tokio::test]
async fn recovery_checks_identical_follower_overlap_against_selected_prefix() {
    verify_recovery(Scenario::Overlap).await;
}
#[tokio::test]
async fn recovery_restores_selected_cells_with_an_empty_follower_witness() {
    verify_recovery(Scenario::SelectedOnly).await;
}
#[tokio::test]
async fn recovery_rejects_conflicting_follower_overlap_without_attachment() {
    verify_recovery(Scenario::Conflict).await;
}
#[tokio::test]
async fn recovery_rejects_omitted_selected_only_cell_without_attachment() {
    verify_recovery(Scenario::MissingCell).await;
}

async fn verify_recovery(scenario: Scenario) {
    let mut f = Fixture::new().await;
    enroll(&mut f).await;
    let mut a = f.cell(4).await;
    let mut b = f.cell(5).await;
    let (_, mut prefix, arange) = f.append(&mut a, 2);
    let (_, bframes, brange) = f.append(&mut b, 2);
    prefix.extend(bframes);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &prefix, &[arange, brange], NOW)
        .await
        .unwrap();
    let (selected, _) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    let selected_through = selected
        .advertisement()
        .bundle_head()
        .unwrap()
        .selected_through();
    // Exercise the state that bundle ACK integration must support: the shared
    // selected prefix is retained in origin while Cell roots still lag.
    let coverage = if matches!(scenario, Scenario::Overlap | Scenario::Conflict) {
        0
    } else {
        selected_through
    };
    f.node = f
        .directory
        .advance_log_coverage(&selected, coverage, NOW)
        .await
        .unwrap();
    let (suffix, fleet) = if matches!(scenario, Scenario::SelectedOnly) {
        (Vec::new(), None)
    } else {
        let (_, suffix, fleet) = f.append(&mut a, 3);
        (suffix, Some(fleet))
    };
    let mut transmitted = prefix
        .iter()
        .map(|frame| frame.encoded().clone())
        .collect::<Vec<_>>();
    if matches!(scenario, Scenario::Conflict) {
        let first = &prefix[0];
        let mut scope = first.scope();
        scope.cell_epoch += 1;
        transmitted[0] = cellule_ltx::encode_node_frame(
            scope,
            first.segment().clone(),
            first.body().clone(),
            Limits::default(),
        )
        .unwrap()
        .encoded()
        .clone();
    }
    let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
    let followers = Arc::new(Followers(
        dirs.iter()
            .enumerate()
            .map(|(i, dir)| {
                (
                    NodeId::from_bytes([i as u8 + 2; 16]),
                    FollowerStore::open(
                        dir.path().to_owned(),
                        Limits::default(),
                        cellule_ltx::DiskBudget::new(1 << 30),
                    )
                    .unwrap(),
                )
            })
            .collect(),
    ));
    for member in [2, 3] {
        let id = NodeId::from_bytes([member; 16]);
        let receipt = followers
            .append(
                id,
                AppendRequest {
                    leader_session: SessionId::from_bytes([1; 16]),
                    log_epoch: EPOCH,
                    frames: transmitted.clone(),
                    covered_through: 0,
                },
            )
            .await
            .unwrap();
        let receipt = if suffix.is_empty() {
            receipt
        } else {
            followers
                .append(
                    id,
                    AppendRequest {
                        leader_session: SessionId::from_bytes([1; 16]),
                        log_epoch: EPOCH,
                        frames: suffix.iter().map(|frame| frame.encoded().clone()).collect(),
                        covered_through: coverage,
                    },
                )
                .await
                .unwrap()
        };
        if coverage > 0 && !suffix.is_empty() {
            assert!(receipt.base_sequence > selected_through);
        }
        f.gate.acknowledge(id, receipt.durable_through).unwrap();
    }
    f.gate.activate_fleet().unwrap();
    if let Some(fleet) = fleet {
        assert_eq!(
            f.gate.prove(fleet.ticket()).await.unwrap().source(),
            DurabilitySource::Fleet
        );
    }
    let fenced = fence(&f).await;
    f.lease.fence();
    let transport: Arc<dyn NodeLogTransport> = followers;
    let recovery = NodeLogRecovery::from_fenced(transport, &fenced, Limits::default()).unwrap();
    let sealed = recovery.ensure_sealed_bounded().await.unwrap();
    let expected_frames = if coverage == 0 {
        prefix.len() + suffix.len()
    } else {
        suffix.len()
    };
    assert_eq!(sealed.frame_count(), expected_frames as u64);
    if matches!(scenario, Scenario::Pruned) {
        assert_eq!(
            sealed.scopes(Limits::default()).unwrap().len(),
            1,
            "the selected-only Cell is absent from follower scopes"
        );
    }
    let inventory = super::super::recovery::inventory_for_owner(&f.layout, fenced.session())
        .await
        .unwrap();
    assert_eq!(
        inventory.len(),
        2,
        "complete binding discovery includes selected-only Cells"
    );
    let manifests = RecoveryManifestStore::new(f.layout.clone(), Limits::default())
        .with_recovery_scratch(f.scratch.path().to_owned());
    let cells = [&a, &b]
        .into_iter()
        .filter(|cell| {
            !matches!(scenario, Scenario::MissingCell)
                || cell.control.value().cell != b.control.value().cell
        })
        .map(|cell| RecoveryCell {
            application: ApplicationId::from_bytes(*cell.authority.layout().application_id()),
            authority: cell.authority.clone(),
            observed: cell.control.clone(),
        })
        .collect();
    let coordinator = RecoveryCoordinator::new(recovery, manifests.clone());
    let result = coordinator
        .recover_sealed(fenced.clone(), cells, sealed)
        .await;
    if matches!(scenario, Scenario::Conflict | Scenario::MissingCell) {
        let expected = if matches!(scenario, Scenario::Conflict) {
            "follower witness conflicts with selected bundle prefix"
        } else {
            "selected bundle Cell is absent from recovery inventory"
        };
        assert!(matches!(result, Err(Error::Node(message)) if message == expected));
        for cell in [&a, &b] {
            assert!(
                cell.authority
                    .load(cell.control.value().cell)
                    .await
                    .unwrap()
                    .unwrap()
                    .value()
                    .recovery
                    .is_none()
            );
        }
        return;
    }
    let controls = result.unwrap();
    assert_eq!(
        controls.len(),
        2,
        "selected-only Cells also require durable recovery"
    );
    for cell in [&a, &b] {
        let control = controls
            .iter()
            .find(|control| control.value().cell == cell.control.value().cell)
            .unwrap();
        let overlay = manifests
            .load_overlay(
                cell.control.value().cell,
                cell.control.value().incarnation,
                control.value().recovery.as_ref().unwrap(),
            )
            .await
            .unwrap();
        let prepared = cell
            .replica
            .prepare_recovered_overlay(&overlay, 1)
            .await
            .unwrap();
        let path = f.scratch.path().join(format!(
            "recovered-{}.sqlite",
            cell.control.value().cell.as_bytes()[0]
        ));
        cell.replica
            .open_root(&prepared.root())
            .await
            .unwrap()
            .restore(&path)
            .await
            .unwrap();
        let db = cellule_ltx::rusqlite::Connection::open(path).unwrap();
        let last = if cell.control.value().cell == a.control.value().cell && fleet.is_some() {
            3
        } else {
            2
        };
        for command in 2..=last {
            let result: String = db
                .query_row(
                    "SELECT result FROM outcomes WHERE request=?1",
                    [format!("request-{command}")],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(result, format!("result-{command}"));
        }
    }
    assert!(
        matches!(
            coordinator
                .finish(&f.directory, fenced, controls, NOW + 31_000)
                .await,
            Err(Error::PendingPublication)
        ),
        "pinned recovery alone cannot close bound Cells or admit transfer"
    );
}
