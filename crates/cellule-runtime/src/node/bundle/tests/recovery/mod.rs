use super::*;
use crate::follower::{FollowerReceipt, FollowerStore};
use crate::node::log_recovery::{NodeLogRecovery, RecoveryCell, RecoveryCoordinator};
use crate::node::log_state::NodeLogStatus;
use crate::node::log_transport::{
    AppendRequest, NodeLogTransport, RetireRequest, SealRequest, TailRequest,
};
use crate::recovery::manifest::RecoveryManifestStore;
use futures_util::future::BoxFuture;
mod cohort;
mod provisional;

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
    Complete,
    PartialRoot,
    LostRootReply,
    LostCatalogReply,
    ExpiredClaim,
    SealInterrupted,
    Overlap,
    SelectedOnly,
    Conflict,
    MissingCell,
}

impl Scenario {
    fn completes(&self) -> bool {
        matches!(
            self,
            Self::Complete
                | Self::PartialRoot
                | Self::LostRootReply
                | Self::LostCatalogReply
                | Self::ExpiredClaim
                | Self::SealInterrupted
        )
    }
}

#[tokio::test]
async fn recovery_resumes_closed_origin_inventory_after_transfer_and_interrupted_log_seal() {
    verify_recovery(Scenario::SealInterrupted).await;
}

#[tokio::test]
async fn recovery_reconciles_lost_original_root_cas_reply() {
    verify_recovery(Scenario::LostRootReply).await;
}
#[tokio::test]
async fn recovery_reconciles_lost_terminal_tombstone_cas_reply() {
    verify_recovery(Scenario::LostCatalogReply).await;
}
#[tokio::test]
async fn expired_claim_cannot_materialize_or_close_original_bound_cells() {
    verify_recovery(Scenario::ExpiredClaim).await;
}

#[tokio::test]
async fn recovery_materializes_full_fleet_suffix_and_closes_quiet_binding_before_transfer() {
    verify_recovery(Scenario::Complete).await;
}

#[tokio::test]
async fn recovery_retries_after_one_root_cas_without_replacing_the_original_manifest() {
    verify_recovery(Scenario::PartialRoot).await;
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
    let faults = Arc::new(super::faults::ReplyFault::default());
    let mut f = Fixture::with_store(faults.clone()).await;
    enroll(&mut f).await;
    let mut a = f.cell(4).await;
    let mut b = f.cell(5).await;
    let quiet = if scenario.completes() {
        Some(f.cell(6).await)
    } else {
        None
    };
    let (_, mut prefix, arange) = f.append(&mut a, 2);
    let (_, bframes, brange) = f.append(&mut b, 2);
    prefix.extend(bframes);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &prefix, &[arange, brange], NOW)
        .await
        .unwrap();
    let selected = if matches!(scenario, Scenario::Overlap | Scenario::Conflict) {
        // Retain recovery coverage for historical stored catalogs whose native
        // frontier still lagged their selected bundle. New selection is atomic.
        f.directory
            .select_catalog(&f.node, &proposal, NOW)
            .await
            .unwrap()
    } else {
        f.directory
            .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
            .await
            .unwrap()
            .0
    };
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
    assert_eq!(
        selected.advertisement().log().unwrap().tiered_through(),
        coverage
    );
    f.node = selected;
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
    let recovery =
        NodeLogRecovery::from_fenced(transport.clone(), &fenced, Limits::default()).unwrap();
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
        inventory.controls.len(),
        if quiet.is_some() { 3 } else { 2 },
        "complete binding discovery includes selected-only Cells"
    );
    let manifests = RecoveryManifestStore::new(f.layout.clone(), Limits::default())
        .with_recovery_scratch(f.scratch.path().to_owned());
    let cells = [&a, &b]
        .into_iter()
        .chain(quiet.as_ref())
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
    let mut coordinator = RecoveryCoordinator::new(recovery, manifests.clone());
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
    let mut expected_roots = std::collections::BTreeMap::new();
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
        expected_roots.insert(*cell.control.value().cell.as_bytes(), prepared.root());
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
    if scenario.completes() {
        let mut cells = Vec::new();
        for cell in [&a, &b].into_iter().chain(quiet.as_ref()) {
            cells.push(RecoveryCell {
                application: ApplicationId::from_bytes(*cell.authority.layout().application_id()),
                authority: cell.authority.clone(),
                observed: cell
                    .authority
                    .load(cell.control.value().cell)
                    .await
                    .unwrap()
                    .unwrap(),
            });
        }
        if matches!(scenario, Scenario::ExpiredClaim) {
            assert!(
                coordinator
                    .recover_and_seal(
                        &f.directory,
                        fenced.clone(),
                        cells,
                        fenced.claim_expires_at_ms() + 1
                    )
                    .await
                    .is_err()
            );
            for cell in [&a, &b] {
                let current = cell
                    .authority
                    .load(cell.control.value().cell)
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(current.value().root, cell.control.value().root);
                assert!(current.value().recovery.is_some());
            }
            return;
        }
        if matches!(scenario, Scenario::PartialRoot) {
            let original_manifest = controls[0]
                .value()
                .recovery
                .as_ref()
                .unwrap()
                .manifest_digest;
            faults.mode.store(6, std::sync::atomic::Ordering::SeqCst);
            assert!(
                coordinator
                    .recover_and_seal(&f.directory, fenced.clone(), cells.clone(), NOW + 31_000)
                    .await
                    .is_err()
            );
            let advanced = a
                .authority
                .load(a.control.value().cell)
                .await
                .unwrap()
                .unwrap();
            assert!(advanced.value().recovery.is_none());
            assert_eq!(advanced.value().root.as_ref().unwrap().commit_sequence, 3);
            let pending = b
                .authority
                .load(b.control.value().cell)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                pending.value().recovery.as_ref().unwrap().manifest_digest,
                original_manifest
            );
            assert!(matches!(
                a.authority
                    .transition(
                        &advanced,
                        advanced
                            .value()
                            .takeover(Owner {
                                session: fenced.claimant(),
                                endpoint: "https://successor.internal:8081".into(),
                            })
                            .unwrap(),
                        Transition::Takeover
                    )
                    .await,
                Err(Error::PendingPublication)
            ));
            faults.mode.store(0, std::sync::atomic::Ordering::SeqCst);
            for cell in &mut cells {
                cell.observed = cell
                    .authority
                    .load(cell.observed.value().cell)
                    .await
                    .unwrap()
                    .unwrap();
            }
        }
        if matches!(scenario, Scenario::LostRootReply) {
            faults.mode.store(8, std::sync::atomic::Ordering::SeqCst);
        } else if matches!(scenario, Scenario::LostCatalogReply) {
            faults.mode.store(2, std::sync::atomic::Ordering::SeqCst);
        }
        let mut completion_fence = fenced.clone();
        if matches!(scenario, Scenario::SealInterrupted) {
            faults.mode.store(4, std::sync::atomic::Ordering::SeqCst);
            assert!(
                coordinator
                    .recover_and_seal(&f.directory, fenced.clone(), cells, NOW + 31_000)
                    .await
                    .is_err()
            );
            faults.mode.store(0, std::sync::atomic::Ordering::SeqCst);
            let original = a
                .authority
                .load(a.control.value().cell)
                .await
                .unwrap()
                .unwrap();
            a.authority
                .transition(
                    &original,
                    original
                        .value()
                        .takeover(Owner {
                            session: fenced.claimant(),
                            endpoint: "https://successor.internal:8081".into(),
                        })
                        .unwrap(),
                    Transition::Takeover,
                )
                .await
                .unwrap();
            completion_fence = f
                .directory
                .claim_expired(fenced.session(), fenced.claimant(), NOW + 31_001)
                .await
                .unwrap();
            let resumed = NodeLogRecovery::from_fenced(
                transport.clone(),
                &completion_fence,
                Limits::default(),
            )
            .unwrap();
            let sealed = resumed.ensure_sealed_bounded().await.unwrap();
            let catalog = crate::cell::catalog::CellCatalog::new(
                f.layout.clone(),
                crate::TenantId::from_bytes([9; 16]),
            );
            let discovered = crate::node::log_recovery::recoverable_cells_from_scopes_with_summary(
                &catalog,
                &a.authority,
                fenced.session(),
                &sealed.scopes(Limits::default()).unwrap(),
                4,
            )
            .await
            .unwrap();
            assert!(discovered.cells.is_empty());
            assert_eq!(
                discovered.summary.control_reads, 0,
                "closed generations use authenticated original roots, even after transfer"
            );
            cells = discovered.cells;
            coordinator = RecoveryCoordinator::new(resumed, manifests.clone());
        }
        let completed = coordinator
            .recover_and_seal(&f.directory, completion_fence, cells, NOW + 31_002)
            .await
            .unwrap();
        assert_eq!(
            completed.controls.len(),
            if matches!(scenario, Scenario::SealInterrupted) {
                0
            } else {
                3
            }
        );
        for cell in [&a, &b].into_iter().chain(quiet.as_ref()) {
            let current = cell
                .authority
                .load(cell.control.value().cell)
                .await
                .unwrap()
                .unwrap();
            assert!(current.value().recovery.is_none());
            let expected = if current.value().cell == a.control.value().cell {
                3
            } else if current.value().cell == b.control.value().cell {
                2
            } else {
                1
            };
            assert_eq!(
                current.value().root.as_ref().unwrap().commit_sequence,
                expected
            );
            let expected_root = expected_roots
                .get(current.value().cell.as_bytes())
                .copied()
                .unwrap_or_else(|| cell.control.value().ltx_root().unwrap());
            assert_eq!(current.value().ltx_root(), Some(expected_root));
            let cold = CellReplica::new(
                cell.authority.layout().clone(),
                *current.value().cell.as_bytes(),
                *current.value().incarnation.as_bytes(),
                Limits::default(),
            )
            .unwrap();
            let destination = f.scratch.path().join(format!(
                "cold-terminal-{}.sqlite",
                current.value().cell.as_bytes()[0]
            ));
            cold.open_root(&expected_root)
                .await
                .unwrap()
                .restore(&destination)
                .await
                .unwrap();
            let restored = cellule_ltx::rusqlite::Connection::open(destination).unwrap();
            for command in 2..=expected {
                let actual: String = restored
                    .query_row(
                        "SELECT result FROM outcomes WHERE request=?1",
                        [format!("request-{command}")],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(actual, format!("result-{command}"));
            }
            if current.value().owner.as_ref().unwrap().session == completed.takeover.claimant() {
                assert!(matches!(scenario, Scenario::SealInterrupted));
                continue;
            }
            let successor = current
                .value()
                .takeover(Owner {
                    session: completed.takeover.claimant(),
                    endpoint: "https://successor.internal:8081".into(),
                })
                .unwrap();
            let transferred = cell
                .authority
                .transition(&current, successor, Transition::Takeover)
                .await
                .unwrap();
            assert_eq!(transferred.value().root, current.value().root);
        }
        return;
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
