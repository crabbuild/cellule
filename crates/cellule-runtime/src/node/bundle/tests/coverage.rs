use super::*;
use crate::node::log_state::NodeLogStatus;

pub(super) async fn enroll(f: &mut Fixture) {
    let mut node = f.node.advertisement().clone();
    node.log = Some(
        NodeLogStatus::open(
            node.node,
            EPOCH,
            vec![NodeId::from_bytes([2; 16]), NodeId::from_bytes([3; 16])],
        )
        .unwrap(),
    );
    node.generation += 1;
    f.node = f
        .directory
        .update_advertisement(&f.node, node, NOW)
        .await
        .unwrap();
}

struct NoExtraIo;
impl crate::node::durability::NodeLogAuthority for NoExtraIo {
    fn activate<'a>(&'a self, _: u64) -> futures_util::future::BoxFuture<'a, Result<()>> {
        panic!("bundle confirmation must not activate followers")
    }
    fn advance_coverage<'a>(
        &'a self,
        _: u64,
        _: u64,
    ) -> futures_util::future::BoxFuture<'a, Result<()>> {
        panic!("bundle confirmation must not perform another authority CAS")
    }
    fn close<'a>(
        &'a self,
        _: &'a crate::node::log::NodeLogRetirementObservation,
    ) -> futures_util::future::BoxFuture<'a, Result<()>> {
        panic!("unused test closure")
    }
}
impl crate::node::log_transport::NodeLogTransport for NoExtraIo {
    fn append<'a>(
        &'a self,
        _: NodeId,
        _: crate::node::log_transport::AppendRequest,
    ) -> futures_util::future::BoxFuture<'a, Result<crate::follower::FollowerReceipt>> {
        panic!("unused test append")
    }
    fn seal<'a>(
        &'a self,
        _: NodeId,
        _: crate::node::log_transport::SealRequest,
    ) -> futures_util::future::BoxFuture<'a, Result<crate::follower::FollowerReceipt>> {
        panic!("unused test seal")
    }
    fn tail<'a>(
        &'a self,
        _: NodeId,
        _: crate::node::log_transport::TailRequest,
    ) -> futures_util::future::BoxFuture<'a, Result<Vec<Bytes>>> {
        panic!("unused test tail")
    }
    fn retire<'a>(
        &'a self,
        _: NodeId,
        _: crate::node::log_transport::RetireRequest,
    ) -> futures_util::future::BoxFuture<'a, Result<crate::follower::FollowerReceipt>> {
        panic!("unused test retirement")
    }
}

#[tokio::test]
async fn public_confirmation_and_later_root_proof_need_no_extra_native_cas() {
    let mut f = Fixture::new().await;
    enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let (_, frames, assignment) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
        .await
        .unwrap();
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    let transport: Arc<dyn crate::node::log_transport::NodeLogTransport> = Arc::new(NoExtraIo);
    let shipper = crate::node::log_shipper::NodeLogShipper::new(
        f.gate.clone(),
        transport.clone(),
        Limits::default(),
    )
    .unwrap();
    let durability = crate::node::durability::NodeDurability::new(
        f.gate.clone(),
        shipper,
        Arc::new(NoExtraIo),
        transport,
        f.lease.clone(),
    );
    f.count.reset();
    assert_eq!(durability.confirm_bundle(&proofs).unwrap(), 1);
    assert_eq!(
        durability
            .prove(assignment.ticket())
            .await
            .unwrap()
            .source(),
        DurabilitySource::Bundle
    );
    assert_eq!(f.count.put_requests(), 0);
    let mut publisher = f.publisher(&cell);
    let root = publisher.materialize_bundle(&proofs[0]).await.unwrap();
    assert_eq!(root.commit_sequence, 2);
    let materialization_puts = f.count.put_requests();
    assert_eq!(
        durability
            .prove_object(assignment.ticket())
            .await
            .unwrap()
            .source(),
        DurabilitySource::Object
    );
    assert_eq!(f.count.put_requests(), materialization_puts);
}

#[tokio::test]
async fn bundle_confirmation_preserves_the_source_of_root_only_gaps() {
    let mut f = Fixture::new().await;
    enroll(&mut f).await;
    let mut a = f.cell(4).await;
    let mut b = f.cell(5).await;
    let mut c = f.cell(6).await;
    let (_, mut frames, arange) = f.append(&mut a, 2);
    let (_, bframes, brange) = f.append(&mut b, 2);
    let (_, cframes, crange) = f.append(&mut c, 2);
    frames.extend(bframes);
    frames.extend(cframes);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[arange, brange, crange], NOW)
        .await
        .unwrap();
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    let (root, bundles): (Vec<_>, Vec<_>) = proofs
        .into_iter()
        .partition(|proof| proof.binding() == a.control.value().bundle_binding.unwrap());
    f.publisher(&a).materialize_bundle(&root[0]).await.unwrap();
    f.gate.prove_object(arange.ticket()).unwrap();
    assert_eq!(
        confirm_selected_coverage(&f.gate, &f.lease, &bundles).unwrap(),
        3
    );
    assert_eq!(
        f.gate.prove(arange.ticket()).await.unwrap().source(),
        DurabilitySource::Object
    );
    for assignment in [brange, crange] {
        assert_eq!(
            f.gate.prove(assignment.ticket()).await.unwrap().source(),
            DurabilitySource::Bundle
        );
    }
    assert_eq!(
        confirm_selected_coverage(&f.gate, &f.lease, &root).unwrap(),
        3
    );
    assert_eq!(
        f.gate.prove(arange.ticket()).await.unwrap().source(),
        DurabilitySource::Bundle
    );
    assert_eq!(
        f.gate
            .confirmed_object_proof(arange.ticket())
            .unwrap()
            .source(),
        DurabilitySource::Object
    );
}

#[tokio::test]
async fn selection_advances_native_coverage_with_one_cas_while_roots_lag() {
    let mut f = Fixture::new().await;
    enroll(&mut f).await;
    let mut a = f.cell(4).await;
    let mut b = f.cell(5).await;
    let (_, mut frames, arange) = f.append(&mut a, 2);
    let (_, bframes, brange) = f.append(&mut b, 2);
    frames.extend(bframes);
    f.count.reset();
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[arange, brange], NOW)
        .await
        .unwrap();
    assert_eq!(f.gate.tiered_through(), 0, "upload grants no coverage");
    let (selected, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(selected.advertisement().log().unwrap().tiered_through(), 2);
    assert_eq!(
        selected
            .advertisement()
            .bundle_head()
            .unwrap()
            .selected_through,
        2
    );
    assert_eq!(
        f.count.put_requests(),
        2,
        "one immutable upload and one combined authority CAS"
    );
    assert_eq!(
        f.gate.tiered_through(),
        0,
        "selection is not local confirmation"
    );
    assert_eq!(
        confirm_selected_coverage(&f.gate, &f.lease, &proofs).unwrap(),
        2
    );
    for assignment in [arange, brange] {
        let proof = f.gate.prove(assignment.ticket()).await.unwrap();
        assert_eq!(proof.ticket(), assignment.ticket());
        assert_eq!(proof.source(), DurabilitySource::Bundle);
    }
    assert_eq!(
        confirm_selected_coverage(&f.gate, &f.lease, &proofs).unwrap(),
        2
    );
    assert_eq!(
        f.count.put_requests(),
        2,
        "local retry performs no second CAS"
    );
    for cell in [&a, &b] {
        assert_eq!(
            cell.authority
                .load(cell.control.value().cell)
                .await
                .unwrap()
                .unwrap()
                .value(),
            cell.control.value()
        );
    }
}

#[tokio::test]
async fn queued_object_root_confirmation_joins_already_selected_native_coverage() {
    let mut f = Fixture::new().await;
    enroll(&mut f).await;
    let mut a = f.cell(4).await;
    let mut b = f.cell(5).await;
    let (_, mut frames, arange) = f.append(&mut a, 2);
    let (_, bframes, brange) = f.append(&mut b, 2);
    frames.extend(bframes);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[arange, brange], NOW)
        .await
        .unwrap();
    let (selected, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    // The shared authority CAS can overtake a root flusher's preview. Preserve
    // the actual interval before the original producer confirms its local gate.
    assert_eq!(selected.advertisement().log().unwrap().tiered_through(), 2);
    assert_eq!(f.gate.tiered_through(), 0);
    let proof = proofs
        .iter()
        .find(|proof| proof.binding() == a.control.value().bundle_binding.unwrap())
        .unwrap();
    f.publisher(&a).materialize_bundle(proof).await.unwrap();
    let authority = Arc::new(super::actor::Authority {
        directory: f.directory.clone(),
        observed: tokio::sync::Mutex::new(selected.clone()),
    });
    let transport: Arc<dyn crate::node::log_transport::NodeLogTransport> = Arc::new(NoExtraIo);
    let shipper = crate::node::log_shipper::NodeLogShipper::new(
        f.gate.clone(),
        transport.clone(),
        Limits::default(),
    )
    .unwrap();
    let durability = crate::node::durability::NodeDurability::new(
        f.gate.clone(),
        shipper,
        authority.clone(),
        transport,
        f.lease.clone(),
    );
    f.count.reset();
    assert_eq!(
        durability
            .prove_object(arange.ticket())
            .await
            .unwrap()
            .source(),
        DurabilitySource::Object
    );
    assert_eq!(
        f.count.put_requests(),
        0,
        "already selected coverage needs no CAS"
    );
    let current = authority.observed.lock().await.clone();
    assert_eq!(current.advertisement(), selected.advertisement());
    assert_eq!(current.advertisement().log().unwrap().tiered_through(), 2);
    assert_eq!(
        f.gate.tiered_through(),
        1,
        "a root confirms only its own ticket"
    );
    assert!(!f.gate.objects_are_covered(&[brange.ticket()]).unwrap());
    assert_eq!(durability.confirm_bundle(&proofs).unwrap(), 2);
    assert_eq!(
        durability.prove(brange.ticket()).await.unwrap().source(),
        DurabilitySource::Bundle
    );
    assert_eq!(f.count.put_requests(), 0);
    assert!(
        f.directory
            .advance_log_coverage(&current, 1, current.advertisement().expires_at_ms())
            .await
            .is_err(),
        "already covered work cannot bypass original lease expiry"
    );
}

#[tokio::test]
async fn cold_proofs_and_replacement_gates_cannot_authorize_local_confirmation() {
    let mut f = Fixture::new().await;
    enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let (_, frames, assignment) = f.append(&mut cell, 2);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
        .await
        .unwrap();
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    let cold = f
        .directory
        .load_bundle_coverage(&cell.authority, &cell.control, Limits::default())
        .await
        .unwrap();
    assert!(matches!(
        confirm_selected_coverage(&f.gate, &f.lease, &[cold]),
        Err(Error::Node("bundle proof grants reconstruction only"))
    ));
    let replacement = DurabilityGate::new(
        SessionId::from_bytes([1; 16]),
        NodeId::from_bytes([1; 16]),
        EPOCH,
        [NodeId::from_bytes([2; 16]), NodeId::from_bytes([3; 16])],
    )
    .unwrap();
    let reserved = replacement.preview(frames.len() as u64).unwrap();
    let duplicate = replacement
        .commit_frames(reserved, &frames)
        .unwrap()
        .unwrap();
    assert_eq!(duplicate.ticket(), assignment.ticket());
    assert!(matches!(
        confirm_selected_coverage(&replacement, &f.lease, &proofs),
        Err(Error::Node(
            "selected capture belongs to another durability gate"
        ))
    ));
    assert_eq!(replacement.tiered_through(), 0);
    let replacement_lease = NodeLeaseGuard::new(NOW, NOW + 30_000).unwrap();
    assert!(matches!(
        confirm_selected_coverage(&f.gate, &replacement_lease, &proofs),
        Err(Error::Fenced)
    ));
    f.lease.fence();
    assert!(matches!(
        confirm_selected_coverage(&f.gate, &f.lease, &proofs),
        Err(Error::Fenced)
    ));
    assert_eq!(f.gate.tiered_through(), 0);
}

#[tokio::test]
async fn one_foreign_assignment_rejects_the_complete_confirmation_batch() {
    let mut f = Fixture::new().await;
    enroll(&mut f).await;
    let mut a = f.cell(4).await;
    let mut b = f.cell(5).await;
    let (_, mut frames, valid) = f.append(&mut a, 2);
    let (_, bframes, _) = f.append(&mut b, 2);
    let foreign = DurabilityGate::new(
        SessionId::from_bytes([1; 16]),
        NodeId::from_bytes([1; 16]),
        EPOCH,
        [NodeId::from_bytes([2; 16]), NodeId::from_bytes([3; 16])],
    )
    .unwrap();
    foreign
        .commit_frames(foreign.preview(frames.len() as u64).unwrap(), &frames)
        .unwrap();
    let invalid = foreign
        .commit_frames(foreign.preview(bframes.len() as u64).unwrap(), &bframes)
        .unwrap()
        .unwrap();
    frames.extend(bframes);
    let prepared = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[valid, invalid], NOW)
        .await
        .unwrap();
    let (_, proofs) = f
        .directory
        .select_node_bundle(&f.node, &prepared, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert!(matches!(
        confirm_selected_coverage(&f.gate, &f.lease, &proofs),
        Err(Error::Node(
            "selected capture belongs to another durability gate"
        ))
    ));
    assert_eq!(
        f.gate.tiered_through(),
        0,
        "valid sibling must not be confirmed"
    );
}

#[tokio::test]
async fn local_confirmation_never_invents_coverage_for_a_skipped_selected_bundle() {
    let mut f = Fixture::new().await;
    enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let (_, first, assignment) = f.append(&mut cell, 2);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &first, &[assignment], NOW)
        .await
        .unwrap();
    let (selected, first_proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    f.node = selected;
    let (_, second, assignment) = f.append(&mut cell, 3);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &second, &[assignment], NOW)
        .await
        .unwrap();
    let (_, second_proofs) = f
        .directory
        .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
        .await
        .unwrap();
    assert_eq!(
        confirm_selected_coverage(&f.gate, &f.lease, &second_proofs).unwrap(),
        0
    );
    assert_eq!(
        f.gate.prove(assignment.ticket()).await.unwrap().source(),
        DurabilitySource::Bundle
    );
    assert_eq!(
        confirm_selected_coverage(&f.gate, &f.lease, &first_proofs).unwrap(),
        2
    );
}

#[tokio::test]
async fn shared_selection_rebases_without_regressing_later_root_coverage() {
    let mut f = Fixture::new().await;
    enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let (_, frames, assignment) = f.append(&mut cell, 2);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
        .await
        .unwrap();
    let stale = f.node.clone();
    let later = f
        .directory
        .advance_log_coverage(&f.node, 2, NOW)
        .await
        .unwrap();
    f.node = later;
    let now = f.heartbeat().await;
    let (selected, _) = f
        .directory
        .select_node_bundle(&stale, &proposal, &f.lease, Limits::default(), now)
        .await
        .unwrap();
    assert_eq!(selected.advertisement().log().unwrap().tiered_through(), 2);
    assert_eq!(selected.advertisement().issued_at_ms(), now);
    assert_eq!(
        selected.advertisement().expires_at_ms(),
        f.node.advertisement().expires_at_ms()
    );
    assert_eq!(
        selected.advertisement().progress(),
        f.node.advertisement().progress()
    );
    assert_eq!(
        selected
            .advertisement()
            .bundle_head()
            .unwrap()
            .selected_through(),
        1
    );
}

#[tokio::test]
async fn reconciliation_refuses_a_matching_head_without_its_native_frontier() {
    let mut f = Fixture::new().await;
    enroll(&mut f).await;
    let mut cell = f.cell(4).await;
    let (_, frames, assignment) = f.append(&mut cell, 2);
    let proposal = f
        .directory
        .prepare_node_bundle(&f.node, &frames, &[assignment], NOW)
        .await
        .unwrap();
    // Historical versions could select a bundle without advancing native log
    // coverage. Such a record remains recoverable, but is not a live CAS reply.
    f.directory
        .select_catalog(&f.node, &proposal, NOW)
        .await
        .unwrap();
    assert!(matches!(
        f.directory
            .select_node_bundle(&f.node, &proposal, &f.lease, Limits::default(), NOW)
            .await,
        Err(Error::Node(
            "selected bundle lacks canonical native coverage"
        ))
    ));
    assert_eq!(f.gate.tiered_through(), 0);
}
