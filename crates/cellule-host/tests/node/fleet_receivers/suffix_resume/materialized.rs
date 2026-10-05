use super::*;

struct PausedRecovery {
    journal: Arc<dyn FleetActionJournal>,
    accepted: AcceptedFleetAction,
    takeover: cellule_runtime::node::NodeTakeoverProof,
    entered: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    resume: tokio::sync::Mutex<tokio::sync::oneshot::Receiver<()>>,
}
impl AcquisitionObserver for PausedRecovery {
    fn before_claim<'a>(&'a self, input: &'a Control) -> AcquisitionObservation<'a> {
        Box::pin(async move {
            let basis =
                RecoveryBasis::new(&self.accepted, input.clone(), self.takeover, clock()).unwrap();
            let original = self
                .journal
                .record_recovery_basis(&self.accepted, &basis)
                .await
                .unwrap();
            assert_eq!(original.control(), input);
            Ok(())
        })
    }
    fn before_activation<'a>(
        &'a self,
        input: &'a Control,
        restored: &'a Control,
    ) -> AcquisitionObservation<'a> {
        Box::pin(async move {
            let basis = self
                .journal
                .load_recovery_basis(&self.accepted)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(basis.control(), input);
            let evidence = RecoveryEvidence::new(basis, restored.clone(), clock()).unwrap();
            assert_eq!(
                self.journal
                    .record_recovery_evidence(&self.accepted, &evidence)
                    .await
                    .unwrap(),
                evidence
            );
            self.entered
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send(())
                .unwrap();
            let _ = (&mut *self.resume.lock().await).await;
            Ok(())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_overlay_materialization_resumes_only_the_original_checked_control() {
    let movement = Movement::new(128 << 20).await;
    let now = clock();
    let identity = MutationIdentity {
        request_id: RequestId::from_bytes([250; 16]),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    };
    let digest = Digest::from_bytes([250; 32]);
    let receipt = movement
        .source
        .handle
        .execute(identity, digest, now, 64, 64, |tx| {
            tx.execute("UPDATE counter SET value=value", [])?;
            Ok(HandlerOutcome::Success(vec![42]))
        })
        .await
        .unwrap();
    suffix::start_suffix_recovery(&movement).await;
    let original = movement
        .inputs
        .authority
        .load(movement.spec.target.cell_id())
        .await
        .unwrap()
        .unwrap();
    let recovery = movement.recovery_inputs.lock().unwrap().clone().unwrap();
    let FleetActionAcceptance::New(accepted) = movement
        .source
        .journal
        .accept_action(
            &movement.action(MovementAction::Recover),
            movement.spec.destination_node,
            movement.spec.destination,
            clock(),
        )
        .await
        .unwrap()
    else {
        panic!("expected new recovery")
    };
    let prepared = movement
        .receiver
        .runtime()
        .prepared_receiver(movement.spec.id)
        .unwrap()
        .unwrap();
    movement
        .receiver
        .runtime()
        .cancel_prepared_receiver(&prepared)
        .unwrap();
    let (entered, captured) = tokio::sync::oneshot::channel();
    let (_resume, paused) = tokio::sync::oneshot::channel();
    let recorder = Arc::new(PausedRecovery {
        journal: movement.source.journal.clone(),
        accepted: accepted.clone(),
        takeover: recovery.takeover,
        entered: Mutex::new(Some(entered)),
        resume: tokio::sync::Mutex::new(paused),
    });
    let node = movement.receiver.clone();
    let inputs = movement.inputs.clone();
    let expected = original.clone();
    let takeover = recovery.clone();
    let future_recorder = recorder.clone();
    let owner = tokio::spawn(async move {
        node.runtime()
            .takeover_restored_observed(
                inputs.catalog,
                inputs.replica,
                inputs.authority,
                expected,
                takeover.takeover,
                takeover.manifests,
                inputs.destination,
                inputs.owner,
                Some(future_recorder),
            )
            .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), captured)
        .await
        .unwrap()
        .unwrap();
    // Simulate an embedding owner's interruption after native materialization
    // and durable recording, before actor admission. Transport cancellation is
    // separately owned by the host action tests and cannot abort that owner.
    owner.abort();
    assert!(matches!(owner.await, Err(error) if error.is_cancelled()));
    assert_eq!(movement.receiver.stats().active_cells(), 0);
    let materialized = movement
        .inputs
        .authority
        .load(original.value().cell)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(materialized.value().state, ControlState::Recovering);
    assert!(materialized.value().recovery.is_none());
    let evidence = movement
        .source
        .journal
        .load_recovery_evidence(&accepted)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(evidence.basis().control(), original.value());
    assert_eq!(evidence.restored(), materialized.value());
    let mut changed = original.value().clone();
    changed.epoch += 1;
    let refused = movement
        .receiver
        .runtime()
        .resume_takeover_restored_observed(
            movement.inputs.catalog.clone(),
            movement.inputs.replica.clone(),
            movement.inputs.authority.clone(),
            changed,
            materialized.clone(),
            recovery.takeover,
            recovery.manifests.clone(),
            movement.inputs.destination.clone(),
            recorder,
        )
        .await;
    assert!(matches!(refused, Err(Error::Fenced)));
    assert_eq!(
        movement
            .inputs
            .authority
            .load(original.value().cell)
            .await
            .unwrap()
            .unwrap()
            .value(),
        materialized.value()
    );
    assert_eq!(movement.receiver.stats().active_cells(), 0);
    let completion = apply(&movement.receiver, movement.action(MovementAction::Recover)).await;
    assert!(
        completion.committed && completion.execution_error.is_none(),
        "{completion:?}"
    );
    let FleetOutcome::Recovered(recovered) = &completion.outcome.outcome else {
        panic!("not recovered")
    };
    assert_eq!(recovered.recovery, evidence);
    assert_eq!(recovered.serving.position.epoch, original.value().epoch + 1);
    movement.event(AttemptEvent::Recovered(recovered.clone()));
    let current = movement
        .inputs
        .authority
        .load(original.value().cell)
        .await
        .unwrap()
        .unwrap();
    let handle = movement
        .receiver
        .runtime()
        .local_handle(movement.inputs.catalog.clone(), &current)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(counter(&handle).await, 43);
    assert_eq!(
        handle.resolve(identity, digest, clock(), 64).await.unwrap(),
        Resolution::Committed(receipt)
    );
    assert_eq!(movement.receiver.stats().active_cells(), 1);
    movement.inspect(249).await;
    movement.shutdown().await;
}
