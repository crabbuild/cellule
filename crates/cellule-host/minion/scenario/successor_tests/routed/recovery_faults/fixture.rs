use super::*;

pub(super) struct FaultFixture {
    pub(super) native: super::super::fixture::Fixture,
    pub(super) observer: Arc<ClosedBootObserver>,
    transport: Arc<CapturedTransport>,
    pub(super) accepted: AcceptedFleetAction,
    pub(super) original: Control,
    resume: tokio::sync::oneshot::Sender<()>,
    pass: tokio::task::JoinHandle<cellule_runtime::Result<FleetReconcileReport>>,
}

impl FaultFixture {
    pub(super) async fn paused(
        write: RecoveryWrite,
        boundary: RecoveryWriteBoundary,
        fail: bool,
    ) -> Self {
        let native = super::super::fixture::Fixture::released_with_recovery(true).await;
        native.claim_without_actor(ControlState::Serving).await;
        // Keep the shared finite constructor out of every caller's inline
        // future; nested native observation exceeds normal debug test stacks.
        Box::pin(Self::pause_claim(native, write, boundary, fail)).await
    }

    pub(super) async fn pause_claim(
        native: super::super::fixture::Fixture,
        write: RecoveryWrite,
        boundary: RecoveryWriteBoundary,
        fail: bool,
    ) -> Self {
        let original = native.records[&native.spec.target.cell_id()]
            .authority
            .load(native.spec.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .clone();
        let observer = native.close_receiver().await;
        let transport = Arc::new(CapturedTransport {
            fleet: native.fleet.clone(),
            activation: Mutex::new(None),
        });
        let (entered, resume) = native
            .journal
            .pause_receiver_recovery_write(write, boundary, fail);
        let driver = native.driver(
            SessionId::from_bytes([206; 16]),
            observer.clone(),
            transport.clone(),
        );
        let pass = tokio::spawn(async move {
            driver
                .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered)
            .await
            .unwrap()
            .unwrap();
        let accepted = native
            .journal
            .load_movement_actions(scope(), &native.released_attempt, MovementAction::Activate)
            .await
            .unwrap()
            .into_iter()
            .find_map(|value| match value {
                FleetActionAcceptance::New(accepted)
                | FleetActionAcceptance::Existing { accepted, .. }
                    if accepted.action().receiver_endpoint() == Some((node_id(2), session(2))) =>
                {
                    Some(accepted)
                }
                _ => None,
            })
            .unwrap();
        assert!(accepted.action().receiver_route().is_some());
        Self {
            native,
            observer,
            transport,
            accepted,
            original,
            resume,
            pass,
        }
    }

    pub(super) async fn current(&self) -> Control {
        self.native.records[&self.native.spec.target.cell_id()]
            .authority
            .load(self.native.spec.target.cell_id())
            .await
            .unwrap()
            .unwrap()
            .value()
            .clone()
    }

    pub(super) async fn release(self, cancel_waiter: bool) -> CompletedFixture {
        if cancel_waiter {
            self.pass.abort();
            assert!(self.pass.await.unwrap_err().is_cancelled());
            self.resume.send(()).unwrap();
        } else {
            self.resume.send(()).unwrap();
            let report = self.pass.await.unwrap().unwrap();
            assert_eq!(
                report.snapshot.head().reserved_restore_bytes(),
                self.native.spec.cost.disk_bytes
            );
        }
        CompletedFixture {
            native: self.native,
            observer: self.observer,
            accepted: self.accepted,
            original: self.original,
            completion: self.transport.activation.lock().unwrap().clone(),
        }
    }
}

pub(super) struct CompletedFixture {
    pub(super) native: super::super::fixture::Fixture,
    observer: Arc<ClosedBootObserver>,
    pub(super) accepted: AcceptedFleetAction,
    pub(super) original: Control,
    pub(super) completion: Option<Arc<FleetActionCompletion>>,
}
impl CompletedFixture {
    pub(super) async fn current(&self) -> cellule_runtime::control::authority::VersionedControl {
        self.native.records[&self.native.spec.target.cell_id()]
            .authority
            .load(self.native.spec.target.cell_id())
            .await
            .unwrap()
            .unwrap()
    }
    pub(super) async fn replay(&self) -> Arc<FleetActionCompletion> {
        self.native.nodes[2]
            .apply_fleet_action(self.accepted.action().clone(), clock().unwrap())
            .await
            .unwrap()
    }
    pub(super) async fn finish(self) {
        Box::pin(self.finish_inner(None, false)).await;
    }
    pub(super) async fn finish_value(self, expected_value: Option<i64>) {
        Box::pin(self.finish_inner(expected_value, false)).await;
    }
    pub(super) async fn finish_after_controller_loss(self, expected_value: i64) {
        Box::pin(self.finish_inner(Some(expected_value), true)).await;
    }
    async fn finish_inner(self, expected_value: Option<i64>, replace_controller: bool) {
        let client = self.native.reopen_journal().await;
        let basis = self
            .native
            .journal
            .load_receiver_recovery_basis(&self.accepted)
            .await
            .unwrap()
            .unwrap();
        let evidence = self
            .native
            .journal
            .load_receiver_recovery_evidence(&self.accepted)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(basis.control(), &self.original);
        assert_eq!(evidence.basis(), &basis);
        assert_eq!(
            client
                .load_receiver_recovery_basis(&self.accepted)
                .await
                .unwrap(),
            Some(basis)
        );
        assert_eq!(
            client
                .load_receiver_recovery_evidence(&self.accepted)
                .await
                .unwrap(),
            Some(evidence)
        );
        let old_snapshot = client.load_snapshot(scope()).await.unwrap();
        let old_lease = old_snapshot.head().controller().unwrap();
        let old_epoch = old_lease.epoch;
        let old_driver = self.native.driver(
            SessionId::from_bytes([206; 16]),
            self.observer.clone(),
            self.native.fleet.clone(),
        );
        if replace_controller {
            let wait = old_lease.expires_at_ms.saturating_sub(clock().unwrap()) + 1;
            tokio::time::sleep(Duration::from_millis(u64::try_from(wait.max(0)).unwrap())).await;
        }
        let claimant = SessionId::from_bytes([if replace_controller { 207 } else { 206 }; 16]);
        let driver = FleetReconciler::new(
            scope(),
            claimant,
            self.native.profile,
            client.clone(),
            self.observer.clone(),
            self.native.fleet.clone(),
        )
        .unwrap();
        let mut report = driver
            .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        if replace_controller {
            assert!(report.snapshot.head().controller().unwrap().epoch > old_epoch);
            assert_eq!(
                report.snapshot.head().controller().unwrap().claimant,
                claimant
            );
            let before = client.load_snapshot(scope()).await.unwrap();
            let error = old_driver
                .reconcile_once(clock, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap_err();
            assert!(
                matches!(&error, cellule_runtime::Error::Facility { name: "fleet-journal", source }
                if matches!(source.downcast_ref::<OperationError>(), Some(OperationError::Fenced))),
                "{error:?}"
            );
            assert_eq!(client.load_snapshot(scope()).await.unwrap(), before);
        }
        for _ in 0..12 {
            if report.snapshot.head().attempts().is_empty() {
                break;
            }
            report = driver
                .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
                .await
                .unwrap();
        }
        assert!(report.snapshot.head().attempts().is_empty(), "{report:?}");
        assert_eq!(report.snapshot.head().reserved_restore_bytes(), 0);
        let record = &self.native.records[&self.native.spec.target.cell_id()];
        let current = record
            .authority
            .load(record.target.cell_id())
            .await
            .unwrap()
            .unwrap();
        let handle = self.native.nodes[2]
            .runtime()
            .local_handle(record.catalog.clone(), &current)
            .await
            .unwrap()
            .unwrap();
        let original = &self.native.acknowledged[&record.target.cell_id()];
        assert_eq!(
            handle
                .resolve(original.identity, original.digest, clock().unwrap(), 64)
                .await
                .unwrap(),
            Resolution::Committed(original.outcome.clone())
        );
        let bytes = handle
            .query(64, 64, |connection| {
                let value: i64 =
                    connection.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
                Ok(value.to_be_bytes().to_vec())
            })
            .await
            .unwrap();
        assert_eq!(
            bytes,
            expected_value.unwrap_or(original.value).to_be_bytes()
        );
        assert_eq!(self.native.nodes[2].stats().active_cells(), 1);
        self.native.shutdown().await;
        client.close().await.unwrap();
    }
}
