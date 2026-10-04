use super::*;

pub(super) struct Fixture {
    _root: tempfile::TempDir,
    pub(super) profile: FleetProfile,
    pub(super) journal: Arc<SqliteJournal>,
    pub(super) nodes: Vec<Arc<CellNode>>,
    boots: Vec<startup::BootOwner>,
    pub(super) records: Arc<HashMap<CellId, Record>>,
    pub(super) acknowledged: HashMap<CellId, Acknowledged>,
    pub(super) fleet: Arc<adapters::LocalFleet>,
    pub(super) spec: MoveAttemptSpec,
    pub(super) released: PublishedPosition,
    pub(super) released_attempt: MoveAttempt,
}

impl Fixture {
    pub(super) async fn released() -> Self {
        let root = tempfile::tempdir().unwrap();
        let profile = FleetProfile {
            controller_lease_ms: 3_000,
            reconcile_interval_ms: 500,
            ..FleetProfile::default()
        };
        let journal = Arc::new(
            SqliteJournal::open(
                root.path().join("closed-receiver-journal.sqlite"),
                scope(),
                profile,
                clock().unwrap(),
            )
            .await
            .unwrap(),
        );
        let mut nodes = Vec::new();
        let mut boots = Vec::new();
        let (records, acknowledged) =
            initialize_without_boot_withdrawal(&root, &journal, &mut nodes, &mut boots, 60_000, 1)
                .await
                .unwrap();
        let fleet = Arc::new(adapters::LocalFleet {
            nodes: nodes.clone(),
            journal: journal.clone(),
            boots: boots.clone(),
            records: records.clone(),
            reader_verifier: None,
            capture_sequence: std::sync::atomic::AtomicU64::new(0),
            lose_release_replies: false,
            lost_release_replies: std::sync::atomic::AtomicUsize::new(0),
            drop_closed_finalize_replies: std::sync::atomic::AtomicUsize::new(0),
            expired_receiver_cleanups: std::sync::atomic::AtomicUsize::new(0),
        });
        let controller = SessionId::from_bytes([206; 16]);
        let driver = FleetReconciler::new(
            scope(),
            controller,
            profile,
            journal.clone(),
            fleet.clone(),
            fleet.clone(),
        )
        .unwrap();
        let first = driver
            .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        assert!(first.snapshot.head().attempts().is_empty());
        let page = nodes[0]
            .runtime()
            .fleet_cells_page(None, 128)
            .await
            .unwrap();
        let CellInventoryEntry::Owned(row) = &page.entries()[0] else {
            panic!("fixture writer missing")
        };
        let spec = MoveAttemptSpec {
            id: AttemptId {
                operation: OperationId::from_bytes([220; 16]).unwrap(),
                sequence: 1,
            },
            target: row.target.clone(),
            incarnation: row.incarnation,
            source_node: node_id(0),
            source: session(0),
            generation: row.generation,
            source_epoch: row.position.as_ref().unwrap().epoch,
            destination_node: node_id(1),
            destination: session(1),
            cost: row.cost.unwrap(),
            snapshot_digest: Digest::from_bytes([221; 32]),
            deadline_ms: clock().unwrap() + 60_000,
        };
        journal
            .compare_exchange(
                &first.snapshot,
                first.snapshot.head().controller().unwrap().epoch,
                clock().unwrap(),
                &JournalTransition::Allocate(spec.clone()),
            )
            .await
            .unwrap();
        let version = journal.load_snapshot(scope()).await.unwrap().registry();
        journal.set_scheduling(version, false).await.unwrap();
        drop(page);
        let preparing = driver
            .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            preparing.snapshot.head().attempts()[0].phase(),
            AttemptPhase::Reserved
        );
        let release = driver
            .reconcile_once(clock, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(release.released, 1);
        let released = release.snapshot.head().attempts()[0]
            .released()
            .unwrap()
            .clone();
        let released_attempt = release.snapshot.head().attempts()[0].clone();
        assert_eq!(
            nodes[1].stats().local_disk_reserved_bytes(),
            spec.cost.disk_bytes
        );

        Self {
            _root: root,
            profile,
            journal,
            nodes,
            boots,
            records,
            acknowledged,
            fleet,
            spec,
            released,
            released_attempt,
        }
    }

    pub(super) fn driver(
        &self,
        claimant: SessionId,
        observer: Arc<dyn FleetObserver>,
        transport: Arc<dyn FleetTransport>,
    ) -> FleetReconciler {
        FleetReconciler::new(
            scope(),
            claimant,
            self.profile,
            self.journal.clone(),
            observer,
            transport,
        )
        .unwrap()
    }

    pub(super) async fn accept_original_activation(&self) -> AcceptedFleetAction {
        let snapshot = self.journal.load_snapshot(scope()).await.unwrap();
        let activating = self
            .journal
            .compare_exchange(
                &snapshot,
                snapshot.head().controller().unwrap().epoch,
                clock().unwrap(),
                &JournalTransition::Attempt {
                    id: self.spec.id,
                    event: AttemptEvent::BeginActivate,
                },
            )
            .await
            .unwrap();
        let action = activating
            .head()
            .movement_action(self.spec.id, MovementAction::Activate, clock().unwrap())
            .unwrap();
        match self
            .journal
            .accept_action(&action, node_id(1), session(1), clock().unwrap())
            .await
            .unwrap()
        {
            FleetActionAcceptance::New(accepted)
            | FleetActionAcceptance::Existing { accepted, .. } => accepted,
        }
    }

    pub(super) async fn claim_without_actor(&self, state: ControlState) {
        let accepted = self.accept_original_activation().await;
        let record = &self.records[&self.spec.target.cell_id()];
        let idle = record
            .authority
            .load(self.spec.target.cell_id())
            .await
            .unwrap()
            .unwrap();
        let basis =
            AcquisitionBasis::new(accepted, idle.value().clone(), clock().unwrap()).unwrap();
        self.journal.record_acquisition_basis(&basis).await.unwrap();
        // Model a crash after the canonical CAS and before actor/result
        // publication. These are real validated authority transitions.
        let recovering = record
            .authority
            .transition(
                &idle,
                idle.value().takeover(owner(1)).unwrap(),
                Transition::Takeover,
            )
            .await
            .unwrap();
        if state == ControlState::Serving {
            let mut serving = recovering.value().clone();
            serving.revision += 1;
            serving.progress += 1;
            serving.state = ControlState::Serving;
            record
                .authority
                .transition(&recovering, serving, Transition::Activate)
                .await
                .unwrap();
        } else {
            assert_eq!(state, ControlState::Recovering);
        }
    }

    pub(super) async fn close_receiver(&self) -> Arc<ClosedBootObserver> {
        let boots = &self.boots;
        let nodes = &self.nodes;
        let journal = &self.journal;
        let fleet = &self.fleet;
        // This node has no host shutdown withdrawal binding in this failure
        // fixture: the test first joins runtime work, then publishes the typed
        // host closure so the controller can prove exactly what it is routing.
        boots[1].guard.as_ref().unwrap().fence();
        nodes[1].shutdown().await.unwrap();
        assert_eq!(nodes[1].state(), NodeState::Stopped);
        let now = clock().unwrap();
        let observed = boots[1]
            .directory
            .load_if_live(session(1), now)
            .await
            .unwrap()
            .unwrap();
        boots[1]
            .directory
            .withdraw_after_drain(&observed, now)
            .await
            .unwrap();
        let snapshot = journal.load_snapshot(scope()).await.unwrap();
        let roster = FleetRoster::collect(
            journal.as_ref(),
            &snapshot,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
        let original = roster
            .enrollments()
            .iter()
            .find(|row| {
                row.spec().target.node == node_id(1)
                    && row.spec().target.session == session(1)
                    && row.status() == EnrollmentStatus::Established
            })
            .unwrap()
            .clone();
        let processes = StoppedNodeProcesses {
            node: nodes[1].clone(),
        };
        let retirement = FleetFailedBootRetirement::capture(
            journal.as_ref(),
            &boots[1].directory,
            &roster,
            &original,
            session(0),
            Instant::now() + Duration::from_secs(5),
            clock,
        )
        .await
        .unwrap();
        let request = retirement.request().clone();
        retirement
            .publish(
                journal.as_ref(),
                &boots[1].directory,
                &processes,
                session(0),
                Instant::now() + Duration::from_secs(5),
                clock,
            )
            .await
            .unwrap()
            .confirmed()
            .unwrap();

        Arc::new(ClosedBootObserver {
            fleet: Arc::clone(fleet),
            request,
            processes,
        })
    }

    pub(super) async fn shutdown(&self) {
        let nodes = &self.nodes;
        let boots = &self.boots;
        let journal = &self.journal;
        for node in nodes {
            node.shutdown().await.unwrap();
            assert_eq!(node.state(), NodeState::Stopped);
            let stats = node.stats();
            assert_eq!(stats.active_cells(), 0);
            assert_eq!(stats.worker_jobs(), 0);
            assert_eq!(stats.retained_bytes(), 0);
            assert_eq!(stats.resident_bytes(), 0);
            assert_eq!(stats.file_descriptors(), 0);
            assert_eq!(stats.local_disk_reserved_bytes(), 0);
        }
        for boot in boots {
            boot.withdraw(journal).await.unwrap();
        }
        journal.close().await.unwrap();
    }
}
