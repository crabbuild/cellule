//! Original acknowledged prefixes and stopped-boot proof share one full barrier.
//! The scoped planner remains partial: this is no replacement/finalization grant.
use super::*;
use cellule_host::fleet::{FleetFailedBootClosure, FleetFailedBootRetirement};

impl SuccessorFixture {
    async fn retire_original(&self) {
        let base = &self.original.base;
        let proof = retire_recovered_members(base.transport.clone(), &base.sealed)
            .await
            .unwrap()
            .confirmed()
            .unwrap();
        base.directory
            .retire_recovered_log(&proof, session(1), crate::scenario::clock().unwrap())
            .await
            .unwrap();
        FleetRecoveredFollowerRetirement::capture(
            base.journal.as_ref(),
            &base.directory,
            &base.roster().await,
            &base.sealed,
            session(1),
            deadline(),
            crate::scenario::clock,
        )
        .await
        .unwrap()
        .publish(
            base.journal.as_ref(),
            &base.directory,
            session(1),
            deadline(),
            crate::scenario::clock,
        )
        .await
        .unwrap()
        .confirmed()
        .unwrap();
        self.original_retirement()
            .await
            .publish(
                base.journal.as_ref(),
                &base.directory,
                &Processes::new(base.process_path()),
                session(1),
                deadline(),
                crate::scenario::clock,
            )
            .await
            .unwrap()
            .confirmed()
            .unwrap();
        assert_eq!(base.failed_boot().await.status(), EnrollmentStatus::Retired);
    }

    async fn original_retirement(&self) -> FleetFailedBootRetirement {
        let base = &self.original.base;
        FleetFailedBootRetirement::capture_retained(
            base.journal.as_ref(),
            &base.directory,
            &base.roster().await,
            &self.original.request,
            session(1),
            deadline(),
            crate::scenario::clock,
        )
        .await
        .unwrap()
    }

    async fn original_closure(&self) -> FleetFailedBootClosure {
        let base = &self.original.base;
        self.original_retirement()
            .await
            .confirm(
                base.journal.as_ref(),
                &base.directory,
                &Processes::new(base.process_path()),
                session(1),
                deadline(),
                crate::scenario::clock,
            )
            .await
            .unwrap()
    }

    fn combined_observation(
        &self,
        inventory: &FleetOriginalWriterSuccessorInventory,
        closure: &FleetFailedBootClosure,
        nodes: Vec<NodeAdvertisement>,
        cells: Vec<FleetOwnedCell>,
    ) -> FleetObservation {
        FleetObservation::new(
            scope(),
            inventory.original().snapshot().registry(),
            inventory.original().snapshot().registry().revision(),
            inventory.interval().0.min(closure.interval().0),
            crate::scenario::clock().unwrap(),
            false,
            nodes,
            cells,
        )
        .unwrap()
    }
}

struct CombinedObserver {
    fixture: Arc<SuccessorFixture>,
    closure_first: bool,
    reads: AtomicUsize,
}
impl FleetObserver for CombinedObserver {
    fn observe<'a>(
        &'a self,
        _: &'a FleetRoster,
        _: i64,
        _: Instant,
    ) -> FleetAdapterFuture<'a, FleetObservation> {
        Box::pin(async move {
            let closure = self.fixture.original_closure().await;
            let inventory = self.fixture.collect_current().await?;
            assert_eq!(closure.snapshot(), inventory.original().snapshot());
            assert_eq!(closure.process(), inventory.original().process());
            assert_eq!(
                closure.canonical().fence(),
                *inventory.original().recovered().fence()
            );
            assert_eq!(
                closure.canonical().log(),
                inventory.original().recovered().log()
            );
            assert_eq!(inventory.proofs().len(), 4);
            assert_eq!(inventory.original().writers().record().catalogs().len(), 2);
            assert_eq!(
                inventory
                    .proofs()
                    .iter()
                    .map(|proof| proof.suffixes().len())
                    .sum::<usize>(),
                2
            );
            let (nodes, cells) = self.fixture.observation_parts(&inventory).await;
            assert_eq!(cells.len(), 2);
            let observation = self
                .fixture
                .combined_observation(&inventory, &closure, nodes, cells);
            self.reads.fetch_add(1, Ordering::AcqRel);
            let observation = if self.closure_first {
                observation
                    .with_failed_boot_closures(vec![closure])?
                    .with_original_writer_successors(inventory)?
            } else {
                observation
                    .with_original_writer_successors(inventory)?
                    .with_failed_boot_closures(vec![closure])?
            };
            Ok(observation)
        })
    }
}

#[tokio::test]
async fn combined_original_observation_reconciles_both_attachment_orders_without_settlement() {
    for closure_first in [false, true] {
        let fixture = Arc::new(fixture().await);
        fixture.retire_original().await;
        let retired = fixture.original.base.failed_boot().await;
        let before = fixture
            .original
            .base
            .journal
            .load_snapshot(scope())
            .await
            .unwrap();
        let observer = Arc::new(CombinedObserver {
            fixture: fixture.clone(),
            closure_first,
            reads: AtomicUsize::new(0),
        });
        let driver = FleetReconciler::new(
            scope(),
            session(1),
            FleetProfile::default(),
            fixture.original.base.journal.clone(),
            observer.clone(),
            Arc::new(FailedSource),
        )
        .unwrap();
        let report = driver
            .reconcile_once(crate::scenario::clock, deadline())
            .await
            .unwrap();
        assert_eq!(observer.reads.load(Ordering::Acquire), 1);
        assert_eq!(report.allocated, 0);
        assert!(
            report
                .blockers
                .contains(&DrainBlocker::IncompleteObservation)
        );
        assert!(report.maintenance_failure.is_some());
        assert_ne!(
            report.snapshot.head().maintenance().unwrap().phase(),
            cellule_runtime::fleet::operations::MaintenancePhase::Completed
        );
        assert_eq!(report.snapshot.registry(), before.registry());
        assert_eq!(fixture.original.base.failed_boot().await, retired);
        assert_eq!(
            fixture
                .original
                .base
                .transport
                .retirements
                .load(Ordering::Acquire),
            2
        );
        drop(driver);
        drop(observer);
        Arc::try_unwrap(fixture).ok().unwrap().close().await;
    }
}

#[tokio::test]
async fn combined_original_observation_refuses_changed_full_head_with_same_registry_in_both_orders()
{
    for closure_first in [false, true] {
        let fixture = fixture().await;
        fixture.retire_original().await;
        let closure = fixture.original_closure().await;
        let old = closure.snapshot().clone();
        let next = fixture
            .original
            .base
            .journal
            .claim_controller(
                scope(),
                old.head().revision(),
                session(1),
                crate::scenario::clock().unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(next.registry(), old.registry());
        assert_ne!(next.head(), old.head());
        let inventory = fixture.collect_current().await.unwrap();
        assert_eq!(inventory.original().snapshot(), &next);
        let (nodes, cells) = fixture.observation_parts(&inventory).await;
        let observation = fixture.combined_observation(&inventory, &closure, nodes, cells);
        let result = if closure_first {
            observation
                .with_failed_boot_closures(vec![closure])
                .unwrap()
                .with_original_writer_successors(inventory)
        } else {
            observation
                .with_original_writer_successors(inventory)
                .unwrap()
                .with_failed_boot_closures(vec![closure])
        };
        assert!(matches!(
            result,
            Err(Error::Node("failed boot observation barrier differs"))
        ));
        fixture.close().await;
    }
}
