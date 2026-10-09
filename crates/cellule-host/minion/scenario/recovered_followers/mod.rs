//! Actual cold recovered ensemble and durable original enrollment publication.
//! This fixture has no Cell suffix; runtime lifecycle tests cover pinned tails.
use super::*;
use bytes::Bytes;
use cellule_host::fleet::{
    FleetEnrollmentAcceptance, FleetRecoveredFollowerRetirement, FleetRoster,
};
use cellule_runtime::{
    Error,
    fleet::operations::{
        EnrollmentEndpoint, EnrollmentEvent, EnrollmentRecord, EnrollmentRole, EnrollmentSpec,
        EnrollmentStatus,
    },
    follower::{FollowerReceipt, FollowerStore},
    node::{
        NodeAdvertisement, NodeCapacity, NodeDirectory, NodeFailureDomain, SealedNodeLog,
        log_recovery::{
            NodeLogRecovery, RecoveryCoordinator, retirement::retire_recovered_members,
        },
        log_transport::{
            AppendRequest, LocalFollowerTransport, LocalRecoveredFollowerTransport,
            NodeLogTransport, RecoveredNodeLogTransport, RecoveredRetireRequest, RetireRequest,
            SealRequest, TailRequest,
        },
    },
    recovery::manifest::RecoveryManifestStore,
};
use ed25519_dalek::SigningKey;
use futures_util::future::BoxFuture;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(unix)]
mod failed_boot;
mod members;
mod tests;
pub(in crate::scenario) use members::Members;

const NOW: i64 = 1_000_000;
const CHECK: i64 = NOW + 10_005;
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

struct Fixture {
    directory: NodeDirectory,
    journal: Arc<SqliteJournal>,
    path: PathBuf,
    transport: Arc<Members>,
    sealed: SealedNodeLog,
    #[cfg(unix)]
    manifests: RecoveryManifestStore,
    #[cfg(unix)]
    recovery_layout: CellStorageLayout,
    originals: Vec<EnrollmentRecord>,
    stores: Vec<FollowerStore>,
    #[cfg(unix)]
    process_request: Option<cellule_host::fleet::FleetFailedBootProcessRequest>,
    _root: tempfile::TempDir,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_process_observation(false).await
    }

    async fn with_process_observation(observe: bool) -> Self {
        Self::with_recovery_inputs(observe, Vec::new(), Vec::new()).await
    }

    async fn with_recovery_inputs(
        observe: bool,
        cells: Vec<cellule_runtime::node::log_recovery::RecoveryCell>,
        frames: Vec<Bytes>,
    ) -> Self {
        Self::with_recovery_inputs_at(observe, cells, frames, NOW).await
    }

    async fn with_recovery_inputs_at(
        observe: bool,
        cells: Vec<cellule_runtime::node::log_recovery::RecoveryCell>,
        frames: Vec<Bytes>,
        now: i64,
    ) -> Self {
        Self::with_recovered_boot_at(observe, cells, frames, now, 0, 1).await
    }

    async fn with_recovery_inputs_at_profile(
        observe: bool,
        cells: Vec<cellule_runtime::node::log_recovery::RecoveryCell>,
        frames: Vec<Bytes>,
        now: i64,
        profile: FleetProfile,
    ) -> Self {
        Self::with_recovered_boot_at_profile(observe, cells, frames, now, 0, 1, profile).await
    }

    async fn with_recovered_boot_at(
        observe: bool,
        cells: Vec<cellule_runtime::node::log_recovery::RecoveryCell>,
        frames: Vec<Bytes>,
        now: i64,
        leader: usize,
        claimant: usize,
    ) -> Self {
        Self::with_recovered_boot_at_profile(
            observe,
            cells,
            frames,
            now,
            leader,
            claimant,
            FleetProfile::default(),
        )
        .await
    }

    async fn with_recovered_boot_at_profile(
        observe: bool,
        cells: Vec<cellule_runtime::node::log_recovery::RecoveryCell>,
        frames: Vec<Bytes>,
        now: i64,
        leader: usize,
        claimant: usize,
        profile: FleetProfile,
    ) -> Self {
        assert!(leader < 3 && claimant < 3 && leader != claimant);
        let check = now + 10_005;
        #[cfg(not(unix))]
        assert!(!observe, "process lifetime stand-in requires Unix");
        let compiled = super::application::compile().unwrap();
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("journal.sqlite");
        let journal = Arc::new(
            SqliteJournal::open(path.clone(), scope(), profile, now)
                .await
                .unwrap(),
        );
        let layout = CellStorageLayout::new(
            Store::new(Arc::new(InMemory::new())),
            ObjectPath::from("recovered-enrollment"),
            [3; 16],
        );
        let directory = NodeDirectory::new(
            layout.clone(),
            scope().fleet,
            Digest::from_bytes([31; 32]),
            compiled.registry().release_digest(),
        );
        let mut intents = Vec::new();
        for index in 0..3 {
            let intent = journal
                .register_initial_intent(
                    &NodeIntent::initial(scope(), node_id(index), session(index)).unwrap(),
                )
                .await
                .unwrap();
            let issued = if index == leader { now } else { now + 9_000 };
            let ad = NodeAdvertisement::sign(
                node_id(index),
                session(index),
                owner(index).endpoint,
                scope().fleet,
                Digest::from_bytes([30; 32]),
                Digest::from_bytes([31; 32]),
                compiled.registry().release_digest(),
                &SigningKey::from_bytes(&[index as u8 + 1; 32]),
                1,
                issued,
                issued + 10_000,
                compiled.registry().module_digests(),
                vec![1],
                NodeFailureDomain::default(),
                NodeCapacity {
                    free_memory_bytes: 1 << 20,
                    free_disk_bytes: 1 << 20,
                    follower_free_bytes: 1 << 20,
                    job_credits: 4,
                    log_protocol: cellule_runtime::node::NODE_LOG_PROTOCOL_VERSION,
                    ..Default::default()
                },
            )
            .unwrap();
            startup::enroll(
                journal.as_ref(),
                &directory,
                &startup::spec(&intent).unwrap(),
                ad,
                issued,
            )
            .await
            .unwrap();
            intents.push(intent);
        }
        let source = directory
            .load(session(leader), now + 9_001)
            .await
            .unwrap()
            .unwrap();
        let prepared = directory
            .prepare_log_enrollment(&source, 4, 1, 3, now + 9_001)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(prepared.followers().len(), 2);
        let attempt = directory
            .prepare_log_enrollment_attempt(&prepared, now + 9_001)
            .await
            .unwrap();
        let mut originals = Vec::new();
        for (index, member) in prepared.followers().iter().enumerate() {
            let target = intents
                .iter()
                .find(|intent| intent.node() == member.node())
                .unwrap();
            let spec = EnrollmentSpec {
                scope: scope(),
                request: Digest::from_bytes([index as u8 + 90; 32]),
                source: Some(EnrollmentEndpoint {
                    node: node_id(leader),
                    session: session(leader),
                    intent_revision: 1,
                }),
                target: EnrollmentEndpoint {
                    node: member.node(),
                    session: member.session(),
                    intent_revision: target.revision(),
                },
                role: EnrollmentRole::Follower { log_epoch: 4 },
            };
            let FleetEnrollmentAcceptance::New(row) =
                journal.accept_enrollment(&spec, now + 9_001).await.unwrap()
            else {
                panic!("new enrollment expected")
            };
            originals.push(row);
        }
        directory
            .commit_log_enrollment(&attempt, now + 9_001)
            .await
            .unwrap();
        if !frames.is_empty() {
            let enrolled = directory
                .load(session(leader), now + 9_001)
                .await
                .unwrap()
                .unwrap();
            directory
                .activate_log(&enrolled, now + 9_002)
                .await
                .unwrap();
        }
        // Keep the second establishment reply unresolved. Canonical retirement
        // must cover Pending as well as the known Established original request.
        originals[0] = journal
            .publish_enrollment_result(
                &originals[0],
                EnrollmentEvent::Established(Digest::from_bytes([70; 32])),
                now + 9_002,
            )
            .await
            .unwrap();
        let version = journal.load_snapshot(scope()).await.unwrap().registry();
        journal.bootstrap_registry(version).await.unwrap();
        let mut stores = Vec::new();
        let mut peers = Vec::new();
        for (index, member) in prepared.log().members().iter().copied().enumerate() {
            let store = FollowerStore::open(
                root.path().join(format!("follower-{index}")),
                Limits::default(),
                DiskBudget::new(1 << 30),
            )
            .unwrap();
            if !frames.is_empty() {
                let receipt = store
                    .append(session(leader), 4, frames.clone(), 0)
                    .await
                    .unwrap();
                assert_eq!(receipt.durable_through, frames.len() as u64);
            }
            peers.push((
                member,
                LocalRecoveredFollowerTransport::new(
                    LocalFollowerTransport::new(member, store.clone()),
                    directory.clone(),
                    session(claimant),
                    move || Ok(check),
                )
                .unwrap(),
            ));
            stores.push(store);
        }
        let transport = Arc::new(Members {
            peers,
            retirements: AtomicUsize::new(0),
        });
        #[cfg(unix)]
        let mut process =
            observe.then(|| failed_boot::Process::start(root.path().join("process-closure")));
        let fenced = directory
            .claim_expired(session(leader), session(claimant), now + 10_001)
            .await
            .unwrap();
        #[cfg(unix)]
        let process_request = if let Some(process) = &mut process {
            assert_eq!(
                fenced.log().unwrap().phase(),
                cellule_runtime::node::log_state::NodeLogPhase::Recovering
            );
            let snapshot = journal.load_snapshot(scope()).await.unwrap();
            let roster = FleetRoster::collect(journal.as_ref(), &snapshot, deadline())
                .await
                .unwrap();
            let original = journal
                .load_enrollment(
                    scope(),
                    startup::spec(&intents[leader]).unwrap().key().unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
            assert!(
                cellule_host::fleet::FleetFailedBootProcessRequest::capture(
                    journal.as_ref(),
                    &directory,
                    &roster,
                    &original,
                    session(claimant),
                    deadline(),
                    || Ok(now + 10_001)
                )
                .await
                .is_err()
            );
            let request = cellule_host::fleet::FleetFailedBootProcessRequest::capture_fenced(
                journal.as_ref(),
                &directory,
                &roster,
                &original,
                session(claimant),
                deadline(),
                || Ok(now + 10_001),
            )
            .await
            .unwrap();
            assert!(request.canonical().is_none());
            process.stop_and_retain(&request);
            let confirmation = request
                .confirm(
                    journal.as_ref(),
                    &directory,
                    &failed_boot::Processes::new(root.path().join("process-closure")),
                    session(claimant),
                    deadline(),
                    || Ok(now + 10_001),
                )
                .await
                .unwrap();
            assert_eq!(confirmation.fence(), request.fence());
            assert_eq!(confirmation.snapshot(), roster.snapshot());
            assert_eq!(confirmation.process().request_digest(), request.digest());
            assert_eq!(confirmation.interval(), (now + 10_001, now + 10_001));
            Some(request)
        } else {
            None
        };
        let recovery =
            NodeLogRecovery::from_fenced(transport.clone(), &fenced, Limits::default()).unwrap();
        let manifests = RecoveryManifestStore::new(layout.clone(), Limits::default());
        let completed = RecoveryCoordinator::new(recovery, manifests.clone())
            .recover_and_seal(&directory, fenced, cells, now + 10_002)
            .await
            .unwrap();
        assert_eq!(completed.controls.len(), frames.len());
        Self {
            directory,
            journal,
            path,
            transport,
            sealed: completed.sealed,
            #[cfg(unix)]
            manifests,
            #[cfg(unix)]
            recovery_layout: layout,
            originals,
            stores,
            #[cfg(unix)]
            process_request,
            _root: root,
        }
    }
    async fn retire(&self) {
        let proof = retire_recovered_members(self.transport.clone(), &self.sealed)
            .await
            .unwrap()
            .confirmed()
            .unwrap();
        self.directory
            .retire_recovered_log(&proof, session(1), CHECK)
            .await
            .unwrap();
    }
    async fn roster(&self) -> FleetRoster {
        let snapshot = self.journal.load_snapshot(scope()).await.unwrap();
        FleetRoster::collect(self.journal.as_ref(), &snapshot, deadline())
            .await
            .unwrap()
    }
    async fn capture(&self) -> FleetRecoveredFollowerRetirement {
        FleetRecoveredFollowerRetirement::capture(
            self.journal.as_ref(),
            &self.directory,
            &self.roster().await,
            &self.sealed,
            session(1),
            deadline(),
            || Ok(CHECK),
        )
        .await
        .unwrap()
    }
    async fn reconstruct(&self) -> SqliteJournal {
        SqliteJournal::open(
            self.path.clone(),
            scope(),
            FleetProfile::default(),
            CHECK + 10,
        )
        .await
        .unwrap()
    }
}
