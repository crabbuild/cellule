//! Exact restart evidence, bounded import, and unchanged execution fences.
use super::*;
use crate::client::tests::{NAMESPACE, PendingCommand, PendingModule, RETAINED_CODE};
use crate::client::{CellTransport, EncodedObservation, EncodedQuery, EncodedResolve};
use crate::registry::{BuildDescriptor, CellModule, ModuleDescriptor, RegistryBuilder};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

struct Transport {
    description: CellDescription,
    descriptions: AtomicUsize,
    commands: Mutex<Vec<EncodedCommand>>,
    resolutions: Mutex<Vec<EncodedResolve>>,
    outcome: Mutex<Resolution>,
    lost: bool,
    started: Notify,
    hold: bool,
}
impl CellTransport for Transport {
    fn describe(
        &self,
        _: CellTarget,
    ) -> Pin<Box<dyn Future<Output = Result<CellDescription>> + Send>> {
        self.descriptions.fetch_add(1, Ordering::SeqCst);
        let description = self.description;
        Box::pin(async move { Ok(description) })
    }
    fn command(
        &self,
        command: EncodedCommand,
    ) -> Pin<Box<dyn Future<Output = Result<StoredOutcome>> + Send>> {
        assert_eq!(command.expected, self.description, "original owner fence");
        let identity = command.identity;
        let operation_digest = command.operation_digest;
        self.commands.lock().unwrap().push(command);
        self.started.notify_one();
        let resolution = self.outcome.lock().unwrap().clone();
        let lost = self.lost;
        let hold = self.hold;
        Box::pin(async move {
            if hold {
                std::future::pending::<()>().await;
            }
            if lost {
                Err(Error::OutcomeUnknown {
                    request_id: identity.request_id,
                    operation_digest,
                    source: Box::new(Error::RuntimeClosed),
                })
            } else {
                match resolution {
                    Resolution::Committed(outcome) => Ok(outcome),
                    _ => Err(Error::Fenced),
                }
            }
        })
    }
    fn query(
        &self,
        _: EncodedQuery,
    ) -> Pin<Box<dyn Future<Output = Result<EncodedObservation>> + Send>> {
        Box::pin(async { Err(Error::Command("unexpected query")) })
    }
    fn resolve(
        &self,
        request: EncodedResolve,
    ) -> Pin<Box<dyn Future<Output = Result<Resolution>> + Send>> {
        self.resolutions.lock().unwrap().push(request);
        let resolution = self.outcome.lock().unwrap().clone();
        Box::pin(async move { Ok(resolution) })
    }
}
fn registry() -> Arc<Registry> {
    let mut builder = RegistryBuilder::new(BuildDescriptor {
        source_revision: "snapshot-test".into(),
        cargo_lock_digest: Digest::from_bytes([5; 32]),
    });
    builder.register(PendingModule).unwrap();
    Arc::new(builder.finish().unwrap())
}
fn target(partition: &[u8]) -> CellTarget {
    CellTarget::new(
        TenantId::from_bytes([1; 16]),
        ApplicationId::from_bytes([2; 16]),
        NAMESPACE,
        partition,
    )
    .unwrap()
}
fn transport(target: &CellTarget, lost: bool, hold: bool, rejected: bool) -> Arc<Transport> {
    let result = encode_wire(&b"original".to_vec(), 64).unwrap();
    Arc::new(Transport {
        description: CellDescription {
            cell: target.cell_id(),
            incarnation: IncarnationId::from_bytes([6; 16]),
            code: RETAINED_CODE,
            schema: 1,
        },
        descriptions: AtomicUsize::new(0),
        commands: Mutex::default(),
        resolutions: Mutex::default(),
        outcome: Mutex::new(Resolution::Committed(if rejected {
            StoredOutcome::Rejected {
                result,
                commit_sequence: 42,
            }
        } else {
            StoredOutcome::Success {
                result,
                commit_sequence: 42,
            }
        })),
        lost,
        hold,
        started: Notify::new(),
    })
}
fn identity() -> MutationIdentity {
    let now = unix_time_ms().unwrap();
    MutationIdentity {
        request_id: RequestId::from_bytes([7; 16]),
        issued_at_ms: now,
        expires_at_ms: now + 60_000,
    }
}
fn round_trip(snapshot: &PreparedCommandSnapshot) -> PreparedCommandSnapshot {
    let header = encode_wire(snapshot, PreparedCommandSnapshot::MAX_ENCODED_BYTES).unwrap();
    let restored = decode_wire(&header, PreparedCommandSnapshot::MAX_ENCODED_BYTES).unwrap();
    assert_eq!(*snapshot, restored);
    restored
}
#[tokio::test]
async fn lost_response_restores_exact_command_and_resolves_original_success_or_rejection() {
    for rejected in [false, true] {
        let target = target(b"restart");
        let original_transport = transport(&target, true, false, rejected);
        let registry = registry();
        let original_client = CellClient::new(registry.clone(), original_transport.clone());
        let prepared = original_client
            .prepare_command::<PendingCommand>(&target, identity(), b"original".to_vec())
            .await
            .unwrap();
        let header = encode_wire(
            &prepared.snapshot(),
            PreparedCommandSnapshot::MAX_ENCODED_BYTES,
        )
        .unwrap();
        let body = prepared.input_bytes().to_vec();
        let pending = prepared.execute().await.unwrap_err();
        let InvocationError::Pending(pending) = pending else {
            panic!("must retain pending evidence")
        };
        drop(original_client);
        let new_transport = transport(&target, false, false, rejected);
        let restarted = CellClient::new(registry, new_transport.clone());
        let snapshot: PreparedCommandSnapshot =
            decode_wire(&header, PreparedCommandSnapshot::MAX_ENCODED_BYTES).unwrap();
        assert_eq!(snapshot.evidence(), pending.as_ref());
        let restored = restarted
            .restore_command::<PendingCommand>(snapshot.clone(), body)
            .unwrap();
        assert_eq!(new_transport.descriptions.load(Ordering::SeqCst), 0);
        assert!(new_transport.commands.lock().unwrap().is_empty());
        let Resolution::Committed(outcome) = restarted.resolve(snapshot.evidence()).await.unwrap()
        else {
            panic!("original outcome")
        };
        let decoded = decode_pending::<Vec<u8>>(restored.evidence(), outcome);
        let result = if rejected {
            let Err(InvocationError::Rejected(result)) = decoded else {
                panic!("rejected")
            };
            *result
        } else {
            decoded.unwrap()
        };
        assert_eq!(result.output, b"original");
        assert_eq!(
            result.receipt,
            Receipt {
                cell: target.cell_id(),
                incarnation: snapshot.evidence.incarnation,
                commit_sequence: 42
            }
        );
        assert!(
            new_transport.commands.lock().unwrap().is_empty(),
            "resolution never retries"
        );
        let requests = new_transport.resolutions.lock().unwrap();
        assert_eq!(requests[0].identity, pending.identity());
        assert_eq!(requests[0].operation_digest, pending.operation_digest());
        assert_eq!(requests[0].max_result_bytes, 64);
        assert_eq!(original_transport.commands.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn cancellation_restores_original_body_and_absent_retry_keeps_identity() {
    let target = target(b"canceled");
    let original = transport(&target, false, true, false);
    let registry = registry();
    let client = CellClient::new(registry.clone(), original.clone());
    let prepared = client
        .prepare_command::<PendingCommand>(&target, identity(), b"original".to_vec())
        .await
        .unwrap();
    let snapshot = round_trip(&prepared.snapshot());
    let body = prepared.input_bytes().to_vec();
    let task = tokio::spawn(prepared.execute());
    original.started.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    drop(client);
    let new_transport = transport(&target, false, false, false);
    let restarted = CellClient::new(registry, new_transport.clone());
    *new_transport.outcome.lock().unwrap() = Resolution::Absent;
    assert_eq!(
        restarted.resolve(snapshot.evidence()).await.unwrap(),
        Resolution::Absent
    );
    *new_transport.outcome.lock().unwrap() = original.outcome.lock().unwrap().clone();
    let restored = restarted
        .restore_command::<PendingCommand>(snapshot.clone(), body.clone())
        .unwrap();
    assert_eq!(restored.snapshot(), snapshot);
    assert_eq!(restored.input_bytes(), body);
    let result = restored.execute().await.unwrap();
    assert_eq!(result.receipt.commit_sequence, 42);
    let commands = new_transport.commands.lock().unwrap();
    assert_eq!(commands[0].identity, snapshot.evidence.identity);
    assert_eq!(
        commands[0].operation_digest,
        snapshot.evidence.operation_digest
    );
    assert_eq!(commands[0].input, body);
    assert_eq!(commands[0].expected, snapshot.description());
}
#[tokio::test]
async fn import_rejects_corrupt_body_target_contract_and_identity_without_io() {
    let target = target(b"corrupt");
    let io = transport(&target, false, false, false);
    let client = CellClient::new(registry(), io.clone());
    let prepared = client
        .prepare_command::<PendingCommand>(&target, identity(), b"original".to_vec())
        .await
        .unwrap();
    let snapshot = prepared.snapshot();
    let body = prepared.input_bytes().to_vec();
    for edit in 0..13 {
        let mut header = snapshot.clone();
        match edit {
            0 => header.module = "wrong".into(),
            1 => header.operation_id += 1,
            2 => header.codec_version += 1,
            3 => header.input_limit += 1,
            4 => header.evidence.max_result_bytes += 1,
            5 => header.code = Digest::from_bytes([0; 32]),
            6 => header.schema += 1,
            7 => header.evidence.target = self::target(b"different"),
            8 => header.evidence.incarnation = IncarnationId::from_bytes([8; 16]),
            9 => header.evidence.identity.request_id = RequestId::from_bytes([8; 16]),
            10 => header.evidence.identity.issued_at_ms = -1,
            11 => header.evidence.identity.expires_at_ms = header.evidence.identity.issued_at_ms,
            12 => header.evidence.operation_digest = Digest::from_bytes([0; 32]),
            _ => unreachable!(),
        }
        assert!(
            client
                .restore_command::<PendingCommand>(header, body.clone())
                .is_err(),
            "edit {edit}"
        );
    }
    for mut damaged in [
        body[..body.len() - 1].to_vec(),
        body.clone(),
        [body.clone(), vec![0]].concat(),
    ] {
        damaged[0] ^= 1;
        assert!(
            client
                .restore_command::<PendingCommand>(snapshot.clone(), damaged)
                .is_err()
        );
    }
    assert_eq!(io.descriptions.load(Ordering::SeqCst), 1, "prepare only");
    assert!(io.commands.lock().unwrap().is_empty());
    assert!(io.resolutions.lock().unwrap().is_empty());
    let bytes = encode_wire(&snapshot, PreparedCommandSnapshot::MAX_ENCODED_BYTES).unwrap();
    for end in 0..bytes.len() {
        assert!(
            decode_wire::<PreparedCommandSnapshot>(
                &bytes[..end],
                PreparedCommandSnapshot::MAX_ENCODED_BYTES
            )
            .is_err()
        );
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(
        decode_wire::<PreparedCommandSnapshot>(
            &trailing,
            PreparedCommandSnapshot::MAX_ENCODED_BYTES
        )
        .is_err()
    );
    let mut unsupported = bytes;
    unsupported[..4].copy_from_slice(&2u32.to_be_bytes());
    assert!(
        decode_wire::<PreparedCommandSnapshot>(
            &unsupported,
            PreparedCommandSnapshot::MAX_ENCODED_BYTES
        )
        .is_err()
    );
}
#[tokio::test]
async fn expired_snapshot_import_preserves_identity_without_authorizing_execution_or_absence() {
    let target = target(b"expired");
    let io = transport(&target, false, false, false);
    *io.outcome.lock().unwrap() = Resolution::Expired;
    let client = CellClient::new(registry(), io.clone());
    let prepared = client
        .prepare_command::<PendingCommand>(&target, identity(), b"original".to_vec())
        .await
        .unwrap();
    let body = prepared.input_bytes().to_vec();
    let mut snapshot = prepared.snapshot();
    snapshot.evidence.identity.issued_at_ms = 1_000;
    snapshot.evidence.identity.expires_at_ms = 61_000;
    snapshot.evidence.operation_digest = command_operation_digest::<PendingCommand>(
        snapshot.description(),
        snapshot.evidence.identity,
        &body,
    )
    .unwrap();
    let snapshot = round_trip(&snapshot);
    let restored = client
        .restore_command::<PendingCommand>(snapshot.clone(), body.clone())
        .unwrap();
    assert!(matches!(
        client.resolve(snapshot.evidence()).await.unwrap(),
        Resolution::Expired
    ));
    assert!(matches!(
        restored.execute().await,
        Err(InvocationError::NotStarted(Error::Command(
            "invalid mutation identity lifetime"
        )))
    ));
    assert!(io.commands.lock().unwrap().is_empty());
    let mut changed = transport(&target, false, false, false);
    Arc::get_mut(&mut changed).unwrap().description.incarnation =
        IncarnationId::from_bytes([9; 16]);
    let restarted = CellClient::new(registry(), changed.clone());
    assert!(
        restarted
            .restore_command::<PendingCommand>(snapshot.clone(), body)
            .is_ok()
    );
    assert!(matches!(
        restarted.resolve(snapshot.evidence()).await,
        Err(InvocationError::NotStarted(Error::Command(
            "pending mutation incarnation changed"
        )))
    ));
    assert!(changed.resolutions.lock().unwrap().is_empty());
    assert!(changed.commands.lock().unwrap().is_empty());
}

struct CeilingModule;
impl CellModule for CeilingModule {
    const NAME: &'static str = PendingModule::NAME;
    fn descriptor(&self) -> &'static ModuleDescriptor {
        static DESCRIPTOR: std::sync::OnceLock<ModuleDescriptor> = std::sync::OnceLock::new();
        DESCRIPTOR.get_or_init(|| {
            let base = PendingModule.descriptor();
            ModuleDescriptor {
                commands: Box::leak(Box::new([OperationDescriptor {
                    input_limit: MAX_WIRE_BYTES as u32,
                    ..base.commands[0]
                }])),
                ..*base
            }
        })
    }
    fn register(self, registry: &mut RegistryBuilder) -> Result<()> {
        PendingModule.register(registry)
    }
}
#[tokio::test]
async fn maximum_legal_input_keeps_separate_bounded_header_and_restores_without_copying_body() {
    let target = target(&[42; 1024]);
    let io = transport(&target, false, false, false);
    let mut builder = RegistryBuilder::new(BuildDescriptor {
        source_revision: "max-input".into(),
        cargo_lock_digest: Digest::from_bytes([5; 32]),
    });
    builder.register(CeilingModule).unwrap();
    let client = CellClient::new(Arc::new(builder.finish().unwrap()), io);
    let prepared = client
        .prepare_command::<PendingCommand>(&target, identity(), vec![7; MAX_WIRE_BYTES - 4])
        .await
        .unwrap();
    let snapshot = round_trip(&prepared.snapshot());
    let body = prepared.input_bytes().to_vec();
    assert_eq!(body.len(), MAX_WIRE_BYTES);
    let pointer = body.as_ptr();
    let restored = client
        .restore_command::<PendingCommand>(snapshot.clone(), body)
        .unwrap();
    assert_eq!(
        restored.input_bytes().as_ptr(),
        pointer,
        "restore takes ownership of the existing buffer"
    );
    assert_eq!(restored.snapshot(), snapshot);
}
