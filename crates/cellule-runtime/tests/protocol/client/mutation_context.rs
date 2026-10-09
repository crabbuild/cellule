//! Actual admitted evidence, transactional writes, replay and non-SDK paths.
use super::*;
use cellule_runtime::{PendingMutation, registry::CommandInvocation};

pub(super) struct RecordMutation;
impl Command for RecordMutation {
    const MODULE: &'static str = MODULE;
    const ID: u32 = 10;
    const CODEC_VERSION: u32 = 1;
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    fn execute(
        context: &mut CommandContext<'_, '_>,
        untrusted: Self::Input,
    ) -> cellule_runtime::Result<CommandResult<Self::Output>> {
        let bytes = match context.mutation_evidence() {
            Some(evidence) => {
                assert_eq!(evidence.target(), context.target());
                assert_eq!(evidence.incarnation(), context.owner_fence().incarnation);
                witness(&evidence, context.sequence())
            }
            None => b"no SDK mutation".to_vec(),
        };
        context.sql(&SqlBatch {
            statements: vec![SqlStatement {
                sql: "INSERT INTO comments(body) VALUES (?)".into(),
                parameters: vec![SqlValue::Blob(bytes.clone())],
            }],
        })?;
        match untrusted.first() {
            Some(1) => Ok(CommandResult::Rejected(bytes)),
            Some(2) => Err(cellule_runtime::Error::Command("test application error")),
            _ => Ok(CommandResult::Success(bytes)),
        }
    }
}

fn witness(evidence: &PendingMutation, sequence: u64) -> Vec<u8> {
    let identity = evidence.identity();
    let mut bytes = evidence.target().cell_id().as_bytes().to_vec();
    bytes.extend_from_slice(evidence.incarnation().as_bytes());
    bytes.extend_from_slice(identity.request_id.as_bytes());
    bytes.extend_from_slice(&identity.issued_at_ms.to_be_bytes());
    bytes.extend_from_slice(&identity.expires_at_ms.to_be_bytes());
    bytes.extend_from_slice(evidence.operation_digest().as_bytes());
    bytes.extend_from_slice(&sequence.to_be_bytes());
    bytes
}

fn peer_client(fixture: &Fixture) -> CellClient {
    let signer = Arc::new(PeerSigner::new(
        SessionId::from_bytes([12; 16]),
        fixture.registry.release_digest(),
        ed25519_dalek::SigningKey::from_bytes(&[13; 32]),
    ));
    CellClient::peer(
        fixture.registry.clone(),
        signer.clone(),
        PeerPrincipal {
            issuer: "https://identity.example".into(),
            subject: "alice".into(),
            actions: vec!["repository.issue.create".into()],
        },
        Arc::new(LoopbackRoundTrip {
            verifier: Arc::new(PeerVerifier::new(
                SessionId::from_bytes([12; 16]),
                fixture.registry.release_digest(),
                signer.verifying_key(),
            )),
            dispatcher: Arc::new(PeerDispatcher::new(
                fixture.registry.clone(),
                Arc::new(LocalResolver {
                    target: fixture.target.clone(),
                    handle: fixture.handle().clone(),
                }),
                Arc::new(RepositoryAuthorizer),
            )),
        }),
    )
}

#[tokio::test]
async fn admitted_mutation_matches_original_local_and_signed_peer_evidence_and_replay() {
    for peer in [false, true] {
        let mut fixture = fixture().await;
        let client = if peer {
            peer_client(&fixture)
        } else {
            CellClient::local(fixture.registry.clone(), fixture.handle().clone())
        };
        // Spoofed identity-shaped input must not become context evidence.
        let mut spoofed = vec![99; 48];
        spoofed[0] = 0;
        let command = client
            .prepare_command::<RecordMutation>(&fixture.target, mutation_identity(87), spoofed)
            .await
            .unwrap();
        let evidence = command.evidence().clone();
        let snapshot = command.snapshot();
        let body = command.input_bytes().to_vec();
        let original = command.clone().execute().await.unwrap();
        assert_eq!(original.receipt.commit_sequence, 1);
        assert_eq!(original.output, witness(&evidence, 1));
        assert_eq!(command.execute().await.unwrap(), original);
        for mode in [1, 2] {
            let command = client
                .prepare_command::<RecordMutation>(
                    &fixture.target,
                    mutation_identity(87 + mode),
                    vec![mode],
                )
                .await
                .unwrap();
            let expected = witness(command.evidence(), 2);
            let evidence = command.evidence().clone();
            let result = command.execute().await;
            match (mode, result) {
                (1, Err(InvocationError::Rejected(committed))) => {
                    assert_eq!(committed.output, expected);
                    assert_eq!(committed.receipt.commit_sequence, 2);
                }
                (2, Err(InvocationError::NotStarted(cellule_runtime::Error::Command(message)))) => {
                    assert!(!peer);
                    assert_eq!(message, "test application error");
                }
                (2, Err(InvocationError::NotStarted(cellule_runtime::Error::Peer(message)))) => {
                    assert!(peer);
                    assert_eq!(message, "remote peer rejected the request");
                }
                (_, other) => panic!("unexpected mode {mode} result {other:?}"),
            }
            if mode == 2 {
                assert!(matches!(
                    client.resolve(&evidence).await.unwrap(),
                    Resolution::Absent
                ));
            }
        }
        assert_eq!(
            client
                .query::<CountComments>(&fixture.target, None, ())
                .await
                .unwrap()
                .output,
            1,
            "rejection/error roll back the evidence write"
        );
        drop(client);
        fixture.take_handle().drain().await.unwrap();
        fixture.runtime.take().unwrap().shutdown().await.unwrap();
        let target = fixture.target.clone();
        let registry = fixture.registry.clone();
        let proof = fixture.proof.clone();
        let replica = fixture.replica.clone();
        let authority = fixture.authority.clone();
        let old_directory = fixture._directory.path().to_owned();
        drop(fixture);
        assert!(
            !old_directory.exists(),
            "original SQLite/cache state destroyed"
        );
        let restored_directory = tempfile::TempDir::new().unwrap();
        let session = SessionId::from_bytes([91; 16]);
        let runtime =
            CellRuntime::new(SqlWorkerPool::new(1, 4).unwrap(), 4 << 20, session).unwrap();
        let idle = authority.load(target.cell_id()).await.unwrap().unwrap();
        let successor = runtime
            .acquire_idle_restored(
                proof,
                replica,
                authority,
                idle,
                restored_directory
                    .path()
                    .join("mutation-context-restored.sqlite"),
                Owner {
                    session,
                    endpoint: "https://context-successor.invalid".into(),
                },
            )
            .await
            .unwrap();
        let client = CellClient::local(registry, successor.clone());
        assert_eq!(
            client
                .restore_command::<RecordMutation>(snapshot, body)
                .unwrap()
                .execute()
                .await
                .unwrap(),
            original
        );
        let stored = successor
            .query(0, 512, |connection| {
                Ok(
                    connection.query_row("SELECT body FROM comments", [], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })?,
                )
            })
            .await
            .unwrap();
        assert_eq!(
            stored, original.output,
            "application witness is in the restored durable root"
        );
        assert_eq!(
            client
                .query::<CountComments>(&target, None, ())
                .await
                .unwrap()
                .output,
            1
        );
        // Real runtime resolution expires before consulting the durable ledger.
        // Snapshot import cannot extend recovery of this known committed result.
        assert!(matches!(
            successor
                .resolve(
                    evidence.identity(),
                    evidence.operation_digest(),
                    evidence.identity().expires_at_ms,
                    512,
                )
                .await
                .unwrap(),
            Resolution::Expired
        ));
        successor.drain().await.unwrap();
        runtime.shutdown().await.unwrap();
    }
}

#[test]
fn direct_registry_dispatch_cannot_fabricate_sdk_mutation_evidence() {
    let registry = registry();
    let mut connection = cellule_ltx::rusqlite::Connection::open_in_memory().unwrap();
    connection.execute_batch(MIGRATION).unwrap();
    let transaction = connection.transaction().unwrap();
    let mut encoder = BoundedEncoder::new(64).unwrap();
    vec![99; 32].encode(&mut encoder).unwrap();
    let bytes = encoder.finish();
    let result = registry
        .execute_command(
            &transaction,
            CommandInvocation {
                module: MODULE,
                operation_id: RecordMutation::ID,
                codec_version: 1,
                schema: 1,
                target: CellTarget::new(
                    TenantId::from_bytes([1; 16]),
                    ApplicationId::from_bytes([2; 16]),
                    NAMESPACE,
                    b"direct",
                )
                .unwrap(),
                owner_fence: cellule_runtime::control::OwnerFence {
                    incarnation: IncarnationId::from_bytes([7; 16]),
                    epoch: 1,
                },
                sequence: 1,
                now_ms: 1_000,
                input: &bytes,
            },
        )
        .unwrap();
    let cellule_runtime::cell::executor::HandlerOutcome::Success(bytes) = result else {
        panic!("expected success");
    };
    let mut decoder = BoundedDecoder::new(&bytes, 512).unwrap();
    assert_eq!(Vec::<u8>::decode(&mut decoder).unwrap(), b"no SDK mutation");
    decoder.finish().unwrap();
}

#[tokio::test]
async fn inbox_effect_identity_does_not_masquerade_as_sdk_mutation_evidence() {
    let fixture = fixture().await;
    let client = effects::effect_client(&fixture, fixture.handle().clone());
    let now_ms = mutation_identity(92).issued_at_ms;
    let source = cellule_runtime::CellId::from_bytes([21; 32]);
    let incarnation = IncarnationId::from_bytes([22; 16]);
    let id = effect_id(source, incarnation, 9, 3);
    let mut input = BoundedEncoder::new(64).unwrap();
    vec![99; 32].encode(&mut input).unwrap();
    let request = wire::EffectRequest {
        target: Some(wire::Target {
            tenant_id: fixture.target.tenant().as_bytes().to_vec(),
            application_id: fixture.target.application().as_bytes().to_vec(),
            namespace_id: fixture.target.namespace().as_bytes().to_vec(),
            partition: fixture.target.partition().to_vec(),
        }),
        destination_incarnation: Vec::new(),
        identity: Some(wire::EffectIdentity {
            effect_id: id.to_vec(),
            source_cell: source.as_bytes().to_vec(),
            source_incarnation: incarnation.as_bytes().to_vec(),
            source_sequence: 9,
            ordinal: 3,
            expires_at_ms: now_ms + 60_000,
        }),
        operation: Some(wire::effect_request::Operation::CellCommand(
            wire::CellCommand {
                command_id: RecordMutation::ID,
                codec_version: 1,
                input: input.finish(),
            },
        )),
    };
    let operation = prost::Message::encode_to_vec(&request);
    let claim = EffectClaim {
        effect_id: id,
        destination: fixture.target.cell_id(),
        operation_digest: effect_operation_digest(fixture.target.cell_id(), id, &operation),
        operation,
        attempt: 1,
        token: [23; 16],
        lease_until_ms: now_ms + 30_000,
        expires_at_ms: now_ms + 60_000,
        created_sequence: 9,
    };
    let committed = client.deliver(&claim, now_ms).await.unwrap();
    let mut decoder = BoundedDecoder::new(committed.result(), 512).unwrap();
    assert_eq!(Vec::<u8>::decode(&mut decoder).unwrap(), b"no SDK mutation");
    decoder.finish().unwrap();
    assert_eq!(client.deliver(&claim, now_ms + 1).await.unwrap(), committed);
    fixture.handle().drain().await.unwrap();
}
