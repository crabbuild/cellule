//! Persisted exact commands over real durable publication and fresh-disk recovery.
use super::*;
use cellule_runtime::{PreparedCommandSnapshot, cell::executor::StoredOutcome};

struct LostMutationReply(Arc<LoopbackRoundTrip>);
impl PeerRoundTrip for LostMutationReply {
    fn send(
        &self,
        target: CellTarget,
        request: Vec<u8>,
        remaining_ms: u32,
    ) -> Pin<Box<dyn Future<Output = cellule_runtime::Result<Vec<u8>>> + Send + 'static>> {
        let inner = self.0.clone();
        Box::pin(async move {
            let now_ms = mutation_identity(99).issued_at_ms;
            let mutation = inner.verifier.verify(&request, now_ms)?.operation_tag() == 10;
            let reply = inner.send(target, request, remaining_ms).await?;
            if mutation {
                // Discard only after the real owner published the durable reply.
                Err(cellule_runtime::Error::PeerTransportUnknown {
                    context: "durable mutation reply deliberately lost",
                    source: Box::new(cellule_runtime::Error::RuntimeClosed),
                })
            } else {
                Ok(reply)
            }
        })
    }
}
#[tokio::test]
async fn persisted_commands_resolve_original_receipts_after_lost_replies_and_fresh_disk_owner_restore()
 {
    let mut original = fixture().await;
    let signer = Arc::new(PeerSigner::new(
        SessionId::from_bytes([12; 16]),
        original.registry.release_digest(),
        ed25519_dalek::SigningKey::from_bytes(&[13; 32]),
    ));
    let round_trip = Arc::new(LoopbackRoundTrip {
        verifier: Arc::new(PeerVerifier::new(
            SessionId::from_bytes([12; 16]),
            original.registry.release_digest(),
            signer.verifying_key(),
        )),
        dispatcher: Arc::new(PeerDispatcher::new(
            original.registry.clone(),
            Arc::new(LocalResolver {
                target: original.target.clone(),
                handle: original.handle().clone(),
            }),
            Arc::new(RepositoryAuthorizer),
        )),
    });
    let client = CellClient::peer(
        original.registry.clone(),
        signer,
        PeerPrincipal {
            issuer: "https://identity.example".into(),
            subject: "alice".into(),
            actions: vec!["repository.issue.create".into()],
        },
        Arc::new(LostMutationReply(round_trip)),
    );
    let files = tempfile::TempDir::new().unwrap();
    let success = client
        .prepare_command::<CreateComment>(&original.target, mutation_identity(81), b"once".to_vec())
        .await
        .unwrap();
    let rejection = client
        .prepare_command::<RejectComment>(
            &original.target,
            mutation_identity(82),
            b"hidden".to_vec(),
        )
        .await
        .unwrap();
    for (name, header, body) in [
        ("success", success.snapshot(), success.input_bytes()),
        ("rejection", rejection.snapshot(), rejection.input_bytes()),
    ] {
        std::fs::write(
            files.path().join(format!("{name}.header")),
            header.to_bytes().unwrap(),
        )
        .unwrap();
        std::fs::write(files.path().join(format!("{name}.body")), body).unwrap();
    }
    let pending_success = match success.execute().await {
        Err(InvocationError::Pending(pending)) => pending,
        other => panic!("expected lost success reply, got {other:?}"),
    };
    let pending_rejection = match rejection.execute().await {
        Err(InvocationError::Pending(pending)) => pending,
        other => panic!("expected lost rejection reply, got {other:?}"),
    };
    drop(client); // Releases the original peer dispatcher and handle too.
    original.take_handle().drain().await.unwrap();
    original.runtime.take().unwrap().shutdown().await.unwrap();
    let target = original.target.clone();
    let authority = original.authority.clone();
    let replica = original.replica.clone();
    let proof = original.proof.clone();
    let registry = original.registry.clone();
    let original_path = original._directory.path().to_owned();
    drop(original);
    assert!(
        !original_path.exists(),
        "all original SQLite/cache state destroyed"
    );

    let idle = authority.load(target.cell_id()).await.unwrap().unwrap();
    let session = SessionId::from_bytes([83; 16]);
    let runtime =
        CellRuntime::new(SqlWorkerPool::new(1, 4).unwrap(), 4 * 1024 * 1024, session).unwrap();
    let handle = runtime
        .acquire_idle_restored(
            proof,
            replica,
            authority,
            idle,
            files.path().join("restored.sqlite"),
            Owner {
                session,
                endpoint: "https://restored.internal:8081".into(),
            },
        )
        .await
        .unwrap();
    let client = CellClient::local(registry, handle.clone());
    let read = |name: &str| {
        let snapshot = PreparedCommandSnapshot::from_bytes(
            &std::fs::read(files.path().join(format!("{name}.header"))).unwrap(),
        )
        .unwrap();
        let body = std::fs::read(files.path().join(format!("{name}.body"))).unwrap();
        (snapshot, body)
    };
    let (header, body) = read("success");
    assert_eq!(header.evidence(), pending_success.as_ref());
    let success = client
        .restore_command::<CreateComment>(header.clone(), body)
        .unwrap();
    let Resolution::Committed(StoredOutcome::Success {
        result,
        commit_sequence,
    }) = client.resolve(header.evidence()).await.unwrap()
    else {
        panic!("original success must resolve")
    };
    assert_eq!(decode::<Vec<u8>>(&result, 64).unwrap(), b"once");
    assert_eq!(commit_sequence, 1);
    let replay = success.execute().await.unwrap();
    assert_eq!(
        replay.receipt,
        Receipt {
            cell: target.cell_id(),
            incarnation: pending_success.incarnation(),
            commit_sequence: 1
        }
    );
    assert_eq!(replay.output, b"once");
    let (header, body) = read("rejection");
    assert_eq!(header.evidence(), pending_rejection.as_ref());
    let rejected = client
        .restore_command::<RejectComment>(header.clone(), body)
        .unwrap();
    let Resolution::Committed(StoredOutcome::Rejected {
        result,
        commit_sequence,
    }) = client.resolve(header.evidence()).await.unwrap()
    else {
        panic!("original rejection must resolve")
    };
    assert_eq!(decode::<Vec<u8>>(&result, 64).unwrap(), b"moderated");
    assert_eq!(commit_sequence, 2);
    let Err(InvocationError::Rejected(replay)) = rejected.execute().await else {
        panic!("original rejection replay")
    };
    assert_eq!(
        replay.receipt,
        Receipt {
            cell: target.cell_id(),
            incarnation: pending_rejection.incarnation(),
            commit_sequence: 2
        }
    );
    let observed = client
        .query::<CountComments>(&target, Some(replay.receipt), ())
        .await
        .unwrap();
    assert_eq!(
        observed.output, 1,
        "no duplicate success or rejected SQL mutation"
    );
    assert_eq!(
        observed.receipt.commit_sequence, 2,
        "recovery/replay does not create another commit"
    );
    drop(client);
    handle.drain().await.unwrap();
    runtime.shutdown().await.unwrap();
}

fn decode<T: WireValue>(bytes: &[u8], limit: u32) -> Result<T, CodecError> {
    let mut decoder = BoundedDecoder::new(bytes, limit)?;
    let value = T::decode(&mut decoder)?;
    decoder.finish()?;
    Ok(value)
}
