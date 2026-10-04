use super::*;

impl SourceFixture {
    pub(super) async fn new() -> Self {
        let mut fixture = Fixture::for_maintenance(0).await;
        assert_eq!(fixture.original.spec().source.unwrap().node, node_id(0));
        let opened = fixture.reader.lifecycle_observation().await.root();
        let now = clock().unwrap();
        fixture
            .handle
            .execute(
                MutationIdentity {
                    request_id: RequestId::from_bytes([222; 16]),
                    issued_at_ms: now,
                    expires_at_ms: now + 30_000,
                },
                Digest::from_bytes([223; 32]),
                now,
                64,
                64,
                |tx| {
                    tx.execute("UPDATE counter SET value = 17", [])?;
                    Ok(HandlerOutcome::Success(Vec::new()))
                },
            )
            .await
            .unwrap();
        fixture
            .reader
            .refresh(&fixture.root.path().join("source-final-reader.sqlite"))
            .await
            .unwrap();
        assert_ne!(fixture.reader.lifecycle_observation().await.root(), opened);
        // Join the exact busy writer through its canonical drain, then restore
        // its published root on another fully managed native boot.
        fixture.handle.drain().await.unwrap();
        let authority = CellAuthority::new(fixture.layout.clone());
        let idle = authority
            .load(fixture.target.cell_id())
            .await
            .unwrap()
            .unwrap();
        let catalog = CellCatalog::new(fixture.layout.clone(), fixture.target.tenant())
            .lookup(fixture.target.cell_id())
            .await
            .unwrap()
            .unwrap();
        let replica = CellReplica::new(
            fixture.layout.clone(),
            *fixture.target.cell_id().as_bytes(),
            *fixture.description.incarnation.as_bytes(),
            Limits {
                max_database_bytes: 64 << 20,
                max_capture_bytes: 16 << 20,
                ..Limits::default()
            },
        )
        .unwrap();
        let successor = fixture.nodes[2]
            .runtime()
            .acquire_idle_restored(
                catalog.clone(),
                replica.clone(),
                authority.clone(),
                idle,
                fixture.root.path().join("source-successor.sqlite"),
                owner(2),
            )
            .await
            .unwrap();
        fixture.boots[2]
            .refresh_capacity(2, fixture.journal.as_ref(), deadline())
            .await
            .unwrap();
        let retirement = Arc::new(
            fixture.managers[1]
                .remove_enrolled(&fixture.original, deadline())
                .await
                .unwrap(),
        );
        assert_eq!(
            retirement.root(),
            fixture.reader.lifecycle_observation().await.root()
        );
        assert!(
            fixture
                .reader
                .lifecycle_observation()
                .await
                .locally_joined()
        );
        // Activation is authorized by the current writer, not the departed
        // source. Pin that actual successor boot's signing key in the same
        // authenticated dispatch path; existing donor fixtures retain origin 0.
        fixture.transport = Arc::new(transport::NativePeers::for_origin(
            &fixture.nodes,
            &fixture.managers,
            &fixture.layout,
            fixture.directory.clone(),
            2,
        ));
        fixture.peer = ReplicaPeerClient::new(
            fixture.nodes[2].application().registry(),
            Arc::new(PeerSigner::new(
                session(2),
                fixture.nodes[2].application().registry().release_digest(),
                SigningKey::from_bytes(&[3; 32]),
            )),
            PeerPrincipal {
                issuer: "managed-owner".into(),
                subject: "live-owner".into(),
                actions: vec!["replica-maintenance".into()],
            },
            fixture.transport.clone(),
        );
        // The same Active receiver can install a new request under the new
        // writer. Its new acceptance must never substitute the original join.
        fixture.boots[1]
            .refresh_capacity(1, fixture.journal.as_ref(), deadline())
            .await
            .unwrap();
        let ad = fixture
            .directory
            .load(session(1), clock().unwrap())
            .await
            .unwrap()
            .unwrap();
        fixture
            .peer
            .activate(
                &fixture.target,
                &fixture.directory,
                ad.advertisement().clone(),
                fixture.description,
            )
            .await
            .unwrap();
        let new_reader = fixture.managers[1]
            .resolve(fixture.target.clone())
            .await
            .unwrap();
        assert_eq!(new_reader.receipt().await, retirement.receipt());
        assert_ne!(
            fixture.managers[1]
                .enrollment_completion(fixture.target.cell_id())
                .await
                .unwrap()
                .unwrap()
                .spec,
            *retirement.original().spec()
        );
        assert_eq!(
            new_reader
                .query::<application::ReadValue>(Some(retirement.receipt()), 0)
                .await
                .unwrap()
                .output,
            17
        );
        drop(new_reader);
        let inputs = Arc::new(FleetSourceReaderInputs {
            retirement,
            node: node_id(2),
            host: fixture.nodes[2].clone(),
            catalog,
            authority,
            replica,
        });
        Self {
            fixture,
            inputs,
            successor,
        }
    }
}
