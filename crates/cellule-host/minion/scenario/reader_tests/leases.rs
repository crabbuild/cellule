//! Caller-driven fixture heartbeats retain exact boot identity and real expiry.
use super::*;
use cellule_runtime::node::{NodeAdvertisement, NodeCapacity, NodeFailureDomain};
use ed25519_dalek::SigningKey;

pub(super) struct ReaderLeases {
    directory: NodeDirectory,
    code: Digest,
    release: Digest,
    receiver: NodeLeaseGuard,
}

fn advertisement(
    index: usize,
    code: Digest,
    release: Digest,
    now: i64,
    lifetime_ms: i64,
) -> NodeAdvertisement {
    NodeAdvertisement::sign(
        node_id(index),
        session(index),
        owner(index).endpoint,
        scope().fleet,
        Digest::from_bytes([30; 32]),
        Digest::from_bytes([31; 32]),
        release,
        &SigningKey::from_bytes(&[index as u8 + 1; 32]),
        1,
        now,
        now + lifetime_ms,
        vec![code],
        vec![1],
        NodeFailureDomain::default(),
        NodeCapacity {
            free_memory_bytes: 1 << 30,
            free_disk_bytes: 1 << 30,
            job_credits: 4,
            log_protocol: cellule_runtime::node::NODE_LOG_PROTOCOL_VERSION,
            ..NodeCapacity::default()
        },
    )
    .unwrap()
}

impl ReaderLeases {
    pub(super) async fn create(
        directory: NodeDirectory,
        code: Digest,
        release: Digest,
        lifetime_ms: i64,
    ) -> Self {
        let now = clock().unwrap();
        for index in [0, 1] {
            directory
                .create(advertisement(index, code, release, now, lifetime_ms), now)
                .await
                .unwrap();
        }
        Self {
            directory,
            code,
            release,
            receiver: NodeLeaseGuard::new(clock().unwrap(), now + lifetime_ms).unwrap(),
        }
    }

    pub(super) fn receiver_guard(&self) -> &NodeLeaseGuard {
        &self.receiver
    }

    pub(super) async fn renew(&self) {
        for index in [0, 1] {
            let now = clock().unwrap();
            // Load live evidence before refresh. Expiry or withdrawal cannot be
            // repaired by recreating an advertisement for this same boot.
            let observed = self
                .directory
                .load(session(index), now)
                .await
                .unwrap()
                .unwrap();
            if observed.advertisement().expires_at_ms() - now > 15_000 {
                continue;
            }
            let next = advertisement(index, self.code, self.release, now, 30_000);
            let refreshed = self.directory.refresh(&observed, next, now).await.unwrap();
            if index == 1 {
                // Local credit advances only after the authoritative CAS reply.
                self.receiver
                    .renew(clock().unwrap(), refreshed.advertisement().expires_at_ms())
                    .unwrap();
            }
        }
    }

    pub(super) fn fence(&self) {
        self.receiver.fence();
    }
}

#[tokio::test]
async fn reader_fixture_renews_signed_boots_and_guard_past_original_expiry() {
    let app = application::compile().unwrap();
    let code = app.registry().module_digests()[0];
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        ObjectPath::from("reader-heartbeat"),
        [3; 16],
    );
    let directory = NodeDirectory::new(
        layout,
        scope().fleet,
        Digest::from_bytes([31; 32]),
        app.registry().release_digest(),
    );
    let leases = ReaderLeases::create(
        directory.clone(),
        code,
        app.registry().release_digest(),
        2_000,
    )
    .await;
    let original = directory
        .load(session(1), clock().unwrap())
        .await
        .unwrap()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    leases.renew().await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while clock().unwrap() < original.advertisement().expires_at_ms() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    for index in [0, 1] {
        let current = directory
            .load(session(index), clock().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.advertisement().node(), node_id(index));
        assert_eq!(current.advertisement().session(), session(index));
        assert_eq!(current.advertisement().generation(), 2);
    }
    leases.receiver_guard().check().unwrap();
    leases.fence();
    assert!(matches!(
        leases.receiver_guard().check(),
        Err(Error::Fenced)
    ));
}
