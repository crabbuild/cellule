//! Public application factories bound to one host runtime and storage identity.

use super::*;
use cellule_app::{ApplicationBuilder, CellApplication};
use cellule_runtime::{CellClient, NamespaceId, TenantId, ltx::CellStorageLayout};
use cellule_store::Store;
use object_store::{memory::InMemory, path::Path};

struct App;
impl CellApplication for App {
    const NAME: &'static str = "host-test";
    fn register(_: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        Ok(())
    }
}

struct OtherApp;
impl CellApplication for OtherApp {
    const NAME: &'static str = "other-host";
    fn register(_: &mut ApplicationBuilder) -> cellule_runtime::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn local_factory_uses_the_host_artifact_and_layout_application_identity() {
    let node = CellNodeBuilder::new(application())
        .with_runtime(SqlWorkerPool::new(1, 4).unwrap(), 16 << 20)
        .with_replica_host(ReplicaHost::default())
        .with_session(SessionId::from_bytes([11; 16]))
        .build_unleased_for_maintenance()
        .unwrap();
    let application = ApplicationId::from_bytes([12; 16]);
    let layout = CellStorageLayout::new(
        Store::new(Arc::new(InMemory::new())),
        Path::from("host-binding"),
        *application.as_bytes(),
    );
    let binding = node.bind_local_application::<App>(layout.clone()).unwrap();
    assert!(std::ptr::eq(binding.compiled(), node.application()));
    assert_eq!(binding.application_id(), application);
    let target = binding
        .scope(TenantId::from_bytes([13; 16]))
        .target_for_scope(NamespaceId::from_bytes([2; 16]), b"orders")
        .unwrap();
    assert_eq!(target.application(), application);
    assert!(matches!(
        node.bind_local_application::<OtherApp>(layout.clone()),
        Err(Error::Registry(_))
    ));

    let mut registry = RegistryBuilder::new(BuildDescriptor {
        source_revision: "other-release".into(),
        cargo_lock_digest: Digest::from_bytes([7; 32]),
    });
    registry.register(Module).unwrap();
    let client =
        CellClient::local_runtime(Arc::new(registry.finish().unwrap()), node.runtime(), layout);
    assert!(matches!(
        node.bind_application::<App>(client, application),
        Err(Error::Registry(_))
    ));
    node.shutdown().await.unwrap();
}
