//! Optional three-process, real-directory follower durability benchmark wiring.
use super::sql_metrics::{PeerPhase, QueryMetrics};
use cellule_peer_http::LoadedPeerTls;
use cellule_runtime::identity::NodeId;
use cellule_runtime::node::NodeDirectory;
use cellule_runtime::{
    ApplicationId, CellRuntime, Digest, Error, NodeLeaseGuard, Result, SessionId,
};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

mod authority;
mod server;
#[cfg(test)]
mod tests;
mod transport;
mod wire;

pub(super) struct Config {
    root: PathBuf,
    pub index: u8,
}

impl Config {
    pub fn from_env() -> Result<Option<Self>> {
        let Some(root) = std::env::var_os("CELLULE_AXUM_FLEET_DIR") else {
            if std::env::var_os("CELLULE_AXUM_FOLLOWER").is_some() {
                return Err(Error::Node("follower requires CELLULE_AXUM_FLEET_DIR"));
            }
            return Ok(None);
        };
        if std::env::var_os("CELLULE_TEST_ENDPOINT").is_none_or(|endpoint| endpoint.is_empty()) {
            return Err(Error::Node(
                "three-process follower fixture requires real S3",
            ));
        }
        let index = match std::env::var("CELLULE_AXUM_FOLLOWER") {
            Ok(value) => match value.as_str() {
                "1" => 1,
                "2" => 2,
                _ => return Err(Error::Node("CELLULE_AXUM_FOLLOWER must be 1 or 2")),
            },
            Err(std::env::VarError::NotPresent) => 0,
            Err(_) => return Err(Error::Node("invalid capacity follower role")),
        };
        Ok(Some(Self {
            root: root.into(),
            index,
        }))
    }

    fn tls(&self) -> Result<Arc<LoadedPeerTls>> {
        LoadedPeerTls::load(
            &self.root.join(format!("node-{}.crt", self.index)),
            &self.root.join(format!("node-{}.key", self.index)),
            &self.root.join("ca.crt"),
            "localhost",
        )
        .map(Arc::new)
        .map_err(transport_error)
    }

    pub async fn serve_follower(
        &self,
        layout: cellule_ltx::CellStorageLayout,
        code: Digest,
        bind: std::net::SocketAddr,
        metrics: Arc<QueryMetrics>,
    ) -> Result<()> {
        server::serve(self, layout, code, bind, metrics).await
    }

    pub async fn start_owner(
        &self,
        layout: cellule_ltx::CellStorageLayout,
        code: Digest,
        session: SessionId,
        application: ApplicationId,
        runtime: &CellRuntime,
        metrics: Arc<QueryMetrics>,
    ) -> Result<Owner> {
        let tls = self.tls()?;
        let directory = NodeDirectory::new(layout, tls.fleet(), code, code);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(transport_error)?;
        let endpoint = format!(
            "https://{}",
            listener.local_addr().map_err(transport_error)?
        );
        let enrollment = authority::Enrollment::start(
            directory.clone(),
            tls.clone(),
            0,
            session,
            endpoint.clone(),
            code,
            None,
        )
        .await?;
        let (stop, stopping) = tokio::sync::oneshot::channel();
        let router = axum::Router::new().route(
            "/internal/capacity/status",
            axum::routing::get(|| async { "capacity leader" }),
        );
        let management_tls = tls.clone();
        let management = tokio::spawn(async move {
            axum::serve(
                management_tls.listener(listener),
                router.into_make_service_with_connect_info::<cellule_peer_http::PeerTlsIdentity>(),
            )
            .with_graceful_shutdown(async move {
                let _ = stopping.await;
            })
            .await
            .map_err(transport_error)
        });
        let result = async {
            runtime.install_node_lease(enrollment.authority.lease.clone())?;
            let peers = enrollment.authority.recruit().await?;
            let members = peers.iter().map(|peer| peer.node()).collect();
            let transport = Arc::new(transport::Transport::new(
                directory, session, tls, peers, metrics,
            )?);
            let config = cellule_runtime::node::durability::NodeDurabilityConfig::new(
                session,
                node(0),
                1,
                members,
                transport,
                enrollment.authority.clone(),
                enrollment.authority.lease.clone(),
                cellule_ltx::Limits::default(),
                runtime.telemetry_handle(),
            )?;
            runtime.install_node_durability(application, config.build()?)?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            if let Err(cleanup) = enrollment.stop().await {
                eprintln!("Capacity enrollment cleanup failed: {cleanup:?}");
            }
            let _ = stop.send(());
            let _ = management.await;
            return Err(error);
        }
        println!("Follower durability: enrolled two original boots over pinned mTLS");
        Ok(Owner {
            enrollment,
            endpoint,
            stop,
            management,
        })
    }
}

pub(super) struct Owner {
    enrollment: authority::Enrollment,
    pub endpoint: String,
    stop: tokio::sync::oneshot::Sender<()>,
    management: tokio::task::JoinHandle<Result<()>>,
}

impl Owner {
    // Call after the runtime drained publications and closed its exact node log.
    pub async fn stop(self) -> Result<()> {
        let _ = self.stop.send(());
        let joined = self.management.await.map_err(Error::FollowerWorkerJoin);
        let enrollment = self.enrollment.stop().await;
        joined.and_then(|result| result).and(enrollment)
    }
}

fn node(index: u8) -> NodeId {
    // Fixture physical identities stay stable across boots. Each numbered peer
    // owns a separate persistent data directory; sessions are always fresh UUIDs.
    NodeId::from_bytes([index + 1; 16])
}

fn clock() -> Result<i64> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(transport_error)?;
    i64::try_from(elapsed.as_millis()).map_err(transport_error)
}

fn transport_error(source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::PeerTransportUnknown {
        context: "capacity follower fixture",
        source: Box::new(source),
    }
}
