//! Test-owned TCP partition of one child's real RustFS transport.
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

pub(super) struct PartitionProxy {
    pub(super) endpoint: String,
    mode: watch::Sender<u8>,
    rejected: Arc<AtomicUsize>,
    joined: Option<JoinHandle<()>>,
}
impl PartitionProxy {
    pub(super) async fn start() -> Self {
        let endpoint = std::env::var("CELLULE_TEST_ENDPOINT").unwrap();
        // This isolated qualification uses plain HTTP to local RustFS. The
        // proxy forwards the original signed bytes, including the Host header.
        let destination = endpoint
            .strip_prefix("http://")
            .unwrap()
            .trim_end_matches('/')
            .to_owned();
        assert!(!destination.contains('/'));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (mode, mut changed) = watch::channel(0_u8);
        let rejected = Arc::new(AtomicUsize::new(0));
        let counted = rejected.clone();
        let joined = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    biased;
                    _ = changed.changed() => {
                        if *changed.borrow() == 2 { break; }
                    }
                    Some(result) = connections.join_next(), if !connections.is_empty() => { result.unwrap(); }
                    incoming = listener.accept() => {
                        let (mut downstream, _) = incoming.unwrap();
                        if *changed.borrow() != 0 {
                            counted.fetch_add(1, Ordering::SeqCst);
                            continue;
                        }
                        let mut cut = changed.clone();
                        let destination = destination.clone();
                        let counted = counted.clone();
                        connections.spawn(async move {
                            let mut upstream = TcpStream::connect(destination).await.unwrap();
                            tokio::select! {
                                biased;
                                _ = cut.changed() => { counted.fetch_add(1, Ordering::SeqCst); }
                                _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream) => {}
                            }
                        });
                    }
                }
            }
            connections.abort_all();
            while let Some(result) = connections.join_next().await {
                if let Err(error) = result {
                    assert!(error.is_cancelled());
                }
            }
        });
        Self {
            endpoint,
            mode,
            rejected,
            joined: Some(joined),
        }
    }
    pub(super) fn partition(&self) {
        self.mode.send(1).unwrap();
    }
    pub(super) fn rejected(&self) -> usize {
        self.rejected.load(Ordering::SeqCst)
    }
    pub(super) async fn shutdown(mut self) {
        self.mode.send(2).unwrap();
        self.joined.take().unwrap().await.unwrap();
    }
}
impl Drop for PartitionProxy {
    fn drop(&mut self) {
        if let Some(joined) = &self.joined {
            joined.abort();
        }
    }
}
