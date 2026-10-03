//! Independent external idempotency store. Its acknowledgment is not a Cell receipt.
use crate::http::{self, Backend, HttpResponse, Reply, RouteError};
use cellule_cookbook_support::LocalNode;
use cellule_cookbook_support::{now_ms, shutdown_signal};
use cellule_cookbook_support_desk::{
    Acknowledgement, BoxError, Notification, NotificationEndpoint, receiver_token,
};
use hyper::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io::{Read as _, Write as _},
    net::Ipv4Addr,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Mutex};
use tokio_util::sync::CancellationToken;

const RECORD_LIMIT: usize = 16 << 10;
const CAPACITY: usize = 4096;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Record {
    version: u8,
    pub(crate) notification: Notification,
    pub(crate) acknowledgement: Acknowledgement,
}
impl Record {
    fn new(notification: Notification) -> Result<Self, BoxError> {
        notification.validate()?;
        Ok(Self {
            version: 1,
            acknowledgement: Acknowledgement {
                key: notification.key_hex(),
                content_digest: notification.content_digest()?,
                applied_count: 1,
            },
            notification,
        })
    }
    fn validate(&self, key: &str) -> Result<(), BoxError> {
        let expected = Self::new(self.notification.clone())?;
        if *self != expected || key != self.acknowledgement.key {
            return Err("support receiver record binding differs".into());
        }
        Ok(())
    }
}
pub(crate) fn read(root: &Path, key: &str) -> Result<Option<Record>, BoxError> {
    crate::effect_hex(key)?;
    let file = match File::open(root.join(format!("{key}.json"))) {
        Ok(value) => value,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(source.into()),
    };
    if !file.metadata()?.is_file() {
        return Err("support receiver record must be a regular file".into());
    }
    let mut bytes = Vec::new();
    file.take(RECORD_LIMIT as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > RECORD_LIMIT {
        return Err("support receiver record exceeds 16 KiB".into());
    }
    let value: Record = serde_json::from_slice(&bytes)?;
    value.validate(key)?;
    Ok(Some(value))
}
#[derive(Debug, thiserror::Error)]
enum StoreError {
    #[error("notification key already binds different input")]
    Conflict,
    #[error("permanent support receiver capacity is full")]
    Capacity,
    #[error("new support notification has expired")]
    Expired,
}
struct Store {
    root: PathBuf,
    keys: BTreeSet<String>,
    // The kernel releases this exclusive directory ownership after process death.
    // A second receiver cannot bypass the local capacity/publication serialization.
    _ownership: File,
}
impl Store {
    fn open(root: PathBuf) -> Result<Self, BoxError> {
        std::fs::create_dir_all(&root)?;
        let ownership = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(".owner.lock"))?;
        ownership.try_lock()?;
        let mut keys = BTreeSet::new();
        for entry in std::fs::read_dir(&root)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| "non-UTF8 support receiver entry")?;
            if name == ".owner.lock" {
                continue;
            }
            if name.starts_with(".pending-") && entry.file_type()?.is_file() {
                // Exclusivity proves these are abandoned pre-publication files.
                std::fs::remove_file(entry.path())?;
                continue;
            }
            let key = name
                .strip_suffix(".json")
                .ok_or("unknown support receiver entry")?;
            if read(&root, key)?.is_none() || !keys.insert(key.to_owned()) || keys.len() > CAPACITY
            {
                return Err("invalid or oversized support receiver inventory".into());
            }
        }
        File::open(&root)?.sync_all()?;
        Ok(Self {
            root,
            keys,
            _ownership: ownership,
        })
    }
    fn apply(&mut self, notification: Notification) -> Result<(Record, bool), BoxError> {
        let expected = Record::new(notification)?;
        let key = &expected.acknowledgement.key;
        if let Some(record) = read(&self.root, key)? {
            return if record == expected {
                if !self.keys.contains(key) && self.keys.len() >= CAPACITY {
                    return Err(StoreError::Capacity.into());
                }
                self.keys.insert(key.clone());
                // A previous publication may have lost its final directory
                // sync. Confirm durability before returning a retry's proof.
                File::open(&self.root)?.sync_all()?;
                Ok((record, false))
            } else {
                Err(StoreError::Conflict.into())
            };
        }
        if self.keys.contains(key) {
            return Err("permanent support receiver record disappeared".into());
        }
        if self.keys.len() >= CAPACITY {
            return Err(StoreError::Capacity.into());
        }
        if expected.notification.expires_at_ms()? <= now_ms()? {
            return Err(StoreError::Expired.into());
        }
        let bytes = serde_json::to_vec(&expected)?;
        if bytes.len() > RECORD_LIMIT {
            return Err("support receiver publication exceeds 16 KiB".into());
        }
        let mut temporary = tempfile::Builder::new()
            .prefix(".pending-")
            .tempfile_in(&self.root)?;
        temporary.write_all(&bytes)?;
        temporary.as_file().sync_all()?;
        temporary.persist_noclobber(self.root.join(format!("{key}.json")))?;
        // A sync failure after publication is uncertain, but this permanent
        // record already occupies capacity. Never admit a replacement for it.
        self.keys.insert(key.clone());
        // Never acknowledge or observe the fault checkpoint before durable publication.
        File::open(&self.root)?.sync_all()?;
        Ok((expected, true))
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Controls {
    pub(crate) key: Option<String>,
    pub(crate) after_publication_ms: u64,
    pub(crate) drop_reply_once: bool,
}
impl Controls {
    pub(crate) fn validate(&self) -> Result<(), BoxError> {
        if self.after_publication_ms > 10000 {
            return Err("support receiver delay must be at most ten seconds".into());
        }
        if let Some(key) = &self.key {
            crate::effect_hex(key)?;
        }
        Ok(())
    }
}
struct Receiver {
    store: Arc<Mutex<Store>>,
    endpoint: NotificationEndpoint,
    credential: String,
    controls: Controls,
    selected: AtomicBool,
    cancel: CancellationToken,
}

/// Starts a separately persisted loopback receiver owned by the demo's source
/// node. Embeddings can instead configure an independent receiver process.
pub(crate) async fn install_owned(
    node: &LocalNode,
    root: PathBuf,
    port: u16,
    controls: Controls,
) -> Result<NotificationEndpoint, BoxError> {
    controls.validate()?;
    let credential = receiver_token()?;
    if credential.is_empty()
        || credential.len() > 256
        || !credential.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err("invalid configured support receiver credential".into());
    }
    let store = tokio::task::spawn_blocking(move || Store::open(root)).await??;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    let endpoint = NotificationEndpoint::new(format!("http://{address}/notifications"))?;
    let shared = Arc::new(Mutex::new(store));
    let server_endpoint = endpoint.clone();
    node.spawn_worker(move |cancel| async move {
        let backend = Arc::new(Receiver {
            store: shared,
            endpoint: server_endpoint,
            credential,
            controls,
            selected: AtomicBool::new(false),
            cancel: cancel.clone(),
        });
        http::serve(listener, backend, cancel)
            .await
            .map_err(|source| ServerError { source })
    })?;
    Ok(endpoint)
}

#[derive(Debug, thiserror::Error)]
#[error("owned support notification receiver failed")]
struct ServerError {
    #[source]
    source: BoxError,
}
enum Operation {
    Health,
    Publish(Notification),
}
impl Backend for Receiver {
    type Operation = Operation;
    fn ready(&self) -> bool {
        !self.cancel.is_cancelled()
    }
    fn credential(&self, _: &Method, _: &str) -> &str {
        &self.credential
    }
    fn parse(
        &self,
        method: &Method,
        path: &str,
        key: Option<&str>,
        body: &[u8],
    ) -> Result<Operation, RouteError> {
        if method == Method::GET && path == "/health" && body.is_empty() {
            return Ok(Operation::Health);
        }
        if method != Method::POST || path != "/notifications" {
            return Err(RouteError {
                status: StatusCode::NOT_FOUND,
                message: "unknown support receiver route",
                source: None,
            });
        }
        if body.len() > 8192 {
            return Err(RouteError {
                status: StatusCode::PAYLOAD_TOO_LARGE,
                message: "support notification exceeds 8192 bytes",
                source: None,
            });
        }
        let notification: Notification =
            serde_json::from_slice(body).map_err(|source| RouteError::bad(source.into()))?;
        notification
            .validate()
            .map_err(|source| RouteError::bad(source.into()))?;
        if key != Some(notification.key_hex().as_str()) || notification.endpoint != self.endpoint {
            return Err(RouteError {
                status: StatusCode::BAD_REQUEST,
                message: "support notification key or endpoint differs",
                source: None,
            });
        }
        Ok(Operation::Publish(notification))
    }
    async fn execute(&self, op: Operation) -> Result<Reply, BoxError> {
        match op {
            Operation::Health => Ok(http::json(&serde_json::json!({"ready":self.ready()}))?.into()),
            Operation::Publish(notification) => {
                let store = self.store.clone();
                let (record, created) =
                    tokio::task::spawn_blocking(move || store.blocking_lock().apply(notification))
                        .await??;
                let selected = created
                    && self
                        .controls
                        .key
                        .as_ref()
                        .is_none_or(|key| *key == record.acknowledgement.key)
                    && !self.selected.swap(true, Ordering::SeqCst);
                crate::emit(
                    &serde_json::json!({"event":"receiver_published","created":created,"selected":selected,"acknowledgement":record.acknowledgement}),
                )?;
                if selected {
                    tokio::select! { ()=tokio::time::sleep(Duration::from_millis(self.controls.after_publication_ms))=>{}, ()=self.cancel.cancelled()=>{} }
                }
                Ok(Reply {
                    response: http::json(&record.acknowledgement)?,
                    lose: selected && self.controls.drop_reply_once,
                })
            }
        }
    }
    fn failure(&self, source: &BoxError) -> HttpResponse {
        let status = match source.downcast_ref::<StoreError>() {
            Some(StoreError::Conflict) => StatusCode::CONFLICT,
            Some(StoreError::Capacity) => StatusCode::INSUFFICIENT_STORAGE,
            Some(StoreError::Expired) => StatusCode::GONE,
            None => StatusCode::INTERNAL_SERVER_ERROR,
        };
        http::error(
            status,
            "support receiver publication failed; retain original input",
        )
    }
}
pub(crate) async fn run(root: PathBuf, port: u16, controls: Controls) -> Result<(), BoxError> {
    controls.validate()?;
    let credential = receiver_token()?;
    if credential.is_empty()
        || credential.len() > 256
        || !credential.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err("invalid configured support receiver credential".into());
    }
    let store = tokio::task::spawn_blocking(move || Store::open(root)).await??;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).await?;
    let address = listener.local_addr()?;
    let cancel = CancellationToken::new();
    let backend = Arc::new(Receiver {
        store: Arc::new(Mutex::new(store)),
        endpoint: NotificationEndpoint::new(format!("http://{address}/notifications"))?,
        credential,
        controls,
        selected: AtomicBool::new(false),
        cancel: cancel.clone(),
    });
    crate::emit(
        &serde_json::json!({"event":"receiver_ready","address":address.to_string(),"endpoint":backend.endpoint}),
    )?;
    let future = http::serve(listener, backend, cancel.clone());
    tokio::pin!(future);
    let (result, drained) = tokio::select! {
        signal=shutdown_signal()=>{cancel.cancel(); (signal.map_err(BoxError::from), future.await)},
        result=&mut future=>(Ok(()),result),
    };
    if drained.is_ok() {
        crate::emit(&serde_json::json!({"event":"drained"}))?;
    }
    if let Err(source) = result {
        if let Err(cleanup) = drained {
            crate::files::log_source(cleanup.as_ref(), "support receiver drain also failed");
        }
        return Err(source);
    }
    drained
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use cellule_cookbook_support_desk::{Actor, Deadline, TicketKey};
    fn notification() -> Notification {
        let now = now_ms().expect("clock");
        Notification {
            deadline: Deadline {
                ticket: TicketKey::new("sample").expect("key"),
                generation: 1,
                due_at_ms: now - 1,
            },
            source_cell: [3; 32],
            agent: Some(Actor::new("agent").expect("agent")),
            endpoint: NotificationEndpoint::new("http://127.0.0.1:19123/notifications")
                .expect("endpoint"),
            escalated_at_ms: now,
        }
    }
    #[test]
    fn immutable_receiver_reopens_exact_record_and_refuses_rebinding_and_second_owner() {
        let root = tempfile::tempdir().expect("directory");
        let original = notification();
        let mut store = Store::open(root.path().into()).expect("open");
        assert!(Store::open(root.path().into()).is_err());
        let (record, created) = store.apply(original.clone()).expect("publish");
        assert!(created);
        assert_eq!(record.acknowledgement.applied_count, 1);
        assert!(!store.apply(original.clone()).expect("retry").1);
        let mut changed = original.clone();
        changed.agent = None;
        assert_eq!(original.key(), changed.key());
        assert!(
            store
                .apply(changed)
                .expect_err("conflict")
                .downcast_ref::<StoreError>()
                .is_some_and(|e| matches!(e, StoreError::Conflict))
        );
        drop(store);
        let mut reopened = Store::open(root.path().into()).expect("reopen");
        assert_eq!(
            reopened.apply(original).expect("cold retry"),
            (record, false)
        );
    }

    #[tokio::test]
    async fn real_http_reply_loss_and_drain_preserve_one_external_application() {
        let root = tempfile::tempdir().expect("receiver root");
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let endpoint =
            NotificationEndpoint::new(format!("http://{address}/notifications")).expect("endpoint");
        let mut notification = notification();
        notification.endpoint = endpoint.clone();
        let key = notification.key_hex();
        let cancel = CancellationToken::new();
        let backend = Arc::new(Receiver {
            store: Arc::new(Mutex::new(Store::open(root.path().into()).expect("store"))),
            endpoint,
            credential: "test-support-receiver".into(),
            controls: Controls {
                key: Some(key.clone()),
                after_publication_ms: 10000,
                drop_reply_once: true,
            },
            selected: AtomicBool::new(false),
            cancel: cancel.clone(),
        });
        let server_cancel = cancel.clone();
        let server =
            tokio::spawn(async move { http::serve(listener, backend, server_cancel).await });
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("client");
        let unauthorized = client
            .post(notification.endpoint.as_str())
            .bearer_auth("wrong")
            .header("Idempotency-Key", &key)
            .json(&notification)
            .send()
            .await
            .expect("auth response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert!(
            read(root.path(), &key)
                .expect("pre-admission read")
                .is_none()
        );
        let first_client = client.clone();
        let first_notification = notification.clone();
        let first_key = key.clone();
        let first = tokio::spawn(async move {
            first_client
                .post(first_notification.endpoint.as_str())
                .bearer_auth("test-support-receiver")
                .header("Idempotency-Key", first_key)
                .json(&first_notification)
                .send()
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while read(root.path(), &key).expect("publication read").is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("actual external publication");
        // Cancellation skips only the diagnostic pause. The accepted request
        // still finishes the reply-loss path and the HTTP owner drains it.
        cancel.cancel();
        assert!(first.await.expect("first join").is_err());
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("drain deadline")
            .expect("server join")
            .expect("server drain");
        let record = read(root.path(), &key)
            .expect("cold external read")
            .expect("record");
        assert_eq!(record.notification, notification);
        assert_eq!(record.acknowledgement.applied_count, 1);
        let listener = TcpListener::bind(address)
            .await
            .expect("restarted listener");
        let cancel = CancellationToken::new();
        let backend = Arc::new(Receiver {
            store: Arc::new(Mutex::new(
                Store::open(root.path().into()).expect("cold store"),
            )),
            endpoint: notification.endpoint.clone(),
            credential: "test-support-receiver".into(),
            controls: Controls::default(),
            selected: AtomicBool::new(false),
            cancel: cancel.clone(),
        });
        let server_cancel = cancel.clone();
        let server =
            tokio::spawn(async move { http::serve(listener, backend, server_cancel).await });
        let response = client
            .post(notification.endpoint.as_str())
            .bearer_auth("test-support-receiver")
            .header("Idempotency-Key", &key)
            .json(&notification)
            .send()
            .await
            .expect("retry response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.json::<Acknowledgement>().await.expect("ack"),
            record.acknowledgement
        );
        let mut changed = notification;
        changed.agent = None;
        assert_eq!(
            client
                .post(changed.endpoint.as_str())
                .bearer_auth("test-support-receiver")
                .header("Idempotency-Key", &key)
                .json(&changed)
                .send()
                .await
                .expect("conflict response")
                .status(),
            StatusCode::CONFLICT
        );
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("second drain")
            .expect("server join")
            .expect("server drain");
        assert_eq!(read(root.path(), &key).expect("record read"), Some(record));
    }
}
