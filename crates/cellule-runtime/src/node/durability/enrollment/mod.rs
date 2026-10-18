//! Bounded, epoch-owned enrollment cohorts; no timer on the first cold Cell.
use super::NodeBundleAuthority;
use crate::control::authority::{CellAuthority, VersionedControl};
use crate::node::bundle::MAX_BUNDLE_ENROLLMENTS;
use crate::node::lease::NodeLeaseGuard;
use crate::{Error, Result};
use futures_util::{
    FutureExt,
    future::{BoxFuture, Shared},
};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};

type Completion = Shared<BoxFuture<'static, std::result::Result<(), Arc<Error>>>>;

#[derive(Default)]
pub(super) struct Enrollment {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    sender: Option<mpsc::Sender<Request>>,
    completion: Option<Completion>,
    joining: bool,
}

struct Request {
    authority: CellAuthority,
    observed: VersionedControl,
    reply: oneshot::Sender<Result<VersionedControl>>,
}

impl Enrollment {
    pub(super) async fn bind(
        &self,
        authority: &Arc<dyn NodeBundleAuthority>,
        lease: &NodeLeaseGuard,
        resources: Option<&crate::fleet::resource::ResourceLedger>,
        cell_authority: &CellAuthority,
        observed: &VersionedControl,
    ) -> Result<VersionedControl> {
        lease.check()?;
        let sender = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Node("bundle enrollment lock poisoned"))?;
            if state.joining {
                return Err(Error::RuntimeClosed);
            }
            if state.sender.is_none() {
                let runtime = tokio::runtime::Handle::try_current().map_err(Error::RuntimeStart)?;
                // Two bounded cohorts (running and queued), including owned
                // decoded metadata and bookkeeping. Standalone manual users
                // without an attached runtime ledger own their host admission.
                let working = resources
                    .map(|resources| {
                        resources.try_reserve_in(
                            crate::fleet::resource::RetentionStage::Publisher,
                            crate::fleet::resource::ResourceCost::zero().with_retained_bytes(
                                2 * MAX_BUNDLE_ENROLLMENTS * crate::control::MAX_CONTROL_BYTES * 4
                                    + (64 << 10),
                            ),
                        )
                    })
                    .transpose()?;
                let (sender, receiver) = mpsc::channel(MAX_BUNDLE_ENROLLMENTS);
                let authority = authority.clone();
                let lease = lease.clone();
                let worker = runtime.spawn(async move {
                    let _working = working;
                    run(authority, lease, receiver).await;
                });
                let completion = async move {
                    worker
                        .await
                        .map_err(|source| Arc::new(Error::FollowerWorkerJoin(source)))
                }
                .boxed()
                .shared();
                state.sender = Some(sender);
                state.completion = Some(completion);
            }
            state.sender.as_ref().cloned().ok_or(Error::RuntimeClosed)?
        };
        // Reserve before cloning bounded control metadata. A waiting caller
        // retains its original activation reservation, not another queued copy.
        let permit = tokio::select! {
            permit = sender.reserve() => permit.map_err(|_| Error::RuntimeClosed)?,
            () = lease.wait_fenced() => return Err(Error::Fenced),
        };
        lease.check()?;
        let (reply, response) = oneshot::channel();
        permit.send(Request {
            authority: cell_authority.clone(),
            observed: observed.clone(),
            reply,
        });
        drop(sender);
        // Once accepted, a caller disconnect cannot abandon catalog obligations
        // or drop a sibling's CAS. Epoch shutdown joins the retained worker.
        response.await.map_err(|_| Error::RuntimeClosed)?
    }

    pub(super) async fn join(&self) -> Result<()> {
        let completion = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Node("bundle enrollment lock poisoned"))?;
            state.joining = true;
            state.sender.take();
            state.completion.clone()
        };
        if let Some(completion) = completion {
            completion.await.map_err(Error::Shared)?;
        }
        Ok(())
    }
}

async fn run(
    authority: Arc<dyn NodeBundleAuthority>,
    lease: NodeLeaseGuard,
    mut receiver: mpsc::Receiver<Request>,
) {
    while let Some(first) = receiver.recv().await {
        let mut requests = Vec::with_capacity(MAX_BUNDLE_ENROLLMENTS);
        requests.push(first);
        // One scheduler yield coalesces ready activations without a batching
        // timer or a requirement to wait for every Cell on the node.
        tokio::task::yield_now().await;
        while requests.len() < MAX_BUNDLE_ENROLLMENTS {
            match receiver.try_recv() {
                Ok(request) => requests.push(request),
                Err(_) => break,
            }
        }
        let cells = requests
            .iter()
            .map(|request| (&request.authority, &request.observed))
            .collect::<Vec<_>>();
        if lease.check().is_err() {
            for request in requests {
                let _ = request.reply.send(Err(Error::Fenced));
            }
            continue;
        }
        let Some(results) = authority.bind_batch(&cells).await else {
            // Legacy adapters release each response after its own callback,
            // without imposing a cohort barrier on independent successes.
            let pending = requests
                .into_iter()
                .map(|request| {
                    let authority = authority.clone();
                    let lease = lease.clone();
                    async move {
                        let result = match lease.check() {
                            Ok(()) => authority.bind(&request.authority, &request.observed).await,
                            Err(_) => Err(Error::Fenced),
                        };
                        let _ = request.reply.send(result);
                    }
                })
                .collect::<Vec<_>>();
            futures_util::future::join_all(pending).await;
            continue;
        };
        if results.len() != requests.len() {
            for request in requests {
                let _ = request
                    .reply
                    .send(Err(Error::Node("bundle enrollment result count differs")));
            }
        } else {
            for (request, result) in requests.into_iter().zip(results) {
                let _ = request.reply.send(result);
            }
        }
    }
}

#[cfg(test)]
mod tests;
