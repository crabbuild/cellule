//! One epoch-owned activation callback, independent of proof waiters.
use std::sync::{Arc, Mutex};

use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};

use super::NodeLogAuthority;
use crate::node::lease::NodeLeaseGuard;
use crate::node::log::DurabilityGate;
use crate::{Error, Result};

type Attempt = std::result::Result<(), Arc<Error>>;
type Completion = Shared<BoxFuture<'static, std::result::Result<Attempt, Arc<Error>>>>;

#[derive(Default)]
pub(super) struct Activation {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    completion: Option<Completion>,
    joining: bool,
}

impl Activation {
    pub(super) async fn wait(
        &self,
        epoch: u64,
        gate: &DurabilityGate,
        authority: &Arc<dyn NodeLogAuthority>,
        lease: &NodeLeaseGuard,
    ) -> Result<()> {
        let completion = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Node("node activation lock poisoned"))?;
            if !state.joining
                && state
                    .completion
                    .as_ref()
                    .is_some_and(|completion| matches!(completion.peek(), Some(Ok(Err(_)))))
            {
                // A completed failed attempt can be retried, but never overlap
                // another callback or restart after shutdown begins joining.
                state.completion = None;
            }
            if let Some(completion) = &state.completion {
                completion.clone()
            } else {
                if state.joining {
                    return Err(Error::RuntimeClosed);
                }
                let runtime = tokio::runtime::Handle::try_current().map_err(Error::RuntimeStart)?;
                let gate = gate.clone();
                let authority = Arc::clone(authority);
                let lease = lease.clone();
                // Dropping a proof waiter (including an Object proof winning)
                // cannot drop an accepted CAS before the adapter joins its reply
                // and updates the version used by later authority mutations.
                let worker = runtime.spawn(async move {
                    lease.check()?;
                    tokio::select! {
                        result = authority.activate(epoch) => result?,
                        () = lease.wait_fenced() => return Err(Error::Fenced),
                    }
                    lease.check()?;
                    gate.activate_fleet()
                });
                let completion = async move {
                    worker
                        .await
                        .map(|result| result.map_err(Arc::new))
                        .map_err(|error| Arc::new(Error::FollowerWorkerJoin(error)))
                }
                .boxed()
                .shared();
                state.completion = Some(completion.clone());
                completion
            }
        };
        completion
            .await
            .map_err(Error::Shared)?
            .map_err(Error::Shared)
    }

    pub(super) async fn join(&self) -> Result<()> {
        let completion = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| Error::Node("node activation lock poisoned"))?;
            state.joining = true;
            state.completion.clone()
        };
        if let Some(completion) = completion {
            // Callback failure grants no Fleet proof. Shutdown may still close
            // through complete Object coverage, which it checks separately.
            // Worker failure remains a source error: its callback did not join.
            let _attempt = completion.await.map_err(Error::Shared)?;
        }
        Ok(())
    }
}
