//! Bounded accepted-job ownership, retained joins and original task failures.

use super::*;
use tokio::sync::watch;

impl ReaderJob {
    pub(super) async fn join(&mut self) -> Result<()> {
        if let Some(task) = self.task.as_mut() {
            let result = task.await;
            self.task = None;
            if let Err(source) = result {
                self.failure = Some(Arc::new(Error::Facility {
                    name: "fleet-reader-task",
                    source: Box::new(source),
                }));
            }
        }
        match &self.failure {
            Some(error) => Err(retained(Arc::clone(error))),
            None => Ok(()),
        }
    }
}

impl ReaderEnrollment {
    /// Own the entire finite protocol before the first journal call. Reaping
    /// finished tasks never drops a still-running accepted opening.
    pub(in crate::read_replicas) async fn activate(
        self: &Arc<Self>,
        manager: ReadReplicaManager,
        request: ActivationRequest,
    ) -> Result<Receipt> {
        self.reap().await?;
        let mut response = {
            let mut jobs = self
                .jobs
                .lock()
                .map_err(|_| Error::Control("reader job bank poisoned"))?;
            if jobs.draining {
                return Err(Error::RuntimeClosed);
            }
            if jobs.jobs.len() >= MAX_JOBS {
                return Err(Error::Capacity("reader enrollment job bound"));
            }
            let reservation = manager
                .runtime
                .try_reserve_node_bytes(MAX_RECORD_BYTES as usize)?;
            let (sender, response) = watch::channel(None);
            let task = tokio::spawn(async move {
                let _reservation = reservation;
                let result = match request {
                    ActivationRequest::Hint(target, origin) => {
                        manager.activate_open(target, origin).await
                    }
                    ActivationRequest::Source(source) => manager.activate_initial(*source).await,
                }
                .map_err(Arc::new);
                let _ = sender.send(Some(result));
            });
            jobs.jobs.push(Arc::new(Mutex::new(ReaderJob {
                task: Some(task),
                failure: None,
                response: response.clone(),
            })));
            response
        };
        loop {
            if let Some(result) = response.borrow().clone() {
                return result.map_err(retained);
            }
            response.changed().await.map_err(|source| Error::Facility {
                name: "fleet-reader-completion",
                source: Box::new(source),
            })?;
        }
    }

    async fn reap(&self) -> Result<()> {
        let jobs = self
            .jobs
            .lock()
            .map_err(|_| Error::Control("reader job bank poisoned"))?
            .jobs
            .clone();
        for job in jobs {
            let Ok(mut state) = job.try_lock() else {
                continue;
            };
            if state.task.as_ref().is_some_and(|task| !task.is_finished()) {
                continue;
            }
            let joined = state.join().await;
            let mut bank = self
                .jobs
                .lock()
                .map_err(|_| Error::Control("reader job bank poisoned"))?;
            if let Err(error) = joined {
                bank.failure.get_or_insert(Arc::new(error));
            }
            bank.jobs.retain(|retained| !Arc::ptr_eq(retained, &job));
        }
        match &self
            .jobs
            .lock()
            .map_err(|_| Error::Control("reader job bank poisoned"))?
            .failure
        {
            Some(error) => Err(retained(Arc::clone(error))),
            None => Ok(()),
        }
    }

    pub(in crate::read_replicas) fn close_admission(&self) -> Result<()> {
        self.jobs
            .lock()
            .map_err(|_| Error::Control("reader job bank poisoned"))?
            .draining = true;
        Ok(())
    }

    // Await the handle in place. Cancellation drops only its lock guard, so
    // the next shutdown waiter joins the same accepted work and original error.
    pub(in crate::read_replicas) async fn join(&self) -> Result<()> {
        let jobs = self
            .jobs
            .lock()
            .map_err(|_| Error::Control("reader job bank poisoned"))?
            .jobs
            .clone();
        for job in jobs {
            if let Err(error) = job.lock().await.join().await {
                self.jobs
                    .lock()
                    .map_err(|_| Error::Control("reader job bank poisoned"))?
                    .failure
                    .get_or_insert(Arc::new(error));
            }
        }
        self.reap().await
    }
}
