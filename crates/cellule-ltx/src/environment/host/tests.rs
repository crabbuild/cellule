use super::*;
use crate::environment::executor::Worker;

#[derive(Default)]
struct HeldExecutor {
    jobs: Mutex<Vec<Box<dyn FnOnce() + Send>>>,
    started: tokio::sync::Notify,
}

impl Executor for HeldExecutor {
    fn dispatch(&self, job: Box<dyn FnOnce() + Send>) -> io::Result<()> {
        self.jobs.lock().unwrap().push(job);
        self.started.notify_one();
        Ok(())
    }

    fn start_worker(&self, job: Box<dyn FnOnce() + Send>) -> io::Result<Box<dyn Worker>> {
        TokioExecutor.start_worker(job)
    }
}

struct PreparationResource(Arc<AtomicBool>);

impl HostResourcePermit for PreparationResource {}

impl Drop for PreparationResource {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn preparation_resources_survive_cancelled_native_waiters_and_release_on_completion() {
    for mode in 0..3 {
        let executor = Arc::new(HeldExecutor::default());
        let jobs = Arc::new(tokio::sync::Semaphore::new(1));
        let released = Arc::new(AtomicBool::new(false));
        let host = Host::default()
            .with_executor(executor.clone())
            .with_job_slots(jobs.clone())
            .with_preparation_resource(Arc::new(PreparationResource(released.clone())));
        let mut work = Box::pin(async move {
            host.run(move || {
                assert_ne!(mode, 2, "injected native failure");
                7
            })
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                result = &mut work => panic!("held original job completed: {result:?}"),
                _ = executor.started.notified() => {}
            }
        })
        .await
        .unwrap();
        drop(work);
        assert!(!released.load(Ordering::SeqCst));
        assert_eq!(jobs.available_permits(), 0);
        let job = executor.jobs.lock().unwrap().pop().unwrap();
        if mode == 1 {
            drop(job);
        } else {
            job();
        }
        assert!(released.load(Ordering::SeqCst));
        assert_eq!(jobs.available_permits(), 1);
    }
}
