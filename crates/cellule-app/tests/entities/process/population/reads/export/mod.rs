//! One owned writer; at most one queued request and one executing flush.

use super::*;
use std::thread::JoinHandle;
use tokio::sync::{mpsc, oneshot};

const QUEUE_CAPACITY: usize = 1;
const STACK_BYTES: usize = 512 * 1024;

enum Request {
    Flush {
        window: usize,
        ordinal: usize,
        at: Instant,
    },
    End {
        window: usize,
        at: Instant,
        reply: oneshot::Sender<usize>,
    },
}

struct Timing {
    window: usize,
    ordinal: usize,
    terminal: bool,
    queue_us: u128,
    flush_us: u128,
}

pub(super) struct Writer<T = observation::NodeObservations> {
    sender: Option<mpsc::Sender<Request>>,
    thread: Option<JoinHandle<T>>,
}

impl Writer {
    pub(super) fn new(
        sync: &Path,
        owner: observation::NodeObservations,
        storage: Arc<observation::StorageCounters>,
        telemetry: Arc<DurabilityRecorder>,
    ) -> Self {
        let mut timings = BufWriter::new(File::create(sync.join("read-export.tsv")).unwrap());
        writeln!(timings, "window\tordinal\tterminal\tqueue_us\tflush_us").unwrap();
        timings.flush().unwrap();
        Self::start(
            owner,
            move |owner| owner.flush_samples(&storage, &telemetry),
            move |timing| {
                writeln!(
                    timings,
                    "{}\t{}\t{}\t{}\t{}",
                    timing.window,
                    timing.ordinal,
                    timing.terminal,
                    timing.queue_us,
                    timing.flush_us
                )
                .unwrap();
                timings.flush().unwrap();
            },
        )
    }
}

impl<T: Send + 'static> Writer<T> {
    fn start(
        mut owner: T,
        mut flush: impl FnMut(&mut T) + Send + 'static,
        mut observe: impl FnMut(Timing) + Send + 'static,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel(QUEUE_CAPACITY);
        let thread = std::thread::Builder::new()
            .name("cellule-read-evidence".into())
            .stack_size(STACK_BYTES)
            .spawn(move || {
                let mut completed = 0;
                while let Some(request) = receiver.blocking_recv() {
                    let (window, ordinal, at, reply) = match request {
                        Request::Flush {
                            window,
                            ordinal,
                            at,
                        } => (window, ordinal, at, None),
                        Request::End { window, at, reply } => (window, 0, at, Some(reply)),
                    };
                    let started = Instant::now();
                    let queue_us = started.duration_since(at).as_micros();
                    flush(&mut owner);
                    observe(Timing {
                        window,
                        ordinal,
                        terminal: reply.is_some(),
                        queue_us,
                        flush_us: started.elapsed().as_micros(),
                    });
                    if let Some(reply) = reply {
                        let _ = reply.send(completed);
                        completed = 0;
                    } else {
                        completed += 1;
                    }
                }
                // Closing admission drains every accepted request before the
                // owner returns, including events recorded after the last End.
                flush(&mut owner);
                owner
            })
            .unwrap();
        Self {
            sender: Some(sender),
            thread: Some(thread),
        }
    }

    pub(super) fn request(&self, window: usize, ordinal: usize) -> bool {
        self.sender
            .as_ref()
            .unwrap()
            .try_send(Request::Flush {
                window,
                ordinal,
                at: Instant::now(),
            })
            .is_ok()
    }

    pub(super) async fn finish_window(&mut self, window: usize) -> usize {
        let (reply, completed) = oneshot::channel();
        self.sender
            .as_ref()
            .unwrap()
            .send(Request::End {
                window,
                at: Instant::now(),
                reply,
            })
            .await
            .unwrap();
        completed.await.unwrap()
    }

    pub(super) fn finish(mut self) -> T {
        drop(self.sender.take());
        self.thread.take().unwrap().join().unwrap()
    }
}

impl<T> Drop for Writer<T> {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(thread) = self.thread.take() {
            // Join on test failure too; never leave a writer using exported
            // files after the fixture's owner or temporary directory disappears.
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests;
