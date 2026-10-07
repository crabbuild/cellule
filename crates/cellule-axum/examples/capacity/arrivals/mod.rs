//! Emit the complete offered schedule, including offers whose wakeup is late.
use super::{Job, Kind};
use std::time::Duration;
use tokio::{sync::mpsc, time::Instant};

#[cfg(test)]
mod tests;

#[derive(Default)]
pub(super) struct Arrivals {
    pub offered: [u64; 2],
    pub dropped: [u64; 2],
    pub warmup_dropped: [u64; 2],
}

pub(super) async fn produce(
    sender: mpsc::Sender<Job>,
    start: Instant,
    warm_end: Instant,
    end: Instant,
    rates: [u64; 2],
) -> Arrivals {
    let mut counts = Arrivals::default();
    let mut indexes = [0_u64; 2];
    loop {
        let write_due = (indexes[0] * 1_000_000_000)
            .checked_div(rates[0])
            .map_or(end, |nanos| start + Duration::from_nanos(nanos));
        let read_due = (indexes[1] * 1_000_000_000)
            .checked_div(rates[1])
            .map_or(end, |nanos| start + Duration::from_nanos(nanos));
        let kind = usize::from(read_due < write_due);
        let due = if kind == 0 { write_due } else { read_due };
        if due >= end {
            break;
        }
        // Wakeup delay cannot erase an offer scheduled inside the window.
        // Its original due time still controls latency and completion gates;
        // a full queue records a drop rather than hiding work as unissued.
        tokio::time::sleep_until(due).await;
        let measured = due >= warm_end;
        counts.offered[kind] += u64::from(measured);
        let job = Job {
            kind: if kind == 0 { Kind::Write } else { Kind::Read },
            index: indexes[kind],
            due,
            measured,
        };
        match sender.try_send(job) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                if measured {
                    counts.dropped[kind] += 1;
                } else {
                    counts.warmup_dropped[kind] += 1;
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => break,
        }
        indexes[kind] += 1;
    }
    counts
}
