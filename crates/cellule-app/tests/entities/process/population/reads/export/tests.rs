use super::*;

#[test]
fn stalled_writer_refuses_full_queue_and_drains_accepted_requests() {
    let (entered, active) = std::sync::mpsc::channel();
    let (release, resume) = std::sync::mpsc::channel();
    let writer = Writer::start(
        0,
        move |count| {
            *count += 1;
            if *count == 1 {
                entered.send(()).unwrap();
                resume.recv().unwrap();
            }
        },
        |_| {},
    );
    assert!(writer.request(0, 1));
    active.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(writer.request(0, 2));
    assert!(!writer.request(0, 3));
    release.send(()).unwrap();
    assert_eq!(writer.finish(), 3); // Two accepted flushes and final close drain.
}

#[tokio::test]
async fn window_acknowledges_flushes_before_returning_ownership() {
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let record = observed.clone();
    let mut writer = Writer::start(
        Vec::new(),
        |values| values.push(values.len()),
        move |timing| {
            record
                .lock()
                .unwrap()
                .push((timing.window, timing.ordinal, timing.terminal))
        },
    );
    assert!(writer.request(0, 1));
    assert_eq!(writer.finish_window(0).await, 1);
    assert_eq!(writer.finish_window(1).await, 0);
    assert_eq!(writer.finish(), [0, 1, 2, 3]);
    assert_eq!(
        *observed.lock().unwrap(),
        [(0, 1, false), (0, 0, true), (1, 0, true)]
    );
}

#[test]
fn drop_joins_writer_and_flushes_before_owner_is_destroyed() {
    let flushed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = flushed.clone();
    {
        let writer = Writer::start(
            (),
            move |_| {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            },
            |_| {},
        );
        assert!(writer.request(0, 1));
    }
    assert_eq!(flushed.load(std::sync::atomic::Ordering::SeqCst), 2);
}
