use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn activation_failure_joins_accepted_work_and_stops_new_admission() {
    let admitted = AtomicUsize::new(0);
    let completed = AtomicUsize::new(0);
    let result = bounded(0..32, |index| {
        admitted.fetch_add(1, Ordering::SeqCst);
        let completed = &completed;
        async move {
            if index == 0 {
                return Err(Error::Node("injected activation failure").into());
            }
            tokio::task::yield_now().await;
            completed.fetch_add(1, Ordering::SeqCst);
            Ok(index)
        }
    })
    .await;
    assert!(result.is_err());
    assert_eq!(admitted.load(Ordering::SeqCst), IN_FLIGHT);
    assert_eq!(completed.load(Ordering::SeqCst), IN_FLIGHT - 1);
}
