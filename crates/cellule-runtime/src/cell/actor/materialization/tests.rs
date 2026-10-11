use super::*;
use crate::fleet::resource::{ResourceCost, ResourceLedger, RetentionStage};

#[test]
fn working_roots_leave_room_for_foreground_and_existing_debt_can_progress() {
    let resources = ResourceLedger::new(ResourceCost::zero().with_retained_bytes(64 << 20));
    let publisher = resources
        .try_reserve_in(
            RetentionStage::Publisher,
            ResourceCost::zero().with_retained_bytes(22 << 20),
        )
        .unwrap();
    let mut selected = resources
        .try_reserve_in(
            RetentionStage::Selection,
            ResourceCost::zero().with_retained_bytes(10 << 20),
        )
        .unwrap();
    let root = reserve_materialization(&resources, 5 << 20, true).unwrap();
    assert!(
        reserve_materialization(&resources, 5 << 20, false).is_err(),
        "parallel root buffers consumed foreground burst headroom"
    );
    let commands = resources
        .try_reserve_in(
            RetentionStage::Request,
            ResourceCost::zero().with_retained_bytes(8 << 20),
        )
        .unwrap();
    let snapshot = resources.snapshot().unwrap();
    assert!(
        snapshot.used.retained_bytes()
            < crate::fleet::resource::retained_admission_threshold(snapshot.limit.retained_bytes())
    );
    drop(commands);
    drop(root);
    // Already-retained debt must not wait forever for the foreground margin.
    let more_debt = resources
        .try_reserve_in(
            RetentionStage::Selection,
            ResourceCost::zero().with_retained_bytes(16 << 20),
        )
        .unwrap();
    let cleanup = reserve_materialization(&resources, 5 << 20, true).unwrap();
    assert!(reserve_materialization(&resources, 5 << 20, false).is_err());
    drop(more_debt);
    selected.shrink_retained(0).unwrap();
    drop(cleanup);
    drop(publisher);
    drop(selected);
    assert_eq!(resources.snapshot().unwrap().used.retained_bytes(), 0);
}
