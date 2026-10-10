//! Concurrent charges and uncertain filesystem/admission settlement.

use super::*;
use std::sync::atomic::AtomicBool;

fn lane(value: u8) -> Lane {
    Lane {
        leader: SessionId::from_bytes([value; 16]),
        epoch: 2,
    }
}

#[test]
fn settling_one_lane_preserves_other_pending_growth_and_external_reservations() {
    let disk = cellule_ltx::DiskBudget::new(400);
    let external = disk.try_reserve(17).unwrap();
    let mut ledger = DiskAccounting::new(disk.try_reserve(200).unwrap());
    ledger.begin(lane(1), 80, 40).unwrap();
    ledger.begin(lane(2), 120, 50).unwrap();
    ledger.grow(lane(1), 20).unwrap();
    assert_eq!(disk.used(), 327);
    assert!(ledger.grow(lane(2), 100).is_err());
    assert_eq!(disk.used(), 327);
    ledger.settle(lane(2), 140).unwrap();
    assert_eq!(ledger.bytes(), 280);
    assert_eq!(ledger.pending[&lane(1)], 140);
    ledger.settle(lane(1), 90).unwrap();
    assert_eq!(ledger.bytes(), 230);
    assert!(ledger.pending.is_empty());
    drop(ledger);
    assert_eq!(disk.used(), 17);
    drop(external);
    assert_eq!(disk.used(), 0);
}

struct RejectSettlement(AtomicBool);

#[test]
fn restored_lane_bytes_are_admitted_before_new_growth_after_an_empty_settlement() {
    let disk = cellule_ltx::DiskBudget::new(100);
    let mut ledger = DiskAccounting::new(disk.try_reserve(50).unwrap());
    ledger.begin(lane(1), 50, 10).unwrap();
    ledger.settle(lane(1), 0).unwrap();
    assert_eq!(disk.used(), 0);
    assert!(ledger.begin(lane(1), 80, 21).is_err());
    assert_eq!(disk.used(), 80);
    assert!(ledger.pending.is_empty());
    ledger.begin(lane(1), 80, 20).unwrap();
    assert_eq!(disk.used(), 100);
    ledger.settle(lane(1), 90).unwrap();
    assert_eq!(disk.used(), 90);
}

impl cellule_ltx::DiskBudgetAdmission for RejectSettlement {
    fn reconcile(&self, used: u64) -> cellule_ltx::Result<()> {
        if used == 110 && self.0.load(Ordering::Relaxed) {
            return Err(cellule_ltx::LtxError::InvalidState(
                "injected admission failure",
            ));
        }
        Ok(())
    }
}

#[test]
fn failed_admission_keeps_its_charge_until_reconciliation_before_new_growth() {
    let disk = cellule_ltx::DiskBudget::new(200);
    let admission = Arc::new(RejectSettlement(AtomicBool::new(true)));
    disk.install_admission(admission.clone()).unwrap();
    let mut ledger = DiskAccounting::new(disk.try_reserve(100).unwrap());
    ledger.begin(lane(1), 100, 50).unwrap();
    assert!(ledger.settle(lane(1), 110).is_err());
    assert_eq!(ledger.pending[&lane(1)], 150);
    assert_eq!(disk.used(), 150);
    assert!(ledger.begin(lane(1), 110, 10).is_err());
    assert_eq!(disk.used(), 150);
    admission.0.store(false, Ordering::Relaxed);
    ledger.begin(lane(1), 110, 10).unwrap();
    assert_eq!(disk.used(), 120);
    ledger.settle(lane(1), 115).unwrap();
    assert_eq!(disk.used(), 115);
}

#[cfg(unix)]
#[test]
fn failed_lane_recount_is_conservative_and_preserves_the_operation_error() {
    let root = tempfile::TempDir::new().unwrap();
    let disk = cellule_ltx::DiskBudget::new(100);
    let shared = Arc::new(Mutex::new(DiskAccounting::new(
        disk.try_reserve(0).unwrap(),
    )));
    let mut observation = AppendObservation::unobserved(lane(1));
    let original = LaneAccounting::begin(
        shared.clone(),
        root.path(),
        lane(1),
        10,
        false,
        &mut observation,
    )
    .unwrap();
    std::fs::create_dir_all(&original.directory).unwrap();
    std::fs::write(original.directory.join("bytes"), [0; 10]).unwrap();
    original.added(10).unwrap();
    original.finish(Ok(()), &mut observation).unwrap();
    assert_eq!(shared.lock().unwrap().settled[&lane(1)], 10);
    let operation = LaneAccounting::begin(
        shared.clone(),
        root.path(),
        lane(1),
        10,
        false,
        &mut observation,
    )
    .unwrap();
    std::fs::create_dir_all(&operation.directory).unwrap();
    std::os::unix::fs::symlink("missing", operation.directory.join("special")).unwrap();
    assert!(matches!(
        operation.finish::<()>(Err(Error::Node("original write error")), &mut observation),
        Err(Error::Node("original write error"))
    ));
    assert_eq!(disk.used(), 20);
    // Another lane releases only its own unused reservation.
    let other = LaneAccounting::begin(
        shared.clone(),
        root.path(),
        lane(2),
        30,
        false,
        &mut observation,
    )
    .unwrap();
    std::fs::create_dir_all(&other.directory).unwrap();
    std::fs::write(other.directory.join("bytes"), [0; 10]).unwrap();
    other.added(10).unwrap();
    other.finish(Ok(()), &mut observation).unwrap();
    assert_eq!(disk.used(), 30);
    assert!(
        LaneAccounting::begin(
            shared.clone(),
            root.path(),
            lane(1),
            0,
            false,
            &mut observation
        )
        .is_err()
    );
    assert_eq!(disk.used(), 30);
    std::fs::remove_file(operation.directory.join("special")).unwrap();
    std::fs::write(operation.directory.join("bytes"), [0; 5]).unwrap();
    let repaired = LaneAccounting::begin(
        shared.clone(),
        root.path(),
        lane(1),
        0,
        false,
        &mut observation,
    )
    .unwrap();
    repaired.finish(Ok(()), &mut observation).unwrap();
    assert_eq!(disk.used(), 15);
    assert!(shared.lock().unwrap().pending.is_empty());
}
