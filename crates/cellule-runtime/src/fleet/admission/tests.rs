use super::*;

#[test]
fn pressure_recovery_cannot_clear_cordon_or_shutdown() {
    let gate = NodeAdmission::default();
    assert!(gate.check_new_role().is_ok());
    assert_eq!(gate.sample().unwrap(), None);
    gate.observe(PressureState::Shedding, 100).unwrap();
    assert!(gate.check_new_role().is_err());
    gate.observe(PressureState::Normal, 200).unwrap();
    assert!(gate.check_new_role().is_ok());
    gate.cordon().unwrap();
    gate.observe(PressureState::Normal, 300).unwrap();
    assert_eq!(gate.mode().unwrap(), NodeMode::Cordoned);
    assert!(gate.check_new_role().is_err());
    gate.begin_drain().unwrap();
    gate.cordon().unwrap();
    gate.observe(PressureState::Normal, 400).unwrap();
    assert_eq!(gate.mode().unwrap(), NodeMode::Draining);
    assert!(gate.check_new_role().is_err());
}

#[test]
fn samples_are_shared_monotonic_and_not_refreshed_by_read_or_cordon() {
    let gate = NodeAdmission::default();
    let clone = gate.clone();
    clone.observe(PressureState::Constrained, 100).unwrap();
    let old = gate.sample().unwrap().unwrap();
    assert_eq!(clone.sample().unwrap(), Some(old));
    assert!(gate.observe(PressureState::Normal, 99).is_err());
    assert!(gate.observe(PressureState::Recovering, 101).is_err());
    assert_eq!(gate.sample().unwrap(), Some(old));
    clone.cordon().unwrap();
    let cordoned = gate.sample().unwrap().unwrap();
    assert_eq!(cordoned.sequence, old.sequence + 1);
    assert_eq!(cordoned.observed_at_ms, old.observed_at_ms);
    gate.cordon().unwrap();
    assert_eq!(gate.sample().unwrap(), Some(cordoned));
}

#[test]
fn startup_hold_uses_shared_role_gate_and_preserves_measurement_time() {
    let gate = NodeAdmission::default();
    gate.observe(PressureState::Normal, 100).unwrap();
    let before = gate.sample().unwrap().unwrap();
    gate.hold_startup().unwrap();
    let held = gate.sample().unwrap().unwrap();
    assert_eq!(held.mode, NodeMode::Cordoned);
    assert_eq!(held.observed_at_ms, before.observed_at_ms);
    assert_eq!(held.sequence, before.sequence + 1);
    assert!(matches!(gate.check_new_role(), Err(Error::CellDraining)));
    assert!(matches!(
        gate.admit::<()>(|| panic!("startup admitted a follower")),
        Err(Error::CellDraining)
    ));
    assert!(gate.hold_startup().is_err());
    gate.confirm_startup(NodeMode::Active).unwrap();
    assert!(gate.check_new_role().is_ok());
    let confirmed = gate.sample().unwrap().unwrap();
    assert_eq!(confirmed.mode, NodeMode::Active);
    assert_eq!(confirmed.observed_at_ms, before.observed_at_ms);
    assert_eq!(confirmed.sequence, held.sequence + 1);
    gate.confirm_startup(NodeMode::Active).unwrap();
    assert_eq!(gate.sample().unwrap(), Some(confirmed));
}

#[test]
fn startup_confirmation_cannot_clear_a_racing_cordon_drain_or_pressure() {
    for mode in [NodeMode::Cordoned, NodeMode::Draining] {
        let gate = NodeAdmission::default();
        gate.hold_startup().unwrap();
        if mode == NodeMode::Cordoned {
            gate.cordon().unwrap();
        } else {
            gate.begin_drain().unwrap();
        }
        gate.confirm_startup(NodeMode::Active).unwrap();
        assert_eq!(gate.mode().unwrap(), mode);
        assert!(matches!(gate.check_new_role(), Err(Error::CellDraining)));
        gate.observe(PressureState::Normal, 100).unwrap();
        gate.confirm_startup(NodeMode::Active).unwrap();
        assert_eq!(gate.mode().unwrap(), mode);
    }
    let gate = NodeAdmission::default();
    assert!(gate.confirm_startup(NodeMode::Active).is_err());
    gate.hold_startup().unwrap();
    gate.observe(PressureState::Shedding, 100).unwrap();
    gate.confirm_startup(NodeMode::Active).unwrap();
    assert_eq!(gate.mode().unwrap(), NodeMode::Active);
    assert!(matches!(
        gate.check_new_role(),
        Err(Error::Capacity("node pressure"))
    ));
}

#[test]
fn startup_hold_is_distinct_from_confirmed_maintenance_mode() {
    for mode in [NodeMode::Active, NodeMode::Cordoned, NodeMode::Draining] {
        let gate = NodeAdmission::default();
        assert!(!gate.startup_held().unwrap());
        gate.hold_startup().unwrap();
        assert!(gate.startup_held().unwrap());
        gate.confirm_startup(mode).unwrap();
        assert!(!gate.startup_held().unwrap());
        assert_eq!(gate.mode().unwrap(), mode);
        if mode != NodeMode::Active {
            assert!(gate.check_new_role().is_err());
        }
    }
}
