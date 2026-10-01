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
