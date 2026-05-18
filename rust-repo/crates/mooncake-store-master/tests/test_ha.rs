use mooncake_store_master::ha::LeaderRole;

#[test]
fn test_leader_role_values() {
    assert_ne!(LeaderRole::Leader, LeaderRole::Standby);
}

#[test]
fn test_leader_role_debug() {
    assert_eq!(format!("{:?}", LeaderRole::Leader), "Leader");
    assert_eq!(format!("{:?}", LeaderRole::Standby), "Standby");
}

#[test]
fn test_leader_role_clone_eq() {
    let r = LeaderRole::Leader;
    assert_eq!(r.clone(), r);
    assert_eq!(r, LeaderRole::Leader);
    assert_ne!(r, LeaderRole::Standby);
}
