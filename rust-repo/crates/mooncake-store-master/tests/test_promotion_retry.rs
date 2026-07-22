use mooncake_store_master::{MasterRuntimeConfig, MasterServiceImpl};

#[test]
fn promotion_candidate_state_starts_empty() {
    let service = MasterServiceImpl::with_runtime_config(MasterRuntimeConfig {
        enable_offload: true,
        promotion_on_hit: true,
        ..Default::default()
    });

    assert_eq!(service.promotion_candidate_count_for_test(), 0);
}
