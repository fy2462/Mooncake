use super::ClientHttpConfig;
use super::MooncakeClient;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ClientConfig<'a> {
    pub(super) master_addrs: &'a [String],
    pub(super) metadata_conn_string: &'a str,
    pub(super) local_host: &'a str,
    pub(super) protocol: &'a str,
    pub(super) device: &'a str,
    pub(super) global_segment_size: u64,
    pub(super) local_buffer_size: u64,
    pub(super) tenant_id: &'a str,
    pub(super) http: ClientHttpConfig,
}

impl MooncakeClient {
    pub(super) fn replicate_config_to_proto(
        &self,
        config: &mooncake_store_core::ReplicateConfig,
    ) -> crate::proto::ReplicateConfig {
        crate::proto::ReplicateConfig {
            replica_num: config.replica_num,
            nof_replica_num: config.nof_replica_num,
            with_soft_pin: config.with_soft_pin,
            with_hard_pin: config.with_hard_pin,
            preferred_segment: self.placement_preferred_segment(&config.preferred_segment),
            prefer_alloc_in_same_node: config.prefer_alloc_in_same_node,
            preferred_segments: config.preferred_segments.clone(),
            preferred_nof_segments: config.preferred_nof_segments.clone(),
            data_type: config.data_type as i32,
            group_ids: config.group_ids.clone(),
            // C++ Client::AttachHostId overwrites a caller-supplied value when
            // the client has a stable physical-host identity.
            host_id: if self.host_id.is_empty() {
                config.host_id.clone()
            } else {
                self.host_id.clone()
            },
        }
    }

    /// C++ forces every CXL write to the current process's segment alias so
    /// the Master returns the shared device offset through a routable name.
    pub(super) fn placement_preferred_segment(&self, configured: &str) -> String {
        Self::placement_preferred_segment_for(&self.protocol, &self.local_hostname, configured)
    }

    pub(super) fn placement_preferred_segment_for(
        protocol: &str,
        local_hostname: &str,
        configured: &str,
    ) -> String {
        if protocol == "cxl" {
            local_hostname.to_string()
        } else {
            configured.to_string()
        }
    }
}
