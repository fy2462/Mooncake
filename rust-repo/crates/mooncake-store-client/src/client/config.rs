use super::ClientHttpConfig;

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
