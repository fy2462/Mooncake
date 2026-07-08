use super::MooncakeClient;
use crate::proto;
use mooncake_store_core::error::StoreResult;
use mooncake_store_core::{NoFSegment, StoreError};
use std::env;
use uuid::Uuid;

impl MooncakeClient {
    /// Build the C++-compatible NoF transfer endpoint string.
    ///
    /// C++ equivalent: `NoFRegisterClient::set_register`.
    pub fn build_nof_te_endpoint(nqn: &str, nsid: u64, traddr: &str, trsvcid: u64) -> String {
        Self::build_nof_te_endpoint_with_trtype(
            nqn,
            nsid,
            traddr,
            trsvcid,
            env::var("MC_NOF_TRTYPE").ok().as_deref(),
        )
    }

    pub fn build_nof_te_endpoint_with_trtype(
        nqn: &str,
        nsid: u64,
        traddr: &str,
        trsvcid: u64,
        trtype: Option<&str>,
    ) -> String {
        let mut trtype = trtype.unwrap_or("RDMA").to_string();
        trtype.make_ascii_uppercase();
        if trtype != "RDMA" && trtype != "TCP" {
            trtype = "RDMA".to_string();
        }
        format!(
            "traddr:{traddr} trsvcid:{trsvcid} subnqn:{nqn} trtype:{trtype} adrfam:IPv4 ns:{nsid}"
        )
    }

    /// Register an SSD exposed through NVMe-oF with the master.
    ///
    /// This mirrors the C++ `NoFRegisterClient::set_register` helper.
    pub async fn register_nof_ssd(
        &mut self,
        nqn: &str,
        nsid: u64,
        traddr: &str,
        trsvcid: u64,
        base: u64,
        size: u64,
    ) -> StoreResult<NoFSegment> {
        self.register_nof_ssd_with_trtype(nqn, nsid, traddr, trsvcid, base, size, None)
            .await
    }

    pub async fn register_nof_ssd_with_trtype(
        &mut self,
        nqn: &str,
        nsid: u64,
        traddr: &str,
        trsvcid: u64,
        base: u64,
        size: u64,
        trtype: Option<&str>,
    ) -> StoreResult<NoFSegment> {
        let endpoint = Self::build_nof_te_endpoint_with_trtype(nqn, nsid, traddr, trsvcid, trtype);
        let segment = NoFSegment {
            id: Uuid::new_v4(),
            name: endpoint.clone(),
            base,
            size,
            te_endpoint: endpoint,
            client_id: self.client_id,
        };
        self.mount_nof_segment(&segment).await?;
        Ok(segment)
    }

    /// Unregister all mounted NoF segments matching the derived endpoint.
    ///
    /// C++ equivalent: `NoFRegisterClient::set_unregister_by_endpoint`.
    pub async fn unregister_nof_ssd_by_endpoint(
        &mut self,
        nqn: &str,
        nsid: u64,
        traddr: &str,
        trsvcid: u64,
    ) -> StoreResult<usize> {
        self.unregister_nof_ssd_by_endpoint_with_trtype(nqn, nsid, traddr, trsvcid, None)
            .await
    }

    pub async fn unregister_nof_ssd_by_endpoint_with_trtype(
        &mut self,
        nqn: &str,
        nsid: u64,
        traddr: &str,
        trsvcid: u64,
        trtype: Option<&str>,
    ) -> StoreResult<usize> {
        let endpoint = Self::build_nof_te_endpoint_with_trtype(nqn, nsid, traddr, trsvcid, trtype);
        let owners = self.get_nof_segments_by_name(&endpoint).await?;
        if owners.is_empty() {
            return Err(StoreError::SegmentNotFound(endpoint));
        }

        let mut removed = 0usize;
        for owner in owners {
            self.unmount_nof_segment_as_owner(owner.segment_id, owner.client_id)
                .await?;
            removed += 1;
        }
        Ok(removed)
    }

    async fn unmount_nof_segment_as_owner(
        &mut self,
        segment_id: Uuid,
        owner_client_id: Uuid,
    ) -> StoreResult<()> {
        self.master
            .unmount_no_f_segment(self.rpc_request(proto::UnmountNoFSegmentRequest {
                segment_id: Some(Self::uuid_to_proto_uuid(segment_id)),
                client_id: Some(Self::uuid_to_proto_uuid(owner_client_id)),
            }))
            .await
            .map_err(Self::rpc_status_to_error)?;
        Ok(())
    }
}
