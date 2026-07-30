# Batch Query IP Parity Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. For this run, execute inline with
> `superpowers:executing-plans`; subagent delegation is not permitted.

**Goal:** Make Rust `QueryIp` and `BatchQueryIp` match the eight ordinary C++
transfer-endpoint query behaviors and record only freshly executed evidence.

**Architecture:** Add eight tonic service tests that distinguish segment names
from transfer endpoints and distinguish unknown clients from known clients
with empty endpoints. Replace the current cached/name-derived query helper with
one shared optional result derived from mounted segment `te_endpoint` values,
then use it from both single and batch RPC paths.

**Tech Stack:** Rust, Tokio, tonic, Mooncake Store master service state,
schema-v2 parity manifests, Cargo, Python validation tools, Git, and repository
pre-commit hooks.

## Global Constraints

- Work in `/home/fy2462/Mooncake/.worktrees/ha-chaos-live` on
  `codex/store-cpp-parity-only`.
- Never edit, format, build, link, load, import, or execute C/C++ reference
  inputs or `mooncake-wheel/tests`; they are read-only source oracles.
- Use `/home/fy2462/Mooncake/.venv/bin/python` for validation tooling only.
- Use `apply_patch` for semantic edits.
- Every pre-commit invocation sets `SKIP=mooncake-code-format`.
- Keep etcd-only HA policy unchanged.
- Do not modify protobufs, public Rust types, persisted state, segment
  publication, client address caches, host identity, metadata registration,
  or metrics.
- Do not begin Store LocalDisk io_uring work.
- If RED output contradicts the inspected address-source/map-insertion cause,
  stop and revise the design before changing production code.
- The eight snapshot variants, unrelated LocalDisk rows, and seven native-link
  blocked wheel BatchReplicaClear rows remain missing.

---

### Task 1: Add the eight C++-authoritative BatchQueryIp tests and capture RED

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_master_mount.rs`

**Interfaces:**
- Add `mount_query_ip_segment(&MasterServiceImpl, Uuid, &str, u64, &str)` to
  mount one 1 MiB Memory segment with the supplied name, base, and exact
  transfer endpoint.
- Add `batch_query_ips(&MasterServiceImpl, &[Uuid]) -> HashMap<String,
  proto::IpList>` to call the tonic `BatchQueryIp` boundary.
- Produce eight exact test names cited by the Store manifest.

- [ ] **Step 1: Add the test-only mount and query helpers**

  Add `use std::collections::{HashMap, HashSet};` and place these helpers above
  the existing `test_query_ip_derives_address_from_mounted_segment`:

  ```rust
  async fn mount_query_ip_segment(
      service: &MasterServiceImpl,
      client_id: Uuid,
      segment_name: &str,
      base_addr: u64,
      te_endpoint: &str,
  ) {
      MasterService::mount_segment(
          service,
          Request::new(proto::MountSegmentRequest {
              client_id: Some(proto_uuid(client_id)),
              segment_name: segment_name.into(),
              size: 1024 * 1024,
              base_addr,
              te_endpoint: te_endpoint.into(),
              protocol: String::new(),
              host_id: String::new(),
          }),
      )
      .await
      .unwrap();
  }

  async fn batch_query_ips(
      service: &MasterServiceImpl,
      client_ids: &[Uuid],
  ) -> HashMap<String, proto::IpList> {
      MasterService::batch_query_ip(
          service,
          Request::new(proto::BatchQueryIpRequest {
              client_ids: client_ids.iter().copied().map(proto_uuid).collect(),
          }),
      )
      .await
      .unwrap()
      .into_inner()
      .ips
  }
  ```

- [ ] **Step 2: Add single-client, unknown-client, de-duplication, and empty
      input tests**

  Add these exact tests:

  ```rust
  #[tokio::test]
  async fn batch_query_ip_single_and_unknown_client_parity() {
      let service = MasterServiceImpl::default();
      let client_id = Uuid::new_v4();
      mount_query_ip_segment(
          &service,
          client_id,
          "query-single-segment",
          0x300000000,
          "127.0.0.1:12345",
      )
      .await;

      let single = batch_query_ips(&service, &[client_id]).await;
      assert_eq!(single[&client_id.to_string()].addresses, ["127.0.0.1"]);

      let unknown = Uuid::new_v4();
      let mixed = batch_query_ips(&service, &[client_id, unknown]).await;
      assert_eq!(mixed[&client_id.to_string()].addresses, ["127.0.0.1"]);
      assert!(!mixed.contains_key(&unknown.to_string()));
  }

  #[tokio::test]
  async fn batch_query_ip_deduplicates_multiple_segment_addresses_parity() {
      let service = MasterServiceImpl::default();
      let client_id = Uuid::new_v4();
      for (index, (name, endpoint)) in [
          ("query-multi-a", "127.0.0.1:12345"),
          ("query-multi-b", "127.0.0.1:12346"),
          ("query-multi-c", "192.168.1.1:12345"),
      ]
      .into_iter()
      .enumerate()
      {
          mount_query_ip_segment(
              &service,
              client_id,
              name,
              0x300000000 + index as u64 * 0x200000,
              endpoint,
          )
          .await;
      }
      let result = batch_query_ips(&service, &[client_id]).await;
      let actual = result[&client_id.to_string()]
          .addresses
          .iter()
          .map(String::as_str)
          .collect::<HashSet<_>>();
      assert_eq!(actual, HashSet::from(["127.0.0.1", "192.168.1.1"]));
  }

  #[tokio::test]
  async fn batch_query_ip_empty_request_parity() {
      let service = MasterServiceImpl::default();
      assert!(batch_query_ips(&service, &[]).await.is_empty());
  }
  ```

- [ ] **Step 3: Add the known-client/empty-endpoint test**

  ```rust
  #[tokio::test]
  async fn batch_query_ip_retains_client_with_only_empty_endpoints_parity() {
      let service = MasterServiceImpl::default();
      let client_id = Uuid::new_v4();
      mount_query_ip_segment(
          &service,
          client_id,
          "query-empty-a",
          0x300000000,
          "",
      )
      .await;
      mount_query_ip_segment(
          &service,
          client_id,
          "query-empty-b",
          0x302000000,
          "",
      )
      .await;

      let result = batch_query_ips(&service, &[client_id]).await;
      assert!(result.contains_key(&client_id.to_string()));
      assert!(result[&client_id.to_string()].addresses.is_empty());
  }
  ```

- [ ] **Step 4: Add the four IPv6 and mixed-family tests**

  Add one test per reference with these literal fixtures and expectations:

  ```rust
  #[tokio::test]
  async fn batch_query_ip_parses_bracketed_ipv6_parity() {
      let service = MasterServiceImpl::default();
      let client_id = Uuid::new_v4();
      mount_query_ip_segment(
          &service,
          client_id,
          "query-bracketed-v6",
          0x300000000,
          "[::1]:17813",
      )
      .await;
      let result = batch_query_ips(&service, &[client_id]).await;
      assert_eq!(result[&client_id.to_string()].addresses, ["::1"]);
  }

  #[tokio::test]
  async fn batch_query_ip_preserves_ipv6_scope_parity() {
      let service = MasterServiceImpl::default();
      let client_id = Uuid::new_v4();
      mount_query_ip_segment(
          &service,
          client_id,
          "query-scoped-v6",
          0x300000000,
          "fe80::a236:bcff:fecb:a1be%eno2:15773",
      )
      .await;
      let result = batch_query_ips(&service, &[client_id]).await;
      assert_eq!(
          result[&client_id.to_string()].addresses,
          ["fe80::a236:bcff:fecb:a1be%eno2"]
      );
  }

  #[tokio::test]
  async fn batch_query_ip_accepts_ipv6_without_port_parity() {
      let service = MasterServiceImpl::default();
      let client_id = Uuid::new_v4();
      mount_query_ip_segment(
          &service,
          client_id,
          "query-raw-v6",
          0x300000000,
          "::1",
      )
      .await;
      let result = batch_query_ips(&service, &[client_id]).await;
      assert_eq!(result[&client_id.to_string()].addresses, ["::1"]);
  }

  #[tokio::test]
  async fn batch_query_ip_mixed_ipv4_ipv6_parity() {
      let service = MasterServiceImpl::default();
      let client_id = Uuid::new_v4();
      mount_query_ip_segment(
          &service,
          client_id,
          "query-mixed-v4",
          0x300000000,
          "192.168.1.1:12345",
      )
      .await;
      mount_query_ip_segment(
          &service,
          client_id,
          "query-mixed-v6",
          0x302000000,
          "[::1]:17813",
      )
      .await;
      let result = batch_query_ips(&service, &[client_id]).await;
      let actual = result[&client_id.to_string()]
          .addresses
          .iter()
          .map(String::as_str)
          .collect::<HashSet<_>>();
      assert_eq!(actual, HashSet::from(["192.168.1.1", "::1"]));
  }
  ```

- [ ] **Step 5: Run the new subset and verify semantic RED**

  Run:

  ```bash
  cd rust-repo
  cargo test -p mooncake-store-master --test test_master_mount batch_query_ip_
  ```

  Expected: `batch_query_ip_empty_request_parity` passes; the other seven tests
  fail because the response contains segment-name-derived addresses, does not
  parse the exact transfer endpoints, or cannot retain an empty address vector.
  Compilation or fixture errors must be corrected and rerun until the output is
  a meaningful semantic failure.

### Task 2: Implement the shared C++-compatible QueryIp semantics and reach GREEN

**Files:**
- Modify: `rust-repo/crates/mooncake-store-master/src/service/helpers.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/mod.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_objects_query.rs`
- Modify: `rust-repo/crates/mooncake-store-master/src/service/grpc_batches.rs`
- Modify: `rust-repo/crates/mooncake-store-master/tests/test_master_mount.rs`

**Interfaces:**
- Replace `addresses_for_client(&MasterState, Uuid) -> Vec<String>` with
  `query_ip_addresses_for_client(&MasterState, Uuid) -> Option<Vec<String>>`.
- Add private `transfer_endpoint_host(&str) -> String` in `helpers.rs`.
- `None` means no mounted segment; `Some(Vec::new())` means mounted segments
  with no nonempty transfer endpoint.

- [ ] **Step 1: Add the endpoint parser and optional segment query helper**

  Replace the current `addresses_for_client` implementation in `helpers.rs`
  with:

  ```rust
  fn transfer_endpoint_host(endpoint: &str) -> String {
      if let Some(rest) = endpoint.strip_prefix('[')
          && let Some(closing) = rest.find(']')
      {
          return rest[..closing].to_string();
      }
      if endpoint.parse::<std::net::Ipv6Addr>().is_ok() {
          return endpoint.to_string();
      }
      if let Some(scope) = endpoint.find('%')
          && let Some(relative_colon) = endpoint[scope..].find(':')
      {
          let colon = scope + relative_colon;
          let host = &endpoint[..colon];
          let address = host.split('%').next().unwrap_or_default();
          if address.parse::<std::net::Ipv6Addr>().is_ok() {
              return host.to_string();
          }
      }
      endpoint
          .rsplit_once(':')
          .map(|(host, _)| host.to_string())
          .unwrap_or_else(|| endpoint.to_string())
  }

  pub(super) fn query_ip_addresses_for_client(
      state: &MasterState,
      client_id: Uuid,
  ) -> Option<Vec<String>> {
      let mut found_segment = false;
      let mut addresses = Vec::new();
      for entry in state.segments.iter() {
          if entry.client_id != client_id {
              continue;
          }
          found_segment = true;
          if entry.segment.te_endpoint.is_empty() {
              continue;
          }
          let host = transfer_endpoint_host(&entry.segment.te_endpoint);
          if !addresses.iter().any(|address| address == &host) {
              addresses.push(host);
          }
      }
      found_segment.then_some(addresses)
  }
  ```

- [ ] **Step 2: Route the single and batch RPCs through the helper**

  In `service/mod.rs`, replace the imported `addresses_for_client` symbol with
  `query_ip_addresses_for_client`.

  In `grpc_objects_query.rs`, replace the current empty-vector check with:

  ```rust
  let Some(addresses) = query_ip_addresses_for_client(&self.state, client_id) else {
      return Err(Status::not_found("client not found"));
  };
  Ok(Response::new(proto::QueryIpResponse { addresses }))
  ```

  In `grpc_batches.rs`, replace each item's query/insertion block with:

  ```rust
  if let Some(addresses) = query_ip_addresses_for_client(&self.state, id) {
      ips.insert(id.to_string(), proto::IpList { addresses });
  }
  ```

- [ ] **Step 3: Correct the existing single-query test fixture**

  In `test_query_ip_derives_address_from_mounted_segment`, change only its
  `MountSegmentRequest` fields so `segment_name` is
  `"query-single-existing-fixture"` and `te_endpoint` is
  `"10.0.0.1:1234"`. Keep the exact `vec!["10.0.0.1"]` assertion.

- [ ] **Step 4: Format and verify GREEN**

  Run:

  ```bash
  cd rust-repo
  rustfmt --edition 2024 crates/mooncake-store-master/tests/test_master_mount.rs \
    crates/mooncake-store-master/src/service/helpers.rs \
    crates/mooncake-store-master/src/service/mod.rs \
    crates/mooncake-store-master/src/service/grpc_objects_query.rs \
    crates/mooncake-store-master/src/service/grpc_batches.rs
  cargo test -p mooncake-store-master --test test_master_mount batch_query_ip_
  cargo test -p mooncake-store-master --test test_master_mount \
    test_query_ip_derives_address_from_mounted_segment -- --exact
  cargo test -p mooncake-store-master --test test_master_mount
  cargo test -p mooncake-store-master service::helpers::tests --lib
  cargo fmt --check -p mooncake-store-master
  git diff --check
  ```

  Expected: all eight new tests, the corrected existing single-query test, the
  complete integration target, and helper library tests pass. Existing
  unrelated compiler warnings may remain, but no new warning may originate
  from the changed symbols.

- [ ] **Step 5: Commit the RED-proven behavior correction and tests**

  ```bash
  git add \
    rust-repo/crates/mooncake-store-master/src/service/helpers.rs \
    rust-repo/crates/mooncake-store-master/src/service/mod.rs \
    rust-repo/crates/mooncake-store-master/src/service/grpc_objects_query.rs \
    rust-repo/crates/mooncake-store-master/src/service/grpc_batches.rs \
    rust-repo/crates/mooncake-store-master/tests/test_master_mount.rs
  git diff --cached --check
  git commit -m "[Store] align batch query ip with C++ endpoints"
  ```

### Task 3: Upgrade exactly the eight freshly proved Store rows

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`

**Interfaces:**
- Map each `MasterServiceTest.BatchQueryIp*` reference to the exact Rust test
  listed in Task 1.
- Preserve all snapshot, LocalDisk, wheel, TE, and TENT dispositions.

- [ ] **Step 1: Update the eight manifest entries**

  For each of the eight ordinary reference rows, set `status` to `covered`,
  replace the remediation backlog with a concise statement of the executed
  result, and set `rust` to one exact object of this form:

  ```json
  {
    "file": "mooncake-store-master/tests/test_master_mount.rs",
    "test": "batch_query_ip_single_and_unknown_client_parity"
  }
  ```

  Use the corresponding exact test name for the other seven mappings. Do not
  update any row whose test did not execute successfully.

- [ ] **Step 2: Validate structure, counts, and source-only planning**

  Run from `rust-repo/tools/store-validation`:

  ```bash
  /home/fy2462/Mooncake/.venv/bin/python -m unittest discover -s tests -v
  /home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
    --manifest parity-map.json --repo-root ../../..
  /home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
    --manifest parity-map.json \
    --manifest transfer-engine-parity-map.json \
    --manifest tent-parity-map.json \
    --manifest wheel-store-parity-map.json \
    --repo-root ../../..
  ```

  Then run strict Store validation and assert its exit code is one solely
  because other rows remain missing, with zero lines beginning `ERROR`:

  ```bash
  strict_log=$(mktemp)
  /home/fy2462/Mooncake/.venv/bin/python validate_parity.py \
    --manifest parity-map.json --repo-root ../../.. --require-complete \
    >"$strict_log" 2>&1
  strict_rc=$?
  test "$strict_rc" -eq 1
  test -z "$(rg '^ERROR' "$strict_log")"
  ```

  Import `plan_parity_runs` and `discover_rust_tests` in a source-only Python
  snippet, assert all eight new test identities are scheduled, and do not call
  `execute_plan`.

- [ ] **Step 3: Commit the manifest evidence**

  ```bash
  git add rust-repo/tools/store-validation/parity-map.json
  git diff --cached --check
  git commit -m "[Store] record batch query ip parity evidence"
  ```

### Task 4: Run final gates for this remediation wave

**Files:**
- Verify only; no planned edits.

- [ ] **Step 1: Run fresh owning tests after both commits**

  ```bash
  cd rust-repo
  cargo test -p mooncake-store-master --test test_master_mount batch_query_ip_
  cargo test -p mooncake-store-master --test test_master_mount
  cargo test -p mooncake-store-master service::helpers::tests --lib
  cargo fmt --check -p mooncake-store-master
  ```

- [ ] **Step 2: Run scoped pre-commit**

  ```bash
  cd ..
  SKIP=mooncake-code-format /home/fy2462/Mooncake/.venv/bin/pre-commit run \
    --files \
    rust-repo/crates/mooncake-store-master/src/service/helpers.rs \
    rust-repo/crates/mooncake-store-master/src/service/mod.rs \
    rust-repo/crates/mooncake-store-master/src/service/grpc_objects_query.rs \
    rust-repo/crates/mooncake-store-master/src/service/grpc_batches.rs \
    rust-repo/crates/mooncake-store-master/tests/test_master_mount.rs \
    rust-repo/tools/store-validation/parity-map.json \
    docs/superpowers/specs/2026-07-31-batch-query-ip-parity-remediation-design.md \
    docs/superpowers/plans/2026-07-31-batch-query-ip-parity-remediation.md
  ```

- [ ] **Step 3: Verify immutable references and inspect the branch**

  ```bash
  git diff --quiet -- mooncake-store mooncake-transfer-engine \
    mooncake-wheel/tests
  git diff --cached --quiet -- mooncake-store mooncake-transfer-engine \
    mooncake-wheel/tests
  git diff --check
  git status --short --branch
  git log -6 --oneline
  ```

  Report exact fresh test and manifest counts, the native-link wheel blocker,
  and the next stable missing-behavior family. Keep the full parity goal active.
