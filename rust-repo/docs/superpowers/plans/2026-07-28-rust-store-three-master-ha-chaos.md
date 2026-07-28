# Rust Store Three-Master HA Chaos Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and execute a deterministic live gate that proves exact Store bytes survive repeated stop/restart cycles in a three-Rust-Master etcd HA group for both small-object and eviction-prone large-object workloads.

**Architecture:** A shell runner owns one uniquely named etcd container, native-link build configuration, retained artifacts, and top-level cleanup. An opt-in Rust integration test owns three real `mooncake-master` child processes, multiple TCP Store clients, leader stabilization, seeded failure selection, byte-integrity workloads, and result serialization. The parity manifest changes only after two consecutive live passes with distinct seeds.

**Tech Stack:** Bash, Docker, etcd 3.5, Rust 2024, Tokio, `mooncake-store-client`, `mooncake-store-master`, native Transfer Engine TCP transport, serde JSON, Cargo integration tests.

## Global Constraints

- etcd is the only accepted election and shared ordered-oplog backend.
- The gate uses three real `mooncake-master` processes and multiple real TCP Store clients; metadata-only and in-process Master simulations do not satisfy it.
- The canonical seed is `0x4d4f_4f4e_4841_4348`; every override is retained in the result JSON.
- Every stable checkpoint requires one unchanged etcd leader view plus a successful client health/remount and Store operation.
- At least one Master remains running during every failure round, and all three Master indices are stopped and restarted during each complete scenario.
- Opted-in missing dependencies and startup failures are `FAIL`; an exact capability `SKIP` is permitted only when the integration test is invoked outside the canonical runner.
- Cleanup targets only recorded child PIDs, the uniquely named etcd container, and the current run's temporary directories.
- Logs and JSON results remain under the configured artifact root on success and failure.
- The existing Soft-RoCE suite remains authoritative for RDMA; this gate must not require RDMA, accelerator, CXL, NUMA, or Kubernetes capabilities.
- Do not mark the C++ chaos parity rows covered until two canonical live runs with different seeds pass.
- Use `.venv/bin/pre-commit` only on touched files, with `SKIP=mooncake-code-format,codespell` where the unavailable hooks require it.

---

### Task 1: Shell runner resource and result contract

**Files:**
- Create: `rust-repo/tools/store-validation/run-ha-chaos-live.sh`
- Create: `rust-repo/tools/store-validation/tests/test_ha_chaos_live_runner.sh`

**Interfaces:**
- Consumes: `MOONCAKE_HA_ARTIFACT_ROOT`, `MOONCAKE_HA_SEED`, `MOONCAKE_HA_ETCD_IMAGE`, `MOONCAKE_HA_CARGO`, `RUSTFLAGS`, and `LD_LIBRARY_PATH`.
- Produces: `<artifact-root>/ha-chaos-result.json`, `<artifact-root>/runner.log`, a unique `mc-store-ha-chaos-<run-id>` container name, and environment variables `MOONCAKE_RUN_HA_CHAOS=1`, `MOONCAKE_HA_ETCD_ENDPOINT`, `MOONCAKE_HA_MASTER_BIN`, `MOONCAKE_HA_RESULT`, `MOONCAKE_HA_ARTIFACT_ROOT`, and `MOONCAKE_HA_SEED` for the Rust test.
- Test injection points: `MOONCAKE_HA_DOCKER`, `MOONCAKE_HA_CARGO`, and `MOONCAKE_HA_RUN_ID`; production defaults are `docker`, `cargo`, and a timestamp/PID identifier.

- [ ] **Step 1: Write the failing runner contract test**

Create fake Docker and Cargo executables in a `mktemp -d` directory. The fake Docker records every argument, returns container id `fake-etcd`, reports host port `42379`, and accepts `rm -f`. The fake Cargo records build/test calls and writes this literal result on the test call:

```json
{"schema_version":1,"status":"PASS","seed":"0x4d4f4f4e48414348","masters":["127.0.0.1:51051","127.0.0.1:51052","127.0.0.1:51053"],"scenarios":{"small":{"status":"PASS"},"large":{"status":"PASS"}}}
```

Invoke the runner with `MOONCAKE_HA_RUN_ID=contract`, then assert:

```bash
grep -F -- 'run -d --name mc-store-ha-chaos-contract -p 127.0.0.1::2379' "$calls"
grep -F -- 'port mc-store-ha-chaos-contract 2379/tcp' "$calls"
grep -F -- 'exec mc-store-ha-chaos-contract etcdctl endpoint health' "$calls"
grep -F -- 'build -p mooncake-store-master --bin mooncake-master' "$calls"
grep -F -- 'test -p mooncake-store-client --features link-native --test test_ha_chaos_live' "$calls"
grep -F -- 'rm -f mc-store-ha-chaos-contract' "$calls"
test "$(jq -r .status "$artifact_root/ha-chaos-result.json")" = PASS
```

Add negative cases proving missing `LD_LIBRARY_PATH`, malformed JSON, top-level `PASS` without both scenario PASS values, Cargo failure, `TERM`, and reused container names all return nonzero while still issuing the exact owned `rm -f` command.

- [ ] **Step 2: Run the contract test to verify RED**

Run:

```bash
bash rust-repo/tools/store-validation/tests/test_ha_chaos_live_runner.sh
```

Expected: FAIL because `run-ha-chaos-live.sh` does not exist.

- [ ] **Step 3: Implement the minimal runner**

Implement these focused functions:

```bash
require_command()                 # validates the configured Docker/Cargo tools
validate_run_id()                 # accepts only [A-Za-z0-9_.-]+
cleanup()                         # removes only $etcd_container when owned
wait_for_etcd()                   # bounded docker exec etcdctl endpoint health
validate_result()                 # Python JSON structural validation
```

Start etcd with an ephemeral loopback port:

```bash
"$docker_cmd" run -d --name "$etcd_container" \
  -p 127.0.0.1::2379 "$etcd_image" \
  etcd --data-dir=/tmp/etcd-data \
  --listen-client-urls=http://0.0.0.0:2379 \
  --advertise-client-urls=http://127.0.0.1:2379
```

Resolve the endpoint from `docker port`, build the Master, and execute exactly one opted-in Rust integration test. Preserve the first nonzero status and install `EXIT`, `INT`, and `TERM` traps before starting Docker.

- [ ] **Step 4: Run the contract test to verify GREEN**

Run:

```bash
bash rust-repo/tools/store-validation/tests/test_ha_chaos_live_runner.sh
```

Expected: PASS, including cleanup and malformed-result cases.

- [ ] **Step 5: Run shell static checks**

Run:

```bash
bash -n rust-repo/tools/store-validation/run-ha-chaos-live.sh
bash -n rust-repo/tools/store-validation/tests/test_ha_chaos_live_runner.sh
git diff --check
```

Expected: all exit 0.

- [ ] **Step 6: Commit the runner contract**

```bash
git add rust-repo/tools/store-validation/run-ha-chaos-live.sh \
  rust-repo/tools/store-validation/tests/test_ha_chaos_live_runner.sh
git commit -m '[Store] add HA chaos live runner contract'
```

### Task 2: Deterministic chaos model and result schema

**Files:**
- Create: `rust-repo/crates/mooncake-store-client/tests/test_ha_chaos_live.rs`

**Interfaces:**
- Produces `GateConfig::from_env() -> Result<Option<GateConfig>, String>` where `None` is the non-opted-in exact skip.
- Produces `ChaosSchedule::new(seed: u64, rounds: usize) -> Result<ChaosSchedule, String>` and `ChaosRound { stop: Vec<usize>, restart: Vec<usize> }`.
- Produces serializable `GateResult`, `ScenarioResult`, and `FailureRecord`; `GateResult::write_atomic(&Path)` writes schema version 1.
- Later tasks consume `ScenarioKind::{Small, Large}`, `MasterSlot`, `MasterCluster`, and `run_scenario` declarations introduced in this file.

- [ ] **Step 1: Write failing deterministic helper tests**

Add literal expectation tests before helper implementations:

```rust
#[test]
fn canonical_schedule_keeps_one_master_alive_and_rotates_every_index() {
    let schedule = ChaosSchedule::new(0x4d4f_4f4e_4841_4348, 4).unwrap();
    assert_eq!(schedule.rounds.len(), 4);
    assert!(schedule.rounds.iter().all(|round| (1..=2).contains(&round.stop.len())));
    assert_eq!(schedule.stopped_indices(), BTreeSet::from([0, 1, 2]));
    assert_eq!(schedule.restarted_indices(), BTreeSet::from([0, 1, 2]));
}

#[test]
fn opted_out_live_gate_is_an_exact_skip() {
    let env = BTreeMap::new();
    assert!(matches!(GateConfig::from_map(&env), Ok(None)));
}

#[test]
fn opted_in_gate_requires_every_external_input() {
    let env = BTreeMap::from([("MOONCAKE_RUN_HA_CHAOS".into(), "1".into())]);
    assert!(GateConfig::from_map(&env).unwrap_err().contains("MOONCAKE_HA_ETCD_ENDPOINT"));
}
```

Add a result test that writes to a temporary path and compares parsed literal fields: schema version 1, seed, top-level `FAIL`, first failure, all three Master addresses, and both scenario objects.

- [ ] **Step 2: Run helper tests to verify RED**

Run:

```bash
cargo test -p mooncake-store-client --test test_ha_chaos_live --no-default-features \
  canonical_schedule_keeps_one_master_alive_and_rotates_every_index -- --exact
```

Expected: compile failure because the helper types do not exist.

- [ ] **Step 3: Implement deterministic helpers and schema**

Use an internal LCG with wrapping arithmetic and explicit integer constants; do not use thread RNG. Force round `i` to include victim `i % 3`, use seeded choice only for the optional second victim, and define every round's restart set as the Masters stopped by that round. Reject `rounds < 3`.

Implement atomic result publication with `NamedTempFile::persist` in the result file's parent directory. `Drop` must not publish PASS; the top-level test constructs a default FAIL result first and updates it only after both scenarios succeed.

- [ ] **Step 4: Run all helper tests to verify GREEN**

Run:

```bash
cargo test -p mooncake-store-client --test test_ha_chaos_live --no-default-features
```

Expected: helper tests PASS and the live test returns immediately with an explicit skip message when opt-in is absent.

- [ ] **Step 5: Commit the deterministic model**

```bash
git add rust-repo/crates/mooncake-store-client/tests/test_ha_chaos_live.rs
git commit -m '[Store] define deterministic HA chaos evidence'
```

### Task 3: Three-Master process lifecycle and stable-leader proof

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/tests/test_ha_chaos_live.rs`
- Modify only if the RED run identifies a product defect: the narrow file under `rust-repo/crates/mooncake-store-master/src/ha/` or `rust-repo/crates/mooncake-store-master/src/main_ha.rs` that owns the failing transition.

**Interfaces:**
- `MasterSlot::spawn(index, address, snapshot_dir, log_path, &GateConfig) -> Result<Self, String>`.
- `MasterSlot::stop(&mut self, timeout: Duration) -> Result<(), String>` and `restart(&mut self) -> Result<(), String>` preserve command, address, snapshot directory, and log identity.
- `MasterCluster::start(config: &GateConfig) -> Result<Self, String>` owns exactly three slots.
- `wait_for_stable_leader(coordinator: &LeaderCoordinator, cluster: &MasterCluster, clients: &mut [MooncakeClient], deadline: Instant) -> Result<MasterView, String>` requires two equal nonempty views separated by a 500 ms quiet interval plus successful `switch_master` and `health_check`.

- [ ] **Step 1: Write the opted-in failing lifecycle test**

Add a `#[tokio::test(flavor = "multi_thread", worker_threads = 8)]` live test which:

```rust
let Some(config) = GateConfig::from_env().expect("valid HA chaos environment") else {
    eprintln!("SKIP test_ha_chaos_live: MOONCAKE_RUN_HA_CHAOS is not enabled");
    return;
};
let mut cluster = MasterCluster::start(&config).await.expect("start three Masters");
assert_eq!(cluster.running_indices(), BTreeSet::from([0, 1, 2]));
let view = wait_for_stable_leader_without_clients(&config, &cluster).await.unwrap();
assert!(cluster.addresses().contains(&view.leader_address));
let leader = cluster.index_for_address(&view.leader_address).unwrap();
cluster.stop(leader).await.unwrap();
let next = wait_for_stable_leader_without_clients(&config, &cluster).await.unwrap();
assert_ne!(next.view_version, view.view_version);
assert_ne!(next.leader_address, view.leader_address);
cluster.restart(leader).await.unwrap();
```

- [ ] **Step 2: Run the canonical runner to verify RED**

Run with the native library environment:

```bash
MOONCAKE_HA_ARTIFACT_ROOT=/tmp/mooncake-ha-chaos-red \
RUSTFLAGS='-L native=/tmp/mooncake-native-libs' \
LD_LIBRARY_PATH='/tmp/mooncake-native-libs:/tmp/mooncake-te-validation/mooncake-transfer-engine/src:/tmp/mooncake-te-validation/mooncake-transfer-engine/tent/src:/tmp/mooncake-te-validation/mooncake-common:/tmp/mooncake-te-validation/mooncake-common/src' \
bash rust-repo/tools/store-validation/run-ha-chaos-live.sh
```

Expected: FAIL at the first missing lifecycle/stability implementation or at a reproducible product HA transition, with three Master logs retained.

- [ ] **Step 3: Implement child lifecycle and coordinator observation**

Reserve three loopback ports before spawning any child. Pass each Master:

```text
--enable-ha
--ha-backend-type etcd
--ha-backend-connstring <endpoint>
--cluster-id <unique namespace>
--ha-lease-ttl-secs 3
--rpc-address 127.0.0.1
--rpc-port <reserved port>
--snapshot-backend-type local-disk
--snapshot-backup-dir <slot-specific directory>
--client-ttl-secs 2
--default-kv-lease-ttl-ms 1
```

Use `Child::try_wait` loops with bounded TERM then `kill`; never search `/proc` by command name. Open logs with append mode and prefix each restart with the exact command, seed, and monotonic timestamp.

- [ ] **Step 4: Diagnose any product failure before changing production code**

If the lifecycle test fails after harness compilation, record the first failing component boundary: etcd view, standby oplog catch-up, promotion, gRPC bind, or client-visible health. State one root-cause hypothesis and add the smallest failing unit/integration regression in the owning crate before editing production code. Do not weaken the live assertion.

- [ ] **Step 5: Run focused HA regressions and lifecycle GREEN**

Run:

```bash
cargo test -p mooncake-store-master --test test_ha --test test_oplog --test test_coordinator
MOONCAKE_HA_ARTIFACT_ROOT=/tmp/mooncake-ha-chaos-lifecycle \
RUSTFLAGS='-L native=/tmp/mooncake-native-libs' \
LD_LIBRARY_PATH='/tmp/mooncake-native-libs:/tmp/mooncake-te-validation/mooncake-transfer-engine/src:/tmp/mooncake-te-validation/mooncake-transfer-engine/tent/src:/tmp/mooncake-te-validation/mooncake-common:/tmp/mooncake-te-validation/mooncake-common/src' \
bash rust-repo/tools/store-validation/run-ha-chaos-live.sh
```

Expected: lifecycle checkpoint PASS; the full result may remain FAIL because scenario workloads are not implemented yet.

- [ ] **Step 6: Commit lifecycle support and any proven narrow fix**

```bash
git add rust-repo/crates/mooncake-store-client/tests/test_ha_chaos_live.rs
git commit -m '[Store] validate three-master HA lifecycle'
```

If Step 4 required a production fix, commit its named regression and exact
owning source file in a separate preceding commit using
`[Store] fix HA <failing-transition>`; never stage a whole `src/` directory.

### Task 4: Multi-client failover and small-object chaos scenario

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/tests/test_ha_chaos_live.rs`
- Modify only after a RED proof: the exact `rust-repo/crates/mooncake-store-client/src/client/*.rs` file owning failover/remount behavior.

**Interfaces:**
- `create_clients(config: &GateConfig, masters: &[String], count: usize, segment_size: u64) -> Result<Vec<MooncakeClient>, String>`.
- `recover_clients(clients: &mut [MooncakeClient], view: &MasterView) -> Result<(), String>` switches every client to the observed leader then calls `health_check` until remount completes.
- `run_small_scenario(config, cluster, clients, schedule) -> Result<ScenarioResult, FailureRecord>`.

- [ ] **Step 1: Write the failing client-remount checkpoint**

Start three clients with 16 MiB segments and all three Master candidates. Put literal key/value `ha-small-bootstrap` / `bootstrap-value` through client 0, stop the current leader, wait for a new stable view, recover the same client objects, and require client 2 to return `bootstrap-value`. The production mutation this catches is replacing `health_check` failover/remount with a channel-only switch.

- [ ] **Step 2: Run the live gate and verify RED**

Run the Task 3 canonical command with artifact root `/tmp/mooncake-ha-chaos-small-red`.

Expected: FAIL if HA remount or oplog replay loses the committed object; otherwise fail at the intentionally unimplemented complete small scenario.

- [ ] **Step 3: Implement the small-object scenario**

Use 100 keys and deterministic values `small:<seed>:<key-index>`. For each scheduled round:

1. stop the scheduled one or two victims;
2. attempt puts/gets through seeded clients during instability and count every success;
3. stabilize a surviving leader and recover all existing clients;
4. put or accept already-exists for every key;
5. read every key through a different seeded client and compare the literal expected bytes;
6. restart that round's victims and require a stable view before the next round.

Require `successful_unstable_operations > 0`, `stable_exact_reads >= 100 * rounds`, and stopped/restarted index sets exactly `{0,1,2}`.

- [ ] **Step 4: Fix only evidence-backed product defects**

For any byte loss, trace: client RPC result → leader oplog append → standby poll/apply → promotion → client remount → replica descriptor → TE read. Add a focused failing regression at the first incorrect boundary, apply one minimal production fix, and rerun that regression before the live gate.

- [ ] **Step 5: Verify the small scenario GREEN**

Run:

```bash
MOONCAKE_HA_SCENARIOS=small \
MOONCAKE_HA_ARTIFACT_ROOT=/tmp/mooncake-ha-chaos-small-green \
RUSTFLAGS='-L native=/tmp/mooncake-native-libs' \
LD_LIBRARY_PATH='/tmp/mooncake-native-libs:/tmp/mooncake-te-validation/mooncake-transfer-engine/src:/tmp/mooncake-te-validation/mooncake-transfer-engine/tent/src:/tmp/mooncake-te-validation/mooncake-common:/tmp/mooncake-te-validation/mooncake-common/src' \
bash rust-repo/tools/store-validation/run-ha-chaos-live.sh
```

Expected: `.scenarios.small.status == "PASS"`, at least four crash/restart rounds, all three victim indices, and exact-read count at least 400.

- [ ] **Step 6: Commit small-object HA parity**

```bash
git add rust-repo/crates/mooncake-store-client/tests/test_ha_chaos_live.rs
git commit -m '[Store] validate small-object HA crash recovery'
```

Commit any evidence-backed product fix first with only its named regression and
exact owning source file.

### Task 5: Eviction-prone large-object chaos scenario

**Files:**
- Modify: `rust-repo/crates/mooncake-store-client/tests/test_ha_chaos_live.rs`
- Modify only after RED proof: the exact eviction, oplog replay, or client data-path source file owning an observed defect.

**Interfaces:**
- `run_large_scenario(config, cluster, clients, schedule) -> Result<ScenarioResult, FailureRecord>`.
- `expected_large_value(seed: u64, key_index: usize, len: usize) -> Vec<u8>` creates a nonuniform key-derived pattern.
- `ScenarioResult.evidence` includes `eviction_requests`, `capacity_rejections`, `successful_unstable_operations`, `stable_exact_reads`, `byte_comparisons`, `crashes`, `restarts`, and `leader_view_versions`.

- [ ] **Step 1: Write the failing pressure-evidence test**

Add a helper test proving 42 keys × 3 MiB exceeds 3 clients × 32 MiB and that each expected value differs at the first, middle, and last bytes for adjacent keys. Add a live assertion requiring at least one observable eviction request, completed offload/eviction transition, or capacity rejection; configured watermarks alone must not satisfy it.

- [ ] **Step 2: Run focused tests to verify RED**

Run:

```bash
cargo test -p mooncake-store-client --test test_ha_chaos_live --no-default-features \
  large_profile_exceeds_capacity_and_values_are_key_distinct -- --exact
```

Expected: FAIL because the large profile/evidence implementation is missing.

- [ ] **Step 3: Implement large values and pressure accounting**

Use three 32 MiB segments, 42 keys, 3 MiB values, four rounds, eviction interval 5 ms, high watermark 0.90, and eviction ratio 0.20. Derive every byte from `(seed, key_index, byte_index)` with wrapping integer operations. Record capacity failures and Master/client eviction metrics available through existing APIs; if no observable counter exists, add the narrowest test-only read of an existing production metric rather than a new behavior flag.

- [ ] **Step 4: Implement crash/restart workload assertions**

During instability allow only unavailable/not-found/capacity-class errors. Every successful read must immediately match exact length and bytes. At each stable checkpoint, read exactly or rewrite then cross-client read every key. Reject any other error class and require nonzero pressure evidence.

- [ ] **Step 5: Run the large scenario to expose real defects**

Run the Task 4 command with `MOONCAKE_HA_SCENARIOS=large` and artifact root `/tmp/mooncake-ha-chaos-large-red`.

Expected: either a reproducible product RED with retained seed/logs, or PASS if the implementation already meets the oracle. A PASS is acceptable only when pressure evidence and exact-byte counters are nonzero.

- [ ] **Step 6: Apply TDD fixes for each proven defect and verify GREEN**

For each defect, add one focused regression, reproduce RED, change one owning production boundary, rerun focused tests, then rerun the large live scenario. Stop and reassess architecture after three failed fix hypotheses rather than stacking changes.

- [ ] **Step 7: Commit large-object HA parity**

```bash
git add rust-repo/crates/mooncake-store-client/tests/test_ha_chaos_live.rs
git commit -m '[Store] validate eviction-prone HA crash recovery'
```

Commit any evidence-backed product fix first with only its named regression and
exact owning source file.

### Task 6: Two-seed acceptance, parity registration, and delivery evidence

**Files:**
- Modify: `rust-repo/tools/store-validation/parity-map.json`
- Modify: `rust-repo/tools/store-validation/run-ha-chaos-live.sh`
- Modify: `rust-repo/tools/store-validation/tests/test_ha_chaos_live_runner.sh`
- Modify if required by repository delivery conventions: `rust-repo/change_logs/2026-07-28-001.md`

**Interfaces:**
- The runner accepts one seed per invocation and always writes a complete schema-version-1 result.
- Parity rows reference `mooncake-store-client/tests/test_ha_chaos_live.rs::three_master_etcd_ha_chaos_preserves_small_and_large_object_bytes`.

- [ ] **Step 1: Add failing final result-validation cases**

Extend the shell contract test with literal invalid results for: missing third Master, missing large scenario, zero crashes, zero restarts, victim set not `{0,1,2}`, zero unstable successes, zero stable exact reads, zero large pressure evidence, and top-level PASS with nested FAIL. Each must make the runner exit nonzero.

- [ ] **Step 2: Run contract tests to verify RED**

Run:

```bash
bash rust-repo/tools/store-validation/tests/test_ha_chaos_live_runner.sh
```

Expected: at least one invalid evidence case is incorrectly accepted.

- [ ] **Step 3: Harden structural result validation**

Validate exact top-level and per-scenario invariants with Python JSON parsing. Do not use grep for nested status. Preserve the Rust-generated failure result when Cargo exits nonzero; create a runner-level FAIL only when no valid result exists.

- [ ] **Step 4: Execute canonical acceptance seed A**

Run:

```bash
MOONCAKE_HA_SEED=0x4d4f4f4e48414348 \
MOONCAKE_HA_ARTIFACT_ROOT=/home/fy2462/workspace/tmp/mooncake/ha-chaos-seed-a \
RUSTFLAGS='-L native=/tmp/mooncake-native-libs' \
LD_LIBRARY_PATH='/tmp/mooncake-native-libs:/tmp/mooncake-te-validation/mooncake-transfer-engine/src:/tmp/mooncake-te-validation/mooncake-transfer-engine/tent/src:/tmp/mooncake-te-validation/mooncake-common:/tmp/mooncake-te-validation/mooncake-common/src' \
bash rust-repo/tools/store-validation/run-ha-chaos-live.sh
```

Expected: PASS with both scenarios, three Masters, all victim indices, nonzero unstable successes, exact reads, and large pressure evidence.

- [ ] **Step 5: Execute canonical acceptance seed B**

Run the same command with:

```text
MOONCAKE_HA_SEED=0x4d4f4f4e48414349
MOONCAKE_HA_ARTIFACT_ROOT=/home/fy2462/workspace/tmp/mooncake/ha-chaos-seed-b
```

Expected: an independent PASS with the same invariants and a different recorded seed.

- [ ] **Step 6: Register both C++ chaos rows as covered**

Change only the two `e2e/chaos_rand_test.cpp` entries from `missing` to `covered`, remove their stale reasons, and point both at the stable Rust test name. Do not change hardware, Redis, K8s, NUMA, or performance rows.

- [ ] **Step 7: Run final focused and parity verification**

Run:

```bash
cargo test -p mooncake-store-client --test test_ha_chaos_live --no-default-features
bash rust-repo/tools/store-validation/tests/test_ha_chaos_live_runner.sh
.venv/bin/python rust-repo/tools/store-validation/validate_parity.py \
  --manifest rust-repo/tools/store-validation/parity-map.json \
  --cpp-root mooncake-store/tests --rust-root rust-repo/crates 2>&1 | \
  rg 'new_test|summary' || true
SKIP=mooncake-code-format,codespell .venv/bin/pre-commit run --files \
  rust-repo/tools/store-validation/run-ha-chaos-live.sh \
  rust-repo/tools/store-validation/tests/test_ha_chaos_live_runner.sh \
  rust-repo/crates/mooncake-store-client/tests/test_ha_chaos_live.rs \
  rust-repo/tools/store-validation/parity-map.json
git diff --check
```

Expected parity summary: `covered=199 missing=16`, with no blocked rows; all focused checks pass.

- [ ] **Step 8: Review the exact delivery diff**

Run:

```bash
git status --short
git diff --stat
git diff -- rust-repo/tools/store-validation/run-ha-chaos-live.sh \
  rust-repo/tools/store-validation/tests/test_ha_chaos_live_runner.sh \
  rust-repo/crates/mooncake-store-client/tests/test_ha_chaos_live.rs \
  rust-repo/tools/store-validation/parity-map.json
```

Confirm every changed line belongs to the HA chaos gate and record both retained result paths in the handoff.

- [ ] **Step 9: Commit final evidence and parity map**

```bash
git add rust-repo/tools/store-validation/run-ha-chaos-live.sh \
  rust-repo/tools/store-validation/tests/test_ha_chaos_live_runner.sh \
  rust-repo/crates/mooncake-store-client/tests/test_ha_chaos_live.rs \
  rust-repo/tools/store-validation/parity-map.json
git commit -m '[Store] gate three-master HA chaos parity'
```

Do not include retained `/home/fy2462/workspace/tmp/mooncake` artifacts in Git.
