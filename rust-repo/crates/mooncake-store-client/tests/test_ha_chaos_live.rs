use std::{
    collections::{BTreeMap, BTreeSet},
    fs::OpenOptions,
    io::Write,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use mooncake_store_client::MooncakeClient;
use mooncake_store_master::ha::{LeaderCoordinator, MasterView};
use serde::Serialize;

#[test]
fn canonical_schedule_keeps_one_master_alive_and_rotates_every_index() {
    let schedule = ChaosSchedule::new(0x4d4f_4f4e_4841_4348, 4).unwrap();
    assert_eq!(schedule.rounds.len(), 4);
    assert!(
        schedule
            .rounds
            .iter()
            .all(|round| (1..=2).contains(&round.stop.len()))
    );
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
    assert!(
        GateConfig::from_map(&env)
            .unwrap_err()
            .contains("MOONCAKE_HA_ETCD_ENDPOINT")
    );
}

#[test]
fn failed_result_publishes_the_complete_schema_atomically() {
    let result_dir = tempfile::tempdir().unwrap();
    let result_path = result_dir.path().join("ha-chaos-result.json");
    let result = GateResult::failed(
        0x4d4f_4f4e_4841_4348,
        "http://127.0.0.1:42379".into(),
        "ha-chaos-contract".into(),
        vec![
            "127.0.0.1:51051".into(),
            "127.0.0.1:51052".into(),
            "127.0.0.1:51053".into(),
        ],
        BTreeMap::from([
            (ScenarioKind::Small, ScenarioResult::passed()),
            (ScenarioKind::Large, ScenarioResult::failed()),
        ]),
        FailureRecord {
            stage: "stable-read".into(),
            message: "expected exact bytes after restart".into(),
        },
    );

    result.write_atomic(&result_path).unwrap();

    let value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(result_path).unwrap()).unwrap();
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["seed"], "0x4d4f4f4e48414348");
    assert_eq!(value["status"], "FAIL");
    assert_eq!(value["first_failure"]["stage"], "stable-read");
    assert_eq!(
        value["masters"],
        serde_json::json!(["127.0.0.1:51051", "127.0.0.1:51052", "127.0.0.1:51053",])
    );
    assert_eq!(value["scenarios"]["small"]["status"], "PASS");
    assert_eq!(value["scenarios"]["large"]["status"], "FAIL");
}

#[test]
fn incomplete_master_result_is_not_published() {
    let result_dir = tempfile::tempdir().unwrap();
    let result_path = result_dir.path().join("ha-chaos-result.json");
    let result = GateResult::failed(
        0x4d4f_4f4e_4841_4348,
        "http://127.0.0.1:42379".into(),
        "ha-chaos-contract".into(),
        Vec::new(),
        complete_scenarios(ResultStatus::Fail),
        FailureRecord {
            stage: "lifecycle".into(),
            message: "Masters have not been allocated".into(),
        },
    );

    assert!(
        result
            .write_atomic(&result_path)
            .unwrap_err()
            .contains("exactly three distinct nonempty master addresses")
    );
    assert!(!result_path.exists());
}

#[test]
fn result_publication_rejects_schema_versions_other_than_one() {
    let result_dir = tempfile::tempdir().unwrap();
    let result_path = result_dir.path().join("ha-chaos-result.json");
    let mut result = complete_failed_result();
    result.schema_version = 2;

    assert!(
        result
            .write_atomic(&result_path)
            .unwrap_err()
            .contains("schema version 1")
    );
    assert!(!result_path.exists());
}

#[test]
fn result_publication_requires_small_and_large_scenarios() {
    let result_dir = tempfile::tempdir().unwrap();
    let result_path = result_dir.path().join("ha-chaos-result.json");
    let mut result = complete_failed_result();
    result.scenarios.remove(&ScenarioKind::Large);

    assert!(
        result
            .write_atomic(&result_path)
            .unwrap_err()
            .contains("small and large scenarios")
    );
    assert!(!result_path.exists());
}

#[test]
fn top_level_pass_requires_both_scenarios_to_pass() {
    let result_dir = tempfile::tempdir().unwrap();
    let result_path = result_dir.path().join("ha-chaos-result.json");
    let mut result = complete_failed_result();
    result.status = ResultStatus::Pass;

    assert!(
        result
            .write_atomic(&result_path)
            .unwrap_err()
            .contains("top-level PASS requires both scenarios to PASS")
    );
    assert!(!result_path.exists());
}

#[tokio::test]
async fn master_rpc_probe_rejects_a_tcp_only_listener() {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let acceptor = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(2)).await;
    });

    let error = probe_master_rpc(&address, Instant::now() + Duration::from_millis(250))
        .await
        .unwrap_err();

    assert!(error.contains("Mooncake gRPC probe"), "{error}");
    acceptor.abort();
}

fn complete_scenarios(status: ResultStatus) -> BTreeMap<ScenarioKind, ScenarioResult> {
    BTreeMap::from([
        (
            ScenarioKind::Small,
            ScenarioResult {
                status,
                evidence: ScenarioEvidence::default(),
            },
        ),
        (
            ScenarioKind::Large,
            ScenarioResult {
                status,
                evidence: ScenarioEvidence::default(),
            },
        ),
    ])
}

fn complete_failed_result() -> GateResult {
    GateResult::failed(
        0x4d4f_4f4e_4841_4348,
        "http://127.0.0.1:42379".into(),
        "ha-chaos-contract".into(),
        vec![
            "127.0.0.1:51051".into(),
            "127.0.0.1:51052".into(),
            "127.0.0.1:51053".into(),
        ],
        complete_scenarios(ResultStatus::Fail),
        FailureRecord {
            stage: "stable-read".into(),
            message: "expected exact bytes after restart".into(),
        },
    )
}

const CANONICAL_ROUNDS: usize = 4;
const CLIENT_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;
const CLIENT_LOCAL_BUFFER_SIZE: u64 = 8 * 1024 * 1024;
const LCG_MULTIPLIER: u64 = 6_364_136_223_846_793_005;
const LCG_INCREMENT: u64 = 1_442_695_040_888_963_407;

#[derive(Debug, Clone)]
struct GateConfig {
    etcd_endpoint: String,
    master_bin: PathBuf,
    result_path: PathBuf,
    artifact_root: PathBuf,
    cluster_namespace: String,
    seed: u64,
    rounds: usize,
}

impl GateConfig {
    fn from_env() -> Result<Option<Self>, String> {
        Self::from_map(&std::env::vars().collect())
    }

    fn from_map(env: &BTreeMap<String, String>) -> Result<Option<Self>, String> {
        if env.get("MOONCAKE_RUN_HA_CHAOS").map(String::as_str) != Some("1") {
            return Ok(None);
        }

        let etcd_endpoint = required_env(env, "MOONCAKE_HA_ETCD_ENDPOINT")?;
        let master_bin = PathBuf::from(required_env(env, "MOONCAKE_HA_MASTER_BIN")?);
        let result_path = PathBuf::from(required_env(env, "MOONCAKE_HA_RESULT")?);
        let artifact_root = PathBuf::from(required_env(env, "MOONCAKE_HA_ARTIFACT_ROOT")?);
        let seed = parse_seed(&required_env(env, "MOONCAKE_HA_SEED")?)?;
        let rounds = match env.get("MOONCAKE_HA_ROUNDS") {
            Some(value) => value
                .parse()
                .map_err(|_| format!("MOONCAKE_HA_ROUNDS must be a positive integer: {value}"))?,
            None => CANONICAL_ROUNDS,
        };
        if rounds < 3 {
            return Err("MOONCAKE_HA_ROUNDS must be at least 3".into());
        }
        let cluster_namespace = format!(
            "ha-chaos-{seed:016x}-{}-{}",
            std::process::id(),
            monotonic_timestamp_ns()?
        );

        Ok(Some(Self {
            etcd_endpoint,
            master_bin,
            result_path,
            artifact_root,
            cluster_namespace,
            seed,
            rounds,
        }))
    }
}

fn required_env(env: &BTreeMap<String, String>, name: &str) -> Result<String, String> {
    env.get(name)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| format!("{name} is required when MOONCAKE_RUN_HA_CHAOS=1"))
}

fn parse_seed(value: &str) -> Result<u64, String> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    u64::from_str_radix(value, 16).map_err(|_| format!("MOONCAKE_HA_SEED is not a u64: {value}"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChaosRound {
    stop: Vec<usize>,
    restart: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ChaosSchedule {
    rounds: Vec<ChaosRound>,
}

impl ChaosSchedule {
    fn new(seed: u64, rounds: usize) -> Result<Self, String> {
        if rounds < 3 {
            return Err("HA chaos schedules require at least 3 rounds".into());
        }

        let mut state = seed;
        let mut schedule = Vec::with_capacity(rounds);
        for round_index in 0..rounds {
            state = state
                .wrapping_mul(LCG_MULTIPLIER)
                .wrapping_add(LCG_INCREMENT);

            let forced_victim = round_index % 3;
            let mut stop = vec![forced_victim];
            if state & 1 == 1 {
                let remaining = [(forced_victim + 1) % 3, (forced_victim + 2) % 3];
                stop.push(remaining[((state >> 32) & 1) as usize]);
                stop.sort_unstable();
            }
            schedule.push(ChaosRound {
                restart: stop.clone(),
                stop,
            });
        }
        Ok(Self { rounds: schedule })
    }

    fn stopped_indices(&self) -> BTreeSet<usize> {
        self.rounds
            .iter()
            .flat_map(|round| round.stop.iter().copied())
            .collect()
    }

    fn restarted_indices(&self) -> BTreeSet<usize> {
        self.rounds
            .iter()
            .flat_map(|round| round.restart.iter().copied())
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
enum ScenarioKind {
    Small,
    Large,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ResultStatus {
    Pass,
    Fail,
}

#[derive(Debug, Clone, Default, Serialize)]
struct ScenarioEvidence {
    eviction_requests: u64,
    capacity_rejections: u64,
    successful_unstable_operations: u64,
    stable_exact_reads: u64,
    byte_comparisons: u64,
    crashes: u64,
    restarts: u64,
    leader_view_versions: Vec<u64>,
    stopped_indices: BTreeSet<usize>,
    restarted_indices: BTreeSet<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct ScenarioResult {
    status: ResultStatus,
    evidence: ScenarioEvidence,
}

impl ScenarioResult {
    fn passed() -> Self {
        Self {
            status: ResultStatus::Pass,
            evidence: ScenarioEvidence::default(),
        }
    }

    fn failed() -> Self {
        Self {
            status: ResultStatus::Fail,
            evidence: ScenarioEvidence::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct FailureRecord {
    stage: String,
    message: String,
}

#[derive(Debug, Clone, Serialize)]
struct GateResult {
    schema_version: u32,
    status: ResultStatus,
    seed: String,
    etcd_endpoint: String,
    cluster_namespace: String,
    masters: Vec<String>,
    scenarios: BTreeMap<ScenarioKind, ScenarioResult>,
    first_failure: Option<FailureRecord>,
    master_logs: Vec<String>,
    client_logs: Vec<String>,
}

impl GateResult {
    fn failed(
        seed: u64,
        etcd_endpoint: String,
        cluster_namespace: String,
        masters: Vec<String>,
        scenarios: BTreeMap<ScenarioKind, ScenarioResult>,
        first_failure: FailureRecord,
    ) -> Self {
        Self {
            schema_version: 1,
            status: ResultStatus::Fail,
            seed: format!("0x{seed:016x}"),
            etcd_endpoint,
            cluster_namespace,
            masters,
            scenarios,
            first_failure: Some(first_failure),
            master_logs: Vec::new(),
            client_logs: Vec::new(),
        }
    }

    fn write_atomic(&self, result_path: &Path) -> Result<(), String> {
        self.validate_for_publication()?;

        let parent = result_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let mut temporary = tempfile::NamedTempFile::new_in(parent)
            .map_err(|error| format!("create result file in {}: {error}", parent.display()))?;
        serde_json::to_writer_pretty(temporary.as_file_mut(), self)
            .map_err(|error| format!("serialize result: {error}"))?;
        temporary
            .as_file_mut()
            .sync_all()
            .map_err(|error| format!("sync result: {error}"))?;
        temporary.persist(result_path).map_err(|error| {
            format!(
                "publish result to {}: {}",
                result_path.display(),
                error.error
            )
        })?;
        Ok(())
    }

    fn validate_for_publication(&self) -> Result<(), String> {
        if self.schema_version != 1 {
            return Err("result publication requires schema version 1".into());
        }
        let master_addresses: BTreeSet<_> = self.masters.iter().map(String::as_str).collect();
        if self.masters.len() != 3
            || self.masters.iter().any(|master| master.trim().is_empty())
            || master_addresses.len() != 3
        {
            return Err(
                "result publication requires exactly three distinct nonempty master addresses"
                    .into(),
            );
        }
        if !self.scenarios.contains_key(&ScenarioKind::Small)
            || !self.scenarios.contains_key(&ScenarioKind::Large)
        {
            return Err("result publication requires small and large scenarios".into());
        }
        if self.status == ResultStatus::Pass
            && self
                .scenarios
                .values()
                .any(|scenario| scenario.status != ResultStatus::Pass)
        {
            return Err("top-level PASS requires both scenarios to PASS".into());
        }
        Ok(())
    }
}

#[derive(Debug)]
struct MasterSlot {
    index: usize,
    address: String,
    snapshot_dir: PathBuf,
    log_path: PathBuf,
    command: Vec<String>,
    seed: u64,
    child: Option<Child>,
}

#[derive(Debug)]
struct MasterCluster {
    slots: Vec<MasterSlot>,
    cluster_namespace: String,
}

impl MasterSlot {
    fn spawn(
        index: usize,
        address: String,
        snapshot_dir: PathBuf,
        log_path: PathBuf,
        config: &GateConfig,
    ) -> Result<Self, String> {
        let (rpc_address, rpc_port) = address
            .rsplit_once(':')
            .ok_or_else(|| format!("invalid Master address: {address}"))?;
        let command = vec![
            config.master_bin.display().to_string(),
            "--enable-ha".into(),
            "--ha-backend-type".into(),
            "etcd".into(),
            "--ha-backend-connstring".into(),
            config.etcd_endpoint.clone(),
            "--cluster-id".into(),
            config.cluster_namespace.clone(),
            "--ha-lease-ttl-secs".into(),
            "3".into(),
            "--rpc-address".into(),
            rpc_address.into(),
            "--rpc-port".into(),
            rpc_port.into(),
            "--snapshot-backend-type".into(),
            "local-disk".into(),
            "--snapshot-backup-dir".into(),
            snapshot_dir.display().to_string(),
            "--client-ttl-secs".into(),
            "2".into(),
            "--default-kv-lease-ttl-ms".into(),
            "1".into(),
        ];
        let child = Some(spawn_master_child(
            &command,
            config.seed,
            &snapshot_dir,
            &log_path,
        )?);
        Ok(Self {
            index,
            address,
            snapshot_dir,
            log_path,
            command,
            seed: config.seed,
            child,
        })
    }

    async fn stop(&mut self, timeout: Duration) -> Result<(), String> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        match child.try_wait() {
            Ok(Some(_)) => return Ok(()),
            Ok(None) => {}
            Err(error) => {
                self.child = Some(child);
                return Err(format!("inspect Master {}: {error}", self.index));
            }
        }

        if let Err(error) = send_sigterm(&child) {
            self.child = Some(child);
            return Err(error);
        }
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match child.try_wait() {
                Ok(Some(_)) => return Ok(()),
                Ok(None) => {}
                Err(error) => {
                    self.child = Some(child);
                    return Err(format!("wait for Master {} TERM: {error}", self.index));
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        if let Err(error) = child.kill() {
            self.child = Some(child);
            return Err(format!(
                "kill Master {} after TERM timeout: {error}",
                self.index
            ));
        }
        let kill_deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < kill_deadline {
            match child.try_wait() {
                Ok(Some(_)) => return Ok(()),
                Ok(None) => {}
                Err(error) => {
                    self.child = Some(child);
                    return Err(format!("wait for Master {} kill: {error}", self.index));
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let error = format!(
            "Master {} pid {} did not exit after TERM and kill",
            self.index,
            child.id()
        );
        self.child = Some(child);
        Err(error)
    }

    fn restart(&mut self) -> Result<(), String> {
        if let Some(child) = self.child.as_mut() {
            match child
                .try_wait()
                .map_err(|error| format!("inspect Master {} for restart: {error}", self.index))?
            {
                None => return Err(format!("Master {} is already running", self.index)),
                Some(_) => self.child = None,
            }
        }
        self.child = Some(spawn_master_child(
            &self.command,
            self.seed,
            &self.snapshot_dir,
            &self.log_path,
        )?);
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.child.is_some()
    }
}

impl Drop for MasterSlot {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };
        if matches!(child.try_wait(), Ok(Some(_))) {
            return;
        }
        let _ = send_sigterm(&child);
        let term_deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < term_deadline {
            if matches!(child.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let _ = child.kill();
        let kill_deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < kill_deadline {
            if matches!(child.try_wait(), Ok(Some(_))) {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}

impl MasterCluster {
    async fn start(config: &GateConfig) -> Result<Self, String> {
        let reservations = (0..3)
            .map(|_| {
                TcpListener::bind(("127.0.0.1", 0))
                    .map_err(|error| format!("reserve Master loopback port: {error}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let addresses = reservations
            .iter()
            .map(|listener| {
                listener
                    .local_addr()
                    .map(|address| address.to_string())
                    .map_err(|error| format!("read reserved Master port: {error}"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let master_root = config.artifact_root.join("masters");
        std::fs::create_dir_all(&master_root).map_err(|error| {
            format!(
                "create Master artifact directory {}: {error}",
                master_root.display()
            )
        })?;
        let mut slots = Vec::with_capacity(3);
        for (index, (reservation, address)) in reservations
            .into_iter()
            .zip(addresses.into_iter())
            .enumerate()
        {
            let snapshot_dir = master_root.join(format!("master-{index}-snapshot"));
            let log_path = master_root.join(format!("master-{index}.log"));
            drop(reservation);
            slots.push(MasterSlot::spawn(
                index,
                address,
                snapshot_dir,
                log_path,
                config,
            )?);
        }

        Ok(Self {
            slots,
            cluster_namespace: config.cluster_namespace.clone(),
        })
    }

    fn running_indices(&mut self) -> BTreeSet<usize> {
        for slot in &mut self.slots {
            if let Some(child) = slot.child.as_mut()
                && matches!(child.try_wait(), Ok(Some(_)))
            {
                slot.child = None;
            }
        }
        self.slots
            .iter()
            .filter(|slot| slot.is_running())
            .map(|slot| slot.index)
            .collect()
    }

    fn addresses(&self) -> BTreeSet<String> {
        self.slots.iter().map(|slot| slot.address.clone()).collect()
    }

    fn index_for_address(&self, address: &str) -> Option<usize> {
        self.slots
            .iter()
            .find(|slot| slot.address == address)
            .map(|slot| slot.index)
    }

    fn is_running_address(&self, address: &str) -> bool {
        self.slots
            .iter()
            .any(|slot| slot.address == address && slot.is_running())
    }

    async fn stop(&mut self, index: usize) -> Result<(), String> {
        self.slots
            .get_mut(index)
            .ok_or_else(|| format!("Master index {index} is out of range"))?
            .stop(Duration::from_secs(5))
            .await
    }

    async fn restart(&mut self, index: usize) -> Result<(), String> {
        self.slots
            .get_mut(index)
            .ok_or_else(|| format!("Master index {index} is out of range"))?
            .restart()
    }
}

fn spawn_master_child(
    command: &[String],
    seed: u64,
    snapshot_dir: &Path,
    log_path: &Path,
) -> Result<Child, String> {
    std::fs::create_dir_all(snapshot_dir).map_err(|error| {
        format!(
            "create Master snapshot directory {}: {error}",
            snapshot_dir.display()
        )
    })?;
    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .map_err(|error| format!("open Master log {}: {error}", log_path.display()))?;
    writeln!(
        log,
        "\n=== Master spawn ===\nmonotonic_ns={}\nseed=0x{seed:016x}\ncommand={}\n",
        monotonic_timestamp_ns()?,
        serde_json::to_string(command)
            .map_err(|error| format!("serialize Master command: {error}"))?
    )
    .map_err(|error| format!("write Master log header {}: {error}", log_path.display()))?;
    log.flush()
        .map_err(|error| format!("flush Master log header {}: {error}", log_path.display()))?;
    let stderr = log
        .try_clone()
        .map_err(|error| format!("clone Master log {}: {error}", log_path.display()))?;
    Command::new(&command[0])
        .args(&command[1..])
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|error| format!("spawn Master command {command:?}: {error}"))
}

fn monotonic_timestamp_ns() -> Result<u128, String> {
    let mut timestamp = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime writes to the valid timespec pointer supplied here.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut timestamp) } != 0 {
        return Err(format!(
            "read monotonic clock: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok((timestamp.tv_sec as u128) * 1_000_000_000 + timestamp.tv_nsec as u128)
}

fn send_sigterm(child: &Child) -> Result<(), String> {
    let pid = libc::pid_t::try_from(child.id())
        .map_err(|_| format!("Master pid {} does not fit pid_t", child.id()))?;
    // SAFETY: pid is taken directly from this owned Child and the signal is SIGTERM.
    if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(format!("send SIGTERM to owned Master pid {pid}: {error}"));
        }
    }
    Ok(())
}

async fn wait_for_stable_leader(
    coordinator: &LeaderCoordinator,
    cluster: &MasterCluster,
    clients: &mut [MooncakeClient],
    deadline: Instant,
) -> Result<MasterView, String> {
    const QUIET_INTERVAL: Duration = Duration::from_millis(500);
    let mut last_observation = "no nonempty etcd view observed".to_string();

    while Instant::now() < deadline {
        let first = read_view_before_deadline(coordinator, deadline).await?;
        let Some(first) = first else {
            last_observation = "etcd view is empty".into();
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        if !cluster.is_running_address(&first.leader_address) {
            last_observation = format!(
                "etcd view {} points to a stopped or unknown Master",
                first.view_version
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        if Instant::now() + QUIET_INTERVAL >= deadline {
            break;
        }
        tokio::time::sleep(QUIET_INTERVAL).await;
        let second = read_view_before_deadline(coordinator, deadline).await?;
        if second.as_ref() != Some(&first) {
            last_observation =
                format!("etcd view changed during quiet interval: {first:?} -> {second:?}");
            continue;
        }

        if let Err(error) = probe_master_rpc(&first.leader_address, deadline).await {
            last_observation = error;
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        let mut clients_healthy = true;
        for client in clients.iter_mut() {
            let operation_timeout = deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_secs(5));
            let switched = tokio::time::timeout(
                operation_timeout,
                client.switch_master(&first.leader_address),
            )
            .await;
            let Ok(Ok(())) = switched else {
                last_observation = format!(
                    "client failed to switch to stable leader {}: {switched:?}",
                    first.leader_address
                );
                clients_healthy = false;
                break;
            };
            let health_timeout = deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_secs(5));
            let health = tokio::time::timeout(health_timeout, client.health_check()).await;
            let Ok(Ok(())) = health else {
                last_observation = format!(
                    "client health check failed on stable leader {}: {health:?}",
                    first.leader_address
                );
                clients_healthy = false;
                break;
            };
            if client.current_master_addr() != first.leader_address {
                last_observation = format!(
                    "client health check failed over away from stable leader {} to {}",
                    first.leader_address,
                    client.current_master_addr()
                );
                clients_healthy = false;
                break;
            }
        }
        if clients_healthy {
            return Ok(first);
        }
    }

    Err(format!(
        "stable leader deadline expired: {last_observation}"
    ))
}

async fn probe_master_rpc(address: &str, deadline: Instant) -> Result<(), String> {
    let connect_timeout = deadline
        .saturating_duration_since(Instant::now())
        .min(Duration::from_secs(2));
    if connect_timeout.is_zero() {
        return Err(format!(
            "Mooncake gRPC probe deadline expired before connecting to {address}"
        ));
    }
    let mut client = tokio::time::timeout(
        connect_timeout,
        mooncake_store_client::proto::master_service_client::MasterServiceClient::connect(format!(
            "http://{address}"
        )),
    )
    .await
    .map_err(|_| format!("Mooncake gRPC probe connect timed out for {address}"))?
    .map_err(|error| format!("Mooncake gRPC probe connect failed for {address}: {error}"))?;

    let rpc_timeout = deadline
        .saturating_duration_since(Instant::now())
        .min(Duration::from_secs(2));
    if rpc_timeout.is_zero() {
        return Err(format!(
            "Mooncake gRPC probe deadline expired before GetStorageConfig on {address}"
        ));
    }
    tokio::time::timeout(
        rpc_timeout,
        client.get_storage_config(tonic::Request::new(
            mooncake_store_client::proto::GetStorageConfigRequest {},
        )),
    )
    .await
    .map_err(|_| format!("Mooncake gRPC probe GetStorageConfig timed out for {address}"))?
    .map_err(|error| {
        format!("Mooncake gRPC probe GetStorageConfig failed for {address}: {error}")
    })?;
    Ok(())
}

async fn read_view_before_deadline(
    coordinator: &LeaderCoordinator,
    deadline: Instant,
) -> Result<Option<MasterView>, String> {
    let timeout = deadline
        .saturating_duration_since(Instant::now())
        .min(Duration::from_secs(2));
    if timeout.is_zero() {
        return Err("stable leader deadline expired while reading etcd".into());
    }
    tokio::time::timeout(timeout, coordinator.read_current_view())
        .await
        .map_err(|_| "timed out reading current etcd leader view".to_string())?
        .map_err(|error| format!("read current etcd leader view: {error}"))
}

async fn wait_for_stable_leader_without_clients(
    config: &GateConfig,
    cluster: &MasterCluster,
) -> Result<MasterView, String> {
    let deadline = Instant::now() + Duration::from_secs(45);
    let coordinator = tokio::time::timeout(
        deadline.saturating_duration_since(Instant::now()),
        LeaderCoordinator::new_etcd(
            vec![config.etcd_endpoint.clone()],
            &cluster.cluster_namespace,
        ),
    )
    .await
    .map_err(|_| "timed out creating HA test coordinator".to_string())?
    .map_err(|error| format!("create HA test coordinator: {error}"))?;
    wait_for_stable_leader(&coordinator, cluster, &mut [], deadline).await
}

async fn create_clients(
    _config: &GateConfig,
    masters: &[String],
    count: usize,
    segment_size: u64,
) -> Result<Vec<MooncakeClient>, String> {
    let mut clients = Vec::with_capacity(count);
    for client_index in 0..count {
        let reservation = TcpListener::bind(("127.0.0.1", 0))
            .map_err(|error| format!("reserve TCP endpoint for client {client_index}: {error}"))?;
        let local_host = reservation
            .local_addr()
            .map_err(|error| format!("read TCP endpoint for client {client_index}: {error}"))?
            .to_string();
        drop(reservation);

        let client = tokio::time::timeout(
            Duration::from_secs(30),
            MooncakeClient::create_with_master_candidates(
                masters,
                "P2PHANDSHAKE",
                &local_host,
                "tcp",
                "",
                segment_size,
                CLIENT_LOCAL_BUFFER_SIZE,
            ),
        )
        .await
        .map_err(|_| format!("client {client_index} creation timed out"))?
        .map_err(|error| format!("create TCP client {client_index}: {error}"))?;
        clients.push(client);
    }
    Ok(clients)
}

async fn recover_clients(clients: &mut [MooncakeClient], view: &MasterView) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    for (client_index, client) in clients.iter_mut().enumerate() {
        loop {
            let switch_timeout = deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_secs(5));
            if switch_timeout.is_zero() {
                return Err(format!(
                    "client recovery deadline expired before switching client {client_index}"
                ));
            }
            tokio::time::timeout(switch_timeout, client.switch_master(&view.leader_address))
                .await
                .map_err(|_| {
                    format!(
                        "client {client_index} switch to leader {} timed out",
                        view.leader_address
                    )
                })?
                .map_err(|error| {
                    format!(
                        "client {client_index} switch to leader {} failed: {error}",
                        view.leader_address
                    )
                })?;

            let health_timeout = deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_secs(5));
            if health_timeout.is_zero() {
                return Err(format!(
                    "client {client_index} remount deadline expired on leader {}",
                    view.leader_address
                ));
            }
            match tokio::time::timeout(health_timeout, client.health_check()).await {
                Ok(Ok(())) if client.current_master_addr() == view.leader_address => break,
                Ok(Ok(())) | Ok(Err(_)) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(_) => {
                    // Cancelling health_check can interrupt remount_all after it
                    // marks the remount in progress. Do not reuse that client and
                    // accidentally treat the next no-op remount as completion.
                    return Err(format!(
                        "client {client_index} health/remount timed out on leader {}",
                        view.leader_address
                    ));
                }
            }
        }
    }
    Ok(())
}

fn scenario_failure(stage: &str, message: impl Into<String>) -> FailureRecord {
    FailureRecord {
        stage: stage.into(),
        message: message.into(),
    }
}

fn advance_seeded_index(state: &mut u64, upper_bound: usize) -> usize {
    *state = state
        .wrapping_mul(LCG_MULTIPLIER)
        .wrapping_add(LCG_INCREMENT);
    ((*state >> 32) as usize) % upper_bound
}

async fn run_small_scenario(
    config: &GateConfig,
    cluster: &mut MasterCluster,
    clients: &mut [MooncakeClient],
    schedule: &ChaosSchedule,
) -> Result<ScenarioResult, FailureRecord> {
    const KEY_COUNT: usize = 100;
    const UNSTABLE_ATTEMPTS_PER_ROUND: usize = 6;

    if clients.len() < 3 {
        return Err(scenario_failure(
            "setup",
            format!(
                "small scenario requires at least three clients, got {}",
                clients.len()
            ),
        ));
    }
    if schedule.rounds.len() < CANONICAL_ROUNDS {
        return Err(scenario_failure(
            "setup",
            format!(
                "small scenario requires at least {CANONICAL_ROUNDS} rounds, got {}",
                schedule.rounds.len()
            ),
        ));
    }

    let keys = (0..KEY_COUNT)
        .map(|key_index| format!("ha-small-{:016x}-{key_index}", config.seed))
        .collect::<Vec<_>>();
    let expected_values = (0..KEY_COUNT)
        .map(|key_index| format!("small:0x{:016x}:{key_index}", config.seed).into_bytes())
        .collect::<Vec<_>>();
    let coordinator = tokio::time::timeout(
        Duration::from_secs(10),
        LeaderCoordinator::new_etcd(
            vec![config.etcd_endpoint.clone()],
            &cluster.cluster_namespace,
        ),
    )
    .await
    .map_err(|_| scenario_failure("setup", "timed out creating small-scenario coordinator"))?
    .map_err(|error| {
        scenario_failure(
            "setup",
            format!("create small-scenario coordinator: {error}"),
        )
    })?;

    let mut evidence = ScenarioEvidence::default();
    let mut rng = config.seed;
    for (round_index, round) in schedule.rounds.iter().enumerate() {
        for &victim in &round.stop {
            cluster.stop(victim).await.map_err(|error| {
                scenario_failure(
                    "stop",
                    format!("round {round_index} failed to stop Master {victim}: {error}"),
                )
            })?;
            evidence.crashes += 1;
            evidence.stopped_indices.insert(victim);
        }
        if cluster.running_indices().is_empty() {
            return Err(scenario_failure(
                "stop",
                format!("round {round_index} stopped every Master"),
            ));
        }

        for _ in 0..UNSTABLE_ATTEMPTS_PER_ROUND {
            let key_index = advance_seeded_index(&mut rng, KEY_COUNT);
            let put_client = advance_seeded_index(&mut rng, clients.len());
            if matches!(
                tokio::time::timeout(
                    Duration::from_secs(1),
                    clients[put_client].put(&keys[key_index], &expected_values[key_index], None,),
                )
                .await,
                Ok(Ok(()))
            ) {
                evidence.successful_unstable_operations += 1;
            }

            let mut get_client = advance_seeded_index(&mut rng, clients.len());
            if get_client == put_client {
                get_client = (get_client + 1) % clients.len();
            }
            if let Ok(Ok(actual)) = tokio::time::timeout(
                Duration::from_secs(1),
                clients[get_client].get(&keys[key_index]),
            )
            .await
            {
                evidence.successful_unstable_operations += 1;
                evidence.byte_comparisons += 1;
                if actual != expected_values[key_index] {
                    return Err(scenario_failure(
                        "unstable-read",
                        format!(
                            "round {round_index} client {get_client} returned wrong bytes for {}",
                            keys[key_index]
                        ),
                    ));
                }
            }
        }

        let stable_view = wait_for_stable_leader(
            &coordinator,
            cluster,
            &mut [],
            Instant::now() + Duration::from_secs(45),
        )
        .await
        .map_err(|error| {
            scenario_failure(
                "stabilize",
                format!("round {round_index} failed to stabilize after crashes: {error}"),
            )
        })?;
        evidence.leader_view_versions.push(stable_view.view_version);
        recover_clients(clients, &stable_view)
            .await
            .map_err(|error| {
                scenario_failure(
                    "recover",
                    format!("round {round_index} failed to recover clients: {error}"),
                )
            })?;

        let mut stable_put_clients = Vec::with_capacity(KEY_COUNT);
        for key_index in 0..KEY_COUNT {
            let put_client = advance_seeded_index(&mut rng, clients.len());
            tokio::time::timeout(
                Duration::from_secs(10),
                clients[put_client].put(&keys[key_index], &expected_values[key_index], None),
            )
            .await
            .map_err(|_| {
                scenario_failure(
                    "stable-put",
                    format!(
                        "round {round_index} client {put_client} timed out putting {}",
                        keys[key_index]
                    ),
                )
            })?
            .map_err(|error| {
                scenario_failure(
                    "stable-put",
                    format!(
                        "round {round_index} client {put_client} failed putting {}: {error}",
                        keys[key_index]
                    ),
                )
            })?;
            stable_put_clients.push(put_client);
        }

        for key_index in 0..KEY_COUNT {
            let put_client = stable_put_clients[key_index];
            let read_offset = 1 + advance_seeded_index(&mut rng, clients.len() - 1);
            let read_client = (put_client + read_offset) % clients.len();
            let actual = tokio::time::timeout(
                Duration::from_secs(10),
                clients[read_client].get(&keys[key_index]),
            )
            .await
            .map_err(|_| {
                scenario_failure(
                    "stable-read",
                    format!(
                        "round {round_index} client {read_client} timed out reading {}",
                        keys[key_index]
                    ),
                )
            })?
            .map_err(|error| {
                scenario_failure(
                    "stable-read",
                    format!(
                        "round {round_index} client {read_client} failed reading {}: {error}",
                        keys[key_index]
                    ),
                )
            })?;
            evidence.byte_comparisons += 1;
            if actual != expected_values[key_index] {
                return Err(scenario_failure(
                    "stable-read",
                    format!(
                        "round {round_index} client {read_client} returned wrong bytes for {}",
                        keys[key_index]
                    ),
                ));
            }
            evidence.stable_exact_reads += 1;
        }

        for &victim in &round.restart {
            cluster.restart(victim).await.map_err(|error| {
                scenario_failure(
                    "restart",
                    format!("round {round_index} failed to restart Master {victim}: {error}"),
                )
            })?;
            evidence.restarts += 1;
            evidence.restarted_indices.insert(victim);
        }
        let restarted_view = wait_for_stable_leader(
            &coordinator,
            cluster,
            &mut [],
            Instant::now() + Duration::from_secs(45),
        )
        .await
        .map_err(|error| {
            scenario_failure(
                "restart",
                format!("round {round_index} failed to stabilize after restart: {error}"),
            )
        })?;
        evidence
            .leader_view_versions
            .push(restarted_view.view_version);
        recover_clients(clients, &restarted_view)
            .await
            .map_err(|error| {
                scenario_failure(
                    "recover",
                    format!("round {round_index} failed to recover clients after restart: {error}"),
                )
            })?;
    }

    let all_indices = BTreeSet::from([0, 1, 2]);
    if evidence.successful_unstable_operations == 0 {
        return Err(scenario_failure(
            "evidence",
            "small scenario produced no successful unstable operations",
        ));
    }
    let minimum_exact_reads = (KEY_COUNT * schedule.rounds.len()) as u64;
    if evidence.stable_exact_reads < minimum_exact_reads {
        return Err(scenario_failure(
            "evidence",
            format!(
                "small scenario recorded {} stable exact reads, expected at least {minimum_exact_reads}",
                evidence.stable_exact_reads
            ),
        ));
    }
    if evidence.stopped_indices != all_indices || evidence.restarted_indices != all_indices {
        return Err(scenario_failure(
            "evidence",
            format!(
                "small scenario victim coverage mismatch: stopped={:?}, restarted={:?}",
                evidence.stopped_indices, evidence.restarted_indices
            ),
        ));
    }

    Ok(ScenarioResult {
        status: ResultStatus::Pass,
        evidence,
    })
}

fn publish_failed_gate_result(
    config: &GateConfig,
    cluster: &MasterCluster,
    scenarios: BTreeMap<ScenarioKind, ScenarioResult>,
    failure: FailureRecord,
) -> Result<(), String> {
    let mut result = GateResult::failed(
        config.seed,
        config.etcd_endpoint.clone(),
        config.cluster_namespace.clone(),
        cluster.addresses().into_iter().collect(),
        scenarios,
        failure,
    );
    result.master_logs = cluster
        .slots
        .iter()
        .map(|slot| slot.log_path.display().to_string())
        .collect();
    result.write_atomic(&config.result_path)
}

struct RemountCheckpoint {
    clients: Vec<MooncakeClient>,
    initial_view: MasterView,
    failover_view: MasterView,
    restarted_index: usize,
}

async fn run_remount_checkpoint(
    config: &GateConfig,
    cluster: &mut MasterCluster,
) -> Result<RemountCheckpoint, FailureRecord> {
    let running = cluster.running_indices();
    if running != BTreeSet::from([0, 1, 2]) {
        return Err(scenario_failure(
            "remount-setup",
            format!("expected three running Masters, got {running:?}"),
        ));
    }
    let initial_view = wait_for_stable_leader_without_clients(config, cluster)
        .await
        .map_err(|error| scenario_failure("remount-setup", error))?;
    if !cluster.addresses().contains(&initial_view.leader_address) {
        return Err(scenario_failure(
            "remount-setup",
            format!(
                "initial leader {} is not one of the three Masters",
                initial_view.leader_address
            ),
        ));
    }
    let mut masters = vec![initial_view.leader_address.clone()];
    masters.extend(
        cluster
            .addresses()
            .into_iter()
            .filter(|address| address != &initial_view.leader_address),
    );
    let mut clients = create_clients(config, &masters, 3, CLIENT_SEGMENT_SIZE)
        .await
        .map_err(|error| scenario_failure("remount-setup", error))?;
    tokio::time::timeout(
        Duration::from_secs(15),
        clients[0].put("ha-small-bootstrap", b"bootstrap-value", None),
    )
    .await
    .map_err(|_| scenario_failure("remount-bootstrap-put", "bootstrap put timed out"))?
    .map_err(|error| {
        scenario_failure(
            "remount-bootstrap-put",
            format!("bootstrap put failed: {error}"),
        )
    })?;

    let restarted_index = cluster
        .index_for_address(&initial_view.leader_address)
        .ok_or_else(|| {
            scenario_failure(
                "remount-failover",
                format!(
                    "cannot locate initial leader {}",
                    initial_view.leader_address
                ),
            )
        })?;
    cluster.stop(restarted_index).await.map_err(|error| {
        scenario_failure(
            "remount-failover",
            format!("stop initial leader {restarted_index}: {error}"),
        )
    })?;
    let failover_view = wait_for_stable_leader_without_clients(config, cluster)
        .await
        .map_err(|error| scenario_failure("remount-failover", error))?;
    if failover_view.view_version == initial_view.view_version
        || failover_view.leader_address == initial_view.leader_address
    {
        return Err(scenario_failure(
            "remount-failover",
            format!("leader did not change: initial={initial_view:?}, failover={failover_view:?}"),
        ));
    }
    recover_clients(&mut clients, &failover_view)
        .await
        .map_err(|error| scenario_failure("remount-recover", error))?;
    let bootstrap = tokio::time::timeout(
        Duration::from_secs(15),
        clients[2].get("ha-small-bootstrap"),
    )
    .await
    .map_err(|_| scenario_failure("remount-bootstrap-read", "bootstrap read timed out"))?
    .map_err(|error| {
        scenario_failure(
            "remount-bootstrap-read",
            format!("bootstrap read failed: {error}"),
        )
    })?;
    if bootstrap != b"bootstrap-value" {
        return Err(scenario_failure(
            "remount-bootstrap-read",
            format!("bootstrap byte mismatch: got {bootstrap:?}"),
        ));
    }

    cluster.restart(restarted_index).await.map_err(|error| {
        scenario_failure(
            "remount-restart",
            format!("restart initial leader {restarted_index}: {error}"),
        )
    })?;
    let running = cluster.running_indices();
    if running != BTreeSet::from([0, 1, 2]) {
        return Err(scenario_failure(
            "remount-restart",
            format!("expected three running Masters after restart, got {running:?}"),
        ));
    }
    let restored_view = wait_for_stable_leader_without_clients(config, cluster)
        .await
        .map_err(|error| scenario_failure("remount-restart", error))?;
    recover_clients(&mut clients, &restored_view)
        .await
        .map_err(|error| scenario_failure("remount-recover", error))?;

    Ok(RemountCheckpoint {
        clients,
        initial_view,
        failover_view,
        restarted_index,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn three_master_etcd_ha_chaos_preserves_small_and_large_object_bytes() {
    let Some(config) = GateConfig::from_env().expect("valid HA chaos environment") else {
        eprintln!("SKIP test_ha_chaos_live: MOONCAKE_RUN_HA_CHAOS is not enabled");
        return;
    };

    let mut cluster = MasterCluster::start(&config)
        .await
        .expect("start three Masters");
    let checkpoint = match run_remount_checkpoint(&config, &mut cluster).await {
        Ok(checkpoint) => checkpoint,
        Err(failure) => {
            let publication = publish_failed_gate_result(
                &config,
                &cluster,
                complete_scenarios(ResultStatus::Fail),
                failure.clone(),
            );
            panic!(
                "remount checkpoint failed at {}: {}; result publication: {publication:?}",
                failure.stage, failure.message
            );
        }
    };
    let RemountCheckpoint {
        mut clients,
        initial_view: view,
        failover_view: next,
        restarted_index: leader,
    } = checkpoint;

    eprintln!(
        "PASS lifecycle checkpoint: leader {} view {} failed over to {} view {}; restarted slot {}",
        view.leader_address, view.view_version, next.leader_address, next.view_version, leader
    );
    let checkpoint_path = config.artifact_root.join("lifecycle-checkpoint.json");
    std::fs::write(
        &checkpoint_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "status": "PASS",
            "seed": format!("0x{:016x}", config.seed),
            "cluster_namespace": config.cluster_namespace.clone(),
            "masters": cluster.addresses(),
            "initial_view": {
                "leader_address": view.leader_address,
                "view_version": view.view_version,
            },
            "failover_view": {
                "leader_address": next.leader_address,
                "view_version": next.view_version,
            },
            "restarted_index": leader,
            "running_indices_after_restart": cluster.running_indices(),
        }))
        .expect("serialize lifecycle checkpoint"),
    )
    .expect("write lifecycle checkpoint");
    let remount_checkpoint_path = config.artifact_root.join("remount-checkpoint.json");
    std::fs::write(
        &remount_checkpoint_path,
        serde_json::to_vec_pretty(&serde_json::json!({
            "status": "PASS",
            "key": "ha-small-bootstrap",
            "value": "bootstrap-value",
            "client_count": clients.len(),
            "segment_size": CLIENT_SEGMENT_SIZE,
            "initial_view_version": view.view_version,
            "failover_view_version": next.view_version,
            "recovered_leader": next.leader_address,
            "read_client_index": 2,
        }))
        .expect("serialize remount checkpoint"),
    )
    .expect("write remount checkpoint");

    let schedule = ChaosSchedule::new(config.seed, config.rounds).expect("valid chaos schedule");
    let mut scenarios = complete_scenarios(ResultStatus::Fail);
    let small_result =
        match run_small_scenario(&config, &mut cluster, &mut clients, &schedule).await {
            Ok(result) => result,
            Err(failure) => {
                publish_failed_gate_result(&config, &cluster, scenarios, failure.clone())
                    .expect("publish failed small-scenario result");
                panic!(
                    "small HA chaos failed at {}: {}",
                    failure.stage, failure.message
                );
            }
        };
    std::fs::write(
        config.artifact_root.join("small-checkpoint.json"),
        serde_json::to_vec_pretty(&small_result).expect("serialize small checkpoint"),
    )
    .expect("write small checkpoint");
    scenarios.insert(ScenarioKind::Small, small_result);
    publish_failed_gate_result(
        &config,
        &cluster,
        scenarios,
        scenario_failure("scenario", "large HA chaos workload is not implemented yet"),
    )
    .expect("publish small-scenario checkpoint result");
}
