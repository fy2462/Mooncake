use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

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

const CANONICAL_ROUNDS: usize = 4;
const LCG_MULTIPLIER: u64 = 6_364_136_223_846_793_005;
const LCG_INCREMENT: u64 = 1_442_695_040_888_963_407;

#[derive(Debug, Clone)]
struct GateConfig {
    etcd_endpoint: String,
    master_bin: PathBuf,
    result_path: PathBuf,
    artifact_root: PathBuf,
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

        Ok(Some(Self {
            etcd_endpoint,
            master_bin,
            result_path,
            artifact_root,
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
}

#[derive(Debug)]
struct MasterSlot {
    index: usize,
    address: String,
    snapshot_dir: PathBuf,
    log_path: PathBuf,
}

#[derive(Debug)]
struct MasterCluster {
    slots: Vec<MasterSlot>,
}

async fn run_scenario(
    kind: ScenarioKind,
    _config: &GateConfig,
    _cluster: &mut MasterCluster,
    _schedule: &ChaosSchedule,
) -> Result<ScenarioResult, FailureRecord> {
    Err(FailureRecord {
        stage: "scenario".into(),
        message: format!("{kind:?} scenario has not been implemented"),
    })
}

#[tokio::test]
async fn three_master_etcd_ha_chaos_preserves_small_and_large_object_bytes() {
    let Some(config) = GateConfig::from_env().expect("valid HA chaos environment") else {
        eprintln!("SKIP test_ha_chaos_live: MOONCAKE_RUN_HA_CHAOS is not enabled");
        return;
    };

    let result = GateResult::failed(
        config.seed,
        config.etcd_endpoint.clone(),
        "unstarted".into(),
        Vec::new(),
        BTreeMap::from([
            (ScenarioKind::Small, ScenarioResult::failed()),
            (ScenarioKind::Large, ScenarioResult::failed()),
        ]),
        FailureRecord {
            stage: "lifecycle".into(),
            message: "three-Master lifecycle is not implemented yet".into(),
        },
    );
    result
        .write_atomic(&config.result_path)
        .expect("publish default FAIL result");
    panic!("three-Master lifecycle is not implemented yet");
}
