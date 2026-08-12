use mooncake_store_master::ha::{LeaderCoordinator, LeaderRole, OpLogRecord};
use mooncake_store_master::oplog::{EtcdOpLogStore, OpLogManager, OpLogStore};
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

fn live_etcd_enabled() -> bool {
    matches!(
        std::env::var("MOONCAKE_ETCD_OPLOG_E2E").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

fn live_etcd_endpoint() -> String {
    std::env::var("MOONCAKE_ETCD_OPLOG_E2E_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:2379".to_string())
}

fn opaque_record(view: u64, payload: &str) -> OpLogRecord {
    OpLogRecord {
        seq: 0,
        producer_view_version: view,
        payload: payload.to_string(),
    }
}

fn put_end_record(view: u64, key: &str, payload: &str) -> OpLogRecord {
    OpLogRecord {
        seq: 0,
        producer_view_version: view,
        payload: serde_json::json!({
            "op": "put_end",
            "schema_version": 1,
            "key": key,
            "payload": payload,
        })
        .to_string(),
    }
}

struct LiveEtcdFixture {
    client: etcd_client::Client,
    namespace: String,
    prefix: String,
    election_key: String,
    view: u64,
    store: EtcdOpLogStore,
}

impl LiveEtcdFixture {
    async fn new(label: &str) -> Option<Self> {
        if !live_etcd_enabled() {
            eprintln!("skipping live etcd oplog e2e; set MOONCAKE_ETCD_OPLOG_E2E=1 to enable");
            return None;
        }

        let mut client = etcd_client::Client::connect([live_etcd_endpoint()], None)
            .await
            .expect("connect live etcd");
        let namespace = format!("ha-etcd-oplog-{label}-{}", Uuid::new_v4().simple());
        let prefix = format!("/oplog/{namespace}");
        let election_key = format!("/election/{namespace}");
        let response = client
            .put(election_key.as_str(), "owner", None)
            .await
            .expect("publish writer election key");
        let view = u64::try_from(
            response
                .header()
                .expect("etcd put response header")
                .revision(),
        )
        .expect("positive etcd revision");
        let store =
            EtcdOpLogStore::new_leader(client.clone(), &namespace, election_key.clone(), view)
                .await
                .expect("create fenced etcd oplog writer");
        Some(Self {
            client,
            namespace,
            prefix,
            election_key,
            view,
            store,
        })
    }

    async fn latest_from_etcd(&self) -> u64 {
        let mut client = self.client.clone();
        let response = client
            .get(format!("{}/latest", self.prefix), None)
            .await
            .expect("read durable latest pointer");
        let value = response
            .kvs()
            .first()
            .expect("durable latest value")
            .value();
        std::str::from_utf8(value)
            .unwrap()
            .parse()
            .expect("decimal latest sequence")
    }

    async fn put_raw_entry_and_latest(&mut self, sequence: u64, value: &[u8]) {
        self.client
            .put(format!("{}/{sequence:020}", self.prefix), value, None)
            .await
            .expect("plant raw etcd oplog entry");
        self.client
            .put(
                format!("{}/latest", self.prefix),
                sequence.to_string(),
                None,
            )
            .await
            .expect("plant matching latest pointer");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_high_availability_test_basic_master_view_operations() {
    if !live_etcd_enabled() {
        eprintln!("skipping live etcd oplog e2e; set MOONCAKE_ETCD_OPLOG_E2E=1 to enable");
        return;
    }

    let namespace = format!("ha-etcd-view-basic-{}", Uuid::new_v4().simple());
    let coordinator = LeaderCoordinator::new_etcd(vec![live_etcd_endpoint()], &namespace)
        .await
        .unwrap();
    assert!(coordinator.read_current_view().await.unwrap().is_none());

    let first = coordinator
        .try_acquire_leadership("0.0.0.0:8888", 2)
        .await
        .unwrap();
    assert!(first.acquired);
    let first_session = first.session.unwrap();
    coordinator
        .try_renew_leadership(&first_session)
        .await
        .unwrap();
    let keepalive = coordinator
        .start_leadership_keepalive(&first_session)
        .await
        .unwrap();

    let current = coordinator
        .read_current_view()
        .await
        .unwrap()
        .expect("leader view after acquisition");
    assert_eq!(current.leader_address, "0.0.0.0:8888");
    assert_eq!(current.view_version, first_session.view.view_version);

    let stable_started = Instant::now();
    assert_eq!(
        coordinator
            .wait_for_view_change(first_session.view.view_version, Duration::from_millis(250))
            .await
            .unwrap(),
        None
    );
    assert!(stable_started.elapsed() >= Duration::from_millis(200));
    assert!(stable_started.elapsed() < Duration::from_secs(1));

    tokio::time::sleep(Duration::from_secs(4)).await;
    let renewed = coordinator.read_current_view().await.unwrap().unwrap();
    assert_eq!(renewed, first_session.view);

    coordinator
        .release_leadership(&first_session)
        .await
        .unwrap();
    drop(keepalive);
    let released_started = Instant::now();
    assert_eq!(
        coordinator
            .wait_for_view_change(first_session.view.view_version, Duration::from_secs(2))
            .await
            .unwrap(),
        None
    );
    assert!(released_started.elapsed() < Duration::from_secs(1));
    assert!(coordinator.read_current_view().await.unwrap().is_none());

    let second = coordinator
        .try_acquire_leadership("0.0.0.0:9999", 2)
        .await
        .unwrap();
    assert!(second.acquired);
    let second_session = second.session.unwrap();
    assert_eq!(second_session.view.leader_address, "0.0.0.0:9999");
    assert!(second_session.view.view_version > first_session.view.view_version);
    coordinator
        .release_leadership(&second_session)
        .await
        .unwrap();
    assert!(coordinator.read_current_view().await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_high_availability_test_wait_for_view_change_returns_promptly_on_leader_loss() {
    if !live_etcd_enabled() {
        eprintln!("skipping live etcd oplog e2e; set MOONCAKE_ETCD_OPLOG_E2E=1 to enable");
        return;
    }

    let namespace = format!("ha-etcd-view-loss-{}", Uuid::new_v4().simple());
    let coordinator = Arc::new(
        LeaderCoordinator::new_etcd(vec![live_etcd_endpoint()], &namespace)
            .await
            .unwrap(),
    );
    let acquired = coordinator
        .try_acquire_leadership("0.0.0.0:5555", 5)
        .await
        .unwrap();
    assert!(acquired.acquired);
    let session = acquired.session.unwrap();
    coordinator.try_renew_leadership(&session).await.unwrap();

    let releaser = Arc::clone(&coordinator);
    let release_session = session.clone();
    let (released_tx, mut released_rx) = tokio::sync::oneshot::channel();
    let started = Instant::now();
    let release_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        releaser.release_leadership(&release_session).await.unwrap();
        released_tx.send(()).unwrap();
    });
    assert_eq!(
        coordinator
            .wait_for_view_change(session.view.view_version, Duration::from_secs(5))
            .await
            .unwrap(),
        None
    );
    let elapsed = started.elapsed();
    released_rx
        .try_recv()
        .expect("leadership release completes before the wait returns");
    release_task.await.unwrap();
    assert!(elapsed >= Duration::from_millis(300));
    assert!(elapsed < Duration::from_secs(3));
    assert!(coordinator.read_current_view().await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_high_availability_test_wait_for_view_change_times_out_when_stable() {
    if !live_etcd_enabled() {
        eprintln!("skipping live etcd oplog e2e; set MOONCAKE_ETCD_OPLOG_E2E=1 to enable");
        return;
    }

    let namespace = format!("ha-etcd-view-stable-{}", Uuid::new_v4().simple());
    let coordinator = LeaderCoordinator::new_etcd(vec![live_etcd_endpoint()], &namespace)
        .await
        .unwrap();
    let acquired = coordinator
        .try_acquire_leadership("0.0.0.0:4444", 5)
        .await
        .unwrap();
    assert!(acquired.acquired);
    let session = acquired.session.unwrap();
    coordinator.try_renew_leadership(&session).await.unwrap();

    let started = Instant::now();
    assert_eq!(
        coordinator
            .wait_for_view_change(session.view.view_version, Duration::from_millis(500))
            .await
            .unwrap(),
        None
    );
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(400));
    assert!(elapsed < Duration::from_secs(3));
    assert_eq!(
        coordinator.read_current_view().await.unwrap(),
        Some(session.view.clone())
    );
    coordinator.release_leadership(&session).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_high_availability_test_wait_for_view_change_returns_current_view_immediately() {
    if !live_etcd_enabled() {
        eprintln!("skipping live etcd oplog e2e; set MOONCAKE_ETCD_OPLOG_E2E=1 to enable");
        return;
    }

    let namespace = format!("ha-etcd-view-current-{}", Uuid::new_v4().simple());
    let coordinator = LeaderCoordinator::new_etcd(vec![live_etcd_endpoint()], &namespace)
        .await
        .unwrap();
    let acquired = coordinator
        .try_acquire_leadership("0.0.0.0:3333", 5)
        .await
        .unwrap();
    assert!(acquired.acquired);
    let session = acquired.session.unwrap();

    let started = Instant::now();
    let current = coordinator
        .wait_for_view_change(0, Duration::from_secs(5))
        .await
        .unwrap()
        .expect("known version zero discovers the positive etcd view version");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(current, session.view);
    coordinator.release_leadership(&session).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_high_availability_test_leadership_monitor_reports_keepalive_loss() {
    if !live_etcd_enabled() {
        eprintln!("skipping live etcd oplog e2e; set MOONCAKE_ETCD_OPLOG_E2E=1 to enable");
        return;
    }

    let namespace = format!("ha-etcd-view-monitor-{}", Uuid::new_v4().simple());
    let coordinator = LeaderCoordinator::new_etcd(vec![live_etcd_endpoint()], &namespace)
        .await
        .unwrap();
    let acquired = coordinator
        .try_acquire_leadership("0.0.0.0:7777", 3)
        .await
        .unwrap();
    assert!(acquired.acquired);
    let session = acquired.session.unwrap();
    coordinator.try_renew_leadership(&session).await.unwrap();
    let mut role = coordinator.subscribe_role_for_session(&session).unwrap();
    let mut loss = coordinator.subscribe_loss_for_session(&session).unwrap();
    assert_eq!(*role.borrow(), LeaderRole::Leader);
    let keepalive = coordinator
        .start_leadership_keepalive(&session)
        .await
        .unwrap();

    let lease_id = session.owner_token.parse::<i64>().unwrap();
    let mut external = etcd_client::Client::connect([live_etcd_endpoint()], None)
        .await
        .unwrap();
    external.lease_revoke(lease_id).await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while *role.borrow_and_update() != LeaderRole::Standby {
            role.changed().await.unwrap();
        }
    })
    .await
    .expect("keepalive loss demotes the production role receiver within five seconds");
    let loss_event = tokio::time::timeout(Duration::from_secs(5), loss.recv())
        .await
        .expect("keepalive loss publishes a session loss event within five seconds")
        .unwrap();
    assert_eq!(loss_event.owner_token, session.owner_token);
    assert_eq!(loss_event.view_version, session.view.view_version);
    assert_eq!(*role.borrow(), LeaderRole::Standby);
    assert!(coordinator.read_current_view().await.unwrap().is_none());
    drop(keepalive);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_high_availability_test_leadership_monitor_ignores_explicit_release() {
    if !live_etcd_enabled() {
        eprintln!("skipping live etcd oplog e2e; set MOONCAKE_ETCD_OPLOG_E2E=1 to enable");
        return;
    }

    let namespace = format!("ha-etcd-view-release-{}", Uuid::new_v4().simple());
    let coordinator = LeaderCoordinator::new_etcd(vec![live_etcd_endpoint()], &namespace)
        .await
        .unwrap();
    let acquired = coordinator
        .try_acquire_leadership("0.0.0.0:6666", 3)
        .await
        .unwrap();
    assert!(acquired.acquired);
    let session = acquired.session.unwrap();
    coordinator.try_renew_leadership(&session).await.unwrap();
    let mut loss = coordinator.subscribe_loss_for_session(&session).unwrap();
    let keepalive = coordinator
        .start_leadership_keepalive(&session)
        .await
        .unwrap();

    coordinator.release_leadership(&session).await.unwrap();
    assert_eq!(*coordinator.subscribe_role().borrow(), LeaderRole::Standby);
    assert!(coordinator.read_current_view().await.unwrap().is_none());
    assert!(
        tokio::time::timeout(Duration::from_secs(1), loss.recv())
            .await
            .is_err(),
        "explicit release must not publish a leadership-loss event"
    );
    drop(keepalive);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_high_availability_test_etcd_store_prefix_watch_cancel_does_not_report_broken() {
    let Some(mut fixture) = LiveEtcdFixture::new("notifier-cancel").await else {
        return;
    };
    let mut notifier = fixture
        .store
        .create_change_notifier()
        .expect("etcd oplog store exposes its production prefix notifier");
    let (entry_tx, entry_rx) = std::sync::mpsc::channel();
    let (error_tx, error_rx) = std::sync::mpsc::channel();
    notifier
        .start(
            1,
            Box::new(move |entry| entry_tx.send(entry).unwrap()),
            Box::new(move |error| error_tx.send(error).unwrap()),
        )
        .unwrap();
    let healthy_deadline = Instant::now() + Duration::from_secs(5);
    while !notifier.is_healthy() && Instant::now() < healthy_deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(notifier.is_healthy(), "prefix watch becomes active");

    let sequence = fixture
        .store
        .append(&opaque_record(fixture.view, "watch-cancel-value"))
        .unwrap();
    fixture.store.flush_async().await.unwrap();
    let observed = entry_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(sequence, 1);
    assert_eq!(observed.seq, 1);
    assert_eq!(observed.payload, "watch-cancel-value");
    assert!(
        notifier.is_healthy(),
        "active watch remains healthy before stop"
    );

    notifier.stop();
    assert!(!notifier.is_healthy());
    std::thread::sleep(Duration::from_millis(200));
    assert!(error_rx.try_recv().is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_ha_oplog_etcd_oplog_store_test_cpp_etcdoplogstoretest_testwriteoplog() {
    let Some(mut fixture) = LiveEtcdFixture::new("write").await else {
        return;
    };

    assert_eq!(
        fixture
            .store
            .append(&opaque_record(fixture.view, "value1"))
            .unwrap(),
        1
    );
    fixture.store.flush_async().await.unwrap();

    assert_eq!(fixture.latest_from_etcd().await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_ha_oplog_etcd_oplog_store_test_cpp_etcdoplogstoretest_testreadoplog() {
    let Some(mut fixture) = LiveEtcdFixture::new("read").await else {
        return;
    };
    fixture
        .store
        .append(&opaque_record(fixture.view, "ignored"))
        .unwrap();
    fixture
        .store
        .append(&put_end_record(fixture.view, "key2", "value2"))
        .unwrap();
    fixture.store.flush_async().await.unwrap();

    let entries = fixture.store.read_since_async(2, 1).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].seq, 2);
    assert_eq!(entries[0].producer_view_version, fixture.view);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&entries[0].payload).unwrap(),
        serde_json::json!({
            "op": "put_end",
            "schema_version": 1,
            "key": "key2",
            "payload": "value2"
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_ha_oplog_etcd_oplog_store_test_cpp_etcdoplogstoretest_testreadoplogsince_empty()
{
    let Some(fixture) = LiveEtcdFixture::new("read-empty").await else {
        return;
    };

    let entries = fixture.store.read_since_async(1_000, 100).await.unwrap();
    assert!(entries.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_ha_oplog_etcd_oplog_store_test_cpp_etcdoplogstoretest_testreadoplogsince_limit()
{
    let Some(mut fixture) = LiveEtcdFixture::new("read-limit").await else {
        return;
    };
    for payload in ["value1", "value2", "value3", "value4", "value5"] {
        fixture
            .store
            .append(&opaque_record(fixture.view, payload))
            .unwrap();
    }
    fixture.store.flush_async().await.unwrap();

    let entries = fixture.store.read_since_async(1, 3).await.unwrap();
    assert_eq!(
        entries.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.payload.as_str())
            .collect::<Vec<_>>(),
        ["value1", "value2", "value3"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_ha_oplog_etcd_oplog_store_test_cpp_etcdoplogstoretest_testdeserializeinvalidjson()
 {
    let Some(mut fixture) = LiveEtcdFixture::new("invalid-json").await else {
        return;
    };
    fixture.put_raw_entry_and_latest(77, b"{invalid-json").await;
    let reader = EtcdOpLogStore::new(fixture.client.clone(), &fixture.namespace)
        .await
        .unwrap();

    let error = reader.read_since_async(77, 1).await.unwrap_err();
    assert!(error.to_string().contains("corrupt etcd oplog record"));
    assert!(error.to_string().contains("00000000000000000077"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_ha_oplog_etcd_oplog_store_test_cpp_etcdoplogstoretest_testcleanupoplogbeforeandboundary()
 {
    let Some(mut fixture) = LiveEtcdFixture::new("cleanup-boundary").await else {
        return;
    };
    for payload in ["value1", "value2", "value3", "value4", "value5"] {
        fixture
            .store
            .append(&opaque_record(fixture.view, payload))
            .unwrap();
    }
    fixture.store.flush_async().await.unwrap();

    fixture.store.cleanup_before(3).unwrap();

    let entries = fixture.store.read_since_async(1, 10).await.unwrap();
    assert_eq!(
        entries.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
        [3, 4, 5]
    );
    assert_eq!(fixture.latest_from_etcd().await, 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_ha_oplog_etcd_oplog_store_test_cpp_etcdoplogstoretest_testserializedeserializeroundtrip()
 {
    let Some(mut fixture) = LiveEtcdFixture::new("serializer-roundtrip").await else {
        return;
    };
    fixture.store.update_latest_sequence_id(41).unwrap();
    let sequence = fixture
        .store
        .append(&put_end_record(
            fixture.view,
            "roundtrip-key",
            "roundtrip-value",
        ))
        .unwrap();
    fixture.store.flush_async().await.unwrap();

    let entries = fixture.store.read_since_async(42, 1).await.unwrap();
    assert_eq!(sequence, 42);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].seq, 42);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&entries[0].payload).unwrap(),
        serde_json::json!({
            "op": "put_end",
            "schema_version": 1,
            "key": "roundtrip-key",
            "payload": "roundtrip-value"
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_ha_oplog_etcd_oplog_store_test_cpp_etcdoplogstoretest_testgetlatestsequenceid()
{
    let Some(mut fixture) = LiveEtcdFixture::new("latest-two").await else {
        return;
    };
    fixture
        .store
        .append(&opaque_record(fixture.view, "v1"))
        .unwrap();
    fixture
        .store
        .append(&opaque_record(fixture.view, "v2"))
        .unwrap();
    fixture.store.flush_async().await.unwrap();

    assert_eq!(fixture.latest_from_etcd().await, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_ha_oplog_etcd_oplog_store_test_cpp_etcdoplogstoretest_testcleanupoplogbefore_empty()
 {
    let Some(mut fixture) = LiveEtcdFixture::new("cleanup-empty").await else {
        return;
    };

    fixture.store.cleanup_before(100).unwrap();

    assert!(
        fixture
            .store
            .read_since_async(1, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_ha_oplog_etcd_oplog_store_test_cpp_etcdoplogstoretest_testclusteridnormalization()
 {
    let Some(fixture) = LiveEtcdFixture::new("prefix-normalization").await else {
        return;
    };
    let trailing_cluster_id = format!(
        "{}///",
        fixture
            .prefix
            .strip_prefix("/oplog/")
            .expect("fixture uses the production oplog prefix")
    );
    let mut normalized_writer = EtcdOpLogStore::new_leader(
        fixture.client.clone(),
        &trailing_cluster_id,
        fixture.election_key.clone(),
        fixture.view,
    )
    .await
    .unwrap();
    normalized_writer.update_latest_sequence_id(998).unwrap();
    let sequence = normalized_writer
        .append(&put_end_record(fixture.view, "norm-key", "norm-val"))
        .unwrap();
    normalized_writer.flush_async().await.unwrap();

    let entries = fixture.store.read_since_async(999, 1).await.unwrap();
    assert_eq!(sequence, 999);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].seq, 999);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&entries[0].payload).unwrap(),
        serde_json::json!({
            "op": "put_end",
            "schema_version": 1,
            "key": "norm-key",
            "payload": "norm-val"
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_ha_oplog_etcd_oplog_store_test_cpp_etcdoplogstoretest_testreadoplogsince_pagination()
 {
    let Some(mut fixture) = LiveEtcdFixture::new("pagination-20").await else {
        return;
    };
    for sequence in 1..=20 {
        fixture
            .store
            .append(&opaque_record(fixture.view, &format!("value-{sequence}")))
            .unwrap();
    }
    fixture.store.flush_async().await.unwrap();

    let entries = fixture.store.read_since_async(1, 20).await.unwrap();
    assert_eq!(entries.len(), 20);
    for (index, entry) in entries.iter().enumerate() {
        let sequence = u64::try_from(index).unwrap() + 1;
        assert_eq!(entry.seq, sequence);
        assert_eq!(entry.payload, format!("value-{sequence}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_ha_oplog_etcd_oplog_store_test_cpp_etcdoplogstoretest_testreadoplogsince_largedataset()
 {
    let Some(mut fixture) = LiveEtcdFixture::new("large-dataset-200").await else {
        return;
    };
    for sequence in 1..=200 {
        fixture
            .store
            .append(&opaque_record(fixture.view, &format!("value-{sequence}")))
            .unwrap();
        if sequence % 100 == 0 {
            fixture.store.flush_async().await.unwrap();
        }
    }

    let entries = fixture.store.read_since_async(1, 150).await.unwrap();
    assert_eq!(entries.len(), 150);
    for (index, entry) in entries.iter().enumerate() {
        let sequence = u64::try_from(index).unwrap() + 1;
        assert_eq!(entry.seq, sequence);
        assert_eq!(entry.payload, format!("value-{sequence}"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_ha_oplog_etcd_oplog_store_test_cpp_etcdoplogstoretest_testupdatelatestsequenceid()
 {
    let Some(mut fixture) = LiveEtcdFixture::new("update-latest").await else {
        return;
    };

    fixture.store.update_latest_sequence_id(12_345).unwrap();

    assert_eq!(fixture.latest_from_etcd().await, 12_345);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cpp_parity_ha_oplog_oplog_manager_test_cpp_oplogmanagertest_testwritetoetcd_success() {
    let Some(fixture) = LiveEtcdFixture::new("manager-write").await else {
        return;
    };
    let LiveEtcdFixture {
        client,
        namespace,
        view,
        store,
        ..
    } = fixture;
    let manager = OpLogManager::new(Some(Box::new(store)), view);

    let sequence = manager
        .append_and_persist("manager-live-value".to_string())
        .unwrap();

    assert_eq!(sequence, 1);
    assert_eq!(manager.latest_sequence(), 1);
    let reader = EtcdOpLogStore::new(client, &namespace).await.unwrap();
    let entries = reader.read_since_async(1, 1).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].seq, 1);
    assert_eq!(entries[0].producer_view_version, view);
    assert_eq!(entries[0].payload, "manager-live-value");
}
