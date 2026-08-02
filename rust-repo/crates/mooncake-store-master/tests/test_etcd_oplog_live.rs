use mooncake_store_master::ha::OpLogRecord;
use mooncake_store_master::oplog::{EtcdOpLogStore, OpLogStore};
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
    prefix: String,
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
        let store = EtcdOpLogStore::new_leader(client.clone(), &prefix, election_key, view)
            .await
            .expect("create fenced etcd oplog writer");
        Some(Self {
            client,
            prefix,
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
    let reader = EtcdOpLogStore::new(fixture.client.clone(), &fixture.prefix)
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
