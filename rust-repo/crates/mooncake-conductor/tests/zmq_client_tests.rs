use mooncake_conductor::types::{EventType, KVEventData};
use mooncake_conductor::zmq_client::{EventHandler, ZmqClient, ZmqClientConfig};
use rmpv::Value;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Default)]
struct RecordingHandler {
    events: Mutex<Vec<(EventType, i64, String)>>,
}

impl EventHandler for RecordingHandler {
    fn handle_event(&self, event: &KVEventData, dp_rank: i64) {
        let pod_name = match event {
            KVEventData::BlockStored(e) => e.pod_name.clone(),
            KVEventData::BlockRemoved(e) => e.pod_name.clone(),
            KVEventData::AllBlocksCleared(e) => e.pod_name.clone(),
            KVEventData::BlockUpdate(e) => e.pod_name.clone(),
        };
        self.events
            .lock()
            .unwrap()
            .push((event.event_type(), dp_rank, pod_name));
    }
}

struct MockPublisher {
    publisher: zmq::Socket,
    replay_thread: Option<std::thread::JoinHandle<()>>,
    replay_stop: Arc<std::sync::atomic::AtomicBool>,
}

impl MockPublisher {
    fn new(ctx: &zmq::Context) -> Self {
        let publisher = ctx.socket(zmq::PUB).unwrap();
        publisher.bind("tcp://127.0.0.1:*").unwrap();

        let router = ctx.socket(zmq::ROUTER).unwrap();
        router.bind("tcp://127.0.0.1:*").unwrap();

        let replay_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let replay_stop_for_thread = replay_stop.clone();
        let replay_thread = std::thread::spawn(move || {
            router.set_rcvtimeo(50).unwrap();
            while !replay_stop_for_thread.load(std::sync::atomic::Ordering::SeqCst) {
                let Ok(frames) = router.recv_multipart(0) else {
                    continue;
                };
                if let Some(identity) = frames.first() {
                    let _ = router.send_multipart([identity.as_slice(), b"OK"], 0);
                }
            }
        });

        Self {
            publisher,
            replay_thread: Some(replay_thread),
            replay_stop,
        }
    }

    fn endpoint(&self) -> String {
        self.publisher.get_last_endpoint().unwrap().unwrap()
    }

    fn send_vllm_stored(&self, seq: i64) {
        let batch = Value::Array(vec![
            Value::Integer(1700000000i64.into()),
            Value::Array(vec![Value::Array(vec![
                Value::String("BlockStored".into()),
                Value::Array(vec![Value::Integer(100u64.into())]),
                Value::Integer(0u64.into()),
                Value::Array(vec![
                    Value::Integer(1i64.into()),
                    Value::Integer(2i64.into()),
                ]),
                Value::Integer(2i64.into()),
            ])]),
            Value::Integer(7i64.into()),
        ]);
        let mut payload = Vec::new();
        rmpv::encode::write_value(&mut payload, &batch).unwrap();
        self.publisher
            .send_multipart([b"vllm".as_slice(), &seq.to_be_bytes(), &payload], 0)
            .unwrap();
    }
}

impl Drop for MockPublisher {
    fn drop(&mut self) {
        self.replay_stop
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(handle) = self.replay_thread.take() {
            handle.join().unwrap();
        }
    }
}

#[test]
fn test_zmq_client_consumes_vllm_pub_event() {
    let ctx = zmq::Context::new();
    let publisher = MockPublisher::new(&ctx);
    let replay = ctx.socket(zmq::ROUTER).unwrap();
    replay.bind("tcp://127.0.0.1:*").unwrap();

    let handler = Arc::new(RecordingHandler::default());
    let client = Arc::new(
        ZmqClient::new(
            ZmqClientConfig {
                cache_pool_key: "pod-a|tenant-a|7".to_string(),
                endpoint: publisher.endpoint(),
                replay_endpoint: replay.get_last_endpoint().unwrap().unwrap(),
                poll_timeout: Duration::from_millis(25),
                replay_timeout: Duration::from_millis(200),
                reconnect_delay: Duration::from_millis(10),
                ..Default::default()
            },
            handler.clone(),
        )
        .unwrap(),
    );

    client.start().unwrap();
    let client_for_thread = client.clone();
    let loop_thread = std::thread::spawn(move || client_for_thread.run_loop());

    std::thread::sleep(Duration::from_millis(250));
    publisher.send_vllm_stored(1);

    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let events = handler.events.lock().unwrap().clone();
        if !events.is_empty() {
            assert_eq!(
                events,
                vec![(EventType::BlockStored, 7, "pod-a|tenant-a|7".into())]
            );
            client.stop();
            loop_thread.join().unwrap();
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    client.stop();
    loop_thread.join().unwrap();
    panic!("timed out waiting for ZMQ event");
}
