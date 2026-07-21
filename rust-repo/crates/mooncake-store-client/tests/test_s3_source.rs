#[cfg(feature = "s3")]
mod s3_tests {
    use axum::{
        Router,
        body::Body,
        extract::{Request, State},
        http::{Method, StatusCode},
        response::Response,
        routing::get,
    };
    use mooncake_store_client::{RemoteSource, RemoteSourceError, S3Config, S3RemoteSource};
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::net::TcpListener;
    use std::sync::Arc;

    const TEST_BUCKET: &str = "test-bucket";
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    // -------------------------------------------------------------------
    // In-memory S3 mock (single catch-all handler)
    // -------------------------------------------------------------------

    type Store = Arc<Mutex<HashMap<String, Vec<u8>>>>;

    #[derive(Clone)]
    struct ServerState {
        objects: Store,
    }

    async fn handle_all(State(state): State<ServerState>, req: Request) -> Response<Body> {
        let path = req.uri().path();
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        match segments.len() {
            0 => Response::builder()
                .status(StatusCode::OK)
                .body(Body::empty())
                .unwrap(),
            1 => {
                let query = req.uri().query().unwrap_or("");
                if query.contains("list-type") {
                    handle_list(&state, segments[0], query).await
                } else {
                    Response::builder()
                        .status(StatusCode::OK)
                        .body(Body::empty())
                        .unwrap()
                }
            }
            _ => {
                let key = segments[1..].join("/");
                match *req.method() {
                    Method::GET => handle_get(&state, &key).await,
                    Method::PUT => {
                        let body = axum::body::to_bytes(req.into_body(), 10 * 1024 * 1024)
                            .await
                            .unwrap_or_default();
                        state.objects.lock().insert(key, body.to_vec());
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Body::empty())
                            .unwrap()
                    }
                    _ => Response::builder()
                        .status(StatusCode::METHOD_NOT_ALLOWED)
                        .body(Body::empty())
                        .unwrap(),
                }
            }
        }
    }

    async fn handle_get(state: &ServerState, key: &str) -> Response<Body> {
        let store = state.objects.lock();
        if let Some(data) = store.get(key) {
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Length", data.len())
                .body(Body::from(data.clone()))
                .unwrap()
        } else {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(
                    "<?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code></Error>",
                ))
                .unwrap()
        }
    }

    async fn handle_list(state: &ServerState, bucket: &str, query: &str) -> Response<Body> {
        let prefix = query_param(query, "prefix").unwrap_or_default().to_string();
        let max_keys: usize = query_param(query, "max-keys")
            .unwrap_or("100")
            .parse()
            .unwrap_or(100);

        let store = state.objects.lock();
        let mut keys: Vec<String> = store
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();
        keys.sort();

        let is_truncated = keys.len() > max_keys;
        keys.truncate(max_keys);

        let mut contents = String::new();
        for k in &keys {
            let sz = store.get(k).map_or(0, |v| v.len());
            contents.push_str(&format!(
                "<Contents><Key>{k}</Key><Size>{sz}</Size></Contents>"
            ));
        }

        let body = format!(
            "<?xml version=\"1.0\"?>\
             <ListBucketResult>\
               <Name>{bucket}</Name>\
               <Prefix>{prefix}</Prefix>\
               <MaxKeys>{max_keys}</MaxKeys>\
               <IsTruncated>{is_truncated}</IsTruncated>\
               {contents}\
             </ListBucketResult>"
        );

        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/xml")
            .body(Body::from(body))
            .unwrap()
    }

    fn query_param<'a>(query: &'a str, name: &str) -> Option<&'a str> {
        for part in query.split('&') {
            let mut kv = part.splitn(2, '=');
            if kv.next() == Some(name) {
                return Some(kv.next().unwrap_or(""));
            }
        }
        None
    }

    async fn start_mock(objects: Store) -> (String, tokio::task::JoinHandle<()>) {
        let state = ServerState { objects };
        let app = Router::new()
            .route("/*path", get(handle_all).put(handle_all))
            .with_state(state);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let tokio_listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(tokio_listener, app).await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        (format!("http://{addr}"), handle)
    }

    fn s3_config(endpoint: &str) -> S3Config {
        S3Config {
            bucket: TEST_BUCKET.to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some(endpoint.to_string()),
            prefix: String::new(),
            access_key_id: Some("fake".to_string()),
            secret_access_key: Some("fake".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn test_s3_config_env_fallbacks_include_checksum_modes() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION", "when_required");
        std::env::set_var(
            "MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION",
            "when_supported",
        );

        let config = S3Config::default().with_mooncake_env_fallbacks();

        std::env::remove_var("MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION");
        std::env::remove_var("MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION");

        assert_eq!(
            config.request_checksum_calculation.as_deref(),
            Some("when_required")
        );
        assert_eq!(
            config.response_checksum_validation.as_deref(),
            Some("when_supported")
        );
    }

    // -------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_mock_s3_missing_key() {
        let store: Store = Arc::new(Mutex::new(HashMap::new()));
        let (endpoint, _server) = start_mock(store).await;
        let config = s3_config(&endpoint);
        let source = S3RemoteSource::new(&config).await.unwrap();

        let result = source.get("no_such_key").await;
        assert!(matches!(result, Err(RemoteSourceError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_mock_s3_get_existing_key() {
        let store: Store = Arc::new(Mutex::new(HashMap::new()));
        store
            .lock()
            .insert("hello".to_string(), b"world data".to_vec());
        let (endpoint, _server) = start_mock(store.clone()).await;
        let config = s3_config(&endpoint);
        let source = S3RemoteSource::new(&config).await.unwrap();

        let data = source.get("hello").await.unwrap();
        assert_eq!(data, b"world data");
    }

    #[tokio::test]
    async fn test_mock_s3_multiple_keys() {
        let store: Store = Arc::new(Mutex::new(HashMap::new()));
        {
            let mut s = store.lock();
            s.insert("k1".into(), b"alpha".to_vec());
            s.insert("k2".into(), b"beta".to_vec());
            s.insert("k3".into(), b"gamma".to_vec());
        }
        let (endpoint, _server) = start_mock(store.clone()).await;
        let config = s3_config(&endpoint);
        let source = S3RemoteSource::new(&config).await.unwrap();

        assert_eq!(source.get("k1").await.unwrap(), b"alpha");
        assert_eq!(source.get("k2").await.unwrap(), b"beta");
        assert_eq!(source.get("k3").await.unwrap(), b"gamma");
    }

    #[tokio::test]
    async fn test_mock_s3_prefetch_keys() {
        let store: Store = Arc::new(Mutex::new(HashMap::new()));
        {
            let mut s = store.lock();
            s.insert("p1".into(), b"prefetch_1".to_vec());
            s.insert("p2".into(), b"prefetch_2".to_vec());
            s.insert("p3".into(), b"prefetch_3".to_vec());
        }
        let (endpoint, _server) = start_mock(store.clone()).await;
        let config = s3_config(&endpoint);
        let source = S3RemoteSource::new(&config).await.unwrap();

        let keys: Vec<String> = ["p1", "p2", "p3"].iter().map(|s| s.to_string()).collect();
        let results = source.prefetch_keys(&keys).await;
        assert_eq!(results.len(), 3);
        for r in &results {
            assert!(r.is_ok(), "prefetch failure: {:?}", r);
        }
    }

    #[tokio::test]
    async fn test_mock_s3_with_prefix() {
        let store: Store = Arc::new(Mutex::new(HashMap::new()));
        store
            .lock()
            .insert("prefix_hello".to_string(), b"prefixed data".to_vec());
        let (endpoint, _server) = start_mock(store.clone()).await;
        let config = S3Config {
            bucket: TEST_BUCKET.to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some(endpoint),
            prefix: "prefix_".to_string(),
            access_key_id: Some("fake".to_string()),
            secret_access_key: Some("fake".to_string()),
            ..Default::default()
        };
        let source = S3RemoteSource::new(&config).await.unwrap();
        let data = source.get("hello").await.unwrap();
        assert_eq!(data, b"prefixed data");
    }

    // -------------------------------------------------------------------
    // Integration: MissHandler + S3RemoteSource chain
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn test_miss_handler_with_s3() {
        use mooncake_store_client::{MissHandler, RemoteSourceConfig};

        let store: Store = Arc::new(Mutex::new(HashMap::new()));
        store.lock().insert("key_001".into(), b"hello".to_vec());
        store.lock().insert("key_002".into(), b"world".to_vec());
        let (endpoint, _server) = start_mock(store).await;
        let source = S3RemoteSource::new(&s3_config(&endpoint)).await.unwrap();
        let handler = MissHandler::new(
            source,
            RemoteSourceConfig {
                enabled: true,
                ..Default::default()
            },
        );

        assert_eq!(handler.handle_miss("key_001").await.unwrap(), b"hello");
        assert!(matches!(
            handler.handle_miss("no_such").await.unwrap_err(),
            mooncake_store_client::RemoteSourceError::NotFound(_)
        ));
        let snap = handler.snapshot();
        assert_eq!(snap.total_misses, 2);
        assert_eq!(snap.successful_fetches, 1);
    }

    #[tokio::test]
    async fn test_miss_handler_batch_fetch_with_s3_hot_cache() {
        use mooncake_store_client::{LocalHotCache, MissHandler, RemoteSourceConfig};

        let store: Store = Arc::new(Mutex::new(HashMap::new()));
        store.lock().insert("k1".into(), b"alpha".to_vec());
        store.lock().insert("k2".into(), b"beta".to_vec());
        let (endpoint, _server) = start_mock(store).await;
        let source = S3RemoteSource::new(&s3_config(&endpoint)).await.unwrap();
        let cache = Arc::new(LocalHotCache::default());
        let handler = MissHandler::new(
            source,
            RemoteSourceConfig {
                enabled: true,
                ..Default::default()
            },
        )
        .with_hot_cache(cache);

        let keys: Vec<String> = ["k1", "k2", "missing"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        handler.batch_fetch(&keys).await;

        // handle_miss should hit hot cache for k1
        assert_eq!(handler.handle_miss("k1").await.unwrap(), b"alpha");
        let snap = handler.snapshot();
        assert_eq!(snap.prefetch_keys_requested, 3);
        assert_eq!(snap.prefetch_keys_succeeded, 2);
        assert!(snap.cache_hits >= 1);
    }

    #[tokio::test]
    async fn test_miss_handler_disabled_skips_s3() {
        use mooncake_store_client::{MissHandler, RemoteSourceConfig};

        let store: Store = Arc::new(Mutex::new(HashMap::new()));
        store.lock().insert("key_001".into(), b"data".to_vec());
        let (endpoint, _server) = start_mock(store).await;
        let source = S3RemoteSource::new(&s3_config(&endpoint)).await.unwrap();
        let handler = MissHandler::new(
            source,
            RemoteSourceConfig {
                enabled: false,
                ..Default::default()
            },
        );

        assert!(matches!(
            handler.handle_miss("key_001").await.unwrap_err(),
            mooncake_store_client::RemoteSourceError::NotFound(_)
        ));
    }
}
