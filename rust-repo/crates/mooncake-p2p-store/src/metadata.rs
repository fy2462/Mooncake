use crate::error::P2pStoreError;
use etcd_client::{Client, Compare, CompareOp, GetOptions, Txn, TxnOp};
use serde::{Deserialize, Serialize};

pub const METADATA_KEY_PREFIX: &str = "mooncake/checkpoint/";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Location {
    pub segment_name: String,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shard {
    #[serde(rename = "size")]
    pub length: u64,
    pub gold: Vec<Location>,
    pub replica_list: Vec<Location>,
}

impl Shard {
    pub fn get_location(&self, retry: usize) -> Option<&Location> {
        if retry == 0 {
            self.get_random_location()
        } else {
            self.get_retry_location(retry - 1)
        }
    }

    pub fn get_random_location(&self) -> Option<&Location> {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        if !self.replica_list.is_empty() {
            let idx = rng.gen_range(0..self.replica_list.len());
            Some(&self.replica_list[idx])
        } else if !self.gold.is_empty() {
            let idx = rng.gen_range(0..self.gold.len());
            Some(&self.gold[idx])
        } else {
            None
        }
    }

    pub fn get_retry_location(&self, retry: usize) -> Option<&Location> {
        if self.replica_list.len() > retry {
            return Some(&self.replica_list[retry]);
        }
        let remain = retry - self.replica_list.len();
        if self.gold.len() > remain {
            return Some(&self.gold[remain]);
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.gold.is_empty() && self.replica_list.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload {
    pub name: String,
    pub size: u64,
    pub size_list: Vec<u64>,
    pub max_shard_size: u64,
    pub shards: Vec<Shard>,
}

impl Payload {
    pub fn is_empty(&self) -> bool {
        self.shards.iter().all(|s| s.is_empty())
    }
}

#[derive(Debug, Clone)]
pub struct PayloadInfo {
    pub name: String,
    pub max_shard_size: u64,
    pub total_size: u64,
    pub size_list: Vec<u64>,
}

// ---------------------------------------------------------------------------
// MetadataStore
// ---------------------------------------------------------------------------

pub struct MetadataStore {
    client: Client,
    key_prefix: String,
}

impl MetadataStore {
    pub async fn new(endpoints: &str, key_prefix: &str) -> Result<Self, P2pStoreError> {
        let endpoints: Vec<&str> = endpoints.split(';').filter(|s| !s.is_empty()).collect();
        let client = Client::connect(endpoints, None)
            .await
            .map_err(|e| P2pStoreError::MetadataError(e.to_string()))?;
        Ok(Self {
            client,
            key_prefix: key_prefix.to_string(),
        })
    }

    fn full_key(&self, name: &str) -> Vec<u8> {
        format!("{}{}", self.key_prefix, name).into_bytes()
    }

    pub async fn create(&mut self, name: &str, payload: &Payload) -> Result<(), P2pStoreError> {
        let key = self.full_key(name);
        let json = serde_json::to_vec(payload)?;

        let txn = Txn::new()
            .when([Compare::version(key.clone(), CompareOp::Equal, 0)])
            .and_then([TxnOp::put(key, json, None)]);

        let resp = self
            .client
            .txn(txn)
            .await
            .map_err(|e| P2pStoreError::MetadataError(e.to_string()))?;

        if !resp.succeeded() {
            return Err(P2pStoreError::MetadataError(format!(
                "key '{}' already exists",
                name
            )));
        }
        Ok(())
    }

    pub async fn put(&mut self, name: &str, payload: &Payload) -> Result<(), P2pStoreError> {
        let key = self.full_key(name);
        let json = serde_json::to_vec(payload)?;
        self.client
            .put(key, json, None)
            .await
            .map_err(|e| P2pStoreError::MetadataError(e.to_string()))?;
        Ok(())
    }

    pub async fn get(&mut self, name: &str) -> Result<(Option<Payload>, i64), P2pStoreError> {
        let key = self.full_key(name);
        let resp = self
            .client
            .get(key, None)
            .await
            .map_err(|e| P2pStoreError::MetadataError(e.to_string()))?;

        if let Some(kv) = resp.kvs().first() {
            let payload: Payload = serde_json::from_slice(kv.value())?;
            Ok((Some(payload), kv.mod_revision()))
        } else {
            Ok((None, -1))
        }
    }

    pub async fn update(
        &mut self,
        name: &str,
        payload: &Payload,
        revision: i64,
    ) -> Result<bool, P2pStoreError> {
        let key = self.full_key(name);

        if payload.is_empty() {
            let txn = Txn::new()
                .when([Compare::mod_revision(
                    key.clone(),
                    CompareOp::Equal,
                    revision,
                )])
                .and_then([TxnOp::delete(key, None)]);

            let resp = self
                .client
                .txn(txn)
                .await
                .map_err(|e| P2pStoreError::MetadataError(e.to_string()))?;
            Ok(resp.succeeded())
        } else {
            let json = serde_json::to_vec(payload)?;
            let txn = Txn::new()
                .when([Compare::mod_revision(
                    key.clone(),
                    CompareOp::Equal,
                    revision,
                )])
                .and_then([TxnOp::put(key, json, None)]);

            let resp = self
                .client
                .txn(txn)
                .await
                .map_err(|e| P2pStoreError::MetadataError(e.to_string()))?;
            Ok(resp.succeeded())
        }
    }

    pub async fn list(&mut self, prefix: &str) -> Result<Vec<Payload>, P2pStoreError> {
        let search_key = format!("{}{}", self.key_prefix, prefix).into_bytes();
        let opts = GetOptions::new().with_prefix();
        let resp = self
            .client
            .get(search_key, Some(opts))
            .await
            .map_err(|e| P2pStoreError::MetadataError(e.to_string()))?;

        let mut results = Vec::new();
        for kv in resp.kvs() {
            let payload: Payload = serde_json::from_slice(kv.value())?;
            results.push(payload);
        }
        Ok(results)
    }
}
