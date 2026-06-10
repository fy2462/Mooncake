use super::storage_backend_config::BucketFile;
use super::*;

impl StorageBackend {
    pub(super) fn bucket_dir(&self) -> PathBuf {
        self.disk_dir.join("buckets")
    }

    pub(super) fn bucket_path(&self, bucket_id: u64) -> PathBuf {
        self.bucket_dir().join(format!("{bucket_id}.bucket"))
    }

    pub(super) fn bucket_config(&self) -> BucketBackendConfig {
        BucketBackendConfig::from_environment()
    }

    pub(super) fn list_bucket_ids(&self) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
        let dir = self.bucket_dir();
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(stem) = name.strip_suffix(".bucket") {
                if let Ok(id) = stem.parse::<u64>() {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    pub(super) fn read_bucket(
        &self,
        bucket_id: u64,
    ) -> Result<BucketFile, Box<dyn std::error::Error>> {
        let reader = BufReader::new(std::fs::File::open(self.bucket_path(bucket_id))?);
        Ok(rmp_serde::decode::from_read(reader)?)
    }

    pub(super) fn write_bucket(
        &self,
        bucket: &BucketFile,
    ) -> Result<(), Box<dyn std::error::Error>> {
        std::fs::create_dir_all(self.bucket_dir())?;
        let path = self.bucket_path(bucket.bucket_id);
        let tmp = path.with_extension("bucket.tmp");
        let mut writer = BufWriter::new(std::fs::File::create(&tmp)?);
        rmp_serde::encode::write_named(&mut writer, bucket)?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }

    pub(super) fn remove_key_from_buckets(
        &self,
        key: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let mut removed = false;
        for bucket_id in self.list_bucket_ids()? {
            let mut bucket = self.read_bucket(bucket_id)?;
            let original_len = bucket.entries.len();
            bucket.entries.retain(|(stored_key, _)| stored_key != key);
            if bucket.entries.len() == original_len {
                continue;
            }
            removed = true;
            if bucket.entries.is_empty() {
                std::fs::remove_file(self.bucket_path(bucket_id))?;
            } else {
                self.write_bucket(&bucket)?;
            }
        }
        Ok(removed)
    }

    pub(super) fn batch_offload_bucket(
        &self,
        entries: &[(String, Vec<u8>)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if entries.is_empty() {
            return Ok(());
        }
        let config = self.bucket_config();
        config.validate()?;
        std::fs::create_dir_all(self.bucket_dir())?;

        for (key, _) in entries {
            self.remove_key_from_buckets(key)?;
        }

        let mut next_bucket_id = self
            .list_bucket_ids()?
            .last()
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        let now_ms = Utc::now().timestamp_millis();
        let now_ns = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(now_ms * 1_000_000);
        let mut bucket = BucketFile {
            bucket_id: next_bucket_id,
            created_at_ms: now_ms,
            last_access_ns: now_ns,
            entries: Vec::new(),
        };
        let mut bucket_size = 0u64;

        for (key, value) in entries {
            let value_len = value.len() as u64;
            if !bucket.entries.is_empty()
                && (bucket.entries.len() >= config.bucket_keys_limit
                    || bucket_size.saturating_add(value_len) > config.bucket_size_limit)
            {
                self.write_bucket(&bucket)?;
                next_bucket_id = next_bucket_id.saturating_add(1);
                bucket = BucketFile {
                    bucket_id: next_bucket_id,
                    created_at_ms: now_ms,
                    last_access_ns: now_ns,
                    entries: Vec::new(),
                };
                bucket_size = 0;
            }
            bucket_size = bucket_size.saturating_add(value_len);
            bucket.entries.push((key.clone(), value.clone()));
        }
        if !bucket.entries.is_empty() {
            self.write_bucket(&bucket)?;
        }
        self.enforce_bucket_total_size(&config)?;
        Ok(())
    }

    pub(super) fn enforce_bucket_total_size(
        &self,
        config: &BucketBackendConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if config.max_total_size == 0 || config.eviction_policy == BucketEvictionPolicy::None {
            return Ok(());
        }
        let mut buckets = Vec::new();
        let mut total = 0u64;
        for bucket_id in self.list_bucket_ids()? {
            let bucket = self.read_bucket(bucket_id)?;
            let size = bucket
                .entries
                .iter()
                .map(|(_, v)| v.len() as u64)
                .sum::<u64>();
            total = total.saturating_add(size);
            buckets.push((bucket_id, bucket.created_at_ms, bucket.last_access_ns, size));
        }
        match config.eviction_policy {
            BucketEvictionPolicy::Fifo => buckets.sort_by_key(|(_, created, _, _)| *created),
            BucketEvictionPolicy::Lru => buckets.sort_by_key(|(_, _, last_access, _)| *last_access),
            BucketEvictionPolicy::None => {}
        }
        for (bucket_id, _, _, size) in buckets {
            if total <= config.max_total_size {
                break;
            }
            let path = self.bucket_path(bucket_id);
            if path.exists() {
                std::fs::remove_file(path)?;
            }
            total = total.saturating_sub(size);
        }
        Ok(())
    }

    pub(super) fn batch_load_bucket(
        &self,
        keys: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, Box<dyn std::error::Error>> {
        let wanted: std::collections::HashSet<&str> = keys.iter().map(String::as_str).collect();
        let mut results = Vec::new();
        for bucket_id in self.list_bucket_ids()? {
            let mut bucket = self.read_bucket(bucket_id)?;
            let mut touched = false;
            for (key, value) in &bucket.entries {
                if wanted.contains(key.as_str()) {
                    results.push((key.clone(), value.clone()));
                    touched = true;
                }
            }
            if touched {
                bucket.last_access_ns = Utc::now()
                    .timestamp_nanos_opt()
                    .unwrap_or_else(|| Utc::now().timestamp_millis() * 1_000_000);
                self.write_bucket(&bucket)?;
            }
        }
        Ok(results)
    }

    pub(super) fn remove_keys_bucket(
        &self,
        keys: &[String],
    ) -> Result<(), Box<dyn std::error::Error>> {
        for key in keys {
            self.remove_key_from_buckets(key)?;
        }
        Ok(())
    }

    pub(super) fn is_exist_bucket(&self, key: &str) -> Result<bool, Box<dyn std::error::Error>> {
        for bucket_id in self.list_bucket_ids()? {
            let bucket = self.read_bucket(bucket_id)?;
            if bucket
                .entries
                .iter()
                .any(|(stored_key, _)| stored_key == key)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn remove_by_regex_bucket(
        &self,
        pattern: &str,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let re = regex::Regex::new(pattern).map_err(|e| format!("invalid regex: {e}"))?;
        let mut removed = 0usize;
        for bucket_id in self.list_bucket_ids()? {
            let mut bucket = self.read_bucket(bucket_id)?;
            let original_len = bucket.entries.len();
            bucket.entries.retain(|(key, _)| !re.is_match(key));
            removed += original_len - bucket.entries.len();
            if bucket.entries.is_empty() {
                std::fs::remove_file(self.bucket_path(bucket_id))?;
            } else if bucket.entries.len() != original_len {
                self.write_bucket(&bucket)?;
            }
        }
        Ok(removed)
    }

    pub(super) fn remove_all_bucket(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let mut count = 0usize;
        for bucket_id in self.list_bucket_ids()? {
            count += self.read_bucket(bucket_id)?.entries.len();
            std::fs::remove_file(self.bucket_path(bucket_id))?;
        }
        Ok(count)
    }

    pub(super) fn scan_meta_bucket(
        &self,
    ) -> Result<Vec<(String, u64)>, Box<dyn std::error::Error>> {
        let mut results = Vec::new();
        for bucket_id in self.list_bucket_ids()? {
            for (key, value) in self.read_bucket(bucket_id)?.entries {
                results.push((key, value.len() as u64));
            }
        }
        Ok(results)
    }
}
