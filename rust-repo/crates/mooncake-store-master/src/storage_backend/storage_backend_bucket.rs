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
        let mut required_size = 0u64;

        for (key, value) in entries {
            let value_len = value.len() as u64;
            required_size = required_size.saturating_add(value_len);
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
        self.enforce_bucket_total_size(&config, required_size)?;
        Ok(())
    }

    pub(super) fn enforce_bucket_total_size(
        &self,
        config: &BucketBackendConfig,
        required_size: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if config.max_total_size == 0 || config.eviction_policy == BucketEvictionPolicy::None {
            return Ok(());
        }
        let disk_deficit = self.bucket_disk_space_deficit(required_size);
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
        let mut freed_for_disk = 0u64;
        for (bucket_id, _, _, size) in buckets {
            let quota_satisfied = total <= config.max_total_size;
            let disk_satisfied = disk_deficit
                .map(|deficit| freed_for_disk >= deficit)
                .unwrap_or(true);
            if quota_satisfied && disk_satisfied {
                break;
            }
            let path = self.bucket_path(bucket_id);
            if path.exists() {
                std::fs::remove_file(path)?;
            }
            total = total.saturating_sub(size);
            freed_for_disk = freed_for_disk.saturating_add(size);
        }
        Ok(())
    }

    fn bucket_disk_space_deficit(&self, required_size: u64) -> Option<u64> {
        match (self.available_space_probe)(&self.bucket_dir()) {
            Ok(available) => required_size
                .saturating_add(MIN_FREE_SPACE_BYTES)
                .checked_sub(available)
                .filter(|deficit| *deficit > 0),
            Err(err) => {
                tracing::warn!(
                    "failed to query available bucket disk space for {:?}: {err}",
                    self.bucket_dir()
                );
                None
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket_file(
        bucket_id: u64,
        created_at_ms: i64,
        last_access_ns: i64,
        size: usize,
    ) -> BucketFile {
        BucketFile {
            bucket_id,
            created_at_ms,
            last_access_ns,
            entries: vec![(format!("key-{bucket_id}"), vec![0; size])],
        }
    }

    #[test]
    fn bucket_eviction_uses_actual_disk_space_deficit() {
        let tmp = tempfile::TempDir::new().unwrap();
        let backend = StorageBackend::new_with_available_space_probe(
            StorageBackendType::Bucket,
            tmp.path(),
            Box::new(|_| Ok(MIN_FREE_SPACE_BYTES + 50)),
        );
        backend.write_bucket(&bucket_file(1, 1, 10, 100)).unwrap();
        backend.write_bucket(&bucket_file(2, 2, 20, 100)).unwrap();
        let config = BucketBackendConfig {
            bucket_size_limit: 1024,
            bucket_keys_limit: 10,
            eviction_policy: BucketEvictionPolicy::Fifo,
            max_total_size: 1024,
        };

        backend.enforce_bucket_total_size(&config, 100).unwrap();

        assert!(!backend.bucket_path(1).exists());
        assert!(backend.bucket_path(2).exists());
    }
}
