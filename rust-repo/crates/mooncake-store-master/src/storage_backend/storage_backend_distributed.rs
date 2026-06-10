use super::*;

impl StorageBackend {
    pub(super) fn distributed_bucket_dirs(&self) -> Vec<PathBuf> {
        (0..self.distributed_bucket_count())
            .map(|bucket| self.disk_dir.join(format!("{bucket:02x}")))
            .collect()
    }

    pub(super) fn remove_by_regex_distributed(
        &self,
        pattern: &str,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let re = regex::Regex::new(pattern).map_err(|e| format!("invalid regex: {e}"))?;
        let mut count = 0;
        for bucket_dir in self.distributed_bucket_dirs() {
            let names = if let Some(adapter) = self.distributed_adapter() {
                adapter.list_files(&bucket_dir)?
            } else if bucket_dir.exists() {
                std::fs::read_dir(&bucket_dir)?
                    .filter_map(|entry| {
                        entry
                            .ok()
                            .map(|entry| entry.file_name().to_string_lossy().into_owned())
                    })
                    .collect()
            } else {
                Vec::new()
            };
            for name in names {
                let key = Self::unescape_distributed_filename(&name);
                if re.is_match(&key) {
                    let path = bucket_dir.join(&name);
                    if let Some(adapter) = self.distributed_adapter() {
                        adapter.delete_file(&path)?;
                    } else {
                        std::fs::remove_file(path)?;
                    }
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    pub(super) fn remove_all_distributed(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let mut count = 0;
        for bucket_dir in self.distributed_bucket_dirs() {
            let names = if let Some(adapter) = self.distributed_adapter() {
                adapter.list_files(&bucket_dir)?
            } else if bucket_dir.exists() {
                std::fs::read_dir(&bucket_dir)?
                    .filter_map(|entry| {
                        entry.ok().and_then(|entry| match entry.file_type() {
                            Ok(file_type) if file_type.is_file() => {
                                Some(entry.file_name().to_string_lossy().into_owned())
                            }
                            _ => None,
                        })
                    })
                    .collect()
            } else {
                std::fs::create_dir_all(&bucket_dir)?;
                Vec::new()
            };
            for name in names {
                let path = bucket_dir.join(name);
                if let Some(adapter) = self.distributed_adapter() {
                    adapter.delete_file(&path)?;
                } else {
                    std::fs::remove_file(path)?;
                }
                count += 1;
            }
        }
        Ok(count)
    }

    pub(super) fn scan_meta_distributed(
        &self,
    ) -> Result<Vec<(String, u64)>, Box<dyn std::error::Error>> {
        let mut results = Vec::new();
        for bucket_dir in self.distributed_bucket_dirs() {
            if let Some(adapter) = self.distributed_adapter() {
                for file in adapter.list_files_with_info(&bucket_dir)? {
                    let key = Self::unescape_distributed_filename(&file.name);
                    results.push((key, file.size));
                }
            } else if bucket_dir.exists() {
                for entry in std::fs::read_dir(&bucket_dir)? {
                    let entry = entry?;
                    if !entry.file_type()?.is_file() {
                        continue;
                    }
                    let name = entry.file_name().to_string_lossy().into_owned();
                    let key = Self::unescape_distributed_filename(&name);
                    let meta = entry.metadata()?;
                    results.push((key, meta.len()));
                }
            }
        }
        Ok(results)
    }
}
