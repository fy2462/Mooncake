use super::storage_backend_config::{OffsetAllocatorIndex, OffsetIndexEntry};
use super::*;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};

const MAX_OFFSET_KEY_LENGTH: usize = 1024 * 1024;

impl StorageBackend {
    pub(super) fn offset_data_path(&self) -> PathBuf {
        self.disk_dir.join("offset_allocator.data")
    }

    pub(super) fn offset_index_path(&self) -> PathBuf {
        self.disk_dir.join("offset_allocator.index.msgpack")
    }

    pub(super) fn read_offset_index(
        &self,
    ) -> Result<OffsetAllocatorIndex, Box<dyn std::error::Error>> {
        let path = self.offset_index_path();
        if !path.exists() {
            return Ok(OffsetAllocatorIndex::default());
        }
        let reader = BufReader::new(std::fs::File::open(path)?);
        Ok(rmp_serde::decode::from_read(reader)?)
    }

    pub(super) fn write_offset_index(
        &self,
        index: &OffsetAllocatorIndex,
    ) -> Result<(), Box<dyn std::error::Error>> {
        std::fs::create_dir_all(&self.disk_dir)?;
        let path = self.offset_index_path();
        let tmp = path.with_extension("index.tmp");
        let mut writer = BufWriter::new(std::fs::File::create(&tmp)?);
        rmp_serde::encode::write_named(&mut writer, index)?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }

    pub(super) fn batch_offload_offset_allocator(
        &self,
        entries: &[(String, Vec<u8>)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if entries.is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(&self.disk_dir)?;
        let mut index = self.read_offset_index()?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(self.offset_data_path())?;
        let mut next_offset = file.seek(SeekFrom::End(0))?;
        for (key, value) in entries {
            if key.len() > MAX_OFFSET_KEY_LENGTH {
                continue;
            }
            file.write_all(value)?;
            index.entries.insert(
                key.clone(),
                OffsetIndexEntry {
                    offset: next_offset,
                    len: value.len() as u64,
                },
            );
            next_offset = next_offset.saturating_add(value.len() as u64);
        }
        file.sync_all()?;
        index.next_offset = next_offset;
        self.write_offset_index(&index)?;
        Ok(())
    }

    pub(super) fn batch_load_offset_allocator(
        &self,
        keys: &[String],
    ) -> Result<Vec<(String, Vec<u8>)>, Box<dyn std::error::Error>> {
        let index = self.read_offset_index()?;
        let data_path = self.offset_data_path();
        if !data_path.exists() {
            return Ok(Vec::new());
        }
        let mut file = std::fs::File::open(data_path)?;
        let mut results = Vec::new();
        for key in keys {
            let Some(entry) = index.entries.get(key) else {
                continue;
            };
            file.seek(SeekFrom::Start(entry.offset))?;
            let mut buf = vec![0u8; entry.len as usize];
            file.read_exact(&mut buf)?;
            results.push((key.clone(), buf));
        }
        Ok(results)
    }

    pub(super) fn remove_keys_offset_allocator(
        &self,
        keys: &[String],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut index = self.read_offset_index()?;
        for key in keys {
            index.entries.remove(key);
        }
        self.write_offset_index(&index)?;
        Ok(())
    }

    pub(super) fn is_exist_offset_allocator(
        &self,
        key: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(self.read_offset_index()?.entries.contains_key(key))
    }

    pub(super) fn remove_by_regex_offset_allocator(
        &self,
        pattern: &str,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let re = regex::Regex::new(pattern).map_err(|e| format!("invalid regex: {e}"))?;
        let mut index = self.read_offset_index()?;
        let original_len = index.entries.len();
        index.entries.retain(|key, _| !re.is_match(key));
        let removed = original_len - index.entries.len();
        self.write_offset_index(&index)?;
        Ok(removed)
    }

    pub(super) fn remove_all_offset_allocator(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let count = self.read_offset_index()?.entries.len();
        let data_path = self.offset_data_path();
        let index_path = self.offset_index_path();
        if data_path.exists() {
            std::fs::remove_file(data_path)?;
        }
        if index_path.exists() {
            std::fs::remove_file(index_path)?;
        }
        Ok(count)
    }

    pub(super) fn scan_meta_offset_allocator(
        &self,
    ) -> Result<Vec<(String, u64)>, Box<dyn std::error::Error>> {
        Ok(self
            .read_offset_index()?
            .entries
            .into_iter()
            .map(|(key, entry)| (key, entry.len))
            .collect())
    }
}
