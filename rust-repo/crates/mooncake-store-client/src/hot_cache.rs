use parking_lot::Mutex;
use std::collections::HashMap;

const DEFAULT_HOT_CACHE_SIZE: usize = 256 * 1024 * 1024;
const DEFAULT_MAX_ENTRIES: usize = 10000;

pub struct LocalHotCache {
    data: Mutex<Vec<u8>>,
    tail: Mutex<usize>,
    entries: Mutex<HashMap<String, (usize, usize)>>,
    max_size: usize,
    max_entries: usize,
}

impl LocalHotCache {
    pub fn new(max_size: usize, max_entries: usize) -> Self {
        let size = max_size.max(4096);
        Self {
            data: Mutex::new(vec![0u8; size]),
            tail: Mutex::new(0),
            entries: Mutex::new(HashMap::new()),
            max_size: size,
            max_entries,
        }
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let entries = self.entries.lock();
        if let Some(&(offset, len)) = entries.get(key) {
            let data = self.data.lock();
            Some(data[offset..offset + len].to_vec())
        } else {
            None
        }
    }

    pub fn put(&self, key: &str, value: &[u8]) {
        if value.len() > self.max_size / 2 {
            return;
        }

        let mut entries = self.entries.lock();
        let mut tail = self.tail.lock();
        let mut data = self.data.lock();

        // evict if needed
        while *tail + value.len() > self.max_size || entries.len() >= self.max_entries {
            if entries.is_empty() {
                *tail = 0;
                break;
            }
            let oldest_key = entries
                .iter()
                .min_by_key(|(_, &(off, _))| off)
                .map(|(k, _)| k.clone());
            if let Some(k) = oldest_key {
                entries.remove(&k);
            }
        }

        let offset = *tail;
        if offset + value.len() > data.len() {
            *tail = 0;
            entries.clear();
        }

        let offset = *tail;
        data[offset..offset + value.len()].copy_from_slice(value);
        entries.insert(key.to_string(), (offset, value.len()));
        *tail = offset + value.len();
    }

    pub fn remove(&self, key: &str) {
        self.entries.lock().remove(key);
    }

    pub fn clear(&self) {
        self.entries.lock().clear();
        *self.tail.lock() = 0;
    }
}

impl Default for LocalHotCache {
    fn default() -> Self {
        Self::new(DEFAULT_HOT_CACHE_SIZE, DEFAULT_MAX_ENTRIES)
    }
}
