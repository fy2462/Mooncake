use parking_lot::Mutex;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(crate) struct CatalogEntry {
    pub is_gold: bool,
    pub addr_list: Vec<usize>,
    pub size_list: Vec<u64>,
    pub max_shard_size: u64,
}

#[derive(Debug, Default)]
pub(crate) struct Catalog {
    entries: Mutex<HashMap<String, CatalogEntry>>,
}

impl Catalog {
    pub fn contains(&self, name: &str) -> bool {
        self.entries.lock().contains_key(name)
    }

    pub fn get(&self, name: &str) -> Option<CatalogEntry> {
        self.entries.lock().get(name).cloned()
    }

    pub fn add(&self, name: String, entry: CatalogEntry) {
        self.entries.lock().insert(name, entry);
    }

    pub fn remove(&self, name: &str) {
        self.entries.lock().remove(name);
    }
}
