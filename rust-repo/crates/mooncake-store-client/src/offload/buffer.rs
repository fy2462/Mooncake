//! Owner-bearing peer-offload buffer lifecycle.
//!
//! C++ keeps offload payloads in a bounded, pre-registered client arena and
//! reclaims abandoned batches after a lease timeout. Rust uses per-batch
//! owner-bearing TE registrations, but preserves the same capacity and TTL
//! contract. Native unregistration failure cannot free the backing bytes
//! because `RemoteReadableRegistration` fails closed.

use crate::memory_ffi::RemoteReadableRegistration;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_GC_INTERVAL_SECONDS: u64 = 1;
const DEFAULT_GC_TTL_MS: u64 = 5_000;

struct OffloadBatch {
    _registrations: Vec<RemoteReadableRegistration>,
    total_bytes: usize,
    expires_at: Instant,
}

struct PoolState {
    next_batch_id: u64,
    retained_bytes: usize,
    batches: HashMap<u64, OffloadBatch>,
}

/// Bounded registry of active remote-read batches.
pub struct OffloadBufferPool {
    state: Mutex<PoolState>,
    max_bytes: usize,
    ttl: Duration,
    gc_interval: Duration,
}

impl OffloadBufferPool {
    /// Construct the production pool using the C++ environment variable names.
    pub fn from_environment(max_bytes: usize) -> Result<Arc<Self>, String> {
        let ttl_ms = parse_positive_env_u64(
            "MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_TTL_MS",
            DEFAULT_GC_TTL_MS,
        )?;
        let gc_interval_seconds = parse_positive_env_u64(
            "MOONCAKE_OFFLOAD_CLIENT_BUFFER_GC_INTERVAL_SECONDS",
            DEFAULT_GC_INTERVAL_SECONDS,
        )?;
        Self::with_limits(
            max_bytes,
            Duration::from_millis(ttl_ms),
            Duration::from_secs(gc_interval_seconds),
        )
    }

    pub fn with_limits(
        max_bytes: usize,
        ttl: Duration,
        gc_interval: Duration,
    ) -> Result<Arc<Self>, String> {
        if max_bytes == 0 {
            return Err("offload client buffer capacity must be positive".to_string());
        }
        if ttl.is_zero() {
            return Err("offload client buffer GC TTL must be positive".to_string());
        }
        if ttl.as_millis() == 0 || ttl.as_millis() > u128::from(u64::MAX) {
            return Err(
                "offload client buffer GC TTL must be representable as positive milliseconds"
                    .to_string(),
            );
        }
        if gc_interval.is_zero() {
            return Err("offload client buffer GC interval must be positive".to_string());
        }
        if Instant::now().checked_add(ttl).is_none() {
            return Err("offload client buffer GC TTL exceeds Instant range".to_string());
        }
        Ok(Arc::new(Self {
            state: Mutex::new(PoolState {
                next_batch_id: 1,
                retained_bytes: 0,
                batches: HashMap::new(),
            }),
            max_bytes,
            ttl,
            gc_interval,
        }))
    }

    /// Reserve capacity before reading or registering payload bytes.
    pub fn try_reserve(
        self: &Arc<Self>,
        total_bytes: usize,
    ) -> Result<OffloadBufferReservation, String> {
        if total_bytes == 0 {
            return Err("offload batch size must be positive".to_string());
        }
        {
            let state = self.state.lock();
            let projected = state
                .retained_bytes
                .checked_add(total_bytes)
                .ok_or_else(|| "offload client buffer accounting overflow".to_string())?;
            if projected <= self.max_bytes {
                drop(state);
                return self.reserve_unchecked_capacity(total_bytes);
            }
        }

        // Match the C++ allocator behavior: an allocation under pressure gets
        // one synchronous expired-batch collection before reporting overflow.
        self.release_expired(Instant::now());
        self.reserve_unchecked_capacity(total_bytes)
    }

    fn reserve_unchecked_capacity(
        self: &Arc<Self>,
        total_bytes: usize,
    ) -> Result<OffloadBufferReservation, String> {
        let mut state = self.state.lock();
        let projected = state
            .retained_bytes
            .checked_add(total_bytes)
            .ok_or_else(|| "offload client buffer accounting overflow".to_string())?;
        if projected > self.max_bytes {
            return Err(format!(
                "offload client buffer capacity exceeded: requested={total_bytes} retained={} capacity={}",
                state.retained_bytes, self.max_bytes
            ));
        }
        state.retained_bytes = projected;
        Ok(OffloadBufferReservation {
            pool: Arc::clone(self),
            bytes: total_bytes,
            active: true,
        })
    }

    /// Release an explicitly completed batch. Dropping the removed batch
    /// unregisters every owner-bearing TE registration outside the pool lock.
    pub fn release(&self, batch_id: u64) -> bool {
        let removed = {
            let mut state = self.state.lock();
            let removed = state.batches.remove(&batch_id);
            if let Some(batch) = &removed {
                state.retained_bytes = state
                    .retained_bytes
                    .checked_sub(batch.total_bytes)
                    .expect("committed offload bytes are included in retained_bytes");
            }
            removed
        };
        removed.is_some()
    }

    /// Reclaim batches whose remote-read lease has expired.
    pub fn release_expired(&self, now: Instant) -> usize {
        let removed = {
            let mut state = self.state.lock();
            let expired_ids = state
                .batches
                .iter()
                .filter_map(|(batch_id, batch)| (now >= batch.expires_at).then_some(*batch_id))
                .collect::<Vec<_>>();
            let mut removed = Vec::with_capacity(expired_ids.len());
            for batch_id in expired_ids {
                if let Some(batch) = state.batches.remove(&batch_id) {
                    state.retained_bytes = state
                        .retained_bytes
                        .checked_sub(batch.total_bytes)
                        .expect("committed offload bytes are included in retained_bytes");
                    removed.push(batch);
                }
            }
            removed
        };
        let count = removed.len();
        drop(removed);
        count
    }

    pub fn ttl_ms(&self) -> u64 {
        u64::try_from(self.ttl.as_millis())
            .expect("validated offload TTL is representable as u64 milliseconds")
    }

    pub fn gc_interval(&self) -> Duration {
        self.gc_interval
    }

    pub fn retained_bytes(&self) -> usize {
        self.state.lock().retained_bytes
    }

    pub fn active_batch_count(&self) -> usize {
        self.state.lock().batches.len()
    }
}

/// Capacity reservation that rolls back automatically on every early error.
pub struct OffloadBufferReservation {
    pool: Arc<OffloadBufferPool>,
    bytes: usize,
    active: bool,
}

impl OffloadBufferReservation {
    pub(crate) fn commit(
        mut self,
        registrations: Vec<RemoteReadableRegistration>,
    ) -> Result<(u64, Vec<u64>), String> {
        if registrations.is_empty() {
            return Err("offload batch must contain at least one registration".to_string());
        }
        let registered_bytes = registrations
            .iter()
            .try_fold(0usize, |total, registration| {
                total
                    .checked_add(registration.len())
                    .ok_or_else(|| "offload registration size overflow".to_string())
            })?;
        if registered_bytes != self.bytes {
            return Err(format!(
                "offload registration bytes {registered_bytes} do not match reservation {}",
                self.bytes
            ));
        }
        let pointers = registrations
            .iter()
            .map(RemoteReadableRegistration::pointer)
            .collect::<Vec<_>>();
        let expires_at = Instant::now()
            .checked_add(self.pool.ttl)
            .ok_or_else(|| "offload batch deadline exceeds Instant range".to_string())?;
        let mut state = self.pool.state.lock();
        let batch_id = state.next_batch_id;
        if batch_id == 0 {
            return Err("offload batch id space exhausted".to_string());
        }
        state.next_batch_id = batch_id
            .checked_add(1)
            .ok_or_else(|| "offload batch id space exhausted".to_string())?;
        match state.batches.entry(batch_id) {
            Entry::Vacant(entry) => {
                entry.insert(OffloadBatch {
                    _registrations: registrations,
                    total_bytes: self.bytes,
                    expires_at,
                });
            }
            Entry::Occupied(_) => {
                return Err("offload batch id collision".to_string());
            }
        }
        self.active = false;
        Ok((batch_id, pointers))
    }
}

impl Drop for OffloadBufferReservation {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.pool.state.lock();
        state.retained_bytes = state
            .retained_bytes
            .checked_sub(self.bytes)
            .expect("active reservation is included in retained_bytes");
    }
}

fn parse_positive_env_u64(name: &str, default: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or_else(|| format!("{name} must be a positive integer")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(format!("failed to read {name}: {error}")),
    }
}
