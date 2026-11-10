use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};

use crate::{Lease, RetryPolicy, WorkError, WorkState, WorkStatus};
use strata_types::{ConsumerId, RecordId};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkKey(Bytes);

impl WorkKey {
    #[must_use]
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug)]
pub struct KeyedWorkState {
    work: WorkState,
    active_by_key: HashMap<WorkKey, RecordId>,
    pending_by_key: HashMap<WorkKey, VecDeque<RecordId>>,
    record_keys: HashMap<RecordId, WorkKey>,
    blocked_keys: HashSet<WorkKey>,
    failure_policy: KeyFailurePolicy,
}

impl KeyedWorkState {
    #[must_use]
    pub fn new(retry_policy: RetryPolicy, failure_policy: KeyFailurePolicy) -> Self {
        Self {
            work: WorkState::new(retry_policy),
            active_by_key: HashMap::new(),
            pending_by_key: HashMap::new(),
            record_keys: HashMap::new(),
            blocked_keys: HashSet::new(),
            failure_policy,
        }
    }

    pub fn register(&mut self, record_id: RecordId, key: WorkKey) {
        self.record_keys.insert(record_id, key.clone());

        if self.blocked_keys.contains(&key) || self.active_by_key.contains_key(&key) {
            self.pending_by_key
                .entry(key)
                .or_default()
                .push_back(record_id);
            return;
        }
        self.active_by_key.insert(key, record_id);
        self.work.register(record_id);
    }

    fn promote_next(&mut self, key: &WorkKey) -> Option<RecordId> {
        self.active_by_key.remove(key);

        if self.blocked_keys.contains(key) {
            return None;
        }

        if let Some(pending) = self.pending_by_key.get_mut(key) {
            let Some(record_id) = pending.pop_front() else {
                self.pending_by_key.remove(key);
                return None;
            };
            if pending.is_empty() {
                self.pending_by_key.remove(key);
            }
            self.active_by_key.insert(key.clone(), record_id);
            self.work.register(record_id);
            return Some(record_id);
        }
        None
    }

    /// Called when a worker acknowledges completion of a record.
    pub fn ack(&mut self, lease: Lease, now: u64) -> Result<Option<RecordId>, WorkError> {
        self.work.ack(lease, now)?;
        let Some(key) = self.record_keys.remove(&lease.record_id) else {
            return Ok(None);
        };
        Ok(self.promote_next(&key))
    }

    /// Called when a worker nacks a record.
    pub fn nack(&mut self, lease: Lease, now: u64) -> Result<Option<RecordId>, WorkError> {
        self.work.nack(lease, now)?;
        let is_dead_lettered = matches!(
            self.work.state(lease.record_id).map(|s| &s.status),
            Some(WorkStatus::DeadLettered)
        );
        if !is_dead_lettered {
            return Ok(None);
        }
        let Some(key) = self.record_keys.remove(&lease.record_id) else {
            return Ok(None);
        };
        match self.failure_policy {
            KeyFailurePolicy::StrictBlock => {
                self.active_by_key.remove(&key);
                self.blocked_keys.insert(key);
                Ok(None)
            }
            KeyFailurePolicy::Relaxed => Ok(self.promote_next(&key)),
        }
    }

    pub fn expire_leases(&mut self, now: u64) -> Vec<RecordId> {
        self.work.expire_leases(now)
    }
    #[must_use]
    pub fn is_key_active(&self, key: &WorkKey) -> bool {
        self.active_by_key.contains_key(key)
    }
    #[must_use]
    pub fn pending_count(&self, key: &WorkKey) -> usize {
        self.pending_by_key.get(key).map_or(0, VecDeque::len)
    }
    #[must_use]
    pub fn is_key_blocked(&self, key: &WorkKey) -> bool {
        self.blocked_keys.contains(key)
    }
    pub fn acquire(
        &mut self,
        consumer_id: ConsumerId,
        now: u64,
        lease_duration: u64,
        max_records: usize,
    ) -> Vec<Lease> {
        self.work
            .acquire(consumer_id, now, lease_duration, max_records)
    }

    pub fn promote_retries(&mut self, now: u64) {
        self.work.promote_retries(now);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeyFailurePolicy {
    #[default]
    StrictBlock, // If record fails/DLQs, subsequent records for key are blocked
    Relaxed, // Subsequent records for key proceed even if predecessor failed
}
