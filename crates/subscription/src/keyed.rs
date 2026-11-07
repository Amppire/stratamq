use bytes::Bytes;
use std::collections::{HashMap, VecDeque};

use crate::{RetryPolicy, WorkState};
use strata_types::RecordId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WorkKey(Bytes);

impl WorkKey {
    #[must_use]
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self(value.into())
    }
}

pub struct KeyedWorkState {
    work: WorkState,
    active_by_key: HashMap<WorkKey, RecordId>,
    pending_by_key: HashMap<WorkKey, VecDeque<RecordId>>,
    record_keys: HashMap<RecordId, WorkKey>,
}

impl KeyedWorkState {
    #[must_use]
    pub fn new(retry_policy: RetryPolicy) -> Self {
        Self {
            work: WorkState::new(retry_policy),
            active_by_key: HashMap::new(),
            pending_by_key: HashMap::new(),
            record_keys: HashMap::new(),
        }
    }

    pub fn register(&mut self, record_id: RecordId, key: WorkKey) {
        self.record_keys.insert(record_id, key.clone());
        let is_active = self.active_by_key.contains_key(&key);
        if !is_active {
            self.active_by_key.insert(key, record_id);
            self.work.register(record_id);
            return;
        }
        self.pending_by_key
            .entry(key)
            .or_default()
            .push_back(record_id);
    }
}
