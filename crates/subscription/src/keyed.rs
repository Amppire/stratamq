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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum KeyFailurePolicy {
    #[default]
    StrictBlock, // If record fails/DLQs, subsequent records for key are blocked
    Relaxed,     // Subsequent records for key proceed even if predecessor failed
}

