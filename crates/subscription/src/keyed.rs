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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RetryDelay;
    use strata_types::{ConsumerId, RecordId};

    fn default_policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 3,
            delay: RetryDelay::Fixed { delay: 1000 },
        }
    }

    #[test]
    fn same_key_processed_sequentially_never_concurrently() {
        let mut keyed = KeyedWorkState::new(default_policy(), KeyFailurePolicy::StrictBlock);
        let key = WorkKey::new("order-100");
        let rec1 = RecordId::new();
        let rec2 = RecordId::new();
        let consumer = ConsumerId::new();

        keyed.register(rec1, key.clone());
        keyed.register(rec2, key.clone());

        // Key should be marked active and pending queue has 1 record
        assert!(keyed.is_key_active(&key));
        assert_eq!(keyed.pending_count(&key), 1);

        // Attempting to acquire 10 records must only yield rec1, never rec2
        let leases = keyed.acquire(consumer, 1000, 5000, 10);
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].record_id, rec1);

        // A second consumer attempting to acquire receives nothing
        let second = keyed.acquire(ConsumerId::new(), 1050, 5000, 10);
        assert!(second.is_empty());
    }

    #[test]
    fn distinct_keys_processed_concurrently() {
        let mut keyed = KeyedWorkState::new(default_policy(), KeyFailurePolicy::StrictBlock);
        let key_a = WorkKey::new("account-A");
        let key_b = WorkKey::new("account-B");
        let rec_a = RecordId::new();
        let rec_b = RecordId::new();
        let consumer = ConsumerId::new();

        keyed.register(rec_a, key_a.clone());
        keyed.register(rec_b, key_b.clone());

        assert!(keyed.is_key_active(&key_a));
        assert!(keyed.is_key_active(&key_b));
        assert_eq!(keyed.pending_count(&key_a), 0);
        assert_eq!(keyed.pending_count(&key_b), 0);

        // Both distinct keys should be ready and acquired simultaneously
        let leases = keyed.acquire(consumer, 1000, 5000, 10);
        assert_eq!(leases.len(), 2);
        let acquired_ids: Vec<RecordId> = leases.iter().map(|l| l.record_id).collect();
        assert!(acquired_ids.contains(&rec_a));
        assert!(acquired_ids.contains(&rec_b));
    }

    #[test]
    fn ack_promotes_next_pending_item() {
        let mut keyed = KeyedWorkState::new(default_policy(), KeyFailurePolicy::StrictBlock);
        let key = WorkKey::new("customer-42");
        let rec1 = RecordId::new();
        let rec2 = RecordId::new();
        let rec3 = RecordId::new();
        let consumer = ConsumerId::new();

        keyed.register(rec1, key.clone());
        keyed.register(rec2, key.clone());
        keyed.register(rec3, key.clone());

        assert_eq!(keyed.pending_count(&key), 2);

        // Acquire and complete record 1
        let leases1 = keyed.acquire(consumer, 1000, 5000, 1);
        assert_eq!(leases1[0].record_id, rec1);

        let promoted1 = keyed.ack(leases1[0], 2000).expect("ack should succeed");
        assert_eq!(promoted1, Some(rec2));
        assert_eq!(keyed.pending_count(&key), 1);
        assert!(keyed.is_key_active(&key));

        // Now record 2 is ready to be acquired
        let leases2 = keyed.acquire(consumer, 2100, 5000, 1);
        assert_eq!(leases2[0].record_id, rec2);

        // Ack record 2 -> record 3 promoted
        let promoted2 = keyed.ack(leases2[0], 2500).expect("ack should succeed");
        assert_eq!(promoted2, Some(rec3));
        assert_eq!(keyed.pending_count(&key), 0);

        // Ack record 3 -> no more pending items, key is no longer active
        let leases3 = keyed.acquire(consumer, 2600, 5000, 1);
        assert_eq!(leases3[0].record_id, rec3);

        let promoted3 = keyed.ack(leases3[0], 3000).expect("ack should succeed");
        assert_eq!(promoted3, None);
        assert!(!keyed.is_key_active(&key));
    }

    #[test]
    fn dead_letter_strict_block_halts_key() {
        let strict_policy = RetryPolicy {
            max_attempts: 1, // First nack transitions to DeadLettered
            delay: RetryDelay::Immediate,
        };
        let mut keyed = KeyedWorkState::new(strict_policy, KeyFailurePolicy::StrictBlock);
        let key = WorkKey::new("ledger-001");
        let rec1 = RecordId::new();
        let rec2 = RecordId::new();
        let consumer = ConsumerId::new();

        keyed.register(rec1, key.clone());
        keyed.register(rec2, key.clone());

        let lease = keyed.acquire(consumer, 1000, 5000, 1)[0];
        assert_eq!(lease.record_id, rec1);

        // Nack rec1, which DLQs it under max_attempts: 1
        let result = keyed.nack(lease, 1500).expect("nack should succeed");
        assert_eq!(result, None); // Nothing is promoted

        // Under StrictBlock, the key is halted
        assert!(keyed.is_key_blocked(&key));
        assert!(!keyed.is_key_active(&key));
        assert_eq!(keyed.pending_count(&key), 1);

        // rec2 cannot be acquired because the key is blocked
        let next_leases = keyed.acquire(consumer, 1600, 5000, 1);
        assert!(next_leases.is_empty());

        // Subsequent registrations for this key also remain pending/blocked
        let rec3 = RecordId::new();
        keyed.register(rec3, key.clone());
        assert_eq!(keyed.pending_count(&key), 2);
    }

    #[test]
    fn dead_letter_relaxed_promotes_next_item() {
        let relaxed_policy = RetryPolicy {
            max_attempts: 1, // First nack transitions to DeadLettered
            delay: RetryDelay::Immediate,
        };
        let mut keyed = KeyedWorkState::new(relaxed_policy, KeyFailurePolicy::Relaxed);
        let key = WorkKey::new("metrics-feed");
        let rec1 = RecordId::new();
        let rec2 = RecordId::new();
        let consumer = ConsumerId::new();

        keyed.register(rec1, key.clone());
        keyed.register(rec2, key.clone());

        let lease = keyed.acquire(consumer, 1000, 5000, 1)[0];
        assert_eq!(lease.record_id, rec1);

        // Nack rec1 -> DLQ. Under Relaxed, rec2 is automatically promoted
        let result = keyed.nack(lease, 1500).expect("nack should succeed");
        assert_eq!(result, Some(rec2));

        assert!(!keyed.is_key_blocked(&key));
        assert!(keyed.is_key_active(&key));
        assert_eq!(keyed.pending_count(&key), 0);

        // rec2 can immediately be acquired by a worker
        let next_leases = keyed.acquire(consumer, 1600, 5000, 1);
        assert_eq!(next_leases.len(), 1);
        assert_eq!(next_leases[0].record_id, rec2);
    }

    #[test]
    fn retry_wait_does_not_promote_and_keeps_key_pinned() {
        let retry_policy = RetryPolicy {
            max_attempts: 3,
            delay: RetryDelay::Fixed { delay: 2000 },
        };
        let mut keyed = KeyedWorkState::new(retry_policy, KeyFailurePolicy::StrictBlock);
        let key = WorkKey::new("session-key");
        let rec1 = RecordId::new();
        let rec2 = RecordId::new();
        let consumer = ConsumerId::new();

        keyed.register(rec1, key.clone());
        keyed.register(rec2, key.clone());

        let lease = keyed.acquire(consumer, 1000, 5000, 1)[0];

        // Transient failure (attempt 1/3) -> transitions to RetryWait
        let result = keyed.nack(lease, 1500).expect("nack should succeed");
        assert_eq!(result, None); // rec2 must NOT be promoted
        assert_eq!(keyed.pending_count(&key), 1);
        assert!(keyed.is_key_active(&key));

        // Before retry delay elapses, nothing can be acquired
        assert!(keyed.acquire(consumer, 2000, 5000, 1).is_empty());

        // After retry delay elapses, promote retries
        keyed.work.promote_retries(3600); // 1500 + 2000 = 3500

        // rec1 should be re-acquired (with bumped generation and delivery count)
        let reacquired = keyed.acquire(consumer, 3600, 5000, 1);
        assert_eq!(reacquired.len(), 1);
        assert_eq!(reacquired[0].record_id, rec1);
        assert_eq!(reacquired[0].generation, 2);
    }

    #[test]
    fn expired_leases_reacquired_without_premature_promotion() {
        let mut keyed = KeyedWorkState::new(default_policy(), KeyFailurePolicy::StrictBlock);
        let key = WorkKey::new("heartbeat-key");
        let rec1 = RecordId::new();
        let rec2 = RecordId::new();
        let consumer = ConsumerId::new();

        keyed.register(rec1, key.clone());
        keyed.register(rec2, key.clone());

        // Lease expires at 1000 + 3000 = 4000
        let leases = keyed.acquire(consumer, 1000, 3000, 1);
        assert_eq!(leases[0].record_id, rec1);

        // Advance time to 5000 and expire leases
        let expired = keyed.expire_leases(5000);
        assert_eq!(expired, vec![rec1]);

        // Key remains active with rec1, rec2 remains in pending queue
        assert!(keyed.is_key_active(&key));
        assert_eq!(keyed.pending_count(&key), 1);

        // rec1 is re-acquired by a healthy worker
        let reacquired = keyed.acquire(ConsumerId::new(), 5100, 3000, 1);
        assert_eq!(reacquired.len(), 1);
        assert_eq!(reacquired[0].record_id, rec1);
    }
}
