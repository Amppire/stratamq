use crate::{FifoReadyQueue, ReadyQueue, WorkError};
use std::collections::{BTreeMap, HashMap, btree_map::Entry};
use strata_types::{ConsumerId, LeaseId, RecordId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    pub record_id: RecordId,
    pub lease_id: LeaseId,
    pub consumer_id: ConsumerId,
    pub generation: u64,
    pub expires_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkStatus {
    Ready,
    Leased {
        lease_id: LeaseId,
        consumer_id: ConsumerId,
        generation: u64,
        expires_at: u64,
    },
    RetryWait {
        available_at: u64,
        generation: u64,
    },
    Complete,
    DeadLettered,
}

#[derive(Debug, Clone)]
pub struct WorkItemState {
    pub status: WorkStatus,
    pub delivery_attempts: u32,
    pub generation: u64,
}

impl Default for WorkItemState {
    fn default() -> Self {
        Self {
            status: WorkStatus::Ready,
            delivery_attempts: 0,
            generation: 0,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExpiryBucket {
    records: Vec<RecordId>,
    positions: HashMap<RecordId, usize>,
}

impl ExpiryBucket {
    #[must_use]
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
            positions: HashMap::new(),
        }
    }

    pub fn push(&mut self, record_id: RecordId) {
        if let Some(&pos) = self.positions.get(&record_id) {
            self.records[pos] = record_id;
            return;
        }
        self.positions.insert(record_id, self.records.len());
        self.records.push(record_id);
    }

    pub fn remove(&mut self, record_id: &RecordId) -> bool {
        if let Some(pos) = self.positions.remove(record_id) {
            self.records.swap_remove(pos);
            if pos < self.records.len() {
                let moved_id = self.records[pos];
                self.positions.insert(moved_id, pos);
            }
            true
        } else {
            false
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    #[must_use]
    pub fn contains(&self, record_id: &RecordId) -> bool {
        self.positions.contains_key(record_id)
    }

    #[must_use]
    pub fn as_slice(&self) -> &[RecordId] {
        &self.records
    }

    #[must_use]
    pub fn into_records(self) -> Vec<RecordId> {
        self.records
    }
}

impl std::ops::Deref for ExpiryBucket {
    type Target = [RecordId];

    fn deref(&self) -> &Self::Target {
        &self.records
    }
}

impl IntoIterator for ExpiryBucket {
    type Item = RecordId;
    type IntoIter = std::vec::IntoIter<RecordId>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

impl<'a> IntoIterator for &'a ExpiryBucket {
    type Item = &'a RecordId;
    type IntoIter = std::slice::Iter<'a, RecordId>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.iter()
    }
}

#[derive(Debug)]
pub struct WorkState {
    items: HashMap<RecordId, WorkItemState>,
    ready: FifoReadyQueue,
    retry_policy: RetryPolicy,
    expiry_wheel: BTreeMap<u64, ExpiryBucket>,
    retry_wheel: BTreeMap<u64, Vec<RecordId>>,
}

#[derive(Debug, Clone)]
pub enum RetryDelay {
    Immediate,
    Fixed {
        delay: u64,
    },
    Exponential {
        initial: u64,
        multiplier: u32,
        max: u64,
    },
}

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub delay: RetryDelay,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            delay: RetryDelay::Immediate,
        }
    }
}

impl RetryPolicy {
    #[must_use]
    pub fn delay_for_attempt(&self, attempt: u32) -> u64 {
        match self.delay {
            RetryDelay::Immediate => 0,
            RetryDelay::Fixed { delay } => delay,
            RetryDelay::Exponential {
                initial,
                multiplier,
                max,
            } => {
                let exponent = attempt.saturating_sub(1);
                let factor = u64::from(multiplier).saturating_pow(exponent);
                initial.saturating_mul(factor).min(max)
            }
        }
    }
}

impl WorkState {
    #[must_use]
    pub fn new(retry_policy: RetryPolicy) -> Self {
        Self {
            items: HashMap::new(),
            ready: FifoReadyQueue::new(),
            retry_policy,
            expiry_wheel: BTreeMap::new(),
            retry_wheel: BTreeMap::new(),
        }
    }

    pub fn register(&mut self, record_id: RecordId) {
        if self.items.contains_key(&record_id) {
            return;
        }
        self.items.insert(record_id, WorkItemState::default());
        self.ready.push(record_id);
    }

    #[must_use]
    pub fn state(&self, record_id: RecordId) -> Option<&WorkItemState> {
        self.items.get(&record_id)
    }

    #[must_use]
    pub fn get_item_state(&self, record_id: &RecordId) -> Option<&WorkItemState> {
        self.items.get(record_id)
    }

    #[must_use]
    pub fn ready_len(&self) -> usize {
        self.ready.len()
    }

    pub fn acquire(
        &mut self,
        consumer_id: ConsumerId,
        now: u64,
        lease_duration: u64,
        max_records: usize,
    ) -> Vec<Lease> {
        let mut leases = Vec::new();
        while leases.len() < max_records {
            let Some(record_id) = self.ready.pop() else {
                break;
            };

            let Some(state) = self.items.get_mut(&record_id) else {
                continue;
            };

            if !matches!(state.status, WorkStatus::Ready) {
                continue;
            }

            state.generation += 1;
            state.delivery_attempts += 1;

            let lease_id = LeaseId::new();
            let expires_at = now.saturating_add(lease_duration);

            state.status = WorkStatus::Leased {
                lease_id,
                consumer_id,
                generation: state.generation,
                expires_at,
            };
            self.expiry_wheel
                .entry(expires_at)
                .or_default()
                .push(record_id);
            leases.push(Lease {
                record_id,
                lease_id,
                consumer_id,
                generation: state.generation,
                expires_at,
            });
        }
        leases
    }

    pub fn ack(&mut self, lease: Lease, now: u64) -> Result<(), WorkError> {
        let state = self
            .items
            .get_mut(&lease.record_id)
            .ok_or(WorkError::NotLeased {
                record_id: lease.record_id,
            })?;
        Self::validate_lease(state, lease, now)?;
        state.status = WorkStatus::Complete;
        Ok(())
    }

    pub fn nack(&mut self, lease: Lease, now: u64) -> Result<(), WorkError> {
        let state = self
            .items
            .get_mut(&lease.record_id)
            .ok_or(WorkError::NotLeased {
                record_id: lease.record_id,
            })?;
        Self::validate_lease(state, lease, now)?;
        if state.delivery_attempts >= self.retry_policy.max_attempts {
            state.status = WorkStatus::DeadLettered;
            return Ok(());
        }
        let retry_delay = self.retry_policy.delay_for_attempt(state.delivery_attempts);
        state.status = WorkStatus::RetryWait {
            available_at: now.saturating_add(retry_delay),
            generation: state.generation,
        };
        self.retry_wheel
            .entry(now.saturating_add(retry_delay))
            .or_default()
            .push(lease.record_id);
        Ok(())
    }

    pub fn extend_lease(
        &mut self,
        lease: Lease,
        now: u64,
        extension: u64,
    ) -> Result<Lease, WorkError> {
        let state = self
            .items
            .get_mut(&lease.record_id)
            .ok_or(WorkError::NotLeased {
                record_id: lease.record_id,
            })?;
        Self::validate_lease(state, lease, now)?;
        let current_expiry = match state.status {
            WorkStatus::Leased { expires_at, .. } => expires_at,
            _ => unreachable!(),
        };
        let new_expiry = lease.expires_at.saturating_add(extension);
        state.status = WorkStatus::Leased {
            lease_id: lease.lease_id,
            consumer_id: lease.consumer_id,
            generation: lease.generation,
            expires_at: new_expiry,
        };
        if let Entry::Occupied(mut entry) = self.expiry_wheel.entry(current_expiry) {
            let bucket = entry.get_mut();
            bucket.remove(&lease.record_id);
            if bucket.is_empty() {
                entry.remove();
            }
        }
        self.expiry_wheel
            .entry(new_expiry)
            .or_default()
            .push(lease.record_id);
        Ok(Lease {
            expires_at: new_expiry,
            ..lease
        })
    }

    pub fn validate_lease(state: &WorkItemState, lease: Lease, now: u64) -> Result<(), WorkError> {
        match state.status {
            WorkStatus::Leased {
                lease_id,
                generation,
                expires_at,
                ..
            } => {
                if lease_id != lease.lease_id {
                    return Err(WorkError::LeaseMismatch {
                        record_id: lease.record_id,
                        lease_id: lease.lease_id,
                    });
                }
                if generation != lease.generation {
                    return Err(WorkError::StaleGeneration {
                        record_id: lease.record_id,
                        current_generation: generation,
                        provided_generation: lease.generation,
                    });
                }
                if expires_at < now {
                    return Err(WorkError::LeaseExpired {
                        record_id: lease.record_id,
                        current_time: now,
                        lease_expiration: expires_at,
                    });
                }
                Ok(())
            }
            WorkStatus::Complete | WorkStatus::DeadLettered => {
                Err(WorkError::AlreadyCompletedOrDeadLettered {
                    record_id: lease.record_id,
                })
            }
            _ => Err(WorkError::NotLeased {
                record_id: lease.record_id,
            }),
        }
    }

    pub fn promote_retries(&mut self, now: u64) {
        let remaining = self.retry_wheel.split_off(&now.saturating_add(1));
        let elapsed = std::mem::replace(&mut self.retry_wheel, remaining);

        for (_timestamp, record_ids) in elapsed {
            for record_id in record_ids {
                let Some(state) = self.items.get_mut(&record_id) else {
                    continue;
                };
                if let WorkStatus::RetryWait { available_at, .. } = state.status
                    && available_at <= now
                {
                    state.status = WorkStatus::Ready;
                    self.ready.push(record_id);
                }
            }
        }
    }

    pub fn expire_leases(&mut self, now: u64) -> (Vec<RecordId>, Vec<RecordId>) {
        let remaining = self.expiry_wheel.split_off(&now);
        let elapsed = std::mem::replace(&mut self.expiry_wheel, remaining);
        let mut ready_ids = Vec::new();
        let mut dead_lettered_ids = Vec::new();

        for (_timestamp, record_ids) in elapsed {
            for record_id in record_ids {
                let Some(state) = self.items.get_mut(&record_id) else {
                    continue;
                };
                if let WorkStatus::Leased { expires_at, .. } = state.status
                    && expires_at < now
                {
                    if state.delivery_attempts >= self.retry_policy.max_attempts {
                        state.status = WorkStatus::DeadLettered;
                        dead_lettered_ids.push(record_id);
                    } else {
                        state.status = WorkStatus::Ready;
                        ready_ids.push(record_id);
                        self.ready.push(record_id);
                    }
                }
            }
        }
        (ready_ids, dead_lettered_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> RetryPolicy {
        RetryPolicy {
            max_attempts: 3,
            delay: RetryDelay::Fixed { delay: 1000 },
        }
    }

    #[test]
    fn worker_can_acquire_and_acknowledge() {
        let mut journal = strata_journal::Journal::new();
        journal
            .append(
                strata_record::ProposedRecord::new("payload"),
                strata_journal::ExpectedRevision::Any,
            )
            .unwrap();
        let record_id = journal.get(0).unwrap().id();
        let consumer = ConsumerId::new();
        let mut work = WorkState::new(policy());
        work.register(record_id);
        let leases = work.acquire(consumer, 1000, 5000, 1);
        assert_eq!(leases.len(), 1);
        work.ack(leases[0], 2000).unwrap();
        assert!(matches!(
            work.state(record_id).unwrap().status,
            WorkStatus::Complete
        ));
    }

    #[test]
    fn leased_record_cannot_be_acquired_by_another_consumer() {
        let mut journal = strata_journal::Journal::new();
        journal
            .append(
                strata_record::ProposedRecord::new("payload"),
                strata_journal::ExpectedRevision::Any,
            )
            .unwrap();
        let record_id = journal.get(0).unwrap().id();
        let mut work = WorkState::new(policy());
        work.register(record_id);
        let first = work.acquire(ConsumerId::new(), 1000, 5000, 1);
        assert_eq!(first.len(), 1);
        let second = work.acquire(ConsumerId::new(), 2000, 5000, 1);
        assert!(second.is_empty());
    }

    #[test]
    fn expired_lease_can_be_reacquired() {
        let mut journal = strata_journal::Journal::new();
        journal
            .append(
                strata_record::ProposedRecord::new("payload"),
                strata_journal::ExpectedRevision::Any,
            )
            .unwrap();
        let record_id = journal.get(0).unwrap().id();
        let mut work = WorkState::new(policy());
        work.register(record_id);
        let first = work.acquire(ConsumerId::new(), 1000, 5000, 1);
        assert_eq!(first.len(), 1);
        work.expire_leases(7000);
        let second = work.acquire(ConsumerId::new(), 8000, 5000, 1);
        assert_eq!(second.len(), 1);
        assert!(second[0].generation > first[0].generation);
    }

    #[test]
    fn expired_lease_at_attempt_limit_is_dead_lettered() {
        let record_id = RecordId::new();
        let mut work = WorkState::new(policy());
        work.register(record_id);

        for now in [1000, 3000] {
            let lease = work.acquire(ConsumerId::new(), now, 1000, 1)[0];
            assert_eq!(work.expire_leases(lease.expires_at + 1).0, vec![record_id]);
        }

        let lease = work.acquire(ConsumerId::new(), 5000, 1000, 1)[0];
        let (ready_ids, dead_lettered_ids) = work.expire_leases(lease.expires_at + 1);
        assert!(ready_ids.is_empty());
        assert_eq!(dead_lettered_ids, vec![record_id]);
        assert!(matches!(
            work.state(record_id).unwrap().status,
            WorkStatus::DeadLettered
        ));
    }

    #[test]
    fn stale_generation_cannot_ack() {
        let mut journal = strata_journal::Journal::new();
        journal
            .append(
                strata_record::ProposedRecord::new("payload"),
                strata_journal::ExpectedRevision::Any,
            )
            .unwrap();
        let record_id = journal.get(0).unwrap().id();
        let mut work = WorkState::new(policy());
        work.register(record_id);
        let old_lease = work.acquire(ConsumerId::new(), 1000, 5000, 1)[0];
        work.expire_leases(7000);
        let new_lease = work.acquire(ConsumerId::new(), 8000, 5000, 1)[0];
        assert!(new_lease.generation > old_lease.generation);
        let result = work.ack(old_lease, 9000);
        assert!(result.is_err());
    }

    #[test]
    fn lease_is_valid_at_exact_expiration_boundary() {
        let id = RecordId::new();
        let mut work = WorkState::new(policy());
        work.register(id);
        let lease = work.acquire(ConsumerId::new(), 1000, 5000, 1)[0];
        let (ready, dead_lettered) = work.expire_leases(6000);
        assert!(ready.is_empty() && dead_lettered.is_empty());
        assert!(matches!(
            work.state(id).unwrap().status,
            WorkStatus::Leased { .. }
        ));
        work.ack(lease, 6000)
            .expect("lease is valid at its expiration timestamp");
    }

    #[test]
    fn lease_expires_after_expiration_boundary() {
        let id = RecordId::new();
        let mut work = WorkState::new(policy());
        work.register(id);
        let lease = work.acquire(ConsumerId::new(), 1000, 5000, 1)[0];
        let (ready, dead_lettered) = work.expire_leases(6001);
        assert_eq!(ready, vec![id]);
        assert!(dead_lettered.is_empty());
        assert!(work.ack(lease, 6001).is_err());
    }

    #[test]
    fn active_lease_can_be_extended() {
        let mut journal = strata_journal::Journal::new();
        journal
            .append(
                strata_record::ProposedRecord::new("payload"),
                strata_journal::ExpectedRevision::Any,
            )
            .unwrap();
        let record_id = journal.get(0).unwrap().id();
        let mut work = WorkState::new(policy());
        work.register(record_id);
        let lease = work.acquire(ConsumerId::new(), 1000, 5000, 1)[0];
        let original_expires_at = lease.expires_at;
        let extended = work.extend_lease(lease, 2000, 5000).unwrap();
        assert!(extended.expires_at > original_expires_at);
        assert!(!work.expiry_wheel.contains_key(&original_expires_at));
        assert_eq!(
            work.expiry_wheel
                .get(&extended.expires_at)
                .map(|b| b.as_slice()),
            Some(&[record_id][..])
        );

        let (ready, dead_lettered) = work.expire_leases(original_expires_at + 1);
        assert!(ready.is_empty() && dead_lettered.is_empty());
    }

    #[test]
    fn repeated_extensions_do_not_accumulate_wheel_entries() {
        let id = RecordId::new();
        let mut work = WorkState::new(policy());
        work.register(id);
        let mut lease = work.acquire(ConsumerId::new(), 1000, 1000, 1)[0];

        for now in (1500..5000).step_by(500) {
            lease = work.extend_lease(lease, now, 1000).unwrap();
            assert_eq!(work.expiry_wheel.len(), 1);
            assert_eq!(
                work.expiry_wheel
                    .get(&lease.expires_at)
                    .map(|b| b.as_slice()),
                Some(&[id][..])
            );
        }
    }

    #[test]
    fn extend_lease_with_retained_older_lease_removes_current_expiry_from_wheel() {
        let id = RecordId::new();
        let mut work = WorkState::new(policy());
        work.register(id);
        let initial_lease = work.acquire(ConsumerId::new(), 1000, 2000, 1)[0];
        assert_eq!(initial_lease.expires_at, 3000);

        let extended_1 = work.extend_lease(initial_lease, 1500, 1000).unwrap();
        assert_eq!(extended_1.expires_at, 4000);
        assert!(!work.expiry_wheel.contains_key(&3000));
        assert!(work.expiry_wheel.contains_key(&4000));

        let extended_2 = work.extend_lease(initial_lease, 2000, 2000).unwrap();
        assert_eq!(extended_2.expires_at, 5000);
        assert!(!work.expiry_wheel.contains_key(&3000));
        assert!(!work.expiry_wheel.contains_key(&4000));
        assert_eq!(
            work.expiry_wheel.get(&5000).map(|b| b.as_slice()),
            Some(&[id][..])
        );
        assert_eq!(work.expiry_wheel.len(), 1);
    }

    #[test]
    fn registered_record_can_be_acquired() {
        let record_id = RecordId::new();
        let consumer = ConsumerId::new();
        let mut work = WorkState::new(policy());

        work.register(record_id);
        let leases = work.acquire(consumer, 1000, 5000, 1);
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].record_id, record_id);
    }

    #[test]
    fn expiry_bucket_removal_updates_moved_record_index() {
        let mut bucket = ExpiryBucket::new();
        let id0 = RecordId::new();
        let id1 = RecordId::new();
        let id2 = RecordId::new();
        let id3 = RecordId::new();
        let id4 = RecordId::new();

        bucket.push(id0);
        bucket.push(id1);
        bucket.push(id2);
        bucket.push(id3);
        bucket.push(id4);

        assert_eq!(bucket.len(), 5);
        assert_eq!(bucket.as_slice(), &[id0, id1, id2, id3, id4]);

        // Removing middle element (id2): id4 moves to index 2
        assert!(bucket.remove(&id2));
        assert_eq!(bucket.len(), 4);
        assert_eq!(bucket.as_slice(), &[id0, id1, id4, id3]);
        assert_eq!(bucket.positions.get(&id4), Some(&2));

        // Removing first element (id0): id3 moves to index 0
        assert!(bucket.remove(&id0));
        assert_eq!(bucket.len(), 3);
        assert_eq!(bucket.as_slice(), &[id3, id1, id4]);
        assert_eq!(bucket.positions.get(&id3), Some(&0));

        // Removing last element (id4): no element is moved
        assert!(bucket.remove(&id4));
        assert_eq!(bucket.len(), 2);
        assert_eq!(bucket.as_slice(), &[id3, id1]);
        assert_eq!(bucket.positions.get(&id3), Some(&0));
        assert_eq!(bucket.positions.get(&id1), Some(&1));

        // Removing non-existent element
        assert!(!bucket.remove(&id0));

        // Remove remaining
        assert!(bucket.remove(&id3));
        assert!(bucket.remove(&id1));
        assert!(bucket.is_empty());
        assert_eq!(bucket.positions.len(), 0);
    }

    #[test]
    fn extend_lease_preserves_other_records_in_same_bucket() {
        let mut work = WorkState::new(policy());
        let id1 = RecordId::new();
        let id2 = RecordId::new();
        let id3 = RecordId::new();

        work.register(id1);
        work.register(id2);
        work.register(id3);

        let leases = work.acquire(ConsumerId::new(), 1000, 5000, 3);
        assert_eq!(leases.len(), 3);
        let lease1 = leases[0];
        let lease2 = leases[1];
        let lease3 = leases[2];

        // All 3 share expires_at = 6000
        assert_eq!(work.expiry_wheel.get(&6000).unwrap().len(), 3);

        // Extend lease2 to 8000
        let extended2 = work.extend_lease(lease2, 2000, 2000).unwrap();
        assert_eq!(extended2.expires_at, 8000);

        // Bucket 6000 now has lease1 and lease3
        let bucket_6000 = work.expiry_wheel.get(&6000).unwrap();
        assert_eq!(bucket_6000.len(), 2);
        assert!(bucket_6000.contains(&lease1.record_id));
        assert!(bucket_6000.contains(&lease3.record_id));
        assert!(!bucket_6000.contains(&lease2.record_id));

        // Bucket 8000 has lease2
        let bucket_8000 = work.expiry_wheel.get(&8000).unwrap();
        assert_eq!(bucket_8000.len(), 1);
        assert!(bucket_8000.contains(&lease2.record_id));

        // At 6001, lease1 and lease3 expire; lease2 does not
        let (ready, dead_lettered) = work.expire_leases(6001);
        assert_eq!(ready.len(), 2);
        assert!(ready.contains(&lease1.record_id));
        assert!(ready.contains(&lease3.record_id));
        assert!(dead_lettered.is_empty());

        // At 8001, lease2 expires
        let (ready2, dead_lettered2) = work.expire_leases(8001);
        assert_eq!(ready2, vec![lease2.record_id]);
        assert!(dead_lettered2.is_empty());
    }

    #[test]
    fn benchmark_expire_leases_with_many_idle_records() {
        use std::time::Instant;

        const IDLE_RECORDS: usize = 100_000;
        const EXPIRED_LEASES: usize = 10;
        const SAMPLES: usize = 10;
        const TARGET_MICROS: u128 = 1_000;

        let mut elapsed_micros = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let mut work = WorkState::new(policy());
            for _ in 0..IDLE_RECORDS {
                work.register(RecordId::new());
            }
            let leases = work.acquire(ConsumerId::new(), 0, 100, EXPIRED_LEASES);
            assert_eq!(leases.len(), EXPIRED_LEASES);

            // Keep workload construction and the result check outside the timed region.
            let start = Instant::now();
            let (ready, dead_lettered) = work.expire_leases(101);
            let elapsed = start.elapsed().as_micros();
            assert_eq!(ready.len(), EXPIRED_LEASES);
            assert!(dead_lettered.is_empty());
            elapsed_micros.push(elapsed);
        }

        elapsed_micros.sort_unstable();
        let median_micros = elapsed_micros[SAMPLES / 2];
        assert!(
            median_micros <= TARGET_MICROS,
            "expire_leases median was {median_micros}µs; target is {TARGET_MICROS}µs"
        );
    }
}
