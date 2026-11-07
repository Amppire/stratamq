use std::collections::HashMap;
use strata_types::{ConsumerId, LeaseId, RecordId};
use crate::{FifoReadyQueue, ReadyQueue, WorkError};

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

pub struct WorkState {
    items: HashMap<RecordId, WorkItemState>,
    ready: FifoReadyQueue,
    retry_policy: RetryPolicy,
}

impl WorkState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            items: HashMap::new(),
            ready: FifoReadyQueue::new(),
            retry_policy: RetryPolicy::default(),
        }
    }

    #[must_use]
    pub fn state(&self, record_id: RecordId) -> Option<&WorkItemState> {
        self.items.get(&record_id)
    }

    #[must_use]
    pub fn get_item_state(&self, record_id: &RecordId) -> Option<&WorkItemState> {
        self.items.get(record_id)
    }

    pub fn acquire(
        &mut self,
        journal: &strata_journal::Journal,
        consumer_id: ConsumerId,
        now: u64,
        lease_duration: u64,
        max_records: usize,
    ) -> Vec<Lease> {
        let mut leases = Vec::new();
        for record in journal.records_by_range(0, journal.len()) {
            if leases.len() >= max_records {
                break;
            }

            let state = self.items.entry(record.id()).or_default();
            let available = match state.status {
                WorkStatus::Ready => true,
                WorkStatus::RetryWait { available_at, .. } => available_at <= now,
                _ => false,
            };

            if !available {
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

            leases.push(Lease {
                record_id: record.id(),
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
        match state.status {
            WorkStatus::Complete | WorkStatus::DeadLettered => {
                Err(WorkError::AlreadyCompletedOrDeadLettered {
                    record_id: lease.record_id,
                })
            }
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
                state.status = WorkStatus::Complete;
                Ok(())
            }
            _ => Err(WorkError::NotLeased {
                record_id: lease.record_id,
            }),
        }
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
        let retry_delay = self
            .retry_policy
            .delay_for_attempt(state.delivery_attempts);
        state.status = WorkStatus::RetryWait {
            available_at: now.saturating_add(retry_delay),
            generation: state.generation,
        };
        Ok(())
    }

    pub fn expire_leases(&mut self, now: u64) {
        for state in self.items.values_mut() {
            if let WorkStatus::Leased { expires_at, .. } = state.status
                && expires_at < now
            {
                state.status = WorkStatus::Ready;
            }
        }
    }

    pub fn extend(
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
        match &mut state.status {
            WorkStatus::Leased {
                lease_id,
                consumer_id,
                generation,
                expires_at,
            } => {
                if *lease_id != lease.lease_id {
                    return Err(WorkError::LeaseMismatch {
                        record_id: lease.record_id,
                        lease_id: lease.lease_id,
                    });
                }
                if *generation != lease.generation {
                    return Err(WorkError::StaleGeneration {
                        record_id: lease.record_id,
                        current_generation: *generation,
                        provided_generation: lease.generation,
                    });
                }
                if *expires_at < now {
                    return Err(WorkError::LeaseExpired {
                        record_id: lease.record_id,
                        current_time: now,
                        lease_expiration: *expires_at,
                    });
                }
                let new_expiry = now.saturating_add(extension);
                *expires_at = new_expiry;
                Ok(Lease {
                    record_id: lease.record_id,
                    lease_id: *lease_id,
                    consumer_id: *consumer_id,
                    generation: *generation,
                    expires_at: new_expiry,
                })
            }
            _ => Err(WorkError::NotLeased {
                record_id: lease.record_id,
            }),
        }
    }

    pub fn extend_lease(
        &mut self,
        lease: Lease,
        now: u64,
        extension: u64,
    ) -> Result<Lease, WorkError> {
        self.extend(lease, now, extension)
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
}

impl Default for WorkState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let mut work = WorkState::new();
        let leases = work.acquire(&journal, consumer, 1000, 5000, 1);
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
        let mut work = WorkState::new();
        let first = work.acquire(&journal, ConsumerId::new(), 1000, 5000, 1);
        assert_eq!(first.len(), 1);
        let second = work.acquire(&journal, ConsumerId::new(), 2000, 5000, 1);
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
        let mut work = WorkState::new();
        let first = work.acquire(&journal, ConsumerId::new(), 1000, 5000, 1);
        assert_eq!(first.len(), 1);
        work.expire_leases(7000);
        let second = work.acquire(&journal, ConsumerId::new(), 8000, 5000, 1);
        assert_eq!(second.len(), 1);
        assert!(second[0].generation > first[0].generation);
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
        let mut work = WorkState::new();
        let old_lease = work.acquire(&journal, ConsumerId::new(), 1000, 5000, 1)[0];
        work.expire_leases(7000);
        let new_lease = work.acquire(&journal, ConsumerId::new(), 8000, 5000, 1)[0];
        assert!(new_lease.generation > old_lease.generation);
        let result = work.ack(old_lease, 9000);
        assert!(result.is_err());
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
        let mut work = WorkState::new();
        let lease = work.acquire(&journal, ConsumerId::new(), 1000, 5000, 1)[0];
        let extended = work.extend(lease, 2000, 5000).unwrap();
        assert!(extended.expires_at > lease.expires_at);
    }
}
