use crate::{FifoReadyQueue, ReadyQueue, WorkError};
use std::collections::{BTreeMap, HashMap};
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

#[derive(Debug)]
pub struct WorkState {
    items: HashMap<RecordId, WorkItemState>,
    ready: FifoReadyQueue,
    retry_policy: RetryPolicy,
    expiry_wheel: BTreeMap<u64, Vec<RecordId>>,
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
        let new_expiry = lease.expires_at.saturating_add(extension);
        state.status = WorkStatus::Leased {
            lease_id: lease.lease_id,
            consumer_id: lease.consumer_id,
            generation: lease.generation,
            expires_at: new_expiry,
        };
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

    pub fn expire_leases(&mut self, now: u64) -> Vec<RecordId> {
        let remaining = self.expiry_wheel.split_off(&now.saturating_add(1));
        let elapsed = std::mem::replace(&mut self.expiry_wheel, remaining);
        let mut expired_ids = Vec::new();

        for (_timestamp, record_ids) in elapsed {
            for record_id in record_ids {
                let Some(state) = self.items.get_mut(&record_id) else {
                    continue;
                };
                if let WorkStatus::Leased { expires_at, .. } = state.status
                    && expires_at <= now
                {
                    state.status = WorkStatus::Ready;
                    self.ready.push(record_id);
                    expired_ids.push(record_id);
                }
            }
        }
        expired_ids
    }
}
