#![forbid(unsafe_code)]

use std::collections::HashMap;
use strata_types::{ConsumerId, JournalId, LeaseId, RecordId, SubscriptionId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionMode {
    Stream,
    Work,
    Broadcast,
    Keyed,
    Ephemeral,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionState {
    Active,
    Inactive,
    Closed,
}
#[derive(Debug, Clone)]
pub struct Subscription {
    id: SubscriptionId,
    journal_id: JournalId,
    mode: SubscriptionMode,
}

impl Subscription {
    #[must_use]
    pub fn new(journal_id: JournalId, mode: SubscriptionMode) -> Self {
        Self {
            id: SubscriptionId::new(),
            journal_id,
            mode,
        }
    }

    #[must_use]
    pub fn id(&self) -> SubscriptionId {
        self.id
    }

    #[must_use]
    pub fn journal_id(&self) -> JournalId {
        self.journal_id
    }

    #[must_use]
    pub fn mode(&self) -> SubscriptionMode {
        self.mode
    }
}

#[derive(Debug, Clone)]
pub struct StreamState {
    committed_position: u64,
    next_position: u64,
}

impl StreamState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            committed_position: 0,
            next_position: 0,
        }
    }

    #[must_use]
    pub fn committed_position(&self) -> u64 {
        self.committed_position
    }

    #[must_use]
    pub fn next_position(&self) -> u64 {
        self.next_position
    }
}

impl Default for StreamState {
    fn default() -> Self {
        Self::new()
    }
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

pub struct WorkState {
    items: HashMap<RecordId, WorkItemState>,
}

impl WorkState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            items: HashMap::new(),
        }
    }
}

impl Default for WorkState {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    pub record_id: RecordId,
    pub lease_id: LeaseId,
    pub consumer_id: ConsumerId,
    pub generation: u64,
    pub expires_at: u64,
}

