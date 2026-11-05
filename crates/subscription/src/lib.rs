#![forbid(unsafe_code)]

use strata_types::{JournalId, SubscriptionId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionMode {
    Stream,
    Work,
    Broadcast,
    Keyed,
    Ephemeral,
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