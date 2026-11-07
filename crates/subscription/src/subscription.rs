use strata_types::{JournalId, SubscriptionId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionMode {
    Stream,
    Work,
    Broadcast,
    Keyed,
    Ephemeral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
