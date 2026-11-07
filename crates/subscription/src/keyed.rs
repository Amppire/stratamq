use std::collections::HashMap;
use strata_types::ConsumerId;

#[derive(Debug, Clone, Default)]
pub struct KeyedState {
    assignments: HashMap<String, ConsumerId>,
}

impl KeyedState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            assignments: HashMap::new(),
        }
    }

    #[must_use]
    pub fn assignment(&self, key: &str) -> Option<ConsumerId> {
        self.assignments.get(key).copied()
    }

    pub fn assign(&mut self, key: impl Into<String>, consumer_id: ConsumerId) {
        self.assignments.insert(key.into(), consumer_id);
    }
}
