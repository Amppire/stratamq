use std::collections::HashMap;
use strata_types::ConsumerId;

#[derive(Debug, Clone, Default)]
pub struct BroadcastState {
    consumer_positions: HashMap<ConsumerId, u64>,
}

impl BroadcastState {
    #[must_use]
    pub fn new() -> Self {
        Self {
            consumer_positions: HashMap::new(),
        }
    }

    #[must_use]
    pub fn position(&self, consumer_id: &ConsumerId) -> Option<u64> {
        self.consumer_positions.get(consumer_id).copied()
    }

    pub fn set_position(&mut self, consumer_id: ConsumerId, position: u64) {
        self.consumer_positions.insert(consumer_id, position);
    }
}
