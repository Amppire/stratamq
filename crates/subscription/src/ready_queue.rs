use std::collections::VecDeque;
use strata_types::RecordId;

pub trait ReadyQueue {
    fn push(&mut self, record_id: RecordId);
    fn pop(&mut self) -> Option<RecordId>;
    fn is_empty(&self) -> bool;
}

#[derive(Debug, Clone)]
pub struct FifoReadyQueue {
    items: VecDeque<RecordId>,
}

impl FifoReadyQueue {
    #[must_use]
    pub fn new() -> Self {
        Self {
            items: VecDeque::new(),
        }
    }
}

impl Default for FifoReadyQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadyQueue for FifoReadyQueue {
    fn push(&mut self, record_id: RecordId) {
        self.items.push_back(record_id);
    }

    fn pop(&mut self) -> Option<RecordId> {
        self.items.pop_front()
    }

    fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}
