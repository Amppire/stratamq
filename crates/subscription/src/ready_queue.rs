use std::collections::VecDeque;
use strata_types::RecordId;

pub trait ReadyQueue {
    fn push(&mut self, record_id: RecordId);
    fn pop(&mut self) -> Option<RecordId>;
    fn is_empty(&self) -> bool;
    fn len(&self) -> usize;
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
    fn len(&self) -> usize {
        self.items.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_preserves_order() {
        let first = RecordId::new();
        let second = RecordId::new();

        let mut queue = FifoReadyQueue::new();

        queue.push(first);
        queue.push(second);

        assert_eq!(queue.pop(), Some(first));
        assert_eq!(queue.pop(), Some(second));
    }
}
