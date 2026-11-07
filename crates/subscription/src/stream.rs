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
