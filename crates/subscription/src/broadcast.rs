use crate::StreamState;

#[derive(Debug, Clone, Default)]
pub struct BroadcastState {
    stream: StreamState,
}

impl BroadcastState {
    #[must_use]
    pub fn new() -> Self {
        Self { stream: StreamState::new() }
    }

    pub fn stream_mut(&mut self) -> &mut StreamState {
        &mut self.stream
    }

    #[must_use]
    pub fn stream(&self) -> &StreamState {
        &self.stream
    }
}