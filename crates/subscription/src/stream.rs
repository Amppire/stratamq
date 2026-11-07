use strata_journal::Journal;
use strata_record::Record;

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

    pub fn fetch<'a>(&mut self, journal: &'a Journal, max_records: usize) -> &'a [Record] {
        let records = journal.records_by_range(self.next_position, max_records);
        self.next_position += records.len() as u64;
        records
    }
    pub fn commit(&mut self) {
        self.committed_position = self.next_position;
    }
    pub fn reset_to_committed(&mut self) {
        self.next_position = self.committed_position;
    }
    pub fn seek(&mut self, position: u64) {
        self.next_position = position;
    }
}

impl Default for StreamState {
    fn default() -> Self {
        Self::new()
    }
}
