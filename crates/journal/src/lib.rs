#![forbid(unsafe_code)]

use std::collections::HashMap;

use strata_record::{Record, Subject};
use strata_types::JournalId;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedRevision {
    Any,
    Exact(u64),
    NoSubject,
}

#[derive(Debug, Error)]
pub enum AppendError {
    #[error("Expected revision {expected:?} does not match actual revision {actual:?}")]
    RevisionMismatch {
        expected: ExpectedRevision,
        actual: Option<u64>,
    },
}

pub struct Journal {
    id: JournalId,
    records: Vec<Record>,
    revisions: HashMap<String, u64>,
}

impl Journal {
    #[must_use]
    pub fn new() -> Self {
        Self {
            id: JournalId::new(),
            records: Vec::new(),
            revisions: HashMap::new(),
        }
    }

    #[must_use]
    pub fn id(&self) -> JournalId {
        self.id
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&Record> {
        self.records.get(index)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    #[must_use]
    pub fn current_revision(&self, subject: &Subject) -> Option<u64> {
        self.revisions.get(subject.as_str()).copied()
    }
}

impl Default for Journal {
    fn default() -> Self {
        Self::new()
    }
}
