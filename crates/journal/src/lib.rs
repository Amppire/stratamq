#![forbid(unsafe_code)]

use std::{collections::HashMap, time::{SystemTime, UNIX_EPOCH}};
use thiserror::Error;

use strata_record::{ProposedRecord, Record, Subject};
use strata_types::JournalId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedRevision {
    Any,
    Exact(u64),
    NoSubject,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AppendError {
    #[error("Expected revision {expected:?} does not match actual revision {actual:?}")]
    RevisionMismatch {
        expected: ExpectedRevision,
        actual: Option<u64>,
    },
    #[error("Revisions cannot be used for records without a subject")]
    RevisionWithoutSubject,
    #[error("System time is before the UNIX epoch")]
    InvalidSystemTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    pub revision: u64,
    pub position: u64,
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
    pub fn get(&self, position: usize) -> Option<&Record> {
        self.records.get(position)
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
        self.revisions.get(subject).copied()
    }

    pub fn append(&mut self, proposed: ProposedRecord, expected_revision: ExpectedRevision) -> Result<AppendResult, AppendError> {
        let subject = proposed.subject().cloned();
        let current_revision = subject.as_ref().and_then(|s| self.revisions.get(s).copied());
        let next_revision = match subject.as_ref() {
            Some(subject) => {
                Self::validate_revision(expected_revision, current_revision)?;
                Some(match current {
                    Some(revision) => revision + 1,
                    None => 0,
                })
            }
            None => {
                if expected_revision != ExpectedRevision::Any {
                    return Err(AppendError::RevisionWithoutSubject);
                }
                None
            }
        };
        let timestamp = current_timestamp()?;
        let record = proposed.commit(timestamp, next_revision);
        let position = self.records.len() as u64;
        self.records.push(record);
        if let (Some(subject), Some(revision)) = (subject, next_revision) {
            self.revisions.insert(subject, revision);
        }
        Ok(AppendResult {
            position,
            revision: next_revision,
        })
    }

    fn validate_revision(expected: ExpectedRevision, actual: Option<u64>) -> Result<(), AppendError> {
        match expected {
            ExpectedRevision::Any => Ok(()),
            ExpectedRevision::Exact(expected) => {
                if actual == Some(expected) => {
                    Ok(())
                }
            }
            ExpectedRevision::NoSubject => {
                if actual.is_none() => {
                    Ok(())
                }
            }
            _ => Err(AppendError::RevisionMismatch {
                expected,
                actual,
            }),
        }
    }
}

impl Default for Journal {
    fn default() -> Self {
        Self::new()
    }
}

fn current_timestamp() -> Result<u64, AppendError> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| AppendError::InvalidSystemTime)?;
    Ok(now.as_millis() as u64)
}
