#![forbid(unsafe_code)]

use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};
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
    pub revision: Option<u64>,
    pub position: u64,
}
pub struct Journal {
    id: JournalId,
    records: Vec<Record>,
    revisions: HashMap<Subject, u64>,
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

    pub fn append(
        &mut self,
        proposed: ProposedRecord,
        expected_revision: ExpectedRevision,
    ) -> Result<AppendResult, AppendError> {
        let subject = proposed.subject().cloned();
        let next_revision = match subject.as_ref() {
            Some(subject) => {
                let current = self.current_revision(subject);
                Self::validate_revision(expected_revision, current)?;
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

    fn validate_revision(
        expected: ExpectedRevision,
        actual: Option<u64>,
    ) -> Result<(), AppendError> {
        match expected {
            ExpectedRevision::Any => Ok(()),
            ExpectedRevision::Exact(expected) if actual == Some(expected) => Ok(()),
            ExpectedRevision::NoSubject if actual.is_none() => Ok(()),
            _ => Err(AppendError::RevisionMismatch { expected, actual }),
        }
    }

    pub fn records_by_subject<'a>(
        &'a self,
        subject: &'a Subject,
    ) -> impl Iterator<Item = &'a Record> + 'a {
        self.records
            .iter()
            .filter(move |record| record.subject() == Some(subject))
    }

    pub fn records_by_range(&self, position: u64, max_records: usize) -> &[Record] {
        let start = position as usize;
        if start >= self.records.len() {
            return &[];
        }
        let end = start.saturating_add(max_records).min(self.records.len());
        &self.records[start..end]
    }
}

impl Default for Journal {
    fn default() -> Self {
        Self::new()
    }
}

fn current_timestamp() -> Result<u64, AppendError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AppendError::InvalidSystemTime)?;
    Ok(now.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subject(name: &str) -> Subject {
        Subject::new(name)
    }
    fn proposed(subject_name: &str) -> ProposedRecord {
        ProposedRecord::new("payload").with_subject(subject(subject_name))
    }

    #[test]
    fn first_subject_gets_revision_zero() {
        let mut journal = Journal::new();
        let result = journal
            .append(proposed("subject"), ExpectedRevision::NoSubject)
            .unwrap();
        assert_eq!(result.revision, Some(0));
        assert_eq!(journal.current_revision(&subject("subject")), Some(0));
    }

    #[test]
    fn second_subject_gets_revision_one() {
        let mut journal = Journal::new();
        journal
            .append(proposed("subject"), ExpectedRevision::NoSubject)
            .unwrap();
        let result = journal
            .append(proposed("subject"), ExpectedRevision::Exact(0))
            .unwrap();
        assert_eq!(result.revision, Some(1));
        assert_eq!(journal.current_revision(&subject("subject")), Some(1));
    }

    #[test]
    fn reject_stale_expected_revision() {
        let mut journal = Journal::new();
        journal
            .append(proposed("subject"), ExpectedRevision::NoSubject)
            .unwrap();
        journal
            .append(proposed("subject"), ExpectedRevision::Exact(0))
            .unwrap();
        let result = journal.append(proposed("subject"), ExpectedRevision::Exact(0));
        assert_eq!(
            result,
            Err(AppendError::RevisionMismatch {
                expected: ExpectedRevision::Exact(0),
                actual: Some(1),
            })
        );
        assert_eq!(journal.len(), 2);
    }

    #[test]
    fn reject_no_subject_revision_when_subject_exists() {
        let mut journal = Journal::new();
        journal
            .append(proposed("subject"), ExpectedRevision::NoSubject)
            .unwrap();
        let result = journal.append(proposed("subject"), ExpectedRevision::NoSubject);
        assert_eq!(
            result,
            Err(AppendError::RevisionMismatch {
                expected: ExpectedRevision::NoSubject,
                actual: Some(0),
            })
        );
    }

    #[test]
    fn append_record_without_subject() {
        let mut journal = Journal::new();
        let result = journal
            .append(ProposedRecord::new("payload"), ExpectedRevision::Any)
            .unwrap();
        assert_eq!(result.position, 0);
        assert_eq!(result.revision, None);
        assert_eq!(journal.len(), 1);
    }

    #[test]
    fn reject_revision_without_subject() {
        let mut journal = Journal::new();
        let result = journal.append(ProposedRecord::new("payload"), ExpectedRevision::NoSubject);
        assert_eq!(result, Err(AppendError::RevisionWithoutSubject));
    }

    #[test]
    fn subject_history() {
        let mut journal = Journal::new();
        journal
            .append(proposed("subject"), ExpectedRevision::NoSubject)
            .unwrap();
        journal
            .append(proposed("subject"), ExpectedRevision::Exact(0))
            .unwrap();
        journal
            .append(proposed("subject"), ExpectedRevision::Exact(1))
            .unwrap();
        let subject = subject("subject");
        let records: Vec<_> = journal.records_by_subject(&subject).collect();
        assert_eq!(records.len(), 3);
    }
}
