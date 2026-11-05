#![forbid(unsafe_code)]

use bytes::Bytes;
use std::collections::BTreeMap;
use strata_types::RecordId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Subject(String);

impl Subject {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct ProposedRecord {
    subject: Option<Subject>,
    key: Option<Bytes>,
    headers: BTreeMap<String, Bytes>,
    payload: Bytes,
}

impl ProposedRecord {
    #[must_use]
    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self {
            subject: None,
            key: None,
            headers: BTreeMap::new(),
            payload: payload.into(),
        }
    }

    #[must_use]
    pub fn with_subject(mut self, subject: Subject) -> Self {
        self.subject = Some(subject);
        self
    }

    #[must_use]
    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    #[must_use]
    pub fn subject(&self) -> Option<&Subject> {
        self.subject.as_ref()
    }

    #[must_use]
    pub fn key(&self) -> Option<&Bytes> {
        self.key.as_ref()
    }

    #[must_use]
    pub fn headers(&self) -> &BTreeMap<String, Bytes> {
        &self.headers
    }

    #[must_use]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }

    #[must_use]
    pub fn commit(self, timestamp: u64, revision: Option<u64>) -> Record {
        Record {
            id: RecordId::new(),
            timestamp,
            subject: self.subject,
            revision,
            key: self.key,
            headers: self.headers,
            payload: self.payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Record {
    id: RecordId,
    timestamp: u64,
    subject: Option<Subject>,
    revision: Option<u64>,
    key: Option<Bytes>,
    headers: BTreeMap<String, Bytes>,
    payload: Bytes,
}

impl Record {
    #[must_use]
    pub fn id(&self) -> RecordId {
        self.id
    }

    #[must_use]
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    #[must_use]
    pub fn subject(&self) -> Option<&Subject> {
        self.subject.as_ref()
    }

    #[must_use]
    pub fn revision(&self) -> Option<u64> {
        self.revision
    }

    #[must_use]
    pub fn key(&self) -> Option<&Bytes> {
        self.key.as_ref()
    }

    #[must_use]
    pub fn headers(&self) -> &BTreeMap<String, Bytes> {
        &self.headers
    }

    #[must_use]
    pub fn payload(&self) -> &Bytes {
        &self.payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proposed_record_has_no_metadata() {
        let proposed = ProposedRecord::new("payload").with_subject(Subject::new("subject"));

        assert_eq!(proposed.subject().map(Subject::as_str), Some("subject"));
    }

    #[test]
    fn commit_assigns_metadata() {
        let proposed = ProposedRecord::new("payload").with_subject(Subject::new("subject"));

        let record = proposed.commit(1234567890, Some(1));

        assert_eq!(record.subject().map(Subject::as_str), Some("subject"));
        assert_eq!(record.revision(), Some(1));
        assert_eq!(record.timestamp(), 1234567890);
    }

    #[test]
    fn test_unsubjected_records() {
        let record = ProposedRecord::new("payload").commit(1234567890, None);

        assert!(record.subject().is_none());
        assert!(record.revision().is_none());
    }
}
