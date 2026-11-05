#![forbid(unsafe_code)]

use bytes::Bytes;
use std::collections::BTreeMap;
use strata_types::RecordId;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Subject(String);

impl Subject {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
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
    pub fn new(
        timestamp: u64,
        subject: Option<Subject>,
        revision: Option<u64>,
        key: Option<Bytes>,
        headers: BTreeMap<String, Bytes>,
        payload: Bytes,
    ) -> Self {
        Self {
            id: RecordId::new(),
            timestamp,
            subject,
            revision,
            key,
            headers,
            payload,
        }
    }

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
    fn test_record_creation() {
        let record = Record::new(
            1234567890,
            Some(Subject::new("test_subject")),
            Some(1),
            None,
            BTreeMap::new(),
            Bytes::from_static(b"test_payload"),
        );

        assert_eq!(record.revision(), Some(1));
        assert_eq!(record.payload(), &Bytes::from_static(b"test_payload"));
    }

    #[test]
    fn test_unsubjected_records() {
        let record = Record::new(
            1234567890,
            None,
            None,
            None,
            BTreeMap::new(),
            Bytes::from_static(b"test_payload"),
        );

        assert!(record.subject().is_none());
        assert!(record.revision().is_none());
    }
}