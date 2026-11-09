//! The [`ProposedRecord`] builder: a record's content before the broker
//! assigns it an identity at commit time.

use crate::record::Record;
use crate::subject::Subject;
use bytes::Bytes;
use std::collections::BTreeMap;
use strata_types::RecordId;

/// A record's content prior to commit. Carries no [`RecordId`]; the id is
/// minted by [`ProposedRecord::commit`] (broker-assigned, NMP §8.1).
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
        Record::from_parts(
            RecordId::new(),
            timestamp,
            self.subject,
            revision,
            self.key,
            self.headers,
            self.payload,
        )
    }
}
