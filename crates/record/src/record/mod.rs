//! The committed [`Record`]: content plus a broker-assigned [`RecordId`].

use crate::subject::Subject;
use bytes::Bytes;
use std::collections::BTreeMap;
use strata_types::RecordId;

#[cfg(test)]
mod tests;

/// A committed record: a [`ProposedRecord`](crate::ProposedRecord)'s content
/// plus the [`RecordId`] assigned at commit time. The id is never carried on
/// the wire; it is minted by the broker (NMP §8.1).
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
    /// Assemble a `Record` from its parts. Crate-internal: the only public path
    /// to a `Record` is [`ProposedRecord::commit`](crate::ProposedRecord::commit),
    /// which mints the id itself, so this never lets external code choose an id.
    #[must_use]
    pub(crate) fn from_parts(
        id: RecordId,
        timestamp: u64,
        subject: Option<Subject>,
        revision: Option<u64>,
        key: Option<Bytes>,
        headers: BTreeMap<String, Bytes>,
        payload: Bytes,
    ) -> Self {
        Self {
            id,
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
