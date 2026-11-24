//! Toy in-memory candidates, used to prove the harness works end to end
//! before any real tournament candidate exists.

use std::sync::Mutex;

use async_trait::async_trait;

use crate::traits::{
    BenchError, CommitReceipt, Durability, ReadRange, RecordBatch, RecordBatchSet, RecoveryReport,
    StorageExperiment,
};

/// Stores every appended batch in a `Vec`. Ignores durability.
#[derive(Debug, Default)]
pub struct MemStorage {
    batches: Mutex<Vec<RecordBatch>>,
}

#[async_trait]
impl StorageExperiment for MemStorage {
    async fn append(
        &self,
        batch: RecordBatch,
        _durability: Durability,
    ) -> Result<CommitReceipt, BenchError> {
        let mut batches = self.batches.lock().expect("mock lock is never poisoned");
        batches.push(batch);
        Ok(CommitReceipt {
            first_offset: batches.len() as u64 - 1,
        })
    }

    async fn read(&self, query: ReadRange) -> Result<RecordBatchSet, BenchError> {
        let batches = self.batches.lock().expect("mock lock is never poisoned");
        let batches = batches
            .iter()
            .skip(query.from_offset as usize)
            .take(query.max_batches as usize)
            .cloned()
            .collect();
        Ok(RecordBatchSet { batches })
    }

    async fn recover(&self) -> Result<RecoveryReport, BenchError> {
        Ok(RecoveryReport {
            recovered_batches: self
                .batches
                .lock()
                .expect("mock lock is never poisoned")
                .len() as u64,
            truncated_bytes: 0,
        })
    }
}
