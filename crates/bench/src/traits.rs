//! Tournament experiment traits: storage, sharding, delivery.
//!
//! Every candidate design implements one of these so the harness can run,
//! time and report on all of them the same way. The supporting types are
//! deliberately minimal; they grow as real candidates need more.

use std::time::Duration;

use async_trait::async_trait;
use strata_record::batch::RecordBatchV0;
use thiserror::Error;

/// The batch type moved through every experiment.
pub type RecordBatch = RecordBatchV0;

/// Failure reported by a candidate implementation.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum BenchError {
    #[error("experiment failed: {0}")]
    Failed(String),
}

// ---------------------------------------------------------------- storage

/// How careful an append must be before it is acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Acknowledge once buffered; may lose data on crash.
    Buffered,
    /// Acknowledge only after the data is fsynced.
    Fsync,
}

/// Proof that an append was committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitReceipt {
    /// Offset assigned to the first batch/record of the append.
    pub first_offset: u64,
}

/// Which stored data a read wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadRange {
    pub from_offset: u64,
    pub max_batches: u32,
}

/// Result of a read.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RecordBatchSet {
    pub batches: Vec<RecordBatch>,
}

/// What survived a crash and restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryReport {
    pub recovered_batches: u64,
    /// Bytes dropped from a torn or corrupt tail.
    pub truncated_bytes: u64,
}

#[async_trait]
pub trait StorageExperiment: Send + Sync {
    async fn append(
        &self,
        batch: RecordBatch,
        durability: Durability,
    ) -> Result<CommitReceipt, BenchError>;
    async fn read(&self, query: ReadRange) -> Result<RecordBatchSet, BenchError>;
    async fn recover(&self) -> Result<RecoveryReport, BenchError>;
}

// --------------------------------------------------------------- sharding

/// Where a key lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardPlacement {
    pub shard: u32,
    pub storage_group: u32,
}

/// The cluster shape a rebalance targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterTopology {
    pub storage_groups: Vec<u32>,
}

/// What a rebalance would move; fewer moves means less data churn.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MovementPlan {
    pub moved_shards: Vec<u32>,
}

pub trait ShardingExperiment: Send + Sync {
    fn locate(&self, key: &[u8]) -> ShardPlacement;
    fn rebalance(&mut self, topology: ClusterTopology) -> MovementPlan;
}

// --------------------------------------------------------------- delivery

/// A consumer-authorised budget for how much may be pushed to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DemandWindow {
    pub max_records: u32,
    pub max_bytes: u64,
    pub deadline: Option<Duration>,
    /// Whether the window refills automatically as it is consumed.
    pub continuous: bool,
}

/// Records handed to a consumer.
#[derive(Debug, Clone, PartialEq)]
pub struct DeliveryBatch {
    pub batch: RecordBatch,
}

#[async_trait]
pub trait DeliveryExperiment: Send + Sync {
    async fn grant(&self, demand: DemandWindow);
    async fn recv(&self) -> Result<DeliveryBatch, BenchError>;
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    fn empty_batch() -> RecordBatch {
        RecordBatch {
            producer_id: 1,
            producer_epoch: 0,
            first_sequence: 0,
            first_timestamp: 0,
            is_transactional: false,
            records: vec![],
        }
    }

    /// Toy in-memory storage: proves the trait is implementable and usable
    /// as `dyn StorageExperiment`.
    #[derive(Default)]
    struct MemStorage {
        batches: Mutex<Vec<RecordBatch>>,
    }

    #[async_trait]
    impl StorageExperiment for MemStorage {
        async fn append(
            &self,
            batch: RecordBatch,
            _durability: Durability,
        ) -> Result<CommitReceipt, BenchError> {
            let mut batches = self.batches.lock().unwrap();
            batches.push(batch);
            Ok(CommitReceipt {
                first_offset: batches.len() as u64 - 1,
            })
        }

        async fn read(&self, query: ReadRange) -> Result<RecordBatchSet, BenchError> {
            let batches = self.batches.lock().unwrap();
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
                recovered_batches: self.batches.lock().unwrap().len() as u64,
                truncated_bytes: 0,
            })
        }
    }

    /// Toy router: `key sum % shards`, one storage group per shard.
    struct ModShards {
        shards: u32,
    }

    impl ShardingExperiment for ModShards {
        fn locate(&self, key: &[u8]) -> ShardPlacement {
            let sum: u32 = key.iter().map(|&b| u32::from(b)).sum();
            let shard = sum % self.shards;
            ShardPlacement {
                shard,
                storage_group: shard,
            }
        }

        fn rebalance(&mut self, topology: ClusterTopology) -> MovementPlan {
            let new_shards = topology.storage_groups.len() as u32;
            let moved_shards = (0..self.shards.min(new_shards)).collect();
            self.shards = new_shards;
            MovementPlan { moved_shards }
        }
    }

    /// Toy delivery: hands out one batch per granted record.
    #[derive(Default)]
    struct CreditDelivery {
        credit: Mutex<u32>,
    }

    #[async_trait]
    impl DeliveryExperiment for CreditDelivery {
        async fn grant(&self, demand: DemandWindow) {
            *self.credit.lock().unwrap() += demand.max_records;
        }

        async fn recv(&self) -> Result<DeliveryBatch, BenchError> {
            let mut credit = self.credit.lock().unwrap();
            if *credit == 0 {
                return Err(BenchError::Failed("no demand granted".into()));
            }
            *credit -= 1;
            Ok(DeliveryBatch {
                batch: empty_batch(),
            })
        }
    }

    #[tokio::test]
    async fn storage_append_then_read_round_trips() {
        let storage: Box<dyn StorageExperiment> = Box::new(MemStorage::default());
        let receipt = storage
            .append(empty_batch(), Durability::Fsync)
            .await
            .unwrap();
        assert_eq!(receipt.first_offset, 0);

        let read = storage
            .read(ReadRange {
                from_offset: 0,
                max_batches: 10,
            })
            .await
            .unwrap();
        assert_eq!(read.batches, vec![empty_batch()]);
        assert_eq!(storage.recover().await.unwrap().recovered_batches, 1);
    }

    #[test]
    fn sharding_locate_is_stable_and_rebalance_reports_moves() {
        let mut router: Box<dyn ShardingExperiment> = Box::new(ModShards { shards: 4 });
        assert_eq!(router.locate(b"abc"), router.locate(b"abc"));

        let plan = router.rebalance(ClusterTopology {
            storage_groups: vec![0, 1, 2, 3, 4],
        });
        assert_eq!(plan.moved_shards, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn delivery_needs_demand_before_recv() {
        let delivery: Box<dyn DeliveryExperiment> = Box::new(CreditDelivery::default());
        assert!(delivery.recv().await.is_err());

        delivery
            .grant(DemandWindow {
                max_records: 1,
                max_bytes: 1_024,
                deadline: None,
                continuous: false,
            })
            .await;
        assert!(delivery.recv().await.is_ok());
        assert!(delivery.recv().await.is_err());
    }
}
