use thiserror::Error;
use strata_types::{LeaseId, RecordId};
use crate::work::Lease;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WorkError {
    #[error("Work item {record_id:?} is not ready for processing")]
    NotReady { record_id: RecordId },
    #[error("Work item {record_id:?} is already leased by another consumer")]
    AlreadyLeased {
        record_id: RecordId,
        current_lease: Lease,
    },
    #[error("Work item {record_id:?} has exceeded the maximum number of delivery attempts")]
    MaxDeliveryAttemptsExceeded { record_id: RecordId },
    #[error("Work item {record_id:?} is not currently leased")]
    NotLeased { record_id: RecordId },
    #[error("Lease Id {lease_id:?} does not match the current lease for work item {record_id:?}")]
    LeaseMismatch {
        record_id: RecordId,
        lease_id: LeaseId,
    },
    #[error(
        "Lease generation is stale for work item {record_id:?}. Current generation: {current_generation:?}, provided generation: {provided_generation:?}"
    )]
    StaleGeneration {
        record_id: RecordId,
        current_generation: u64,
        provided_generation: u64,
    },
    #[error(
        "Lease has expired for work item {record_id:?}. Current time: {current_time:?}, lease expiration: {lease_expiration:?}"
    )]
    LeaseExpired {
        record_id: RecordId,
        current_time: u64,
        lease_expiration: u64,
    },
    #[error(
        "Record {record_id:?} has already been completed or dead-lettered and cannot be processed further"
    )]
    AlreadyCompletedOrDeadLettered { record_id: RecordId },
    #[error("Record {record_id:?} has already been completed")]
    AlreadyCompleted { record_id: RecordId },
}
