use crate::{error::DemandError, messages::DemandWindow};

#[derive(Debug, Clone)]
pub struct DemandLimits {
    pub max_demand_records: u32, // Default: 100,000 records
    pub max_demand_bytes: u32,   // Default: 64 MiB
}

impl Default for DemandLimits {
    fn default() -> Self {
        Self {
            max_demand_records: 100_000,
            max_demand_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DemandTracker {
    remaining_records: u32,
    remaining_bytes: u32,
    continuous: bool,
    wait_deadline_ms: Option<u64>,
    limits: DemandLimits,
}

impl DemandTracker {
    #[must_use]
    pub fn new(limits: DemandLimits) -> Self {
        Self {
            remaining_records: 0,
            remaining_bytes: 0,
            continuous: false,
            wait_deadline_ms: None,
            limits,
        }
    }

    /// Apply a new DEMAND command, resetting or initializing the credit window
    pub fn grant(&mut self, demand: DemandWindow, now: u64) {
        // Enforce server bounds to prevent integer overflow attacks
        self.remaining_records = demand.max_records.min(self.limits.max_demand_records);
        self.remaining_bytes = demand.max_bytes.min(self.limits.max_demand_bytes);
        self.continuous = demand.continuous;

        self.wait_deadline_ms = if demand.wait > 0 {
            Some(now.saturating_add(demand.wait as u64))
        } else {
            None
        };
    }

    /// Replenish credits with WINDOW_UPDATE (saturating up to server limits)
    pub fn replenish(&mut self, records: u32, bytes: u32) {
        self.remaining_records = self
            .remaining_records
            .saturating_add(records)
            .min(self.limits.max_demand_records);

        self.remaining_bytes = self
            .remaining_bytes
            .saturating_add(bytes)
            .min(self.limits.max_demand_bytes);
    }

    /// Check if at least 1 record and 1 byte can be delivered
    #[must_use]
    pub fn can_deliver(&self, now: u64) -> bool {
        if self.remaining_records == 0 || self.remaining_bytes == 0 {
            return false;
        }
        if let Some(deadline) = self.wait_deadline_ms
            && now >= deadline
        {
            return false;
        }
        true
    }

    /// Deduct delivered batch credits from the window
    pub fn deduct(&mut self, records: u32, bytes: u32) -> Result<(), DemandError> {
        if records > self.remaining_records || bytes > self.remaining_bytes {
            return Err(DemandError::CreditExceeded {
                requested_records: records,
                available_records: self.remaining_records,
                requested_bytes: bytes,
                available_bytes: self.remaining_bytes,
            });
        }

        self.remaining_records = self.remaining_records.saturating_sub(records);
        self.remaining_bytes = self.remaining_bytes.saturating_sub(bytes);
        Ok(())
    }

    #[must_use]
    pub fn remaining_records(&self) -> u32 {
        self.remaining_records
    }

    #[must_use]
    pub fn remaining_bytes(&self) -> u32 {
        self.remaining_bytes
    }

    #[must_use]
    pub fn is_continuous(&self) -> bool {
        self.continuous
    }
}
