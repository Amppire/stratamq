#[cfg(test)]
mod tests {
    use crate::{DemandError, DemandLimits, DemandTracker, DemandMessage, FrameHeader, Opcode};

    #[test]
    fn check_opcode_mapping() {
        assert_eq!(Opcode::try_from(0x0004), Ok(Opcode::Ping));
        assert_eq!(Opcode::Ping as u16, 0x0004);
        // Ensure 0x0005 (unmapped) is rejected and delivers an error
        assert_eq!(Opcode::try_from(0x0005), Err(0x0005));
    }

    #[test]
    fn credit_exhaustion_halts_delivery() {
        let mut tracker = DemandTracker::new(DemandLimits::default());
        let demand = DemandMessage {
            max_records: 10,
            max_bytes: 1000,
            wait: 5000,
            continuous: true,
        };
        tracker.grant(demand, 1000);

        assert!(tracker.can_deliver(1500));
        tracker.deduct(10, 500).expect("deduct should succeed");

        // Zero records remaining -> delivery must halt
        assert_eq!(tracker.remaining_records(), 0);
        assert!(!tracker.can_deliver(1600));

        // Replenish records -> delivery resumes
        tracker.replenish(5, 0);
        assert!(tracker.can_deliver(1600));
    }

    #[test]
    fn over_deduction_returns_error() {
        let mut tracker = DemandTracker::new(DemandLimits::default());
        tracker.grant(
            DemandMessage {
                max_records: 5,
                max_bytes: 500,
                wait: 0,
                continuous: false,
            },
            1000,
        );

        let err = tracker.deduct(6, 100).unwrap_err();
        assert!(matches!(err, DemandError::CreditExceeded { .. }));
    }

    #[test]
    fn saturating_replenishment_respects_limits() {
        let limits = DemandLimits {
            max_demand_records: 1000,
            max_demand_bytes: 50_000,
        };
        let mut tracker = DemandTracker::new(limits);
        // Malicious client tries integer overflow (u32::MAX)
        tracker.replenish(u32::MAX, u32::MAX);

        assert_eq!(tracker.remaining_records(), 1000);
        assert_eq!(tracker.remaining_bytes(), 50_000);
    }
}
