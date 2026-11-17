#[cfg(test)]
mod tests {
    use crate::{
        Capabilities, DemandError, DemandLimits, DemandMessage, DemandTracker, HelloMessage,
        Opcode, ProtocolError, ServerHandshakeConfig, WelcomeMessage, negotiate,
    };
    use bytes::BytesMut;
    use strata_types::SessionId;

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

    #[test]
    fn welcome_message_roundtrip_preserves_session_id() {
        let session_id = SessionId::new();
        let welcome = WelcomeMessage {
            version: 0,
            session_id,
            capabilities: Capabilities::BATCH_PUBLISH | Capabilities::WORK_LEASES,
            max_frame_bytes: 4 * 1024 * 1024,
            max_batch_records: 5000,
            max_demand_records: 2500,
            heartbeat_interval: 15000,
        };

        let mut buf = BytesMut::new();
        welcome.encode(&mut buf);
        assert_eq!(buf.len(), 44);

        let mut bytes = buf.freeze();
        let decoded = WelcomeMessage::decode(&mut bytes).expect("decode should succeed");

        assert_eq!(decoded.session_id, session_id);
        assert_eq!(decoded.capabilities, welcome.capabilities);
        assert_eq!(decoded.max_demand_records, welcome.max_demand_records);
    }

    #[test]
    fn capability_negotiation_masks_unsupported_bits() {
        let config = ServerHandshakeConfig {
            version: 0,
            capabilities: Capabilities::BATCH_PUBLISH | Capabilities::DEMAND_WINDOW,
            ..Default::default()
        };

        // Client asks for BATCH_PUBLISH + ATOMIC_OPS + an unassigned bit (1 << 35)
        let requested = Capabilities::BATCH_PUBLISH | Capabilities::ATOMIC_OPS;
        let hello = HelloMessage {
            version: 0,
            compression: 0,
            client_id: "test-worker".into(),
            capabilities: requested,
        };

        let welcome = negotiate(&hello, &config).expect("handshake should succeed");
        // Server only grants BATCH_PUBLISH (the intersection)
        assert_eq!(welcome.capabilities, Capabilities::BATCH_PUBLISH);
    }

    #[test]
    fn incompatible_version_rejected() {
        let config = ServerHandshakeConfig::default();
        let hello = HelloMessage {
            version: 0x05, // Different from Draft 0
            compression: 0,
            client_id: "legacy-client".into(),
            capabilities: Capabilities::all(),
        };

        let err = negotiate(&hello, &config).unwrap_err();
        assert!(matches!(
            err,
            ProtocolError::IncompatibleVersion {
                expected: 0,
                actual: 5
            }
        ));
    }
}
