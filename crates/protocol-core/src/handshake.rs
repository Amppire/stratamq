use strata_types::SessionId;
use crate::{capabilities::Capabilities, error::ProtocolError, messages::{HelloMessage, WelcomeMessage}};

pub const PROTOCOL_VERSION: u8 = 0;

#[derive(Debug, Clone)]
pub struct ServerHandshakeConfig {
    pub version: u8,
    pub capabilities: Capabilities,
    pub max_frame_bytes: u32,
    pub max_batch_records: u32,
    pub heartbeat_interval_ms: u32,
}

impl Default for ServerHandshakeConfig {
    fn default() -> Self {
        Self {
            version: PROTOCOL_VERSION,
            capabilities: Capabilities::all(),
            max_frame_bytes: 16 * 1024 * 1024,
            max_batch_records: 10_000,
            heartbeat_interval_ms: 30_000,
        }
    }
}

pub fn negotiate(hello: &HelloMessage, config: &ServerHandshakeConfig) -> Result<WelcomeMessage, ProtocolError> {
    // 1. Base protocol compatibility check
    if hello.version != config.version {
        return Err(ProtocolError::IncompatibleVersion {
            expected: config.version,
            actual: hello.version,
        });
    }

    // 2. Capability negotiation: clear unknown or unsupported bits
    let negotiated_capabilities = hello.capabilities & config.capabilities;
    
    // 3. Generate session Id
    let session_id = SessionId::new();

    Ok(WelcomeMessage {
        session_id,
        version: config.version,
        capabilities: negotiated_capabilities,
        max_frame_bytes: config.max_frame_bytes,
        max_batch_records: config.max_batch_records,
        heartbeat_interval_ms: config.heartbeat_interval_ms,
    })
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

use super::*;

    #[test]
    fn welcome_message_roundtrip_preserves_session_id() {
        let session_id = SessionId::new();
        let welcome = WelcomeMessage {
            version: 0,
            session_id,
            capabilities: Capabilities::BATCH_PUBLISH | Capabilities::WORK_LEASES,
            max_frame_bytes: 4 * 1024 * 1024,
            max_batch_records: 5000,
            heartbeat_interval_ms: 15000,
        };

        let mut buf = BytesMut::new();
        welcome.encode(&mut buf);
        assert_eq!(buf.len(), 40);

        let mut bytes = buf.freeze();
        let decoded = WelcomeMessage::decode(&mut bytes).expect("decode should succeed");

        assert_eq!(decoded.session_id, session_id);
        assert_eq!(decoded.capabilities, welcome.capabilities);
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