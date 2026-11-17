use crate::{
    capabilities::Capabilities,
    error::ProtocolError,
    messages::{HelloMessage, WelcomeMessage},
};
use strata_types::SessionId;

pub const PROTOCOL_VERSION: u8 = 0;

#[derive(Debug, Clone)]
pub struct ServerHandshakeConfig {
    pub version: u8,
    pub capabilities: Capabilities,
    pub max_frame_bytes: u32,
    pub max_batch_records: u32,
    pub max_demand_records: u32,
    pub heartbeat_interval: u32,
}

impl Default for ServerHandshakeConfig {
    fn default() -> Self {
        Self {
            version: PROTOCOL_VERSION,
            capabilities: Capabilities::BATCH_PUBLISH | Capabilities::DEMAND_WINDOW,
            max_frame_bytes: 16 * 1024 * 1024,
            max_batch_records: 10_000,
            max_demand_records: 10_000,
            heartbeat_interval: 30_000,
        }
    }
}

pub fn negotiate(
    hello: &HelloMessage,
    config: &ServerHandshakeConfig,
) -> Result<WelcomeMessage, ProtocolError> {
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
        max_demand_records: config.max_demand_records,
        heartbeat_interval: config.heartbeat_interval,
    })
}
