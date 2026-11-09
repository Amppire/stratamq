use bytes::{Buf, BufMut, Bytes, BytesMut};
use crate::{ProtocolError, capabilities::Capabilities};
use strata_types::SessionId;
use uuid::Uuid;

pub const MAX_CLIENT_ID_LEN: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloMessage {
    pub version: u8,
    pub compression: u8,
    pub client_id: String,
    pub capabilities: Capabilities,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WelcomeMessage {
    pub version: u8,
    pub session_id: SessionId,
    pub capabilities: Capabilities,
    pub max_frame_bytes: u32,
    pub max_batch_records: u32,
    pub heartbeat_interval_ms: u32,
}

impl HelloMessage {
    pub fn encode(&self, dst: &mut BytesMut) {
        dst.put_u8(self.version);
        dst.put_u8(self.compression);
        let id_bytes = self.client_id.as_bytes();
        dst.put_u16(id_bytes.len() as u16);
        dst.extend_from_slice(id_bytes);
        dst.put_u64(self.capabilities.bits());
    }

    pub fn decode(src: &mut Bytes) -> Result<Self, ProtocolError> {
        if src.remaining() < 4 {
            return Err(ProtocolError::MalformedPayload("Truncated Hello".into()));
        }

        let version = src.get_u8();
        let compression = src.get_u8();
        let id_len = src.get_u16() as usize;

        if id_len > MAX_CLIENT_ID_LEN {
            return Err(ProtocolError::ClientIdTooLong {
                len: id_len,
                max: MAX_CLIENT_ID_LEN,
            });
        }

        if src.remaining() < id_len + 8 {
            return Err(ProtocolError::MalformedPayload("Truncated client_id or capabilities".into()));
        }

        let client_id = String::from_utf8(src.split_to(id_len).to_vec())
            .map_err(|e| ProtocolError::MalformedPayload(e.to_string()))?;
        let capabilities = Capabilities::from_bits_truncate(src.get_u64());

        Ok(Self {
            version,
            compression,
            client_id,
            capabilities,
        })
    }
}

impl WelcomeMessage {
    pub fn encode(&self, dst: &mut BytesMut) {
        dst.put_u8(self.version);
        dst.put_slice(&[0u8; 3]);
        dst.put_slice(self.session_id.as_uuid().as_bytes());
        dst.put_u64(self.capabilities.bits());
        dst.put_u32(self.max_frame_bytes);
        dst.put_u32(self.max_batch_records);
        dst.put_u32(self.heartbeat_interval_ms);
    }

    pub fn decode(src: &mut Bytes) -> Result<Self, ProtocolError> {
        // 1 (version) + 3 (pad) + 16 (session_id) + 8 (caps) + 4 (frame) + 4 (batch) + 4 (hb) = 40 bytes
        if src.remaining() < 40 {
            return Err(ProtocolError::MalformedPayload("Truncated Welcome".into()));
        }

        let version = src.get_u8();
        src.advance(3); // Skip padding

        let mut session_bytes = [0u8; 16];
        src.copy_to_slice(&mut session_bytes);
        let session_id = SessionId::from(Uuid::from_bytes(session_bytes));

        let capabilities = Capabilities::from_bits_truncate(src.get_u64());
        let max_frame_bytes = src.get_u32();
        let max_batch_records = src.get_u32();
        let heartbeat_interval_ms = src.get_u32();

        Ok(Self {
            version,
            session_id,
            capabilities,
            max_frame_bytes,
            max_batch_records,
            heartbeat_interval_ms,
        })
    }
}