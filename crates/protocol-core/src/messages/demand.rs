use crate::error::ProtocolError;
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DemandWindow {
    pub max_records: u32,
    pub max_bytes: u32,
    pub wait: u32,
    pub continuous: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowUpdateMessage {
    pub additional_records: u32,
    pub additional_bytes: u32,
}

impl DemandWindow {
    pub fn encode(&self, dst: &mut BytesMut) {
        dst.put_u32(self.max_records);
        dst.put_u32(self.max_bytes);
        dst.put_u32(self.wait);
        dst.put_u8(if self.continuous { 1 } else { 0 });
        dst.put_slice(&[0u8; 3]); // Reserved padding
    }

    pub fn decode(src: &mut Bytes) -> Result<Self, ProtocolError> {
        if src.remaining() < 16 {
            return Err(ProtocolError::MalformedPayload(
                "Truncated DemandWindow".into(),
            ));
        }
        let max_records = src.get_u32();
        let max_bytes = src.get_u32();
        let wait = src.get_u32();
        let continuous = src.get_u8() != 0;
        src.advance(3); // Skip reserved

        Ok(Self {
            max_records,
            max_bytes,
            wait,
            continuous,
        })
    }
}

impl WindowUpdateMessage {
    pub fn encode(&self, dst: &mut BytesMut) {
        dst.put_u32(self.additional_records);
        dst.put_u32(self.additional_bytes);
    }

    pub fn decode(src: &mut Bytes) -> Result<Self, ProtocolError> {
        if src.remaining() < 8 {
            return Err(ProtocolError::MalformedPayload(
                "Truncated WindowUpdate".into(),
            ));
        }
        let additional_records = src.get_u32();
        let additional_bytes = src.get_u32();
        Ok(Self {
            additional_records,
            additional_bytes,
        })
    }
}
