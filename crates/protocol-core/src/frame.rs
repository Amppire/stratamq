use crate::{Opcode, ProtocolError};
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const FRAME_HEADER_LEN: usize = 24;
pub const PROTOCOL_VERSION_DRAFT0: u8 = 0x00;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub version: u8,
    pub flags: u8,
    pub opcode: Opcode,
    pub stream_id: u32,
    pub request_id: u64,
    pub payload_len: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub header: FrameHeader,
    pub payload: Bytes,
}

impl FrameHeader {
    pub fn encode(&self, dst: &mut BytesMut) {
        dst.put_u8(self.version);
        dst.put_u8(self.flags);
        dst.put_u16(self.opcode as u16);
        dst.put_u32(self.stream_id);
        dst.put_u64(self.request_id);
        dst.put_u32(self.payload_len);
        dst.put_u32(0); // 4-byte reserved / padding
    }

    pub fn decode(src: &[u8]) -> Result<Self, ProtocolError> {
        if src.len() < FRAME_HEADER_LEN {
            return Err(ProtocolError::MalformedPayload("Truncated frame header".into()));
        }
        let mut buf = src;
        let version = buf.get_u8();
        if version != PROTOCOL_VERSION_DRAFT0 {
            return Err(ProtocolError::UnsupportedVersion { version });
        }
        let flags = buf.get_u8();
        let raw_opcode = buf.get_u16();
        let opcode = Opcode::try_from(raw_opcode)
            .map_err(|op| ProtocolError::InvalidOpcode { opcode: op })?;
        let stream_id = buf.get_u32();
        let request_id = buf.get_u64();
        let payload_len = buf.get_u32();
        let _reserved = buf.get_u32();

        Ok(Self {
            version,
            flags,
            opcode,
            stream_id,
            request_id,
            payload_len,
        })
    }
}
