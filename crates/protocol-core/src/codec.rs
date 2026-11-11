use crate::{FRAME_HEADER_LEN, Frame, FrameHeader, ProtocolError};
use bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

pub const DEFAULT_MAX_FRAME_BYTES: u32 = 16 * 1024 * 1024; // 16 MiB

#[derive(Debug, Clone)]
pub struct CodecLimits {
    pub max_frame_bytes: u32,
}

impl Default for CodecLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }
}

pub struct FrameCodec {
    limits: CodecLimits,
}

impl FrameCodec {
    pub fn new(limits: CodecLimits) -> Self {
        Self { limits }
    }
}

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Step 1: Wait until full 24-byte header is present
        if src.len() < FRAME_HEADER_LEN {
            return Ok(None);
        }

        // Step 2: Peek at header without advancing read cursor
        let header = FrameHeader::decode(&src[..FRAME_HEADER_LEN])?;

        // Step 3: Defensive check - reject oversized frames before allocating payload
        if header.payload_len > self.limits.max_frame_bytes {
            return Err(ProtocolError::FrameTooLarge {
                size: header.payload_len,
                max: self.limits.max_frame_bytes,
            });
        }

        let total_frame_len = FRAME_HEADER_LEN + header.payload_len as usize;

        // Step 4: Wait until the complete payload is in the buffer
        if src.len() < total_frame_len {
            src.reserve((total_frame_len - src.len()).min(64 * 1024));
            return Ok(None);
        }

        // Step 5: Zero-copy extraction
        src.advance(FRAME_HEADER_LEN);
        let payload: Bytes = src.split_to(header.payload_len as usize).freeze();

        Ok(Some(Frame { header, payload }))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload_len =
            u32::try_from(item.payload.len()).map_err(|_| ProtocolError::FrameTooLarge {
                size: u32::MAX,
                max: self.limits.max_frame_bytes,
            })?;
        if payload_len > self.limits.max_frame_bytes {
            return Err(ProtocolError::FrameTooLarge {
                size: payload_len,
                max: self.limits.max_frame_bytes,
            });
        }
        if item.header.payload_len != payload_len {
            return Err(ProtocolError::MalformedPayload(
                "Header payload length does not match payload".into(),
            ));
        }
        let total_len = FRAME_HEADER_LEN + item.payload.len();
        dst.reserve(total_len);
        item.header.encode(dst);
        dst.extend_from_slice(&item.payload);
        Ok(())
    }
}
