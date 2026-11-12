#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Opcode {
    Hello = 0x0001,
    Welcome = 0x0002,
    Auth = 0x0003,
    Ping = 0x0004,
    Metadata = 0x0010,
    Publish = 0x0100,
    PublishBatch = 0x0101,
    PublishResult = 0x0102,
    Subscribe = 0x0200,
    Subscribed = 0x0201,
    Demand = 0x0210,
    WindowUpdate = 0x0211,
    DeliverBatch = 0x0220,
    AckBatch = 0x0230,
    Nack = 0x0231,
    ExtendLease = 0x0232,
    Seek = 0x0240,
    CommitCursor = 0x0241,
    CreateReplay = 0x0300,
    BeginOperation = 0x0310,
    CommitOperation = 0x0311,
    Hint = 0x03F0,
    Error = 0x7FFE,
    Goaway = 0x7FFF,
}

impl TryFrom<u16> for Opcode {
    type Error = u16;
    fn try_from(val: u16) -> Result<Self, u16> {
        match val {
            0x0001 => Ok(Self::Hello),
            0x0002 => Ok(Self::Welcome),
            0x0003 => Ok(Self::Auth),
            0x0004 => Ok(Self::Ping),
            0x0010 => Ok(Self::Metadata),
            0x0100 => Ok(Self::Publish),
            0x0101 => Ok(Self::PublishBatch),
            0x0102 => Ok(Self::PublishResult),
            0x0200 => Ok(Self::Subscribe),
            0x0201 => Ok(Self::Subscribed),
            0x0210 => Ok(Self::Demand),
            0x0211 => Ok(Self::WindowUpdate),
            0x0220 => Ok(Self::DeliverBatch),
            0x0230 => Ok(Self::AckBatch),
            0x0231 => Ok(Self::Nack),
            0x0232 => Ok(Self::ExtendLease),
            0x0240 => Ok(Self::Seek),
            0x0241 => Ok(Self::CommitCursor),
            0x0300 => Ok(Self::CreateReplay),
            0x0310 => Ok(Self::BeginOperation),
            0x0311 => Ok(Self::CommitOperation),
            0x03F0 => Ok(Self::Hint),
            0x7FFE => Ok(Self::Error),
            0x7FFF => Ok(Self::Goaway),
            unknown => Err(unknown),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FrameHeader;

    #[test]
    fn ping_opcode_mapping() {
        assert_eq!(Opcode::try_from(0x0004), Ok(Opcode::Ping));
        assert_eq!(Opcode::Ping as u16, 0x0004);
        // Ensure 0x0005 (previous separate Pong mapping) is unmapped and rejected
        assert_eq!(Opcode::try_from(0x0005), Err(0x0005));
    }

    #[test]
    fn new_opcode_registry_values_mapping() {
        assert_eq!(Opcode::try_from(0x0300), Ok(Opcode::CreateReplay));
        assert_eq!(Opcode::CreateReplay as u16, 0x0300);

        assert_eq!(Opcode::try_from(0x0310), Ok(Opcode::BeginOperation));
        assert_eq!(Opcode::BeginOperation as u16, 0x0310);

        assert_eq!(Opcode::try_from(0x0311), Ok(Opcode::CommitOperation));
        assert_eq!(Opcode::CommitOperation as u16, 0x0311);

        assert_eq!(Opcode::try_from(0x03F0), Ok(Opcode::Hint));
        assert_eq!(Opcode::Hint as u16, 0x03F0);
    }

    #[test]
    fn frame_header_decode_accepts_new_opcodes_and_ping() {
        for (raw_op, expected_op) in [
            (0x0004, Opcode::Ping),
            (0x0300, Opcode::CreateReplay),
            (0x0310, Opcode::BeginOperation),
            (0x0311, Opcode::CommitOperation),
            (0x03F0, Opcode::Hint),
        ] {
            let header = FrameHeader {
                version: 0x00,
                flags: 0,
                opcode: expected_op,
                stream_id: 1,
                request_id: 42,
                payload_len: 0,
            };
            let mut buf = bytes::BytesMut::new();
            header.encode(&mut buf);
            let decoded = FrameHeader::decode(&buf).expect("should decode valid opcode");
            assert_eq!(decoded.opcode, expected_op);
            assert_eq!(decoded.opcode as u16, raw_op);
        }

        // Test that 0x0005 is rejected by FrameHeader::decode
        let mut raw_bytes = [0u8; 24];
        raw_bytes[0] = 0x00; // version
        raw_bytes[1] = 0x00; // flags
        raw_bytes[2..4].copy_from_slice(&0x0005u16.to_be_bytes()); // opcode 0x0005
        assert!(FrameHeader::decode(&raw_bytes).is_err());
    }
}
