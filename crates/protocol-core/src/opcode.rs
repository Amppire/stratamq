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
