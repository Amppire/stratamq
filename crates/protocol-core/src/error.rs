use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("Frame payload size {size} exceeds maximum allowable limit {max}")]
    FrameTooLarge { size: u32, max: u32 },

    #[error("Unsupported protocol version: {version}")]
    UnsupportedVersion { version: u8 },

    #[error("Incompatible protocol version. Expected {expected}, got {actual}")]
    IncompatibleVersion { expected: u8, actual: u8 },

    #[error("Unknown or invalid opcode: 0x{opcode:04X}")]
    InvalidOpcode { opcode: u16 },

    #[error("IO error while reading or writing frames: {0}")]
    Io(String),

    #[error("Client ID length {len} exceeds maximum allowed {max}")]
    ClientIdTooLong { len: usize, max: usize },

    #[error("Malformed session payload: {0}")]
    MalformedPayload(String),
}

impl From<std::io::Error> for ProtocolError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err.to_string())
    }
}
