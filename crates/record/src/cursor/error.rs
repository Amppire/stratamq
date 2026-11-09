/// An error from reading past the end of a [`Cursor`](super::Cursor)'s slice.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CursorError {
    /// A read asked for more bytes than remain in the slice.
    #[error("unexpected end of input: need {need} bytes at offset {offset}, have {have}")]
    UnexpectedEof {
        /// Byte offset the read started at.
        offset: usize,
        /// Number of bytes requested.
        need: usize,
        /// Total length of the underlying slice.
        have: usize,
    },
    /// Offset arithmetic overflowed `usize` (a hostile/garbage length field).
    #[error("length arithmetic overflow at offset {offset}")]
    Overflow {
        /// Byte offset the read started at.
        offset: usize,
    },
}
