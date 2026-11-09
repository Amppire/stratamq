//! A bounded, panic-free forward reader over a byte slice.
//!
//! [`Cursor`] is the shared primitive for decoding the big-endian wire formats
//! in this crate (batch headers, record entries, batch bodies). Every read is
//! length-checked, so truncated or inconsistent input yields a
//! [`CursorError`] rather than an out-of-bounds panic. It carries no knowledge
//! of any particular format; callers map [`CursorError`] into their own error
//! type.

mod error;
#[cfg(test)]
mod tests;

pub use error::CursorError;

/// A forward cursor over a byte slice. Reads advance the position and are
/// bounds-checked; the borrowed slices returned by [`Cursor::take`] live as
/// long as the input.
#[derive(Debug, Clone)]
pub struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    /// Create a cursor positioned at the start of `bytes`.
    #[must_use]
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    /// Current read offset.
    #[must_use]
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Total length of the underlying slice.
    #[must_use]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Whether the slice is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// Bytes not yet consumed.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    /// Whether every byte has been consumed.
    #[must_use]
    pub fn is_exhausted(&self) -> bool {
        self.pos == self.bytes.len()
    }

    /// Take the next `n` bytes, advancing the position.
    ///
    /// # Errors
    /// [`CursorError::UnexpectedEof`] if fewer than `n` bytes remain, or
    /// [`CursorError::Overflow`] if the end offset overflows `usize`.
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], CursorError> {
        let end = self
            .pos
            .checked_add(n)
            .ok_or(CursorError::Overflow { offset: self.pos })?;
        if end > self.bytes.len() {
            return Err(CursorError::UnexpectedEof {
                offset: self.pos,
                need: n,
                have: self.bytes.len(),
            });
        }
        let slice = &self.bytes[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    /// Read a single byte.
    ///
    /// # Errors
    /// [`CursorError`] if no bytes remain.
    pub fn u8(&mut self) -> Result<u8, CursorError> {
        Ok(self.take(1)?[0])
    }

    /// Read a big-endian `u16`.
    ///
    /// # Errors
    /// [`CursorError`] if fewer than 2 bytes remain.
    pub fn u16(&mut self) -> Result<u16, CursorError> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }

    /// Read a big-endian `u32`.
    ///
    /// # Errors
    /// [`CursorError`] if fewer than 4 bytes remain.
    pub fn u32(&mut self) -> Result<u32, CursorError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    /// Read a big-endian `u64`.
    ///
    /// # Errors
    /// [`CursorError`] if fewer than 8 bytes remain.
    pub fn u64(&mut self) -> Result<u64, CursorError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
}
