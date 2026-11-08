//! A bounded, panic-free forward reader over a byte slice.
//!
//! [`Cursor`] is the shared primitive for decoding the big-endian wire formats
//! in this crate (batch headers, record entries, batch bodies). Every read is
//! length-checked, so truncated or inconsistent input yields a
//! [`CursorError`] rather than an out-of-bounds panic. It carries no knowledge
//! of any particular format; callers map [`CursorError`] into their own error
//! type.

/// An error from reading past the end of a [`Cursor`]'s slice.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_integers_big_endian_in_order() {
        let bytes = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0xAB, 0xCD, 0xEF,
        ];
        let mut c = Cursor::new(&bytes);
        assert_eq!(c.u64().unwrap(), 0x0102_0304_0506_0708);
        assert_eq!(c.u16().unwrap(), 0xABCD);
        assert_eq!(c.u8().unwrap(), 0xEF);
        assert!(c.is_exhausted());
    }

    #[test]
    fn take_returns_exact_slice_and_advances() {
        let bytes = [10, 20, 30, 40];
        let mut c = Cursor::new(&bytes);
        assert_eq!(c.take(2).unwrap(), &[10, 20]);
        assert_eq!(c.position(), 2);
        assert_eq!(c.remaining(), 2);
        assert_eq!(c.take(2).unwrap(), &[30, 40]);
        assert!(c.is_exhausted());
    }

    #[test]
    fn eof_is_an_error_not_a_panic() {
        let bytes = [1, 2, 3];
        let mut c = Cursor::new(&bytes);
        let err = c.u32().unwrap_err();
        assert_eq!(
            err,
            CursorError::UnexpectedEof {
                offset: 0,
                need: 4,
                have: 3
            }
        );
        // A failed read does not advance the position.
        assert_eq!(c.position(), 0);
    }

    #[test]
    fn take_past_end_errors() {
        let bytes = [1, 2];
        let mut c = Cursor::new(&bytes);
        assert!(c.take(3).is_err());
    }

    #[test]
    fn overflow_offset_errors_cleanly() {
        let bytes = [1, 2, 3];
        let mut c = Cursor::new(&bytes);
        c.take(1).unwrap();
        let err = c.take(usize::MAX).unwrap_err();
        assert!(matches!(err, CursorError::Overflow { offset: 1 }));
    }

    #[test]
    fn empty_slice_reports_empty() {
        let c = Cursor::new(&[]);
        assert!(c.is_empty());
        assert!(c.is_exhausted());
        assert_eq!(c.remaining(), 0);
    }
}
