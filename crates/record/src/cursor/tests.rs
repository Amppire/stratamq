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
