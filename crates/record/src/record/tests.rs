use crate::{ProposedRecord, Subject};

#[test]
fn proposed_record_has_no_metadata() {
    let proposed = ProposedRecord::new("payload").with_subject(Subject::new("subject"));

    assert_eq!(proposed.subject().map(Subject::as_str), Some("subject"));
}

#[test]
fn commit_assigns_metadata() {
    let proposed = ProposedRecord::new("payload").with_subject(Subject::new("subject"));

    let record = proposed.commit(1234567890, Some(1));

    assert_eq!(record.subject().map(Subject::as_str), Some("subject"));
    assert_eq!(record.revision(), Some(1));
    assert_eq!(record.timestamp(), 1234567890);
}

#[test]
fn test_unsubjected_records() {
    let record = ProposedRecord::new("payload").commit(1234567890, None);

    assert!(record.subject().is_none());
    assert!(record.revision().is_none());
}
