use super::*;

#[test]
fn new_and_as_str_round_trip() {
    let subject = Subject::new("orders.us");
    assert_eq!(subject.as_str(), "orders.us");
}
