#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! define_uuid_id {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(Uuid);

        impl $name {
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }
            #[must_use]
            pub fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self(value)
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

define_uuid_id!(RecordId);
define_uuid_id!(JournalId);
define_uuid_id!(SubscriptionId);
define_uuid_id!(ConsumerId);
define_uuid_id!(ProducerId);
define_uuid_id!(LeaseId);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_id() {
        let id = RecordId::new();
        assert!(!id.as_uuid().is_nil());
    }

    #[test]
    fn record_ids_are_unique() {
        let first = RecordId::new();
        let second = RecordId::new();

        assert_ne!(first, second);
    }

    #[test]
    fn record_id_conversion() {
        let original = RecordId::new();

        let uuid: Uuid = original.into();
        let restored = RecordId::from(uuid);

        assert_eq!(original, restored);
    }
}