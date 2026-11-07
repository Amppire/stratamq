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

/// A source of wall-clock time, injected wherever the system needs "now".
///
/// Readings are milliseconds since the Unix epoch. Implementations are **not**
/// required to be monotonic: a reading may be lower than an earlier one, for
/// example after an NTP correction. Callers that need ordered timestamps must
/// enforce that themselves (the journal clamps its record timestamps).
pub trait Clock: Send + Sync + 'static {
    /// Returns the current time in milliseconds since the Unix epoch.
    fn now_millis(&self) -> u64;
}

/// The production [`Clock`], backed by the operating system's wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    /// Returns `0` if the system clock reads earlier than the Unix epoch.
    fn now_millis(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

/// A manually driven [`Clock`] for deterministic tests.
///
/// Clones share the same underlying time, so a test can keep one handle and
/// advance the clock after handing another to the code under test.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Debug, Clone, Default)]
pub struct MockClock {
    now: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[cfg(any(test, feature = "test-utils"))]
impl MockClock {
    /// Creates a clock that reads `initial_millis` until it is changed.
    #[must_use]
    pub fn new(initial_millis: u64) -> Self {
        Self {
            now: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(initial_millis)),
        }
    }

    /// Moves the clock forward by `delta` milliseconds.
    pub fn advance_millis(&self, delta: u64) {
        self.now
            .fetch_add(delta, std::sync::atomic::Ordering::SeqCst);
    }

    /// Sets the clock to `millis`, which may be earlier than the current reading.
    pub fn set_millis(&self, millis: u64) {
        self.now.store(millis, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl Clock for MockClock {
    fn now_millis(&self) -> u64 {
        self.now.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_reads_after_epoch() {
        assert!(SystemClock.now_millis() > 0);
    }

    #[test]
    fn mock_clock_starts_at_initial_value() {
        let clock = MockClock::new(1_000);
        assert_eq!(clock.now_millis(), 1_000);
    }

    #[test]
    fn mock_clock_advances_and_sets() {
        let clock = MockClock::new(1_000);
        clock.advance_millis(5_000);
        assert_eq!(clock.now_millis(), 6_000);
        clock.set_millis(42);
        assert_eq!(clock.now_millis(), 42);
    }

    #[test]
    fn mock_clock_clones_share_time() {
        let clock = MockClock::new(0);
        let handle = clock.clone();
        handle.advance_millis(250);
        assert_eq!(clock.now_millis(), 250);
    }

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
