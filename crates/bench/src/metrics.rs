//! HDR latency sampling (p50 / p95 / p99 / p99.9 / p99.99).

use std::time::Duration;

use hdrhistogram::Histogram;

use crate::schema::LatencyUs;

/// Slowest latency the histogram tracks (60 s, in microseconds). Slower
/// samples are clamped to this instead of failing the run.
const MAX_TRACKABLE_US: u64 = 60_000_000;

/// Significant decimal digits kept per value. Three digits give exact
/// results below ~2 ms and <= 0.1% error above, so 1 us precision holds
/// where it matters.
const SIGNIFICANT_DIGITS: u8 = 3;

/// Records operation latencies and summarises them as percentiles.
#[derive(Debug, Clone)]
pub struct LatencyRecorder {
    histogram: Histogram<u64>,
}

impl LatencyRecorder {
    #[must_use]
    pub fn new() -> Self {
        let histogram = Histogram::new_with_bounds(1, MAX_TRACKABLE_US, SIGNIFICANT_DIGITS)
            .expect("histogram bounds are valid constants");
        Self { histogram }
    }

    /// Records one operation's latency, clamped to the trackable range.
    pub fn record(&mut self, latency: Duration) {
        let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
        self.histogram.saturating_record(micros);
    }

    /// Number of samples recorded so far.
    #[must_use]
    pub fn len(&self) -> u64 {
        self.histogram.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.histogram.is_empty()
    }

    /// Reads percentiles, max and mean (all in microseconds).
    #[must_use]
    pub fn snapshot(&self) -> LatencyUs {
        LatencyUs {
            p50: self.histogram.value_at_quantile(0.50),
            p95: self.histogram.value_at_quantile(0.95),
            p99: self.histogram.value_at_quantile(0.99),
            p999: self.histogram.value_at_quantile(0.999),
            p9999: self.histogram.value_at_quantile(0.9999),
            max: self.histogram.max(),
            mean: self.histogram.mean(),
        }
    }
}

impl Default for LatencyRecorder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn micros(us: u64) -> Duration {
        Duration::from_micros(us)
    }

    #[test]
    fn small_values_are_recorded_exactly() {
        let mut recorder = LatencyRecorder::new();
        recorder.record(micros(1));
        recorder.record(micros(2));
        let snap = recorder.snapshot();
        assert_eq!(snap.p50, 1);
        assert_eq!(snap.max, 2);
    }

    #[test]
    fn percentiles_of_a_known_distribution() {
        let mut recorder = LatencyRecorder::new();
        for us in 1..=10_000 {
            recorder.record(micros(us));
        }
        let snap = recorder.snapshot();
        // 0.1% histogram error allowed above ~2 ms.
        assert!((4_995..=5_005).contains(&snap.p50), "p50 = {}", snap.p50);
        assert!((9_890..=9_910).contains(&snap.p99), "p99 = {}", snap.p99);
        assert!((10_000..=10_010).contains(&snap.max), "max = {}", snap.max);
        assert!((snap.mean - 5_000.5).abs() < 5.0, "mean = {}", snap.mean);
    }

    #[test]
    fn slow_tail_is_visible_in_high_percentiles() {
        // 0.2% of samples are slow: past p99, so p99 stays fast while
        // p99.9 and p99.99 expose the tail.
        let mut recorder = LatencyRecorder::new();
        for _ in 0..9_980 {
            recorder.record(micros(100));
        }
        for _ in 0..20 {
            recorder.record(Duration::from_secs(1));
        }
        let snap = recorder.snapshot();
        assert_eq!(snap.p50, 100);
        assert_eq!(snap.p99, 100);
        assert!(snap.p999 >= 999_000, "p999 = {}", snap.p999);
        assert!(snap.p9999 >= 999_000, "p9999 = {}", snap.p9999);
        assert!(snap.max >= 999_000);
    }

    #[test]
    fn over_range_values_are_clamped_not_dropped() {
        let mut recorder = LatencyRecorder::new();
        recorder.record(Duration::from_secs(3_600));
        assert_eq!(recorder.len(), 1);
        assert!(recorder.snapshot().max >= 59_000_000);
    }

    #[test]
    fn new_recorder_is_empty() {
        assert!(LatencyRecorder::new().is_empty());
    }
}
