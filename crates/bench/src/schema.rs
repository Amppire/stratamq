//! Machine-readable JSON result schema (P0 Spec Appendix B).
//!
//! Every tournament run, from any developer, serialises to this one shape so
//! results stay directly comparable.

use serde::{Deserialize, Serialize};

/// One benchmark run: the "report card" written to `reports/`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BenchResult {
    /// Scenario id, e.g. `STORE-W1`.
    pub scenario: String,
    /// Candidate under test, e.g. `hybrid-wal-object`.
    pub implementation: String,
    /// Git commit the run was built from.
    pub git_sha: String,
    pub environment: Environment,
    pub semantics: Semantics,
    pub latency_us: LatencyUs,
    pub throughput: Throughput,
    pub resources: Resources,
}

/// The machine the run executed on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Environment {
    pub os: String,
    pub cores: u32,
    pub ram_gb: u32,
}

/// Guarantees the run was configured with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Semantics {
    /// e.g. `fsync`.
    pub durability: String,
    pub replication: u32,
}

/// Latency percentiles in microseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyUs {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
    pub p9999: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Throughput {
    pub records_s: u64,
    pub mb_s: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Resources {
    pub cpu_pct: f64,
    pub rss_mb: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The example from issue #11, plus the `p9999` field the issue's
    /// requirements call for but its example omits.
    const EXAMPLE: &str = r#"{
        "scenario": "STORE-W1",
        "implementation": "hybrid-wal-object",
        "git_sha": "abc123",
        "environment": { "os": "mac", "cores": 10, "ram_gb": 32 },
        "semantics": { "durability": "fsync", "replication": 1 },
        "latency_us": { "p50": 120, "p95": 450, "p99": 980, "p999": 1420, "p9999": 2100 },
        "throughput": { "records_s": 85000, "mb_s": 85.0 },
        "resources": { "cpu_pct": 45.2, "rss_mb": 82.5 }
    }"#;

    #[test]
    fn example_parses_into_expected_values() {
        let result: BenchResult = serde_json::from_str(EXAMPLE).unwrap();
        assert_eq!(result.scenario, "STORE-W1");
        assert_eq!(result.environment.cores, 10);
        assert_eq!(result.latency_us.p999, 1420);
        assert_eq!(result.latency_us.p9999, 2100);
        assert_eq!(result.throughput.records_s, 85_000);
    }

    #[test]
    fn json_round_trip_is_lossless() {
        let original: BenchResult = serde_json::from_str(EXAMPLE).unwrap();
        let json = serde_json::to_string(&original).unwrap();
        let back: BenchResult = serde_json::from_str(&json).unwrap();
        assert_eq!(original, back);
    }

    #[test]
    fn missing_field_is_rejected() {
        let without_resources = EXAMPLE.replace(
            r#""resources": { "cpu_pct": 45.2, "rss_mb": 82.5 }"#,
            r#""unused": 0"#,
        );
        assert!(serde_json::from_str::<BenchResult>(&without_resources).is_err());
    }
}
