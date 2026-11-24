//! Drives a candidate with a generated workload, times every operation and
//! assembles the [`BenchResult`] report.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use strata_record::ProposedRecord;
use thiserror::Error;

use crate::generator::{GeneratorError, WorkloadGenerator, WorkloadSpec};
use crate::metrics::LatencyRecorder;
use crate::schema::{BenchResult, Environment, Resources, Semantics, Throughput};
use crate::traits::{BenchError, Durability, RecordBatch, StorageExperiment};

/// What to run and how to label the result.
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// Scenario id, e.g. `STORE-W1`.
    pub scenario: String,
    /// Candidate name, e.g. `hybrid-wal-object`.
    pub implementation: String,
    pub workload: WorkloadSpec,
    /// Number of append operations to issue.
    pub operations: u64,
    pub payload_bytes: usize,
    pub durability: Durability,
}

#[derive(Debug, Error)]
pub enum RunError {
    #[error(transparent)]
    Generator(#[from] GeneratorError),
    #[error(transparent)]
    Bench(#[from] BenchError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Appends `config.operations` single-record batches, timing each append.
///
/// Batch construction happens outside the timed region so only the
/// candidate's own work is measured.
pub async fn run_storage(
    config: &RunConfig,
    storage: &dyn StorageExperiment,
) -> Result<BenchResult, RunError> {
    let mut generator = WorkloadGenerator::new(config.workload)?;
    let mut recorder = LatencyRecorder::new();
    let payload = vec![0u8; config.payload_bytes];

    let run_start = Instant::now();
    for sequence in 0..config.operations {
        let key_id = generator.next_key_id();
        let batch = make_batch(sequence, key_id, &payload);

        let op_start = Instant::now();
        storage.append(batch, config.durability).await?;
        recorder.record(op_start.elapsed());
    }
    let seconds = run_start.elapsed().as_secs_f64().max(1e-9);

    let total_mb = (config.operations as f64 * config.payload_bytes as f64) / 1_000_000.0;
    Ok(BenchResult {
        scenario: config.scenario.clone(),
        implementation: config.implementation.clone(),
        git_sha: git_sha(),
        environment: environment(),
        semantics: Semantics {
            durability: durability_name(config.durability).to_owned(),
            replication: 1,
        },
        latency_us: recorder.snapshot(),
        throughput: Throughput {
            records_s: (config.operations as f64 / seconds) as u64,
            mb_s: total_mb / seconds,
        },
        // Not measured yet: needs a system-stats dependency.
        resources: Resources {
            cpu_pct: 0.0,
            rss_mb: 0.0,
        },
    })
}

fn make_batch(sequence: u64, key_id: u64, payload: &[u8]) -> RecordBatch {
    let record = ProposedRecord::new(payload.to_vec())
        .with_key(key_id.to_be_bytes().to_vec())
        .commit(0, None);
    RecordBatch {
        producer_id: 1,
        producer_epoch: 0,
        first_sequence: sequence,
        first_timestamp: 0,
        is_transactional: false,
        records: vec![record],
    }
}

fn durability_name(durability: Durability) -> &'static str {
    match durability {
        Durability::Buffered => "buffered",
        Durability::Fsync => "fsync",
    }
}

fn environment() -> Environment {
    Environment {
        os: std::env::consts::OS.to_owned(),
        cores: std::thread::available_parallelism()
            .map_or(1, |n| u32::try_from(n.get()).unwrap_or(u32::MAX)),
        // Not measured yet: needs a system-stats dependency.
        ram_gb: 0,
    }
}

/// Commit the binary was built from (captured by `build.rs`), or `"unknown"`
/// when the build had no git checkout.
fn git_sha() -> String {
    env!("STRATA_BENCH_GIT_SHA").to_owned()
}

/// Writes `result` as pretty JSON to `dir/<scenario>-<implementation>-<unix
/// secs>.json`, creating `dir` if needed. The JSON is parsed back into a
/// [`BenchResult`] first, so only a document that matches the schema is
/// written. Returns the file path.
pub fn write_report(dir: &Path, result: &BenchResult) -> Result<PathBuf, RunError> {
    let json = serde_json::to_string_pretty(result)?;
    serde_json::from_str::<BenchResult>(&json)?;

    std::fs::create_dir_all(dir)?;
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let stem = format!(
        "{}-{}-{secs}",
        sanitize(&result.scenario),
        sanitize(&result.implementation)
    );

    // Create exclusively so a run in the same second never replaces an
    // earlier report; on a collision retry with `-1`, `-2`, ...
    let mut attempt = 0u32;
    loop {
        let name = if attempt == 0 {
            format!("{stem}.json")
        } else {
            format!("{stem}-{attempt}.json")
        };
        let path = dir.join(name);
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(mut file) => {
                if let Err(e) = file.write_all(json.as_bytes()) {
                    // Don't leave a truncated report behind; it would also
                    // make this name look taken to later runs.
                    drop(file);
                    let _ = std::fs::remove_file(&path);
                    return Err(e.into());
                }
                return Ok(path);
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => attempt += 1,
            Err(e) => return Err(e.into()),
        }
    }
}

/// Keeps file names to `[A-Za-z0-9_-]` so labels cannot escape `dir`.
fn sanitize(label: &str) -> String {
    label
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generator::KeyDistribution;
    use crate::mock::MemStorage;

    fn config(operations: u64) -> RunConfig {
        RunConfig {
            scenario: "STORE-W1".into(),
            implementation: "mem".into(),
            workload: WorkloadSpec {
                distribution: KeyDistribution::Uniform,
                key_space: 100,
                seed: 1,
            },
            operations,
            payload_bytes: 64,
            durability: Durability::Fsync,
        }
    }

    #[tokio::test]
    async fn run_storage_fills_in_the_report() {
        let storage = MemStorage::default();
        let result = run_storage(&config(1_000), &storage).await.unwrap();

        assert_eq!(result.scenario, "STORE-W1");
        assert_eq!(result.semantics.durability, "fsync");
        assert!(result.environment.cores >= 1);
        assert!(result.throughput.records_s > 0);
        assert!(result.latency_us.p50 <= result.latency_us.p99);
        assert!(result.latency_us.p99 <= result.latency_us.max);
        assert_eq!(storage.recover().await.unwrap().recovered_batches, 1_000);
    }

    #[tokio::test]
    async fn zero_operations_reports_zero_throughput() {
        let result = run_storage(&config(0), &MemStorage::default())
            .await
            .unwrap();
        assert_eq!(result.throughput.records_s, 0);
    }

    #[tokio::test]
    async fn invalid_workload_surfaces_as_a_generator_error() {
        let mut bad = config(10);
        bad.workload.key_space = 0;
        let err = run_storage(&bad, &MemStorage::default()).await.unwrap_err();
        assert!(matches!(
            err,
            RunError::Generator(GeneratorError::EmptyKeySpace)
        ));
    }

    #[tokio::test]
    async fn written_report_parses_back_to_the_same_result() {
        let result = run_storage(&config(100), &MemStorage::default())
            .await
            .unwrap();
        let dir = std::env::temp_dir().join(format!("strata-bench-test-{}", std::process::id()));

        let path = write_report(&dir, &result).unwrap();
        let back: BenchResult =
            serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();

        assert_eq!(back, result);
    }

    #[tokio::test]
    async fn same_second_reports_do_not_overwrite_each_other() {
        let result = run_storage(&config(10), &MemStorage::default())
            .await
            .unwrap();
        let dir = std::env::temp_dir().join(format!("strata-bench-collide-{}", std::process::id()));

        let paths: Vec<_> = (0..3)
            .map(|_| write_report(&dir, &result).unwrap())
            .collect();
        let files = std::fs::read_dir(&dir).unwrap().count();
        std::fs::remove_dir_all(&dir).unwrap();

        assert_eq!(files, 3);
        assert_eq!(
            paths.iter().collect::<std::collections::HashSet<_>>().len(),
            3
        );
    }

    #[test]
    fn git_sha_is_captured_at_build_time() {
        let sha = git_sha();
        assert!(!sha.is_empty());
        assert!(sha == "unknown" || sha.len() == 40);
    }

    #[test]
    fn labels_cannot_escape_the_report_directory() {
        assert_eq!(sanitize("../evil/name"), "___evil_name");
    }
}
